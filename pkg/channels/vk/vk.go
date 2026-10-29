package vk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/SevereCloud/vksdk/v3/api"
	"github.com/SevereCloud/vksdk/v3/api/params"
	"github.com/SevereCloud/vksdk/v3/events"
	"github.com/SevereCloud/vksdk/v3/longpoll-bot"
	"github.com/SevereCloud/vksdk/v3/object"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/identity"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
	"github.com/sipeed/picoclaw/pkg/utils"
)

type VKChannel struct {
	*channels.BaseChannel
	vk          *api.VK
	lp          *longpoll.LongPoll
	channelName string
	bc          *config.Channel
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewVKChannel(channelName string, bc *config.Channel, bus *bus.MessageBus) (*VKChannel, error) {
	var vkCfg config.VKSettings
	if err := bc.Decode(&vkCfg); err != nil {
		return nil, err
	}

	vk := api.NewVK(vkCfg.Token.String())

	base := channels.NewBaseChannel(
		channelName,
		&vkCfg,
		bus,
		bc.AllowFrom,
		channels.WithMaxMessageLength(4000),
		channels.WithGroupTrigger(bc.GroupTrigger),
		channels.WithReasoningChannelID(bc.ReasoningChannelID),
	)

	return &VKChannel{
		BaseChannel: base,
		vk:          vk,
		channelName: channelName,
		bc:          bc,
	}, nil
}

func (c *VKChannel) getVKCfg() *config.VKSettings {
	var v config.VKSettings
	if err := c.bc.Decode(&v); err != nil {
		return nil
	}
	return &v
}

func (c *VKChannel) Start(ctx context.Context) error {
	logger.InfoC("vk", "Starting VK bot (Long Poll mode)...")

	c.ctx, c.cancel = context.WithCancel(ctx)

	groupID := c.getVKCfg().GroupID
	if groupID == 0 {
		c.cancel()
		return fmt.Errorf("group_id is required for VK bot")
	}

	c.SetRunning(true)

	go c.runWithReconnect(groupID)

	return nil
}

// runWithReconnect runs the VK Long Poll loop with automatic reconnection.
//
// The VK Long Poll server periodically drops the connection (e.g. returning
// "context deadline exceeded" after a long idle or during server maintenance).
// longpoll.LongPoll.Run() returns an error in that case and does NOT restart
// itself, which silently kills the channel until the gateway is restarted.
//
// To make the channel self-healing we wrap Run() in a retry loop that, on any
// error or clean exit, recreates the LongPoll (fresh server/key/ts), re-registers
// the MessageNew handler on the new object, and starts polling again. A capped
// exponential backoff avoids hammering the API when the server is down.
func (c *VKChannel) runWithReconnect(groupID int) {
	backoff := 3 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		select {
		case <-c.ctx.Done():
			logger.InfoC("vk", "VK bot stopped")
			return
		default:
		}

		lp, err := longpoll.NewLongPoll(c.vk, groupID)
		if err != nil {
			logger.ErrorCF("vk", "Failed to create long poll, retrying", map[string]any{
				"error":   err.Error(),
				"backoff": backoff.String(),
			})
			if !c.sleep(backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		c.lp = lp

		lp.MessageNew(func(_ context.Context, obj events.MessageNewObject) {
			c.handleMessage(obj.Message)
		})

		logger.InfoCF("vk", "VK bot connected", map[string]any{
			"group_id": groupID,
		})

		err = lp.RunWithContext(c.ctx)
		if err != nil {
			logger.ErrorCF("vk", "Long poll failed, reconnecting", map[string]any{
				"error":   err.Error(),
				"backoff": backoff.String(),
			})
		} else {
			logger.InfoC("vk", "Long poll exited cleanly, reconnecting")
		}

		// Stop requested while polling?
		select {
		case <-c.ctx.Done():
			logger.InfoC("vk", "VK bot stopped")
			return
		default:
		}

		if !c.sleep(backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// sleep waits for the given duration, aborting early if the channel is stopped.
// It reports false if the context was cancelled before the duration elapsed.
func (c *VKChannel) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-c.ctx.Done():
		return false
	}
}

func (c *VKChannel) Stop(ctx context.Context) error {
	logger.InfoC("vk", "Stopping VK bot...")
	c.SetRunning(false)

	if c.lp != nil {
		c.lp.Shutdown()
	}

	if c.cancel != nil {
		c.cancel()
	}

	return nil
}

func (c *VKChannel) handleMessage(msg object.MessagesMessage) {
	if msg.Action.Type != "" {
		return
	}

	if bool(msg.Out) {
		return
	}

	peerID := msg.PeerID
	chatID := strconv.Itoa(peerID)

	fromID := msg.FromID
	userID := strconv.Itoa(fromID)

	platformID := userID
	sender := bus.SenderInfo{
		Platform:    "vk",
		PlatformID:  platformID,
		CanonicalID: identity.BuildCanonicalID("vk", platformID),
		DisplayName: c.getUserName(fromID),
	}

	if !c.IsAllowedSender(sender) {
		logger.DebugCF("vk", "Message from unauthorized user", map[string]any{
			"peer_id": peerID,
		})
		return
	}

	text := msg.Text
	var mediaRefs []string

	// Download audio messages (voice) and store them in the media store so the
	// ASR pipeline can transcribe them. Other attachments become text markers.
	if len(msg.Attachments) > 0 {
		text, mediaRefs = c.processAttachments(msg.Attachments, chatID, strconv.Itoa(msg.ConversationMessageID))
	}

	if text == "" && len(mediaRefs) == 0 {
		return
	}

	groupTrigger := c.bc.GroupTrigger
	isGroupChat := peerID != fromID

	if isGroupChat {
		isMentioned := c.isMentioned(msg)
		if isMentioned {
			text = c.stripBotMention(text)
		}
		respond, cleaned := c.ShouldRespondInGroup(isMentioned, text)
		if !respond {
			return
		}
		text = cleaned
		_ = groupTrigger
	}

	chatType := "direct"
	if isGroupChat {
		chatType = "group"
	}

	messageID := strconv.Itoa(msg.ConversationMessageID)

	metadata := map[string]string{
		"user_id":  userID,
		"is_group": fmt.Sprintf("%t", isGroupChat),
	}

	c.HandleInboundContext(c.ctx, chatID, text, mediaRefs, bus.InboundContext{
		Channel:   "vk",
		ChatID:    chatID,
		ChatType:  chatType,
		SenderID:  userID,
		MessageID: messageID,
		Mentioned: isGroupChat && c.isMentioned(msg),
		Raw:       metadata,
	}, sender)
}

func (c *VKChannel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}

	peerID, err := strconv.Atoi(msg.ChatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID %s: %w", msg.ChatID, channels.ErrSendFailed)
	}

	if msg.Content == "" {
		return nil, nil
	}

	var messageIDs []string
	chunks := channels.SplitMessage(msg.Content, 4000)

	for _, chunk := range chunks {
		if chunk == "" {
			continue
		}

		b := params.NewMessagesSendBuilder()
		b.Message(chunk)
		b.RandomID(0)
		b.PeerID(peerID)

		if msg.ReplyToMessageID != "" {
			if replyID, err := strconv.Atoi(msg.ReplyToMessageID); err == nil {
				b.ReplyTo(replyID)
			}
		}

		resp, err := c.vk.MessagesSend(b.Params)
		if err != nil {
			logger.ErrorCF("vk", "Failed to send message", map[string]any{
				"error":   err.Error(),
				"peer_id": peerID,
			})
			return messageIDs, fmt.Errorf("failed to send message: %w", err)
		}

		messageIDs = append(messageIDs, strconv.Itoa(resp))
	}

	return messageIDs, nil
}

// SendMedia implements channels.MediaSender. It uploads media parts (audio,
// images, documents, video) to VK and sends them as message attachments.
//
// VK distinguishes "voice messages" (audio_message) from regular audio files
// (audio). We upload audio parts as documents with type "audio_message" when
// the filename suggests a voice reply (contains "voice"), so they render as
// playable voice bubbles; otherwise they are uploaded as regular documents.
func (c *VKChannel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
	if !c.IsRunning() {
		return nil, channels.ErrNotRunning
	}

	peerID, err := strconv.Atoi(msg.ChatID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat ID %s: %w", msg.ChatID, channels.ErrSendFailed)
	}

	store := c.GetMediaStore()
	if store == nil {
		return nil, fmt.Errorf("no media store available: %w", channels.ErrSendFailed)
	}

	var messageIDs []string

	for _, part := range msg.Parts {
		localPath, err := store.Resolve(part.Ref)
		if err != nil {
			logger.ErrorCF("vk", "Failed to resolve media ref", map[string]any{
				"ref":   part.Ref,
				"error": err.Error(),
			})
			continue
		}

		file, err := os.Open(localPath)
		if err != nil {
			logger.ErrorCF("vk", "Failed to open media file", map[string]any{
				"path":  localPath,
				"error": err.Error(),
			})
			continue
		}

		filename := part.Filename
		if filename == "" {
			filename = filepath.Base(localPath)
		}

		isVoice := part.Type == "audio" && strings.Contains(strings.ToLower(filename), "voice")

		var attachment string
		if isVoice {
			// VK voice messages (audio_message) are strict: the uploaded file
			// MUST be OGG/Opus and MUST carry a valid ".ogg" filename in the
			// multipart body, otherwise VK silently stores it as a regular
			// document (or rejects it with "wrong_mp3_file"). The SDK helper
			// UploadMessagesDoc passes `title` as the multipart filename, so we
			// upload manually with a guaranteed ".ogg" name, and read the
			// resulting attachment from DocsSaveResponse.AudioMessage (NOT
			// .Doc, which is empty for voice uploads).
			att, err := c.uploadVoiceMessage(peerID, filename, file)
			file.Close()
			if err != nil {
				logger.ErrorCF("vk", "Failed to upload voice message to VK", map[string]any{
					"file":  filename,
					"error": err.Error(),
				})
				return messageIDs, fmt.Errorf("vk upload voice: %w", channels.ErrTemporary)
			}
			attachment = att
		} else {
			saved, err := c.vk.UploadMessagesDoc(peerID, "doc", filename, filename, file)
			file.Close()
			if err != nil {
				logger.ErrorCF("vk", "Failed to upload media to VK", map[string]any{
					"type":  part.Type,
					"file":  filename,
					"error": err.Error(),
				})
				return messageIDs, fmt.Errorf("vk upload media: %w", channels.ErrTemporary)
			}
			attachment = fmt.Sprintf("doc%d_%d", saved.Doc.OwnerID, saved.Doc.ID)
		}

		b := params.NewMessagesSendBuilder()
		b.Message(part.Caption)
		b.RandomID(0)
		b.PeerID(peerID)
		b.Attachment(attachment)

		if msg.Context.MessageID != "" {
			if replyID, err := strconv.Atoi(msg.Context.MessageID); err == nil {
				b.ReplyTo(replyID)
			}
		}

		resp, err := c.vk.MessagesSend(b.Params)
		if err != nil {
			logger.ErrorCF("vk", "Failed to send media message", map[string]any{
				"attachment": attachment,
				"error":      err.Error(),
			})
			return messageIDs, fmt.Errorf("vk send media: %w", channels.ErrTemporary)
		}

		messageIDs = append(messageIDs, strconv.Itoa(resp))
	}

	return messageIDs, nil
}

// uploadVoiceMessage uploads an OGG/Opus file as a VK voice message
// (audio_message) and returns the message attachment string
// ("doc<owner>_<id>"). It deliberately bypasses the vksdk UploadMessagesDoc
// helper for two reasons:
//
//  1. The helper passes `title` as the multipart filename. Callers commonly
//     pass an empty title, which makes VK reject the upload (or fall back to a
//     regular document) because voice uploads require a valid ".ogg" filename.
//  2. The helper returns only DocsSaveResponse.Doc. For audio_message uploads
//     VK populates DocsSaveResponse.AudioMessage instead, leaving Doc empty
//     (owner_id=0, id=0) and producing a broken "doc0_0" attachment.
func (c *VKChannel) uploadVoiceMessage(peerID int, filename string, file io.Reader) (string, error) {
	if !strings.HasSuffix(strings.ToLower(filename), ".ogg") {
		// VK only accepts OGG/Opus for voice messages. Strip any other
		// extension and force ".ogg" so the multipart filename is valid.
		filename = strings.TrimSuffix(filename, filepath.Ext(filename)) + ".ogg"
	}

	server, err := c.vk.DocsGetMessagesUploadServer(api.Params{
		"peer_id": peerID,
		"type":    "audio_message",
	})
	if err != nil {
		return "", fmt.Errorf("get voice upload server: %w", err)
	}

	body, err := c.vk.UploadFile(server.UploadURL, file, "file", filename)
	if err != nil {
		return "", fmt.Errorf("upload voice file: %w", err)
	}

	var uploadErr struct {
		Err string `json:"error"`
	}
	if err := json.Unmarshal(body, &uploadErr); err == nil && uploadErr.Err != "" {
		return "", fmt.Errorf("vk voice upload: %s", uploadErr.Err)
	}

	var uploadResp struct {
		File string `json:"file"`
	}
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("decode voice upload response: %w", err)
	}
	if uploadResp.File == "" {
		return "", fmt.Errorf("vk voice upload: empty file token")
	}

	saved, err := c.vk.DocsSave(api.Params{
		"file":  uploadResp.File,
		"title": filename,
	})
	if err != nil {
		return "", fmt.Errorf("save voice message: %w", err)
	}

	if saved.AudioMessage.ID != 0 {
		return fmt.Sprintf("doc%d_%d", saved.AudioMessage.OwnerID, saved.AudioMessage.ID), nil
	}
	if saved.Doc.ID != 0 {
		return fmt.Sprintf("doc%d_%d", saved.Doc.OwnerID, saved.Doc.ID), nil
	}

	return "", fmt.Errorf("vk voice upload: no attachment returned (type=%q)", saved.Type)
}

// StartTyping implements channels.TypingCapable. VK typing indicators via
// MessagesSetActivity only last ~5 seconds, so we repeat them every 4 seconds
// until the stop function is called.
func (c *VKChannel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return func() {}, err
	}

	groupID := c.getVKCfg().GroupID

	sendTyping := func() {
		b := params.NewMessagesSetActivityBuilder()
		b.Type("typing")
		b.PeerID(peerID)
		if groupID != 0 {
			b.GroupID(groupID)
		}
		_, _ = c.vk.MessagesSetActivity(b.Params)
	}

	sendTyping()

	typingCtx, cancel := context.WithCancel(ctx)
	maxCtx, maxCancel := context.WithTimeout(typingCtx, 60*time.Second)
	go func() {
		defer maxCancel()
		ticker := time.NewTicker(4 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-maxCtx.Done():
				return
			case <-ticker.C:
				sendTyping()
			}
		}
	}()

	return cancel, nil
}

// SendPlaceholder implements channels.PlaceholderCapable.
func (c *VKChannel) SendPlaceholder(ctx context.Context, chatID string) (string, error) {
	phCfg := c.bc.Placeholder
	if !phCfg.Enabled {
		return "", nil
	}

	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return "", err
	}

	b := params.NewMessagesSendBuilder()
	b.Message(phCfg.GetRandomText())
	b.RandomID(0)
	b.PeerID(peerID)

	resp, err := c.vk.MessagesSend(b.Params)
	if err != nil {
		return "", err
	}

	return strconv.Itoa(resp), nil
}

// EditMessage implements channels.MessageEditor.
func (c *VKChannel) EditMessage(ctx context.Context, chatID string, messageID string, content string) error {
	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return err
	}
	mid, err := strconv.Atoi(messageID)
	if err != nil {
		return err
	}

	b := params.NewMessagesEditBuilder()
	b.PeerID(peerID)
	b.MessageID(mid)
	b.Message(content)

	_, err = c.vk.MessagesEdit(b.Params)
	if err != nil {
		// If the message was already edited (race or duplicate attempt), treat
		// as success so the Manager does not resend a duplicate.
		if strings.Contains(err.Error(), "can't edit this message") || strings.Contains(err.Error(), "message was not changed") {
			return nil
		}
		return err
	}
	return nil
}

// DeleteMessage implements channels.MessageDeleter.
//
// It is used by the Manager's TTL janitor to remove orphaned "Thinking…"
// placeholders whose turn never produced a final outbound message (crash,
// restart, hard abort, LLM error). Without this the placeholder lingers in the
// chat forever as an empty bubble.
func (c *VKChannel) DeleteMessage(ctx context.Context, chatID string, messageID string) error {
	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return err
	}
	mid, err := strconv.Atoi(messageID)
	if err != nil {
		return err
	}

	b := params.NewMessagesDeleteBuilder()
	b.PeerID(peerID)
	b.MessageIDs([]int{mid})
	b.DeleteForAll(true)

	_, err = c.vk.MessagesDelete(b.Params)
	if err != nil {
		// If the message is already gone (race with a normal edit/delete, or
		// VK purged it), treat as success so the caller stops retrying.
		msg := err.Error()
		if strings.Contains(msg, "message not found") || strings.Contains(msg, "can't delete") {
			return nil
		}
		return err
	}
	return nil
}

func (c *VKChannel) isMentioned(msg object.MessagesMessage) bool {
	return false
}

func (c *VKChannel) stripBotMention(text string) string {
	return strings.TrimSpace(text)
}

func (c *VKChannel) getUserName(userID int) string {
	users, err := c.vk.UsersGet(api.Params{
		"user_ids": userID,
	})
	if err != nil || len(users) == 0 {
		return strconv.Itoa(userID)
	}

	user := users[0]
	return fmt.Sprintf("%s %s", user.FirstName, user.LastName)
}

// processAttachments converts attachments into a text representation and
// downloads audio messages (voice) into the media store for ASR transcription.
// It returns the text content and the list of media refs.
func (c *VKChannel) processAttachments(attachments []object.MessagesMessageAttachment, chatID, messageID string) (string, []string) {
	var parts []string
	var mediaRefs []string

	scope := channels.BuildMediaScope("vk", chatID, messageID)

	for _, att := range attachments {
		switch att.Type {
		case "photo":
			parts = append(parts, "[photo]")
		case "video":
			parts = append(parts, "[video]")
		case "audio":
			parts = append(parts, "[audio]")
		case "doc":
			if att.Doc.Title != "" {
				parts = append(parts, fmt.Sprintf("[document: %s]", att.Doc.Title))
			} else {
				parts = append(parts, "[document]")
			}
		case "audio_message":
			parts = append(parts, "[voice]")
			if ref := c.downloadAudioMessage(att, scope); ref != "" {
				mediaRefs = append(mediaRefs, ref)
			}
		case "sticker":
			parts = append(parts, "[sticker]")
		}
	}

	return strings.Join(parts, " "), mediaRefs
}

// downloadAudioMessage downloads a VK voice message and registers it with the
// media store. Returns the media ref, or "" on failure.
func (c *VKChannel) downloadAudioMessage(att object.MessagesMessageAttachment, scope string) string {
	// Pick the best available link (MP3 preferred for broadest ASR support).
	urlStr := att.AudioMessage.LinkMp3
	ext := ".mp3"
	if urlStr == "" {
		urlStr = att.AudioMessage.LinkOgg
		ext = ".ogg"
	}
	if urlStr == "" {
		logger.WarnCF("vk", "Audio message has no downloadable link", nil)
		return ""
	}

	localPath := utils.DownloadFile(urlStr, "voice"+ext, utils.DownloadOptions{
		LoggerPrefix: "vk",
		Timeout:      60 * time.Second,
	})
	if localPath == "" {
		logger.ErrorCF("vk", "Failed to download audio message", map[string]any{
			"url": urlStr,
		})
		return ""
	}

	store := c.GetMediaStore()
	if store == nil {
		return localPath // fallback: raw path
	}

	ref, err := store.Store(localPath, media.MediaMeta{
		Filename:      "voice" + ext,
		ContentType:   "audio/" + strings.TrimPrefix(ext, "."),
		Source:        "vk",
		CleanupPolicy: media.CleanupPolicyDeleteOnCleanup,
	}, scope)
	if err != nil {
		logger.ErrorCF("vk", "Failed to store audio message in media store", map[string]any{
			"error": err.Error(),
		})
		return localPath
	}

	return ref
}

func (c *VKChannel) VoiceCapabilities() channels.VoiceCapabilities {
	return channels.VoiceCapabilities{ASR: true, TTS: true}
}
