// Package vkchannel is an out-of-tree VK (VKontakte) channel for PicoClaw /
// Hermes, packaged as a drop-in plugin module.
//
// It is NOT a fork of pkg/channels/vk. It lives outside the main repository and
// registers itself through the public extension points:
//
//	channels.RegisterFactory(config.ChannelVK, ...)   // channel constructor
//	config.RegisterChannelSettings("vk", VKSettings{}) // config validation
//
// Because it only touches exported API, it survives upstream refactors of the
// channel internals and needs no patch to pkg/channels/manager.go.
package vkplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

const (
	// maxMessageLength is the VK hard limit for a single message body.
	maxMessageLength = 4000
	// typingRefresh is how often the "typing" activity is re-sent. VK clears
	// the indicator after ~5s, so 4s keeps it continuously visible.
	typingRefresh = 4 * time.Second
	// typingMaxDuration caps a single typing indicator so a leaked stop
	// function cannot keep the goroutine alive forever.
	typingMaxDuration = 60 * time.Second
	// voiceDownloadTimeout bounds the download of an inbound voice message.
	voiceDownloadTimeout = 60 * time.Second
)

// VKSettings is the channel-specific settings block. It is registered with
// config.RegisterChannelSettings so that `type: "vk"` passes config validation
// and the "settings" JSON block decodes into this struct.
type VKSettings struct {
	Token   config.SecureString `json:"token,omitzero" yaml:"token,omitempty" env:"PICOCLAW_CHANNELS_VK_TOKEN"`
	GroupID int                 `json:"group_id"       yaml:"-"               env:"PICOCLAW_CHANNELS_VK_GROUP_ID"`
}

// Channel is the VK bot channel (Long Poll transport).
type Channel struct {
	*channels.BaseChannel

	vk          *api.VK
	channelName string
	bc          *config.Channel
	settings    *VKSettings

	mu     sync.Mutex
	lp     *longpoll.LongPoll
	ctx    context.Context
	cancel context.CancelFunc
}

// Compile-time assertions: the Manager relies on these capabilities for the
// placeholder lifecycle and for routing media/voice.
var (
	_ channels.Channel                 = (*Channel)(nil)
	_ channels.MediaSender             = (*Channel)(nil)
	_ channels.TypingCapable           = (*Channel)(nil)
	_ channels.PlaceholderCapable      = (*Channel)(nil)
	_ channels.MessageEditor           = (*Channel)(nil)
	_ channels.MessageDeleter          = (*Channel)(nil)
	_ channels.VoiceCapabilityProvider = (*Channel)(nil)
)

// New builds a VK channel from the generic channel config.
func New(channelName string, bc *config.Channel, messageBus *bus.MessageBus) (*Channel, error) {
	if bc == nil {
		return nil, fmt.Errorf("vk: channel config is nil")
	}

	settings := &VKSettings{}
	if err := bc.Decode(settings); err != nil {
		return nil, fmt.Errorf("vk: decode settings: %w", err)
	}
	if settings.Token.String() == "" {
		return nil, fmt.Errorf("vk: token is required (put it in .security.yml)")
	}
	if settings.GroupID == 0 {
		return nil, fmt.Errorf("vk: group_id is required")
	}

	base := channels.NewBaseChannel(
		channelName,
		settings,
		messageBus,
		bc.AllowFrom,
		channels.WithMaxMessageLength(maxMessageLength),
		channels.WithGroupTrigger(bc.GroupTrigger),
		channels.WithReasoningChannelID(bc.ReasoningChannelID),
	)

	return &Channel{
		BaseChannel: base,
		vk:          api.NewVK(settings.Token.String()),
		channelName: channelName,
		bc:          bc,
		settings:    settings,
	}, nil
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Start launches the Long Poll loop in the background.
func (c *Channel) Start(ctx context.Context) error {
	logger.InfoC("vk", "Starting VK bot (Long Poll mode)...")

	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	runCtx := c.ctx
	c.mu.Unlock()

	c.SetRunning(true)
	go c.runWithReconnect(runCtx, c.settings.GroupID)

	return nil
}

// Stop shuts the Long Poll down and cancels the reconnect loop.
func (c *Channel) Stop(ctx context.Context) error {
	logger.InfoC("vk", "Stopping VK bot...")
	c.SetRunning(false)

	c.mu.Lock()
	lp := c.lp
	cancel := c.cancel
	c.mu.Unlock()

	if lp != nil {
		lp.Shutdown()
	}
	if cancel != nil {
		cancel()
	}
	return nil
}

// runWithReconnect keeps the Long Poll alive.
//
// VK's Long Poll server periodically drops the connection (idle timeout,
// maintenance, "context deadline exceeded"). longpoll.Run() returns an error in
// that case and does NOT restart itself — without this loop the channel dies
// silently until the gateway is restarted. On every exit we rebuild the
// LongPoll (fresh server/key/ts), re-register the handler and poll again, with
// capped exponential backoff.
func (c *Channel) runWithReconnect(ctx context.Context, groupID int) {
	backoff := 3 * time.Second
	const maxBackoff = 60 * time.Second
	// healthySession is how long a Long Poll session must live before we treat
	// the disconnect as transient and reset the backoff to its minimum.
	const healthySession = 30 * time.Second

	for {
		if ctx.Err() != nil {
			logger.InfoC("vk", "VK bot stopped")
			return
		}

		lp, err := longpoll.NewLongPoll(c.vk, groupID)
		if err != nil {
			logger.ErrorCF("vk", "Failed to create long poll, retrying", map[string]any{
				"error":   err.Error(),
				"backoff": backoff.String(),
			})
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		c.mu.Lock()
		c.lp = lp
		c.mu.Unlock()

		lp.MessageNew(func(_ context.Context, obj events.MessageNewObject) {
			c.handleMessage(ctx, obj.Message)
		})

		logger.InfoCF("vk", "VK bot connected", map[string]any{"group_id": groupID})

		sessionStart := time.Now()
		err = lp.RunWithContext(ctx)
		if err != nil {
			logger.ErrorCF("vk", "Long poll failed, reconnecting", map[string]any{
				"error":   err.Error(),
				"backoff": backoff.String(),
			})
		} else {
			logger.InfoC("vk", "Long poll exited cleanly, reconnecting")
		}

		if ctx.Err() != nil {
			logger.InfoC("vk", "VK bot stopped")
			return
		}
		// Reset the backoff after a session that lived long enough to be
		// considered healthy; otherwise keep growing it.
		if time.Since(sessionStart) > healthySession {
			backoff = 3 * time.Second
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// ---------------------------------------------------------------------------
// Inbound
// ---------------------------------------------------------------------------

func (c *Channel) handleMessage(ctx context.Context, msg object.MessagesMessage) {
	// Ignore service actions (joins, pins, ...) and our own outbound echoes.
	if msg.Action.Type != "" || bool(msg.Out) {
		return
	}

	peerID := msg.PeerID
	chatID := strconv.Itoa(peerID)
	fromID := msg.FromID
	userID := strconv.Itoa(fromID)

	sender := bus.SenderInfo{
		Platform:    "vk",
		PlatformID:  userID,
		CanonicalID: identity.BuildCanonicalID("vk", userID),
		DisplayName: c.getUserName(fromID),
	}

	if !c.IsAllowedSender(sender) {
		logger.DebugCF("vk", "Message from unauthorized user", map[string]any{"peer_id": peerID})
		return
	}

	text := msg.Text
	var mediaRefs []string
	if len(msg.Attachments) > 0 {
		text, mediaRefs = c.processAttachments(msg.Attachments, chatID, strconv.Itoa(msg.ConversationMessageID))
	}
	if text == "" && len(mediaRefs) == 0 {
		return
	}

	isGroupChat := peerID != fromID
	mentioned := false
	if isGroupChat {
		mentioned = c.isMentioned(msg)
		if mentioned {
			text = c.stripBotMention(text)
		}
		respond, cleaned := c.ShouldRespondInGroup(mentioned, text)
		if !respond {
			return
		}
		text = cleaned
	}

	chatType := "direct"
	if isGroupChat {
		chatType = "group"
	}

	if err := c.HandleInboundContext(ctx, chatID, text, mediaRefs, bus.InboundContext{
		Channel:   "vk",
		ChatID:    chatID,
		ChatType:  chatType,
		SenderID:  userID,
		MessageID: strconv.Itoa(msg.ConversationMessageID),
		Mentioned: mentioned,
		Raw: map[string]string{
			"user_id":  userID,
			"is_group": strconv.FormatBool(isGroupChat),
		},
	}, sender); err != nil {
		logger.ErrorCF("vk", "Failed to publish inbound message", map[string]any{
			"peer_id": peerID,
			"error":   err.Error(),
		})
	}
}

// processAttachments turns attachments into text markers and downloads voice
// messages into the media store so the ASR pipeline can transcribe them.
func (c *Channel) processAttachments(
	attachments []object.MessagesMessageAttachment,
	chatID, messageID string,
) (string, []string) {
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
func (c *Channel) downloadAudioMessage(att object.MessagesMessageAttachment, scope string) string {
	// MP3 is preferred: VK serves it for every audio_message and it has the
	// widest ASR support.
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
		Timeout:      voiceDownloadTimeout,
	})
	if localPath == "" {
		logger.ErrorCF("vk", "Failed to download audio message", map[string]any{"url": urlStr})
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

// ---------------------------------------------------------------------------
// Outbound
// ---------------------------------------------------------------------------

// Send delivers a text message, splitting it to VK's 4000-char limit.
func (c *Channel) Send(ctx context.Context, msg bus.OutboundMessage) ([]string, error) {
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
	for _, chunk := range channels.SplitMessage(msg.Content, maxMessageLength) {
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
				"peer_id": peerID,
				"error":   err.Error(),
			})
			return messageIDs, fmt.Errorf("failed to send message: %w", err)
		}
		messageIDs = append(messageIDs, strconv.Itoa(resp))
	}
	return messageIDs, nil
}

// SendMedia implements channels.MediaSender.
//
// VK separates "voice messages" (audio_message, playable bubbles) from regular
// documents. An audio part whose filename contains "voice" is uploaded as a
// voice message; everything else goes as a document.
func (c *Channel) SendMedia(ctx context.Context, msg bus.OutboundMediaMessage) ([]string, error) {
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

// uploadVoiceMessage uploads an OGG/Opus file as a VK voice message and returns
// the attachment string ("doc<owner>_<id>").
//
// It deliberately bypasses the vksdk UploadMessagesDoc helper:
//
//  1. The helper passes `title` as the multipart filename. Voice uploads
//     require a valid ".ogg" filename, otherwise VK rejects the file or
//     silently stores it as a regular document.
//  2. The helper returns only DocsSaveResponse.Doc. For audio_message uploads
//     VK populates DocsSaveResponse.AudioMessage instead, leaving Doc empty and
//     producing a broken "doc0_0" attachment.
func (c *Channel) uploadVoiceMessage(peerID int, filename string, file io.Reader) (string, error) {
	if !strings.HasSuffix(strings.ToLower(filename), ".ogg") {
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

// ---------------------------------------------------------------------------
// Capabilities: typing / placeholder / edit / delete / voice
// ---------------------------------------------------------------------------

// StartTyping implements channels.TypingCapable. VK clears the indicator after
// ~5s, so it is re-sent every 4s until the returned stop function is called.
// The stop function is idempotent.
func (c *Channel) StartTyping(ctx context.Context, chatID string) (func(), error) {
	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return func() {}, err
	}

	groupID := c.settings.GroupID
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
	maxCtx, maxCancel := context.WithTimeout(typingCtx, typingMaxDuration)
	go func() {
		defer maxCancel()
		ticker := time.NewTicker(typingRefresh)
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

	var once sync.Once
	stop := func() {
		once.Do(cancel)
	}
	return stop, nil
}

// SendPlaceholder implements channels.PlaceholderCapable.
func (c *Channel) SendPlaceholder(ctx context.Context, chatID string) (string, error) {
	if !c.bc.Placeholder.Enabled {
		return "", nil
	}

	peerID, err := strconv.Atoi(chatID)
	if err != nil {
		return "", err
	}

	b := params.NewMessagesSendBuilder()
	b.Message(c.bc.Placeholder.GetRandomText())
	b.RandomID(0)
	b.PeerID(peerID)

	resp, err := c.vk.MessagesSend(b.Params)
	if err != nil {
		return "", err
	}
	return strconv.Itoa(resp), nil
}

// EditMessage implements channels.MessageEditor.
func (c *Channel) EditMessage(ctx context.Context, chatID, messageID, content string) error {
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

	if _, err := c.vk.MessagesEdit(b.Params); err != nil {
		// Already edited (race or duplicate attempt) — treat as success so the
		// Manager does not resend a duplicate message.
		msg := err.Error()
		if strings.Contains(msg, "can't edit this message") || strings.Contains(msg, "message was not changed") {
			return nil
		}
		return err
	}
	return nil
}

// DeleteMessage implements channels.MessageDeleter. Used by the Manager to
// clean up orphaned "Thinking…" placeholders whose turn never produced a final
// outbound message (crash, restart, hard abort, LLM error).
func (c *Channel) DeleteMessage(ctx context.Context, chatID, messageID string) error {
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

	if _, err := c.vk.MessagesDelete(b.Params); err != nil {
		// Already gone (race with a normal edit/delete, or VK purged it) —
		// treat as success so the caller stops retrying.
		msg := err.Error()
		if strings.Contains(msg, "message not found") || strings.Contains(msg, "can't delete") {
			return nil
		}
		return err
	}
	return nil
}

// VoiceCapabilities implements channels.VoiceCapabilityProvider. ASR is
// inbound transcription (Groq Whisper); TTS is outbound voice bubbles.
func (c *Channel) VoiceCapabilities() channels.VoiceCapabilities {
	return channels.VoiceCapabilities{ASR: true, TTS: true}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// isMentioned reports whether the bot was mentioned. VK community bots receive
// messages that mention the community, but the SDK does not expose a reliable
// mention flag, so group replies are governed by GroupTrigger only.
func (c *Channel) isMentioned(msg object.MessagesMessage) bool { return false }

func (c *Channel) stripBotMention(text string) string { return strings.TrimSpace(text) }

func (c *Channel) getUserName(userID int) string {
	users, err := c.vk.UsersGet(api.Params{"user_ids": userID})
	if err != nil || len(users) == 0 {
		return strconv.Itoa(userID)
	}
	user := users[0]
	return strings.TrimSpace(user.FirstName + " " + user.LastName)
}
