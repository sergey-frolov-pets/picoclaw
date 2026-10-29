package main

import (
	"encoding/json"
	"fmt"

	_ "github.com/sipeed/picoclaw/pkg/channels/vkplugin"

	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

func main() {
	// 1) Тип "vk" проходит валидацию конфига?
	var chans config.ChannelsConfig
	raw := json.RawMessage(`{"vk":{"enabled":true,"allow_from":["707974165"],"settings":{"group_id":241164672,"token":"tok_test"}}}`)
	if err := json.Unmarshal(raw, &chans); err != nil {
		fmt.Println("UNMARSHAL FAIL:", err)
		return
	}
	if err := config.InitChannelList(chans); err != nil {
		fmt.Println("INIT FAIL:", err)
		return
	}
	fmt.Println("OK: config accepted type=vk (settings decoded, group_id =", "(decoded)", ")")

	// 2) Фабрика зарегистрирована?
	cfg := &config.Config{Channels: chans}
	f, found := channels.LookupFactoryForTest("vk")
	if !found {
		fmt.Println("FAIL: vk factory not registered")
		return
	}
	ch, err := f("vk", "vk", cfg, nil)
	if err != nil {
		fmt.Println("FACTORY FAIL:", err)
		return
	}
	fmt.Printf("OK: factory built channel %q\n", ch.Name())

	// 3) Capabilities
	_, media := ch.(channels.MediaSender)
	_, typ := ch.(channels.TypingCapable)
	_, ph := ch.(channels.PlaceholderCapable)
	_, ed := ch.(channels.MessageEditor)
	_, del := ch.(channels.MessageDeleter)
	vp, voice := ch.(channels.VoiceCapabilityProvider)
	fmt.Printf("capabilities: media=%v typing=%v placeholder=%v edit=%v delete=%v voice=%v\n", media, typ, ph, ed, del, voice)
	if voice {
		fmt.Printf("voice caps: %+v\n", vp.VoiceCapabilities())
	}
}
