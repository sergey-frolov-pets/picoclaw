package vkplugin

import (
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

// ChannelType is the config `type` value this plugin answers to.
const ChannelType = "vk"

// init wires the plugin into PicoClaw/Hermes through two public registries:
//
//	config.RegisterChannelSettings — makes `type: "vk"` a valid channel type and
//	  decodes its `settings` block into VKSettings (without this, config
//	  validation rejects the channel with "unknown type").
//	channels.RegisterFactory — constructs the channel instance.
//
// Both are exported API, so no patch to the host repository is required.
func init() {
	config.RegisterChannelSettings(ChannelType, VKSettings{})

	channels.RegisterFactory(
		ChannelType,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			if bc == nil {
				return nil, channels.ErrSendFailed
			}
			return New(channelName, bc, b)
		},
	)
}
