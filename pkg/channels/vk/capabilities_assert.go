package vk

import "github.com/sipeed/picoclaw/pkg/channels"

// Compile-time assertions: the VK channel must satisfy the capabilities the
// Manager relies on for placeholder lifecycle management.
var (
	_ channels.PlaceholderCapable = (*VKChannel)(nil)
	_ channels.MessageEditor      = (*VKChannel)(nil)
	_ channels.MessageDeleter     = (*VKChannel)(nil)
)
