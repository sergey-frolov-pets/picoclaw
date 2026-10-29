// Package vkchannel provides a VK (VKontakte) channel for PicoClaw / Hermes as
// an out-of-tree plugin.
//
// Import it for its side effects only:
//
//	import _ "github.com/<you>/hermes-vk-plugin/vkchannel"
//
// The blank import runs init(), which registers the channel factory and the
// settings prototype with the host. Nothing else in the host needs to change.
package vkplugin
