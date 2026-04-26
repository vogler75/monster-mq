package broker

import (
	"bytes"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"monstermq.io/edge/internal/auth"
)

// AuthHook bridges mochi's Auth/ACL callbacks to our Cache.
type AuthHook struct {
	mqtt.HookBase
	cache *auth.Cache
}

func NewAuthHook(cache *auth.Cache) *AuthHook { return &AuthHook{cache: cache} }

func (h *AuthHook) ID() string { return "monstermq-auth" }

func (h *AuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{mqtt.OnConnectAuthenticate, mqtt.OnACLCheck}, []byte{b})
}

func (h *AuthHook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	username := string(cl.Properties.Username)
	password := string(pk.Connect.Password)
	return h.cache.Validate(username, password)
}

func (h *AuthHook) OnACLCheck(cl *mqtt.Client, topic string, write bool) bool {
	username := string(cl.Properties.Username)
	return h.cache.Allow(username, topic, write)
}
