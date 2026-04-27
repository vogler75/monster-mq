package mqttclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// Address is one inbound or outbound mapping rule. Field shapes mirror the
// dashboard's MqttClientAddressInput.
type Address struct {
	Mode        string `json:"mode"`        // SUBSCRIBE | PUBLISH
	RemoteTopic string `json:"remoteTopic"` // topic on the remote broker
	LocalTopic  string `json:"localTopic"`  // topic on the local broker
	QoS         int    `json:"qos"`
	Retain      bool   `json:"retain"`
	// RemovePath: when true, strip the literal prefix of the source-side topic
	// pattern (everything before the first + or #) from the matched topic
	// before mapping to the destination side.
	RemovePath bool `json:"removePath"`
}

// Config is the persisted JSON config (DeviceConfig.Config) for one bridge.
type Config struct {
	BrokerURL         string    `json:"brokerUrl"`
	ClientID          string    `json:"clientId"`
	Username          string    `json:"username,omitempty"`
	Password          string    `json:"password,omitempty"`
	CleanSession      bool      `json:"cleanSession"`
	KeepAlive         int       `json:"keepAlive,omitempty"`
	ConnectionTimeout int       `json:"connectionTimeout,omitempty"`
	ReconnectDelay    int       `json:"reconnectDelay,omitempty"`
	Addresses         []Address `json:"addresses"`
}

// LocalPublisher is implemented by mochi-mqtt's *Server (Publish).
type LocalPublisher func(topic string, payload []byte, retain bool, qos byte) error

// LocalSubscriber lets the bridge listen to the local broker for outbound
// publishes. Implemented via the in-process pubsub bus.
type LocalSubscriber interface {
	Subscribe(filters []string, buffer int) (id int, ch <-chan LocalMessage)
	Unsubscribe(id int)
}

// LocalMessage is what the bus delivers to the bridge.
type LocalMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// Connector is one bridge to one remote broker.
type Connector struct {
	name      string
	cfg       Config
	publisher LocalPublisher
	subBus    LocalSubscriber
	logger    *slog.Logger

	mu     sync.Mutex
	client paho.Client
	subID  int
	stopCh chan struct{}

	IncIn  func()
	IncOut func()
}

func NewConnector(name string, cfg Config, publisher LocalPublisher, subBus LocalSubscriber, logger *slog.Logger) *Connector {
	return &Connector{
		name: name, cfg: cfg, publisher: publisher, subBus: subBus, logger: logger,
		stopCh: make(chan struct{}),
	}
}

func (c *Connector) Name() string { return c.name }

// Start dials the remote broker and registers inbound/outbound forwarders.
func (c *Connector) Start(ctx context.Context) error {
	opts := paho.NewClientOptions().AddBroker(c.cfg.BrokerURL)
	opts.SetClientID(c.cfg.ClientID)
	if c.cfg.Username != "" {
		opts.SetUsername(c.cfg.Username)
	}
	if c.cfg.Password != "" {
		opts.SetPassword(c.cfg.Password)
	}
	opts.SetCleanSession(c.cfg.CleanSession)
	if c.cfg.KeepAlive > 0 {
		opts.SetKeepAlive(time.Duration(c.cfg.KeepAlive) * time.Second)
	}
	if c.cfg.ConnectionTimeout > 0 {
		opts.SetConnectTimeout(time.Duration(c.cfg.ConnectionTimeout) * time.Second)
	}
	if c.cfg.ReconnectDelay > 0 {
		opts.SetMaxReconnectInterval(time.Duration(c.cfg.ReconnectDelay) * time.Second)
	} else {
		opts.SetMaxReconnectInterval(30 * time.Second)
	}
	opts.SetAutoReconnect(true)
	if strings.HasPrefix(c.cfg.BrokerURL, "ssl://") || strings.HasPrefix(c.cfg.BrokerURL, "tls://") || strings.HasPrefix(c.cfg.BrokerURL, "wss://") {
		opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	opts.SetOnConnectHandler(func(_ paho.Client) {
		c.logger.Info("bridge connected", "name", c.name, "url", c.cfg.BrokerURL)
		c.subscribeInbound()
	})
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		c.logger.Warn("bridge connection lost", "name", c.name, "err", err)
	})

	client := paho.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("bridge %s: connect timeout", c.name)
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("bridge %s: %w", c.name, err)
	}
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()

	c.startOutbound(ctx)
	return nil
}

func (c *Connector) subscribeInbound() {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()
	if client == nil {
		return
	}
	for _, a := range c.cfg.Addresses {
		if !strings.EqualFold(a.Mode, "SUBSCRIBE") {
			continue
		}
		addr := a // capture
		tok := client.Subscribe(addr.RemoteTopic, byte(addr.QoS), func(_ paho.Client, m paho.Message) {
			localTopic := mapInboundTopic(addr, m.Topic())
			if c.IncIn != nil {
				c.IncIn()
			}
			if err := c.publisher(localTopic, m.Payload(), addr.Retain, byte(addr.QoS)); err != nil {
				c.logger.Warn("bridge inbound publish failed", "name", c.name, "topic", localTopic, "err", err)
			}
		})
		if !tok.WaitTimeout(5 * time.Second) {
			c.logger.Warn("bridge subscribe timeout", "name", c.name, "topic", addr.RemoteTopic)
		}
	}
}

// mapInboundTopic maps an incoming topic from the remote broker to the local
// topic to publish under, respecting the address's removePath flag and the
// LocalTopic prefix (if it has no wildcards).
func mapInboundTopic(a Address, remoteTopic string) string {
	t := remoteTopic
	if a.RemovePath {
		if prefix := literalPrefix(a.RemoteTopic); prefix != "" {
			t = strings.TrimPrefix(t, prefix)
			t = strings.TrimPrefix(t, "/")
		}
	}
	if a.LocalTopic == "" || strings.ContainsAny(a.LocalTopic, "+#") {
		return t
	}
	if t == "" {
		return strings.TrimRight(a.LocalTopic, "/")
	}
	return strings.TrimRight(a.LocalTopic, "/") + "/" + t
}

func (c *Connector) startOutbound(ctx context.Context) {
	filters := []string{}
	addrByFilter := map[string]Address{}
	for _, a := range c.cfg.Addresses {
		if !strings.EqualFold(a.Mode, "PUBLISH") {
			continue
		}
		filters = append(filters, a.LocalTopic)
		addrByFilter[a.LocalTopic] = a
	}
	if len(filters) == 0 {
		return
	}
	id, ch := c.subBus.Subscribe(filters, 256)
	c.mu.Lock()
	c.subID = id
	c.mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				addr := pickAddress(addrByFilter, msg.Topic)
				remote := mapOutboundTopic(addr, msg.Topic)
				c.mu.Lock()
				client := c.client
				c.mu.Unlock()
				if client == nil || !client.IsConnected() {
					continue
				}
				if c.IncOut != nil {
					c.IncOut()
				}
				_ = client.Publish(remote, byte(addr.QoS), addr.Retain, msg.Payload)
			}
		}
	}()
}

func pickAddress(filters map[string]Address, topic string) Address {
	for filter, a := range filters {
		if filter == "#" || matchTopic(filter, topic) {
			return a
		}
	}
	return Address{}
}

// mapOutboundTopic maps a locally-published topic to the topic to forward to
// the remote broker.
func mapOutboundTopic(a Address, localTopic string) string {
	t := localTopic
	if a.RemovePath {
		if prefix := literalPrefix(a.LocalTopic); prefix != "" {
			t = strings.TrimPrefix(t, prefix)
			t = strings.TrimPrefix(t, "/")
		}
	}
	if a.RemoteTopic == "" || strings.ContainsAny(a.RemoteTopic, "+#") {
		return t
	}
	if t == "" {
		return strings.TrimRight(a.RemoteTopic, "/")
	}
	return strings.TrimRight(a.RemoteTopic, "/") + "/" + t
}

// literalPrefix returns the longest literal prefix of an MQTT topic pattern —
// i.e. everything up to but not including the first wildcard segment.
//
//	"sensor/#"        → "sensor"
//	"a/b/+/c"         → "a/b"
//	"+/x"             → ""
//	"plain/topic"     → "plain/topic"
func literalPrefix(pattern string) string {
	parts := strings.Split(pattern, "/")
	for i, p := range parts {
		if p == "+" || p == "#" {
			return strings.Join(parts[:i], "/")
		}
	}
	return pattern
}

func (c *Connector) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	if c.subID != 0 && c.subBus != nil {
		c.subBus.Unsubscribe(c.subID)
		c.subID = 0
	}
	if c.client != nil {
		c.client.Disconnect(250)
		c.client = nil
	}
}

func matchTopic(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tt := strings.Split(topic, "/")
	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tt) {
			return false
		}
		if p == "+" {
			continue
		}
		if p != tt[i] {
			return false
		}
	}
	return len(pp) == len(tt)
}
