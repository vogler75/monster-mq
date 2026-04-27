package broker

import (
	"bytes"
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/vogler75/mochi-mqtt-server/v2"
	"github.com/vogler75/mochi-mqtt-server/v2/packets"

	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
)

// StorageHook persists retained messages, sessions, subscriptions, and dispatches
// every published message to:
//   * the in-process pubsub bus (for GraphQL topicUpdates)
//   * the archive group manager (for last-value + history fanout)
//   * the metrics collector (one IncIn per publish, IncOut per Sent packet)
type StorageHook struct {
	mqtt.HookBase
	store    *stores.Storage
	bus      *pubsub.Bus
	archives ArchiveDispatcher
	logger   *slog.Logger
	nodeID   string
	metrics  MetricsCounter
}

// ArchiveDispatcher receives every published message for archive-group fanout.
// Implemented by archive.Manager. Kept as an interface here to avoid an import cycle.
type ArchiveDispatcher interface {
	Dispatch(msg stores.BrokerMessage)
}

// MetricsCounter is implemented by metrics.Collector.
type MetricsCounter interface {
	IncIn()
	IncOut()
}

func NewStorageHook(s *stores.Storage, bus *pubsub.Bus, dispatcher ArchiveDispatcher, nodeID string, logger *slog.Logger, m MetricsCounter) *StorageHook {
	return &StorageHook{store: s, bus: bus, archives: dispatcher, logger: logger, nodeID: nodeID, metrics: m}
}

func (h *StorageHook) ID() string { return "monstermq-storage" }

func (h *StorageHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribed,
		mqtt.OnPublished,
		mqtt.OnRetainMessage,
		mqtt.OnPacketSent,
	}, []byte{b})
}

func (h *StorageHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	info := stores.SessionInfo{
		ClientID:        cl.ID,
		NodeID:          h.nodeID,
		CleanSession:    cl.Properties.Clean,
		Connected:       true,
		UpdateTime:      time.Now(),
		ClientAddress:   cl.Net.Remote,
		ProtocolVersion: int(cl.Properties.ProtocolVersion),
	}
	if err := h.store.Sessions.SetClient(context.Background(), info); err != nil {
		h.logger.Warn("session persist failed", "client", cl.ID, "err", err)
	}
}

func (h *StorageHook) OnDisconnect(cl *mqtt.Client, _ error, _ bool) {
	if err := h.store.Sessions.SetConnected(context.Background(), cl.ID, false); err != nil {
		h.logger.Warn("session disconnect persist failed", "client", cl.ID, "err", err)
	}
}

func (h *StorageHook) OnSubscribed(cl *mqtt.Client, pk packets.Packet, _ []byte) {
	subs := make([]stores.MqttSubscription, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		subs = append(subs, stores.MqttSubscription{
			ClientID:          cl.ID,
			TopicFilter:       f.Filter,
			QoS:               f.Qos,
			NoLocal:           f.NoLocal,
			RetainAsPublished: f.RetainAsPublished,
			RetainHandling:    f.RetainHandling,
		})
	}
	if err := h.store.Subscriptions.AddSubscriptions(context.Background(), subs); err != nil {
		h.logger.Warn("subscriptions persist failed", "client", cl.ID, "err", err)
	}
}

func (h *StorageHook) OnUnsubscribed(cl *mqtt.Client, pk packets.Packet) {
	subs := make([]stores.MqttSubscription, 0, len(pk.Filters))
	for _, f := range pk.Filters {
		subs = append(subs, stores.MqttSubscription{ClientID: cl.ID, TopicFilter: f.Filter})
	}
	if err := h.store.Subscriptions.DelSubscriptions(context.Background(), subs); err != nil {
		h.logger.Warn("subscriptions delete failed", "client", cl.ID, "err", err)
	}
}

func (h *StorageHook) OnPacketSent(_ *mqtt.Client, pk packets.Packet, _ []byte) {
	if h.metrics != nil && pk.FixedHeader.Type == packets.Publish {
		h.metrics.IncOut()
	}
}

func (h *StorageHook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	if h.metrics != nil {
		h.metrics.IncIn()
	}
	msg := stores.BrokerMessage{
		MessageUUID: uuid.NewString(),
		MessageID:   pk.PacketID,
		TopicName:   pk.TopicName,
		Payload:     append([]byte(nil), pk.Payload...),
		QoS:         pk.FixedHeader.Qos,
		IsRetain:    pk.FixedHeader.Retain,
		IsDup:       pk.FixedHeader.Dup,
		ClientID:    cl.ID,
		Time:        time.Now().UTC(),
	}
	if pk.Properties.MessageExpiryInterval > 0 {
		v := pk.Properties.MessageExpiryInterval
		msg.MessageExpiryInterval = &v
	}
	h.bus.Publish(msg)
	if h.archives != nil {
		h.archives.Dispatch(msg)
	}
}

// OnRetainMessage is called when a message with retain=true is published. r=1 set, r=-1 clear.
func (h *StorageHook) OnRetainMessage(cl *mqtt.Client, pk packets.Packet, r int64) {
	ctx := context.Background()
	if r == -1 || len(pk.Payload) == 0 {
		_ = h.store.Retained.DelAll(ctx, []string{pk.TopicName})
		return
	}
	msg := stores.BrokerMessage{
		MessageUUID: uuid.NewString(),
		TopicName:   pk.TopicName,
		Payload:     append([]byte(nil), pk.Payload...),
		QoS:         pk.FixedHeader.Qos,
		IsRetain:    true,
		ClientID:    cl.ID,
		Time:        time.Now().UTC(),
	}
	if err := h.store.Retained.AddAll(ctx, []stores.BrokerMessage{msg}); err != nil {
		h.logger.Warn("retained persist failed", "topic", pk.TopicName, "err", err)
	}
}
