package broker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/vogler75/mochi-mqtt-server/v2"
	"github.com/vogler75/mochi-mqtt-server/v2/packets"

	"monstermq.io/edge/internal/stores"
)

// QueueHook persists publishes for offline persistent (clean=false) subscribers
// in the configured QueueStore and replays them when the client reconnects.
//
// Without this hook the broker still works — mochi-mqtt holds inflight messages
// in memory per client — but those messages are lost when the broker restarts.
// With it enabled, every publish that matches a disconnected persistent
// session's subscription is enqueued to a row in the messagequeue table; on
// reconnect the rows are dequeued and written directly to the now-online client.
type QueueHook struct {
	mqtt.HookBase
	store  *stores.Storage
	server *mqtt.Server
	logger *slog.Logger
}

func NewQueueHook(s *stores.Storage, server *mqtt.Server, logger *slog.Logger) *QueueHook {
	return &QueueHook{store: s, server: server, logger: logger}
}

func (h *QueueHook) ID() string { return "monstermq-queue" }

func (h *QueueHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnPublished,
		mqtt.OnSessionEstablished,
	}, []byte{b})
}

// OnPublished walks every persisted subscription, finds matches for the topic
// where the owning session is persistent (clean=false) and currently
// disconnected, and enqueues a copy of the message for each.
func (h *QueueHook) OnPublished(_ *mqtt.Client, pk packets.Packet) {
	ctx := context.Background()
	subs, err := h.collectOfflineSubscribers(ctx, pk.TopicName)
	if err != nil {
		h.logger.Warn("queue hook: collect offline subs failed", "topic", pk.TopicName, "err", err)
		return
	}
	if len(subs) == 0 {
		return
	}
	msg := stores.BrokerMessage{
		MessageUUID: uuid.NewString(),
		MessageID:   pk.PacketID,
		TopicName:   pk.TopicName,
		Payload:     append([]byte(nil), pk.Payload...),
		QoS:         pk.FixedHeader.Qos,
		IsRetain:    pk.FixedHeader.Retain,
		Time:        time.Now().UTC(),
	}
	if err := h.store.Queue.EnqueueMulti(ctx, msg, subs); err != nil {
		h.logger.Warn("queue hook: enqueue failed", "topic", pk.TopicName, "n", len(subs), "err", err)
	}
}

// collectOfflineSubscribers returns the client IDs of persistent sessions whose
// subscriptions match topic and that are currently not connected.
func (h *QueueHook) collectOfflineSubscribers(ctx context.Context, topic string) ([]string, error) {
	var matches []string
	err := h.store.Subscriptions.IterateSubscriptions(ctx, func(sub stores.MqttSubscription) bool {
		if !topicMatches(sub.TopicFilter, topic) {
			return true
		}
		matches = append(matches, sub.ClientID)
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, cid := range matches {
		if seen[cid] {
			continue
		}
		seen[cid] = true
		info, err := h.store.Sessions.GetSession(ctx, cid)
		if err != nil || info == nil {
			continue
		}
		if info.CleanSession {
			continue
		}
		if info.Connected {
			continue
		}
		out = append(out, cid)
	}
	return out, nil
}

// OnSessionEstablished dequeues any stored messages for the (re)connecting
// client and writes them out as PUBLISH packets. Only runs for persistent
// (clean=false) sessions.
//
// Mochi-mqtt also maintains an in-memory inflight buffer per client that
// survives a clean=false disconnect (within the same process). On reconnect,
// mochi calls cl.ResendInflightMessages BEFORE this hook fires. So if mochi
// already had something to resend, the client just received it via that path
// and we must NOT also replay our DB queue, or every message arrives twice.
//
// Gating rule:
//   - mochi inflight non-empty  → in-process reconnect; mochi handled it.
//                                 Purge our DB queue so it doesn't double-fire.
//   - mochi inflight empty      → post-restart (or first attach); mochi has no
//                                 history. Drain our DB queue and replay.
func (h *QueueHook) OnSessionEstablished(cl *mqtt.Client, _ packets.Packet) {
	if cl.Properties.Clean {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if cl.State.Inflight.Len() > 0 {
		if _, err := h.store.Queue.PurgeForClient(ctx, cl.ID); err != nil {
			h.logger.Warn("queue hook: purge after mochi inflight resend failed", "client", cl.ID, "err", err)
		}
		return
	}

	for {
		batch, err := h.store.Queue.Dequeue(ctx, cl.ID, 100)
		if err != nil {
			h.logger.Warn("queue hook: dequeue failed", "client", cl.ID, "err", err)
			return
		}
		if len(batch) == 0 {
			return
		}
		for _, m := range batch {
			pk := packets.Packet{
				FixedHeader: packets.FixedHeader{
					Type:   packets.Publish,
					Qos:    m.QoS,
					Retain: false,
				},
				TopicName: m.TopicName,
				Payload:   m.Payload,
				Origin:    cl.ID,
			}
			if m.QoS > 0 {
				if pid, err := cl.NextPacketID(); err == nil {
					pk.PacketID = uint16(pid)
				}
			}
			if err := cl.WritePacket(pk); err != nil {
				h.logger.Warn("queue hook: write packet failed", "client", cl.ID, "topic", m.TopicName, "err", err)
				// leave the message for the next reconnect via visibility timeout
				return
			}
			// Best effort: ack on successful write. For QoS 1/2 a more rigorous
			// design would wait for PUBACK / PUBCOMP via OnQosComplete before
			// removing the row; for QoS 0 the row is removed immediately.
			if err := h.store.Queue.Ack(ctx, cl.ID, m.MessageUUID); err != nil {
				h.logger.Warn("queue hook: ack failed", "client", cl.ID, "uuid", m.MessageUUID, "err", err)
			}
		}
	}
}

func topicMatches(pattern, topic string) bool {
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
