package mqttclient

import (
	"monstermq.io/edge/internal/pubsub"
	"monstermq.io/edge/internal/stores"
)

// BusAdapter wraps the in-process pubsub.Bus to satisfy LocalSubscriber.
type BusAdapter struct{ Bus *pubsub.Bus }

func (a *BusAdapter) Subscribe(filters []string, buffer int) (int, <-chan LocalMessage) {
	id, raw := a.Bus.Subscribe(filters, buffer)
	out := make(chan LocalMessage, buffer)
	go func() {
		defer close(out)
		for m := range raw {
			out <- LocalMessage{
				Topic:   m.TopicName,
				Payload: m.Payload,
				QoS:     m.QoS,
				Retain:  m.IsRetain,
			}
		}
	}()
	return id, out
}

func (a *BusAdapter) Unsubscribe(id int) { a.Bus.Unsubscribe(id) }

// Compile-time assertion that BrokerMessage stays compatible with what we map.
var _ = stores.BrokerMessage{}
