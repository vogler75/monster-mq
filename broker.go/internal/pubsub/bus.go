package pubsub

import (
	"strings"
	"sync"

	"monstermq.io/edge/internal/stores"
)

// Bus is an in-process pub/sub used by GraphQL subscriptions to deliver
// MQTT-style topic events. Subscribers can express MQTT-style topic filters
// (with + and # wildcards).
type Bus struct {
	mu   sync.RWMutex
	next int
	subs map[int]*sub
}

type sub struct {
	filters []string
	ch      chan stores.BrokerMessage
}

func NewBus() *Bus { return &Bus{subs: map[int]*sub{}} }

func (b *Bus) Subscribe(filters []string, buffer int) (id int, ch <-chan stores.BrokerMessage) {
	if buffer <= 0 {
		buffer = 16
	}
	c := make(chan stores.BrokerMessage, buffer)
	b.mu.Lock()
	b.next++
	id = b.next
	b.subs[id] = &sub{filters: filters, ch: c}
	b.mu.Unlock()
	return id, c
}

func (b *Bus) Unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.subs[id]; ok {
		close(s.ch)
		delete(b.subs, id)
	}
}

func (b *Bus) Publish(msg stores.BrokerMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, s := range b.subs {
		if anyMatch(s.filters, msg.TopicName) {
			select {
			case s.ch <- msg:
			default:
				// drop on slow subscriber to keep the broker hot path nonblocking
			}
		}
	}
}

func anyMatch(filters []string, topic string) bool {
	for _, f := range filters {
		if matchTopic(f, topic) {
			return true
		}
	}
	return false
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
