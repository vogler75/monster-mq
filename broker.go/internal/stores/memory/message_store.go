// Package memory provides an in-memory MessageStore for last-value caching.
// Nothing is persisted — useful for fast volatile current-state lookups.
package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"monstermq.io/edge/internal/stores"
)

type MessageStore struct {
	name string
	mu   sync.RWMutex
	data map[string]stores.BrokerMessage
}

func NewMessageStore(name string) *MessageStore {
	return &MessageStore{name: name, data: map[string]stores.BrokerMessage{}}
}

func (s *MessageStore) Name() string                  { return s.name }
func (s *MessageStore) Type() stores.MessageStoreType { return stores.MessageStoreMemory }
func (s *MessageStore) Close() error                  { return nil }
func (s *MessageStore) EnsureTable(_ context.Context) error { return nil }

func (s *MessageStore) Get(_ context.Context, topic string) (*stores.BrokerMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m, ok := s.data[topic]; ok {
		copy := m
		return &copy, nil
	}
	return nil, nil
}

func (s *MessageStore) AddAll(_ context.Context, msgs []stores.BrokerMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		s.data[m.TopicName] = m
	}
	return nil
}

func (s *MessageStore) DelAll(_ context.Context, topics []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range topics {
		delete(s.data, t)
	}
	return nil
}

func (s *MessageStore) FindMatchingMessages(_ context.Context, pattern string, yield func(stores.BrokerMessage) bool) error {
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		if !matchTopic(pattern, k) {
			continue
		}
		s.mu.RLock()
		m, ok := s.data[k]
		s.mu.RUnlock()
		if !ok {
			continue
		}
		copy := m
		if !yield(copy) {
			return nil
		}
	}
	return nil
}

func (s *MessageStore) FindMatchingTopics(_ context.Context, pattern string, yield func(string) bool) error {
	s.mu.RLock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		if matchTopic(pattern, k) {
			if !yield(k) {
				return nil
			}
		}
	}
	return nil
}

func (s *MessageStore) PurgeOlderThan(_ context.Context, t time.Time) (stores.PurgeResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k, m := range s.data {
		if m.Time.Before(t) {
			delete(s.data, k)
			n++
		}
	}
	return stores.PurgeResult{DeletedRows: n}, nil
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

var _ stores.MessageStore = (*MessageStore)(nil)
