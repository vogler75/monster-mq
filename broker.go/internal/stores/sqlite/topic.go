package sqlite

import (
	"encoding/json"
	"strings"
)

// MaxFixedTopicLevels mirrors the Kotlin MessageStoreSQLite layout so the same
// physical SQLite file is byte-compatible across both brokers.
const MaxFixedTopicLevels = 9

// SplitTopic returns (fixed, rest, last) where fixed are the first 9 levels
// (right-padded with empty strings), rest is the JSON-encoded array of any
// remaining levels, and last is the final level of the topic.
func SplitTopic(topic string) (fixed [MaxFixedTopicLevels]string, restJSON string, last string) {
	parts := strings.Split(topic, "/")
	for i := 0; i < MaxFixedTopicLevels && i < len(parts); i++ {
		fixed[i] = parts[i]
	}
	if len(parts) > MaxFixedTopicLevels {
		rest := parts[MaxFixedTopicLevels:]
		b, _ := json.Marshal(rest)
		restJSON = string(b)
	} else {
		restJSON = "[]"
	}
	if len(parts) > 0 {
		last = parts[len(parts)-1]
	}
	return
}

// MatchTopic checks whether a concrete MQTT topic matches a subscription pattern.
// Supports + (single level) and # (multi level, must be the last level) wildcards.
func MatchTopic(pattern, topic string) bool {
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
