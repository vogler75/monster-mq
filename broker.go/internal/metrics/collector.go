package metrics

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/stores"
)

// Collector tallies per-second message rate counters and periodically persists
// a snapshot to the metrics store. Callers increment counters from hot paths.
type Collector struct {
	store    stores.MetricsStore
	logger   *slog.Logger
	nodeID   string
	interval time.Duration

	in            atomic.Int64
	out           atomic.Int64
	mqttBridgeIn  atomic.Int64
	mqttBridgeOut atomic.Int64

	mu     sync.RWMutex
	latest BrokerSnapshot

	stopCh chan struct{}
}

type BrokerSnapshot struct {
	MessagesIn       float64 `json:"messagesIn"`
	MessagesOut      float64 `json:"messagesOut"`
	MqttClientIn     float64 `json:"mqttClientIn"`
	MqttClientOut    float64 `json:"mqttClientOut"`
	NodeSessionCount int     `json:"nodeSessionCount"`
	SubscriptionCount int    `json:"subscriptionCount"`
	QueuedMessages   int64   `json:"queuedMessagesCount"`
}

func New(store stores.MetricsStore, nodeID string, interval time.Duration, logger *slog.Logger) *Collector {
	if interval <= 0 {
		interval = time.Second
	}
	return &Collector{
		store: store, nodeID: nodeID, interval: interval, logger: logger,
		stopCh: make(chan struct{}),
	}
}

func (c *Collector) IncIn()             { c.in.Add(1) }
func (c *Collector) IncOut()            { c.out.Add(1) }
func (c *Collector) IncBridgeIn(n int)  { c.mqttBridgeIn.Add(int64(n)) }
func (c *Collector) IncBridgeOut(n int) { c.mqttBridgeOut.Add(int64(n)) }

func (c *Collector) Latest() BrokerSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latest
}

// Start begins the periodic snapshot loop. counts is called every tick to
// observe the current session/subscription/queue size, since the collector
// itself does not own those data structures.
func (c *Collector) Start(ctx context.Context, counts func() (sessions, subs int, queued int64)) {
	go func() {
		t := time.NewTicker(c.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case now := <-t.C:
				inN := c.in.Swap(0)
				outN := c.out.Swap(0)
				biN := c.mqttBridgeIn.Swap(0)
				boN := c.mqttBridgeOut.Swap(0)
				sessions, subs, queued := 0, 0, int64(0)
				if counts != nil {
					sessions, subs, queued = counts()
				}
				snap := BrokerSnapshot{
					MessagesIn:        float64(inN) / c.interval.Seconds(),
					MessagesOut:       float64(outN) / c.interval.Seconds(),
					MqttClientIn:      float64(biN) / c.interval.Seconds(),
					MqttClientOut:     float64(boN) / c.interval.Seconds(),
					NodeSessionCount:  sessions,
					SubscriptionCount: subs,
					QueuedMessages:    queued,
				}
				c.mu.Lock()
				c.latest = snap
				c.mu.Unlock()
				if c.store != nil {
					payload, _ := json.Marshal(snap)
					if err := c.store.StoreMetrics(ctx, stores.MetricBroker, now.UTC(), c.nodeID, string(payload)); err != nil {
						c.logger.Warn("metrics persist failed", "err", err)
					}
				}
			}
		}
	}()
}

func (c *Collector) Stop() {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

// RunRetention starts a background goroutine that purges metrics rows older
// than `retention` every `interval`. Both args must be > 0; otherwise this is
// a no-op (e.g. user explicitly disabled retention by setting RetentionHours
// to 0). Stops on the same stopCh as the collector loop.
func (c *Collector) RunRetention(ctx context.Context, retention, interval time.Duration) {
	if c.store == nil || retention <= 0 || interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		// Purge once on startup to drop stale rows from before the broker came up.
		c.purgeOnce(ctx, retention)
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-t.C:
				c.purgeOnce(ctx, retention)
			}
		}
	}()
}

func (c *Collector) purgeOnce(ctx context.Context, retention time.Duration) {
	cutoff := time.Now().Add(-retention)
	if _, err := c.store.PurgeOlderThan(ctx, cutoff); err != nil {
		c.logger.Warn("metrics retention purge failed",
			"cutoff", cutoff.Format(time.RFC3339), "err", err)
	}
}
