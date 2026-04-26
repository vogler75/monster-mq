package archive

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"monstermq.io/edge/internal/stores"
)

// Group is one archive group: it owns a last-value MessageStore and a
// MessageArchive. Incoming messages that match any of the group's topic filters
// are buffered and flushed on a tick to amortize disk I/O.
type Group struct {
	cfg      stores.ArchiveGroupConfig
	lastVal  stores.MessageStore
	archive  stores.MessageArchive
	logger   *slog.Logger
	mu       sync.Mutex
	pending  []stores.BrokerMessage
	flushCh  chan struct{}
	stopCh   chan struct{}
	flushDur time.Duration
}

func NewGroup(cfg stores.ArchiveGroupConfig, lastVal stores.MessageStore, archive stores.MessageArchive, logger *slog.Logger) *Group {
	return &Group{
		cfg:      cfg,
		lastVal:  lastVal,
		archive:  archive,
		logger:   logger,
		flushCh:  make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		flushDur: 250 * time.Millisecond,
	}
}

func (g *Group) Name() string                       { return g.cfg.Name }
func (g *Group) Config() stores.ArchiveGroupConfig  { return g.cfg }
func (g *Group) LastValue() stores.MessageStore     { return g.lastVal }
func (g *Group) Archive() stores.MessageArchive     { return g.archive }

// Start begins the background flush loop.
func (g *Group) Start() {
	go g.run()
}

func (g *Group) Stop() {
	close(g.stopCh)
}

// Matches returns true if topic should be archived by this group.
func (g *Group) Matches(topic string, retain bool) bool {
	if !g.cfg.Enabled {
		return false
	}
	if g.cfg.RetainedOnly && !retain {
		return false
	}
	for _, f := range g.cfg.TopicFilters {
		if matchTopic(strings.TrimSpace(f), topic) {
			return true
		}
	}
	return false
}

func (g *Group) Submit(msg stores.BrokerMessage) {
	g.mu.Lock()
	g.pending = append(g.pending, msg)
	pendingLen := len(g.pending)
	g.mu.Unlock()
	if pendingLen >= 100 {
		select {
		case g.flushCh <- struct{}{}:
		default:
		}
	}
}

func (g *Group) run() {
	t := time.NewTicker(g.flushDur)
	defer t.Stop()
	for {
		select {
		case <-g.stopCh:
			g.flush()
			return
		case <-t.C:
			g.flush()
		case <-g.flushCh:
			g.flush()
		}
	}
}

func (g *Group) flush() {
	g.mu.Lock()
	if len(g.pending) == 0 {
		g.mu.Unlock()
		return
	}
	batch := g.pending
	g.pending = nil
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if g.lastVal != nil {
		if err := g.lastVal.AddAll(ctx, batch); err != nil {
			g.logger.Warn("archive lastval write failed", "group", g.cfg.Name, "n", len(batch), "err", err)
		}
	}
	if g.archive != nil {
		if err := g.archive.AddHistory(ctx, batch); err != nil {
			g.logger.Warn("archive history write failed", "group", g.cfg.Name, "n", len(batch), "err", err)
		}
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
