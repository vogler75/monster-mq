package archive

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"monstermq.io/edge/internal/stores"
)

// Group is one archive group: it owns a last-value MessageStore and a
// MessageArchive. Incoming messages that match any of the group's topic
// filters are pushed onto a bounded channel; a background goroutine batches
// them and flushes on either a ticker or when the batch hits MaxBatchSize.
//
// The channel is bounded so a stalled backend (DB lock, slow disk, network
// hiccup) cannot grow memory without limit — overflow drops the new message
// and increments an atomic counter exposed via Dropped(). This mirrors the
// JVM broker's ArrayBlockingQueue + drop-on-full behaviour.
type Group struct {
	cfg     stores.ArchiveGroupConfig
	lastVal stores.MessageStore
	archive stores.MessageArchive
	logger  *slog.Logger

	inCh   chan stores.BrokerMessage
	stopCh chan struct{}
	doneCh chan struct{}

	bufferSize    int
	maxBatchSize  int
	flushInterval time.Duration

	dropped atomic.Uint64
}

func NewGroup(cfg stores.ArchiveGroupConfig, lastVal stores.MessageStore, archive stores.MessageArchive,
	bufferSize, maxBatchSize int, flushInterval time.Duration, logger *slog.Logger) *Group {
	if bufferSize <= 0 {
		bufferSize = 100_000
	}
	if maxBatchSize <= 0 {
		maxBatchSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 250 * time.Millisecond
	}
	return &Group{
		cfg:           cfg,
		lastVal:       lastVal,
		archive:       archive,
		logger:        logger,
		inCh:          make(chan stores.BrokerMessage, bufferSize),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		bufferSize:    bufferSize,
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
	}
}

func (g *Group) Name() string                      { return g.cfg.Name }
func (g *Group) Config() stores.ArchiveGroupConfig { return g.cfg }
func (g *Group) LastValue() stores.MessageStore    { return g.lastVal }
func (g *Group) Archive() stores.MessageArchive    { return g.archive }

// Dropped returns the number of messages rejected because the buffer was
// full. Used by metrics and tests.
func (g *Group) Dropped() uint64 { return g.dropped.Load() }

// PendingLen reports how many messages are queued but not yet flushed.
func (g *Group) PendingLen() int { return len(g.inCh) }

// BufferSize is the configured capacity of the in-memory queue.
func (g *Group) BufferSize() int { return g.bufferSize }

// Start begins the background flush loop.
func (g *Group) Start() {
	go g.run()
}

// Stop signals the flush loop to drain remaining messages and exit. Blocks
// until the goroutine has finished, so callers can trust that no further
// writes to the underlying stores will happen after Stop returns.
func (g *Group) Stop() {
	close(g.stopCh)
	<-g.doneCh
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

// Submit hands a message to the group. Non-blocking: if the buffer is full
// the message is dropped and the counter advances. Logged at warn the first
// time and on every 10000th drop afterwards to avoid log floods.
func (g *Group) Submit(msg stores.BrokerMessage) {
	select {
	case g.inCh <- msg:
	default:
		n := g.dropped.Add(1)
		if n == 1 || n%10000 == 0 {
			g.logger.Warn("archive group buffer full, dropping message",
				"group", g.cfg.Name, "dropped", n, "bufferSize", g.bufferSize)
		}
	}
}

func (g *Group) run() {
	defer close(g.doneCh)
	t := time.NewTicker(g.flushInterval)
	defer t.Stop()
	batch := make([]stores.BrokerMessage, 0, g.maxBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		g.flushBatch(batch)
		batch = batch[:0]
	}
	for {
		select {
		case <-g.stopCh:
			// Drain whatever is still buffered before exiting.
			for {
				select {
				case msg := <-g.inCh:
					batch = append(batch, msg)
					if len(batch) >= g.maxBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case <-t.C:
			flush()
		case msg := <-g.inCh:
			batch = append(batch, msg)
			if len(batch) >= g.maxBatchSize {
				flush()
			}
		}
	}
}

func (g *Group) flushBatch(batch []stores.BrokerMessage) {
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
