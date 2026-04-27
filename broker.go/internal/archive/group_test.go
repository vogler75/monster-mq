package archive

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

// fakeLastVal is a MessageStore stub that records every batch and can be
// configured to sleep inside AddAll, simulating a stalled backend.
type fakeLastVal struct {
	mu      sync.Mutex
	batches [][]stores.BrokerMessage
	sleep   time.Duration
	count   atomic.Int64
}

func (f *fakeLastVal) Name() string                     { return "fakeLV" }
func (f *fakeLastVal) Type() stores.MessageStoreType    { return stores.MessageStoreMemory }
func (f *fakeLastVal) Close() error                     { return nil }
func (f *fakeLastVal) EnsureTable(_ context.Context) error { return nil }

func (f *fakeLastVal) Get(_ context.Context, _ string) (*stores.BrokerMessage, error) {
	return nil, nil
}
func (f *fakeLastVal) DelAll(_ context.Context, _ []string) error { return nil }
func (f *fakeLastVal) FindMatchingMessages(_ context.Context, _ string, _ func(stores.BrokerMessage) bool) error {
	return nil
}
func (f *fakeLastVal) FindMatchingTopics(_ context.Context, _ string, _ func(string) bool) error {
	return nil
}
func (f *fakeLastVal) PurgeOlderThan(_ context.Context, _ time.Time) (stores.PurgeResult, error) {
	return stores.PurgeResult{}, nil
}

func (f *fakeLastVal) AddAll(_ context.Context, msgs []stores.BrokerMessage) error {
	if f.sleep > 0 {
		time.Sleep(f.sleep)
	}
	f.mu.Lock()
	cp := make([]stores.BrokerMessage, len(msgs))
	copy(cp, msgs)
	f.batches = append(f.batches, cp)
	f.mu.Unlock()
	f.count.Add(int64(len(msgs)))
	return nil
}

func (f *fakeLastVal) totalRows() int64 { return f.count.Load() }
func (f *fakeLastVal) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func cfgWithFilter(name, filter string) stores.ArchiveGroupConfig {
	return stores.ArchiveGroupConfig{
		Name: name, Enabled: true, TopicFilters: []string{filter},
	}
}

// TestGroupFlushesEverythingWhenStoreIsFast confirms the happy path: if the
// store keeps up, every Submit ends up in a batch and Stop drains the rest.
func TestGroupFlushesEverythingWhenStoreIsFast(t *testing.T) {
	lv := &fakeLastVal{}
	g := NewGroup(cfgWithFilter("g1", "#"), lv, nil,
		1000, 100, 20*time.Millisecond, discardLogger())
	g.Start()

	const N = 250
	for i := 0; i < N; i++ {
		g.Submit(stores.BrokerMessage{TopicName: "x", Payload: []byte{byte(i)}})
	}
	g.Stop() // blocks until drain finishes

	if got := lv.totalRows(); got != int64(N) {
		t.Fatalf("rows written = %d, want %d", got, N)
	}
	if g.Dropped() != 0 {
		t.Fatalf("dropped = %d, want 0", g.Dropped())
	}
}

// TestGroupBatchesOnSize: with MaxBatchSize=10 and a fast store, exactly N/10
// batches should be produced (modulo a possible trailing partial batch on
// shutdown).
func TestGroupBatchesOnSize(t *testing.T) {
	lv := &fakeLastVal{}
	g := NewGroup(cfgWithFilter("g2", "#"), lv, nil,
		1000, 10, time.Hour /* never time-flush */, discardLogger())
	g.Start()
	for i := 0; i < 50; i++ {
		g.Submit(stores.BrokerMessage{TopicName: "y", Payload: []byte{byte(i)}})
	}
	g.Stop()
	if got := lv.totalRows(); got != 50 {
		t.Fatalf("rows = %d want 50", got)
	}
	// 50 messages / batchSize 10 → 5 size-triggered batches.
	if got := lv.batchCount(); got < 5 || got > 6 {
		t.Fatalf("batches = %d want ~5", got)
	}
}

// TestGroupDropsWhenBufferFull: the central P0 invariant — if the backend
// stalls, memory must stay bounded. With a small buffer and a slow store,
// pushing N >> bufferSize messages must result in dropped > 0 and PendingLen
// never exceeding the configured cap.
func TestGroupDropsWhenBufferFull(t *testing.T) {
	const bufSize = 50
	lv := &fakeLastVal{sleep: 200 * time.Millisecond}
	g := NewGroup(cfgWithFilter("g3", "#"), lv, nil,
		bufSize, 10, 50*time.Millisecond, discardLogger())
	g.Start()

	const N = 5000
	for i := 0; i < N; i++ {
		g.Submit(stores.BrokerMessage{TopicName: "z", Payload: []byte{byte(i)}})
		if pending := g.PendingLen(); pending > bufSize {
			t.Fatalf("pending=%d exceeded bufSize=%d", pending, bufSize)
		}
	}
	dropped := g.Dropped()
	if dropped == 0 {
		t.Fatalf("expected drops with slow store, got 0")
	}
	written := lv.totalRows()
	t.Logf("submitted=%d dropped=%d written=%d (still in flight at this point)", N, dropped, written)
	if int64(dropped)+written > int64(N)+int64(bufSize) {
		t.Fatalf("dropped + written + buffered exceeds submitted: %d+%d > %d", dropped, written, N)
	}

	// We don't wait for full drain here — the slow store would take ~minutes.
	// The contract under test is "memory stays bounded under overload",
	// which we've already verified above.
	go g.Stop()
}

// TestGroupRespectsRetainedOnly verifies the orthogonal Matches gate still
// works after the rewrite.
func TestGroupRespectsRetainedOnly(t *testing.T) {
	cfg := cfgWithFilter("g4", "#")
	cfg.RetainedOnly = true
	g := NewGroup(cfg, nil, nil, 100, 10, time.Hour, discardLogger())
	if g.Matches("any/topic", false) {
		t.Fatal("non-retained should not match RetainedOnly group")
	}
	if !g.Matches("any/topic", true) {
		t.Fatal("retained should match RetainedOnly group")
	}
}

// TestGroupDefaults verifies that zero/negative ctor args fall back to
// the documented defaults rather than producing a degenerate group.
func TestGroupDefaults(t *testing.T) {
	g := NewGroup(cfgWithFilter("g5", "#"), nil, nil, 0, 0, 0, discardLogger())
	if g.bufferSize != 100_000 {
		t.Fatalf("bufferSize default = %d", g.bufferSize)
	}
	if g.maxBatchSize != 1000 {
		t.Fatalf("maxBatchSize default = %d", g.maxBatchSize)
	}
	if g.flushInterval != 250*time.Millisecond {
		t.Fatalf("flushInterval default = %v", g.flushInterval)
	}
	if g.purgeInterval != time.Hour {
		t.Fatalf("purgeInterval default = %v", g.purgeInterval)
	}
}

// purgeRecorder is a MessageStore that just counts PurgeOlderThan calls so
// the test can assert the per-group retention loop fired.
type purgeRecorder struct {
	fakeLastVal
	purges atomic.Int32
}

func (p *purgeRecorder) PurgeOlderThan(_ context.Context, _ time.Time) (stores.PurgeResult, error) {
	p.purges.Add(1)
	return stores.PurgeResult{}, nil
}

// TestGroupRunsInitialPurgeOnStart: with LastValRetention set, Start must
// trigger one purge synchronously (before the ticker has a chance to fire).
func TestGroupRunsInitialPurgeOnStart(t *testing.T) {
	pr := &purgeRecorder{}
	cfg := cfgWithFilter("g6", "#")
	cfg.LastValRetention = "1h"
	cfg.PurgeInterval = "1h" // ticker irrelevant for this assertion
	g := NewGroup(cfg, pr, nil, 100, 10, time.Hour, discardLogger())
	g.Start()
	defer g.Stop()
	if got := pr.purges.Load(); got != 1 {
		t.Fatalf("expected 1 initial purge, got %d", got)
	}
}

// TestGroupRetentionTickerFires: with a 50ms purge interval and 1h
// retention, several ticks must produce additional purge calls.
func TestGroupRetentionTickerFires(t *testing.T) {
	pr := &purgeRecorder{}
	cfg := cfgWithFilter("g7", "#")
	cfg.LastValRetention = "1h"
	cfg.PurgeInterval = "50ms"
	g := NewGroup(cfg, pr, nil, 100, 10, time.Hour, discardLogger())
	g.Start()
	time.Sleep(180 * time.Millisecond) // expect ~3 ticks plus the initial
	g.Stop()
	if got := pr.purges.Load(); got < 3 {
		t.Fatalf("expected at least 3 purges (1 initial + ~3 ticked), got %d", got)
	}
}

// TestGroupSkipsPurgeWhenRetentionUnset: with no retention configured,
// even the initial purge must be a no-op against the underlying store.
func TestGroupSkipsPurgeWhenRetentionUnset(t *testing.T) {
	pr := &purgeRecorder{}
	cfg := cfgWithFilter("g8", "#") // no Retention set
	cfg.PurgeInterval = "20ms"
	g := NewGroup(cfg, pr, nil, 100, 10, time.Hour, discardLogger())
	g.Start()
	time.Sleep(80 * time.Millisecond)
	g.Stop()
	if got := pr.purges.Load(); got != 0 {
		t.Fatalf("expected 0 purges (no retention configured), got %d", got)
	}
}
