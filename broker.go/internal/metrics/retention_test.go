package metrics

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

// fakeMetricsStore counts purge calls and records the cutoff each time.
type fakeMetricsStore struct {
	purges atomic.Int32
	last   atomic.Pointer[time.Time]
}

func (f *fakeMetricsStore) StoreMetrics(_ context.Context, _ stores.MetricKind, _ time.Time, _ string, _ string) error {
	return nil
}
func (f *fakeMetricsStore) GetLatest(_ context.Context, _ stores.MetricKind, _ string) (time.Time, string, error) {
	return time.Time{}, "", nil
}
func (f *fakeMetricsStore) GetHistory(_ context.Context, _ stores.MetricKind, _ string, _, _ time.Time, _ int) ([]stores.MetricsRow, error) {
	return nil, nil
}
func (f *fakeMetricsStore) PurgeOlderThan(_ context.Context, t time.Time) (int64, error) {
	f.purges.Add(1)
	tcopy := t
	f.last.Store(&tcopy)
	return 0, nil
}
func (f *fakeMetricsStore) Close() error { return nil }

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestRunRetentionFiresInitialAndTickedPurges: with a 50ms interval, an
// initial call plus ~3 tick-driven calls should land within 180ms.
func TestRunRetentionFiresInitialAndTickedPurges(t *testing.T) {
	store := &fakeMetricsStore{}
	c := New(store, "test-node", time.Hour, discardLog())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.RunRetention(ctx, time.Hour /* retention */, 50*time.Millisecond /* tick */)
	time.Sleep(180 * time.Millisecond)
	c.Stop()
	if got := store.purges.Load(); got < 3 {
		t.Fatalf("expected at least 3 purges, got %d", got)
	}
	cutoff := store.last.Load()
	if cutoff == nil {
		t.Fatal("no cutoff recorded")
	}
	// Cutoff should be ~now-1h (within a few seconds).
	want := time.Now().Add(-time.Hour)
	if d := cutoff.Sub(want).Abs(); d > 5*time.Second {
		t.Fatalf("cutoff drift %v from now-1h, want < 5s", d)
	}
}

// TestRunRetentionNoOpWhenDisabled: retention=0 must not start a goroutine
// or call the store.
func TestRunRetentionNoOpWhenDisabled(t *testing.T) {
	store := &fakeMetricsStore{}
	c := New(store, "test-node", time.Hour, discardLog())
	c.RunRetention(context.Background(), 0, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if got := store.purges.Load(); got != 0 {
		t.Fatalf("expected 0 purges with retention=0, got %d", got)
	}
}
