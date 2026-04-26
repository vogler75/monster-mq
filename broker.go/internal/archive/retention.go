package archive

import (
	"context"
	"strings"
	"time"
)

// RunRetention starts a per-group goroutine that purges last-value and archive
// rows older than the configured retention window. The window is parsed from
// strings like "30d", "12h", "60m". Returns a stop function.
func (m *Manager) RunRetention(ctx context.Context) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(15 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				m.purgeOnce(ctx, now)
			}
		}
	}()
	// run once immediately to clean up stale data on boot
	go m.purgeOnce(ctx, time.Now())
	return cancel
}

func (m *Manager) purgeOnce(ctx context.Context, now time.Time) {
	for _, g := range m.Snapshot() {
		cfg := g.Config()
		if d := parseDuration(cfg.LastValRetention); d > 0 && g.LastValue() != nil {
			cutoff := now.Add(-d)
			if _, err := g.LastValue().PurgeOlderThan(ctx, cutoff); err != nil {
				m.logger.Warn("retention lastval purge failed", "group", cfg.Name, "err", err)
			}
		}
		if d := parseDuration(cfg.ArchiveRetention); d > 0 && g.Archive() != nil {
			cutoff := now.Add(-d)
			if _, err := g.Archive().PurgeOlderThan(ctx, cutoff); err != nil {
				m.logger.Warn("retention archive purge failed", "group", cfg.Name, "err", err)
			}
		}
	}
}

// parseDuration accepts MonsterMQ-style retention strings like "30d", "12h",
// "1500m", "45s" and returns the duration; 0 if unparseable.
func parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var n int
		_, err := fmtSscanf(s, "%dd", &n)
		if err == nil {
			return time.Duration(n) * 24 * time.Hour
		}
	}
	return 0
}

// indirection so we don't pull fmt into the package documentation summary.
var fmtSscanf = func(s, format string, a ...any) (int, error) {
	return sscan(s, format, a...)
}

// minimal scanf wrapper (avoids importing fmt in main file just for one call)
func sscan(s, format string, a ...any) (int, error) {
	return _sscan(s, format, a...)
}
