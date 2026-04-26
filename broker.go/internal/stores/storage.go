package stores

import (
	"context"
	"fmt"

	"monstermq.io/edge/internal/config"
)

// Storage is the aggregate of all stores used by the edge broker.
// Every backend (sqlite/postgres/mongodb) builds its own Storage instance.
type Storage struct {
	Backend       config.StoreType
	Sessions      SessionStore
	Subscriptions SessionStore // sessions and subscriptions share one backend table family
	Queue         QueueStore
	Retained      MessageStore
	Users         UserStore
	ArchiveConfig ArchiveConfigStore
	DeviceConfig  DeviceConfigStore
	Metrics       MetricsStore

	// Closer is invoked on shutdown to release the underlying database handles.
	Closer func() error
}

func (s *Storage) Close() error {
	if s.Closer != nil {
		return s.Closer()
	}
	return nil
}

// EnsureTables initialises every store's schema. Call once at startup.
func (s *Storage) EnsureTables(ctx context.Context, ensure ...interface{ EnsureTable(context.Context) error }) error {
	for _, e := range ensure {
		if err := e.EnsureTable(ctx); err != nil {
			return fmt.Errorf("ensure table: %w", err)
		}
	}
	return nil
}
