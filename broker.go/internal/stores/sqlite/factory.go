package sqlite

import (
	"context"
	"time"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

// Build wires up a full Storage backed by a single SQLite file. The schema is
// byte-compatible with the Kotlin MonsterMQ broker so the same DB can be opened
// by either implementation.
//
// The returned *DB is exposed so the archive manager can create per-group
// last-value/archive tables on the same connection.
func Build(ctx context.Context, cfg *config.Config) (*stores.Storage, *DB, error) {
	db, err := Open(cfg.SQLite.Path)
	if err != nil {
		return nil, nil, err
	}

	retained := NewMessageStore("retainedmessages", db)
	sessions := NewSessionStore(db)
	queue := NewQueueStore(db, 30*time.Second)
	users := NewUserStore(db)
	archives := NewArchiveConfigStore(db)
	devices := NewDeviceConfigStore(db)
	metrics := NewMetricsStore(db)

	for _, t := range []interface{ EnsureTable(context.Context) error }{
		retained, sessions, queue, users, archives, devices, metrics,
	} {
		if err := t.EnsureTable(ctx); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
	}

	storage := &stores.Storage{
		Backend:       config.StoreSQLite,
		Sessions:      sessions,
		Subscriptions: sessions,
		Queue:         queue,
		Retained:      retained,
		Users:         users,
		ArchiveConfig: archives,
		DeviceConfig:  devices,
		Metrics:       metrics,
		Closer:        db.Close,
	}
	return storage, db, nil
}
