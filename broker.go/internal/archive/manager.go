package archive

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

// Manager owns the active set of archive groups and dispatches every published
// message to all matching groups.
type Manager struct {
	cfg     *config.Config
	storage *stores.Storage
	logger  *slog.Logger
	sqliteDB *sqlite.DB

	mu     sync.RWMutex
	groups map[string]*Group
}

func NewManager(cfg *config.Config, storage *stores.Storage, sqliteDB *sqlite.DB, logger *slog.Logger) *Manager {
	return &Manager{cfg: cfg, storage: storage, logger: logger, sqliteDB: sqliteDB, groups: map[string]*Group{}}
}

// Load creates archive groups from the persistent ArchiveConfigStore. If no
// "Default" group exists yet, one is provisioned and saved.
func (m *Manager) Load(ctx context.Context) error {
	configs, err := m.storage.ArchiveConfig.GetAll(ctx)
	if err != nil {
		return err
	}
	hasDefault := false
	for _, c := range configs {
		if c.Name == "Default" {
			hasDefault = true
			break
		}
	}
	if !hasDefault {
		def := stores.ArchiveGroupConfig{
			Name:          "Default",
			Enabled:       true,
			TopicFilters:  []string{"#"},
			LastValType:   stores.MessageStoreSQLite,
			ArchiveType:   stores.ArchiveSQLite,
			PayloadFormat: stores.PayloadDefault,
		}
		if err := m.storage.ArchiveConfig.Save(ctx, def); err != nil {
			return err
		}
		configs = append(configs, def)
	}
	for _, c := range configs {
		if err := m.startGroup(ctx, c); err != nil {
			m.logger.Warn("archive group start failed", "name", c.Name, "err", err)
		}
	}
	return nil
}

func (m *Manager) startGroup(ctx context.Context, c stores.ArchiveGroupConfig) error {
	if !c.Enabled {
		return nil
	}
	var (
		lastVal stores.MessageStore
		arch    stores.MessageArchive
	)
	switch c.LastValType {
	case stores.MessageStoreSQLite:
		if m.sqliteDB == nil {
			return fmt.Errorf("group %s wants SQLite last-value store but no SQLite DB configured", c.Name)
		}
		ms := sqlite.NewMessageStore("lv_"+c.Name, m.sqliteDB)
		if err := ms.EnsureTable(ctx); err != nil {
			return fmt.Errorf("ensure lastval table for %s: %w", c.Name, err)
		}
		lastVal = ms
	case stores.MessageStoreNone, "":
		lastVal = nil
	default:
		m.logger.Warn("unsupported last-value type for edge build", "group", c.Name, "type", c.LastValType)
	}
	switch c.ArchiveType {
	case stores.ArchiveSQLite:
		if m.sqliteDB == nil {
			return fmt.Errorf("group %s wants SQLite archive but no SQLite DB configured", c.Name)
		}
		ma := sqlite.NewMessageArchive("ar_"+c.Name, m.sqliteDB, c.PayloadFormat)
		if err := ma.EnsureTable(ctx); err != nil {
			return fmt.Errorf("ensure archive table for %s: %w", c.Name, err)
		}
		arch = ma
	case stores.ArchiveNone, "":
		arch = nil
	default:
		m.logger.Warn("unsupported archive type for edge build", "group", c.Name, "type", c.ArchiveType)
	}
	g := NewGroup(c, lastVal, arch, m.logger)
	g.Start()
	m.mu.Lock()
	m.groups[c.Name] = g
	m.mu.Unlock()
	m.logger.Info("archive group started", "name", c.Name, "filters", c.TopicFilters)
	return nil
}

// Dispatch routes msg to every matching group.
func (m *Manager) Dispatch(msg stores.BrokerMessage) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, g := range m.groups {
		if g.Matches(msg.TopicName, msg.IsRetain) {
			g.Submit(msg)
		}
	}
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.groups {
		g.Stop()
	}
	m.groups = map[string]*Group{}
}

// Snapshot returns a stable view of the current groups for resolvers.
func (m *Manager) Snapshot() []*Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out
}
