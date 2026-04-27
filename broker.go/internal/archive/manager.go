package archive

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	storememory "monstermq.io/edge/internal/stores/memory"
	storemongo "monstermq.io/edge/internal/stores/mongodb"
	storepg "monstermq.io/edge/internal/stores/postgres"
	storesqlite "monstermq.io/edge/internal/stores/sqlite"
)

// Manager owns the active set of archive groups and dispatches every published
// message to all matching groups. It supports SQLite, PostgreSQL and MongoDB
// per-group last-value and archive stores.
type Manager struct {
	cfg      *config.Config
	storage  *stores.Storage
	logger   *slog.Logger
	sqliteDB *storesqlite.DB
	pgDB     *storepg.DB
	mongoDB  *storemongo.DB

	mu          sync.RWMutex
	groups      map[string]*Group
	deployError map[string]string // last deploy error per group name (for the dashboard)
}

func NewManager(cfg *config.Config, storage *stores.Storage,
	sqliteDB *storesqlite.DB, pgDB *storepg.DB, mongoDB *storemongo.DB,
	logger *slog.Logger) *Manager {
	return &Manager{
		cfg: cfg, storage: storage, logger: logger,
		sqliteDB: sqliteDB, pgDB: pgDB, mongoDB: mongoDB,
		groups:      map[string]*Group{},
		deployError: map[string]string{},
	}
}

// DeployError returns the last error encountered while trying to start the
// named group, or empty string if the group is healthy.
func (m *Manager) DeployError(name string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.deployError[name]
}

// Load creates archive groups from the persistent ArchiveConfigStore. If no
// "Default" group exists yet, one is provisioned with the broker's primary
// backend (so a Postgres-backed broker gets a Postgres-backed Default group).
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
			LastValType:   m.defaultLastValType(),
			ArchiveType:   m.defaultArchiveType(),
			PayloadFormat: stores.PayloadDefault,
		}
		if err := m.storage.ArchiveConfig.Save(ctx, def); err != nil {
			return err
		}
		configs = append(configs, def)
	}
	for _, c := range configs {
		if err := m.startGroup(ctx, c); err != nil {
			m.recordDeployError(c.Name, err)
			m.logger.Error("archive group start failed", "name", c.Name, "err", err)
		}
	}
	return nil
}

func (m *Manager) recordDeployError(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.deployError, name)
		return
	}
	m.deployError[name] = err.Error()
}

func (m *Manager) defaultLastValType() stores.MessageStoreType {
	switch m.cfg.DefaultStoreType {
	case config.StorePostgres:
		return stores.MessageStorePostgres
	case config.StoreMongoDB:
		return stores.MessageStoreMongoDB
	}
	return stores.MessageStoreSQLite
}

func (m *Manager) defaultArchiveType() stores.MessageArchiveType {
	switch m.cfg.DefaultStoreType {
	case config.StorePostgres:
		return stores.ArchivePostgres
	case config.StoreMongoDB:
		return stores.ArchiveMongoDB
	}
	return stores.ArchiveSQLite
}

func (m *Manager) startGroup(ctx context.Context, c stores.ArchiveGroupConfig) error {
	if !c.Enabled {
		return nil
	}
	lastVal, err := m.buildLastValStore(ctx, c)
	if err != nil {
		return err
	}
	arch, err := m.buildArchiveStore(ctx, c)
	if err != nil {
		return err
	}
	flushInterval := time.Duration(m.cfg.Archive.FlushIntervalMs) * time.Millisecond
	g := NewGroup(c, lastVal, arch,
		m.cfg.Archive.BufferSize, m.cfg.Archive.MaxBatchSize, flushInterval,
		m.logger)
	g.Start()
	m.mu.Lock()
	m.groups[c.Name] = g
	delete(m.deployError, c.Name)
	m.mu.Unlock()
	m.logger.Info("archive group started",
		"name", c.Name, "filters", c.TopicFilters,
		"lastValType", c.LastValType, "archiveType", c.ArchiveType)
	return nil
}

// Per-group store and archive names match the Kotlin/JVM broker:
//   last-value store →  "<groupName>Lastval"   (SQL table: <groupname>lastval)
//   message archive  →  "<groupName>Archive"   (SQL table: <groupname>archive)
// so the same physical database can be opened by either implementation.

// LastValName / ArchiveName are exported so tests and the dashboard can derive
// the same names production uses. Both are deterministic from the group name.
func LastValName(group string) string { return group + "Lastval" }
func ArchiveName(group string) string { return group + "Archive" }

// validGroupNamePattern restricts group names to characters that are safe to
// embed in a SQL identifier. Group names flow into CREATE TABLE / DROP TABLE
// statements via fmt.Sprintf, so anything outside this set could be SQL
// injection.
var validGroupNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,62}$`)

// ValidateGroupName returns an error if name is not a safe SQL identifier
// fragment. Called from the GraphQL create/update resolvers.
func ValidateGroupName(name string) error {
	if !validGroupNamePattern.MatchString(name) {
		return fmt.Errorf("invalid group name %q: must match [A-Za-z][A-Za-z0-9_]{0,62}", name)
	}
	return nil
}

func (m *Manager) buildLastValStore(ctx context.Context, c stores.ArchiveGroupConfig) (stores.MessageStore, error) {
	name := LastValName(c.Name)
	switch c.LastValType {
	case stores.MessageStoreMemory:
		return storememory.NewMessageStore(name), nil
	case stores.MessageStoreSQLite:
		if m.sqliteDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=SQLITE but no SQLite DB configured", c.Name)
		}
		s := storesqlite.NewMessageStore(name, m.sqliteDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on sqlite: %w", name, err)
		}
		return s, nil
	case stores.MessageStorePostgres:
		if m.pgDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=POSTGRES but no Postgres DB configured", c.Name)
		}
		s := storepg.NewMessageStore(name, m.pgDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on postgres: %w", name, err)
		}
		return s, nil
	case stores.MessageStoreMongoDB:
		if m.mongoDB == nil {
			return nil, fmt.Errorf("group %s: lastValType=MONGODB but no MongoDB configured", c.Name)
		}
		s := storemongo.NewMessageStore(name, m.mongoDB)
		if err := s.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on mongo: %w", name, err)
		}
		return s, nil
	case stores.MessageStoreNone, "":
		return nil, nil
	}
	return nil, fmt.Errorf("group %s: unsupported lastValType %q", c.Name, c.LastValType)
}

func (m *Manager) buildArchiveStore(ctx context.Context, c stores.ArchiveGroupConfig) (stores.MessageArchive, error) {
	name := ArchiveName(c.Name)
	switch c.ArchiveType {
	case stores.ArchiveSQLite:
		if m.sqliteDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=SQLITE but no SQLite DB configured", c.Name)
		}
		a := storesqlite.NewMessageArchive(name, m.sqliteDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on sqlite: %w", name, err)
		}
		return a, nil
	case stores.ArchivePostgres:
		if m.pgDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=POSTGRES but no Postgres DB configured", c.Name)
		}
		a := storepg.NewMessageArchive(name, m.pgDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on postgres: %w", name, err)
		}
		return a, nil
	case stores.ArchiveMongoDB:
		if m.mongoDB == nil {
			return nil, fmt.Errorf("group %s: archiveType=MONGODB but no MongoDB configured", c.Name)
		}
		a := storemongo.NewMessageArchive(name, m.mongoDB, c.PayloadFormat)
		if err := a.EnsureTable(ctx); err != nil {
			return nil, fmt.Errorf("ensure %s on mongo: %w", name, err)
		}
		return a, nil
	case stores.ArchiveNone, "":
		return nil, nil
	}
	return nil, fmt.Errorf("group %s: unsupported archiveType %q", c.Name, c.ArchiveType)
}

// Reload re-reads the archive config table and reconciles the running groups
// with the persisted state: starts new ones, restarts changed ones, stops
// removed ones. Call this after any GraphQL mutation that touched an archive
// group config.
func (m *Manager) Reload(ctx context.Context) error {
	configs, err := m.storage.ArchiveConfig.GetAll(ctx)
	if err != nil {
		return err
	}
	wanted := map[string]stores.ArchiveGroupConfig{}
	for _, c := range configs {
		if c.Enabled {
			wanted[c.Name] = c
		}
	}

	m.mu.Lock()
	current := m.groups
	m.mu.Unlock()

	// Stop groups that are gone or now disabled or whose config changed.
	for name, g := range current {
		w, keep := wanted[name]
		if !keep || !configsEqual(g.Config(), w) {
			g.Stop()
			m.mu.Lock()
			delete(m.groups, name)
			m.mu.Unlock()
		}
	}
	// Start groups that should be running but aren't.
	for name, c := range wanted {
		m.mu.RLock()
		_, running := m.groups[name]
		m.mu.RUnlock()
		if running {
			continue
		}
		if err := m.startGroup(ctx, c); err != nil {
			m.recordDeployError(name, err)
			m.logger.Error("archive group reload failed", "name", name, "err", err)
		}
	}
	return nil
}

func configsEqual(a, b stores.ArchiveGroupConfig) bool {
	if a.Name != b.Name || a.Enabled != b.Enabled || a.RetainedOnly != b.RetainedOnly {
		return false
	}
	if a.LastValType != b.LastValType || a.ArchiveType != b.ArchiveType {
		return false
	}
	if a.PayloadFormat != b.PayloadFormat {
		return false
	}
	if a.LastValRetention != b.LastValRetention || a.ArchiveRetention != b.ArchiveRetention {
		return false
	}
	if len(a.TopicFilters) != len(b.TopicFilters) {
		return false
	}
	for i := range a.TopicFilters {
		if a.TopicFilters[i] != b.TopicFilters[i] {
			return false
		}
	}
	return true
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
