package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"monstermq.io/edge/internal/stores"
)

const archiveConfigTable = "archiveconfigs"

type ArchiveConfigStore struct {
	db *DB
}

func NewArchiveConfigStore(db *DB) *ArchiveConfigStore { return &ArchiveConfigStore{db: db} }

func (a *ArchiveConfigStore) Close() error { return nil }

func (a *ArchiveConfigStore) EnsureTable(ctx context.Context) error {
	_, err := a.db.Exec(`CREATE TABLE IF NOT EXISTS ` + archiveConfigTable + ` (
        name TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0,
        topic_filter TEXT NOT NULL,
        retained_only INTEGER NOT NULL DEFAULT 0,
        last_val_type TEXT NOT NULL,
        archive_type TEXT NOT NULL,
        last_val_retention TEXT,
        archive_retention TEXT,
        purge_interval TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        payload_format TEXT DEFAULT 'DEFAULT'
    )`)
	return err
}

func rowToArchive(scanner interface{ Scan(...any) error }) (*stores.ArchiveGroupConfig, error) {
	var (
		cfg            stores.ArchiveGroupConfig
		enabled        int
		retainedOnly   int
		topicFilter    string
		lvType         string
		arType         string
		lvRet          sql.NullString
		arRet          sql.NullString
		purgeInt       sql.NullString
		payloadFormat  sql.NullString
		createdAt      sql.NullString
		updatedAt      sql.NullString
	)
	if err := scanner.Scan(&cfg.Name, &enabled, &topicFilter, &retainedOnly, &lvType, &arType, &lvRet, &arRet, &purgeInt, &payloadFormat, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	cfg.Enabled = enabled == 1
	cfg.RetainedOnly = retainedOnly == 1
	cfg.TopicFilters = splitFilter(topicFilter)
	cfg.LastValType = stores.MessageStoreType(lvType)
	cfg.ArchiveType = stores.MessageArchiveType(arType)
	cfg.LastValRetention = lvRet.String
	cfg.ArchiveRetention = arRet.String
	cfg.PurgeInterval = purgeInt.String
	if payloadFormat.Valid {
		cfg.PayloadFormat = stores.PayloadFormat(payloadFormat.String)
	} else {
		cfg.PayloadFormat = stores.PayloadDefault
	}
	cfg.CreatedAt = parseSqliteTimestamp(createdAt)
	cfg.UpdatedAt = parseSqliteTimestamp(updatedAt)
	return &cfg, nil
}

// parseSqliteTimestamp accepts the formats SQLite's CURRENT_TIMESTAMP can
// emit. Returns the zero time if unparseable so callers can ignore the
// field gracefully.
func parseSqliteTimestamp(s sql.NullString) time.Time {
	if !s.Valid || s.String == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339,
		"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s.String); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func splitFilter(s string) []string {
	if s == "" {
		return nil
	}
	out := strings.Split(s, ",")
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
	}
	return out
}

func joinFilter(s []string) string { return strings.Join(s, ",") }

func (a *ArchiveConfigStore) GetAll(ctx context.Context) ([]stores.ArchiveGroupConfig, error) {
	rows, err := a.db.Conn().QueryContext(ctx,
		`SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, created_at, updated_at FROM `+archiveConfigTable+` ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.ArchiveGroupConfig{}
	for rows.Next() {
		c, err := rowToArchive(rows)
		if err != nil {
			return nil, err
		}
		if c != nil {
			out = append(out, *c)
		}
	}
	return out, rows.Err()
}

func (a *ArchiveConfigStore) Get(ctx context.Context, name string) (*stores.ArchiveGroupConfig, error) {
	row := a.db.Conn().QueryRowContext(ctx,
		`SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, created_at, updated_at FROM `+archiveConfigTable+` WHERE name = ?`, name)
	return rowToArchive(row)
}

func (a *ArchiveConfigStore) Save(ctx context.Context, cfg stores.ArchiveGroupConfig) error {
	_, err := a.db.Exec(`INSERT INTO `+archiveConfigTable+` (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (name) DO UPDATE SET
            enabled = excluded.enabled,
            topic_filter = excluded.topic_filter,
            retained_only = excluded.retained_only,
            last_val_type = excluded.last_val_type,
            archive_type = excluded.archive_type,
            last_val_retention = excluded.last_val_retention,
            archive_retention = excluded.archive_retention,
            purge_interval = excluded.purge_interval,
            payload_format = excluded.payload_format,
            updated_at = CURRENT_TIMESTAMP`,
		cfg.Name, boolToInt(cfg.Enabled), joinFilter(cfg.TopicFilters), boolToInt(cfg.RetainedOnly),
		string(cfg.LastValType), string(cfg.ArchiveType), nullStr(cfg.LastValRetention), nullStr(cfg.ArchiveRetention), nullStr(cfg.PurgeInterval), string(cfg.PayloadFormat))
	return err
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (a *ArchiveConfigStore) Update(ctx context.Context, cfg stores.ArchiveGroupConfig) error {
	return a.Save(ctx, cfg)
}

func (a *ArchiveConfigStore) Delete(ctx context.Context, name string) error {
	_, err := a.db.Exec(`DELETE FROM `+archiveConfigTable+` WHERE name = ?`, name)
	return err
}
