package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"monstermq.io/edge/internal/stores"
)

const metricsTable = "metrics"

type MetricsStore struct {
	db *DB
}

func NewMetricsStore(db *DB) *MetricsStore { return &MetricsStore{db: db} }

func (m *MetricsStore) Close() error { return nil }

func (m *MetricsStore) EnsureTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + metricsTable + ` (
            timestamp TEXT NOT NULL,
            metric_type TEXT NOT NULL,
            identifier TEXT NOT NULL,
            metrics TEXT NOT NULL,
            PRIMARY KEY (timestamp, metric_type, identifier)
        )`,
		`CREATE INDEX IF NOT EXISTS metrics_timestamp_idx ON ` + metricsTable + ` (timestamp)`,
		`CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx ON ` + metricsTable + ` (metric_type, identifier, timestamp)`,
	}
	for _, q := range stmts {
		if _, err := m.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricsStore) StoreMetrics(ctx context.Context, kind stores.MetricKind, ts time.Time, identifier, payload string) error {
	_, err := m.db.Exec(`INSERT OR REPLACE INTO `+metricsTable+` (timestamp, metric_type, identifier, metrics) VALUES (?, ?, ?, ?)`,
		ts.UTC().Format(time.RFC3339Nano), string(kind), identifier, payload)
	return err
}

func (m *MetricsStore) GetLatest(ctx context.Context, kind stores.MetricKind, identifier string) (time.Time, string, error) {
	row := m.db.Conn().QueryRowContext(ctx,
		`SELECT timestamp, metrics FROM `+metricsTable+` WHERE metric_type = ? AND identifier = ? ORDER BY timestamp DESC LIMIT 1`,
		string(kind), identifier)
	var (
		tsStr   string
		payload string
	)
	if err := row.Scan(&tsStr, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, "", nil
		}
		return time.Time{}, "", err
	}
	t, _ := time.Parse(time.RFC3339Nano, tsStr)
	return t, payload, nil
}

func (m *MetricsStore) GetHistory(ctx context.Context, kind stores.MetricKind, identifier string, from, to time.Time, limit int) ([]stores.MetricsRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := m.db.Conn().QueryContext(ctx,
		`SELECT timestamp, metrics FROM `+metricsTable+` WHERE metric_type = ? AND identifier = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT ?`,
		string(kind), identifier, from.UTC().Format(time.RFC3339Nano), to.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.MetricsRow{}
	for rows.Next() {
		var (
			tsStr   string
			payload string
		)
		if err := rows.Scan(&tsStr, &payload); err != nil {
			return nil, err
		}
		t, _ := time.Parse(time.RFC3339Nano, tsStr)
		out = append(out, stores.MetricsRow{Timestamp: t, Payload: payload})
	}
	return out, rows.Err()
}

func (m *MetricsStore) PurgeOlderThan(ctx context.Context, t time.Time) (int64, error) {
	res, err := m.db.Exec(`DELETE FROM `+metricsTable+` WHERE timestamp < ?`, t.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
