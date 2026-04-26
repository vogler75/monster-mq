package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"monstermq.io/edge/internal/stores"
)

const deviceConfigTable = "deviceconfigs"

type DeviceConfigStore struct {
	db *DB
}

func NewDeviceConfigStore(db *DB) *DeviceConfigStore { return &DeviceConfigStore{db: db} }

func (d *DeviceConfigStore) Close() error { return nil }

func (d *DeviceConfigStore) EnsureTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + deviceConfigTable + ` (
            name TEXT PRIMARY KEY,
            namespace TEXT NOT NULL,
            node_id TEXT NOT NULL,
            config TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            type TEXT DEFAULT 'MQTT_CLIENT',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_node_id ON ` + deviceConfigTable + ` (node_id)`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_enabled ON ` + deviceConfigTable + ` (enabled)`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_namespace ON ` + deviceConfigTable + ` (namespace)`,
	}
	for _, q := range stmts {
		if _, err := d.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func scanDevice(scanner interface{ Scan(...any) error }) (*stores.DeviceConfig, error) {
	var (
		dc        stores.DeviceConfig
		enabled   int
		createdAt sql.NullString
		updatedAt sql.NullString
	)
	if err := scanner.Scan(&dc.Name, &dc.Namespace, &dc.NodeID, &dc.Config, &enabled, &dc.Type, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	dc.Enabled = enabled == 1
	dc.CreatedAt = parseTime(createdAt)
	dc.UpdatedAt = parseTime(updatedAt)
	return &dc, nil
}

func (d *DeviceConfigStore) GetAll(ctx context.Context) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM `+deviceConfigTable+` ORDER BY name`)
}

func (d *DeviceConfigStore) GetByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM `+deviceConfigTable+` WHERE node_id = ? ORDER BY name`, nodeID)
}

func (d *DeviceConfigStore) GetEnabledByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM `+deviceConfigTable+` WHERE node_id = ? AND enabled = 1 ORDER BY name`, nodeID)
}

func (d *DeviceConfigStore) query(ctx context.Context, q string, args ...any) ([]stores.DeviceConfig, error) {
	rows, err := d.db.Conn().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.DeviceConfig{}
	for rows.Next() {
		dc, err := scanDevice(rows)
		if err != nil {
			return nil, err
		}
		if dc != nil {
			out = append(out, *dc)
		}
	}
	return out, rows.Err()
}

func (d *DeviceConfigStore) Get(ctx context.Context, name string) (*stores.DeviceConfig, error) {
	row := d.db.Conn().QueryRowContext(ctx,
		`SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM `+deviceConfigTable+` WHERE name = ?`, name)
	return scanDevice(row)
}

func (d *DeviceConfigStore) Save(ctx context.Context, dc stores.DeviceConfig) error {
	_, err := d.db.Exec(`INSERT INTO `+deviceConfigTable+` (name, namespace, node_id, config, enabled, type, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT (name) DO UPDATE SET
            namespace = excluded.namespace,
            node_id = excluded.node_id,
            config = excluded.config,
            enabled = excluded.enabled,
            type = excluded.type,
            updated_at = datetime('now')`,
		dc.Name, dc.Namespace, dc.NodeID, dc.Config, boolToInt(dc.Enabled), dc.Type)
	return err
}

func (d *DeviceConfigStore) Delete(ctx context.Context, name string) error {
	_, err := d.db.Exec(`DELETE FROM `+deviceConfigTable+` WHERE name = ?`, name)
	return err
}

func (d *DeviceConfigStore) Toggle(ctx context.Context, name string, enabled bool) (*stores.DeviceConfig, error) {
	if _, err := d.db.Exec(`UPDATE `+deviceConfigTable+` SET enabled = ?, updated_at = datetime('now') WHERE name = ?`, boolToInt(enabled), name); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}

func (d *DeviceConfigStore) Reassign(ctx context.Context, name, nodeID string) (*stores.DeviceConfig, error) {
	if _, err := d.db.Exec(`UPDATE `+deviceConfigTable+` SET node_id = ?, updated_at = datetime('now') WHERE name = ?`, nodeID, name); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}
