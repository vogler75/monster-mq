package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"monstermq.io/edge/internal/stores"
)

// MessageArchive is the byte-compatible Go port of MessageArchiveSQLite.kt.
type MessageArchive struct {
	name      string
	tableName string
	db        *DB
	format    stores.PayloadFormat
}

func NewMessageArchive(name string, db *DB, format stores.PayloadFormat) *MessageArchive {
	return &MessageArchive{
		name:      name,
		tableName: strings.ToLower(name),
		db:        db,
		format:    format,
	}
}

func (a *MessageArchive) Name() string                      { return a.name }
func (a *MessageArchive) Type() stores.MessageArchiveType   { return stores.ArchiveSQLite }
func (a *MessageArchive) Close() error                      { return nil }

func (a *MessageArchive) EnsureTable(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
            topic TEXT NOT NULL,
            time TEXT NOT NULL,
            payload_blob BLOB,
            payload_json TEXT,
            qos INTEGER,
            retained BOOLEAN,
            client_id TEXT,
            message_uuid TEXT,
            PRIMARY KEY (topic, time)
        )`, a.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_time_idx ON %s (time)", a.tableName, a.tableName),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_topic_time_idx ON %s (topic, time)", a.tableName, a.tableName),
	}
	for _, q := range stmts {
		if _, err := a.db.Exec(q); err != nil {
			return fmt.Errorf("create %s: %w", a.tableName, err)
		}
	}
	return nil
}

func (a *MessageArchive) AddHistory(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	q := fmt.Sprintf(`INSERT INTO %s (topic, time, payload_blob, payload_json, qos, retained, client_id, message_uuid)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                      ON CONFLICT (topic, time) DO NOTHING`, a.tableName)

	a.db.Lock()
	defer a.db.Unlock()
	tx, err := a.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, m := range msgs {
		var payloadBlob []byte
		var payloadJSON sql.NullString
		if a.format == stores.PayloadJSON && isProbablyJSON(m.Payload) {
			payloadJSON = sql.NullString{String: string(m.Payload), Valid: true}
		} else {
			payloadBlob = m.Payload
		}
		if _, err := stmt.ExecContext(ctx,
			m.TopicName,
			m.Time.UTC().Format(time.RFC3339Nano),
			payloadBlob,
			payloadJSON,
			int(m.QoS),
			m.IsRetain,
			m.ClientID,
			m.MessageUUID,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (a *MessageArchive) GetHistory(ctx context.Context, topic string, from, to *time.Time, limit int) ([]stores.ArchivedMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	pattern := strings.ReplaceAll(topic, "#", "%")
	pattern = strings.ReplaceAll(pattern, "+", "%")
	q := fmt.Sprintf("SELECT topic, time, payload_blob, qos, client_id FROM %s WHERE topic LIKE ?", a.tableName)
	args := []any{pattern}
	if from != nil {
		q += " AND time >= ?"
		args = append(args, from.UTC().Format(time.RFC3339Nano))
	}
	if to != nil {
		q += " AND time <= ?"
		args = append(args, to.UTC().Format(time.RFC3339Nano))
	}
	q += " ORDER BY time DESC LIMIT ?"
	args = append(args, limit)

	rows, err := a.db.Conn().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]stores.ArchivedMessage, 0, limit)
	for rows.Next() {
		var (
			t       string
			topic   string
			payload []byte
			qos     int
			cid     sql.NullString
		)
		if err := rows.Scan(&topic, &t, &payload, &qos, &cid); err != nil {
			return nil, err
		}
		ts, _ := time.Parse(time.RFC3339Nano, t)
		out = append(out, stores.ArchivedMessage{
			Topic:     topic,
			Timestamp: ts,
			Payload:   payload,
			QoS:       byte(qos),
			ClientID:  cid.String,
		})
	}
	return out, rows.Err()
}

func (a *MessageArchive) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	q := fmt.Sprintf("DELETE FROM %s WHERE time < ?", a.tableName)
	res, err := a.db.Exec(q, t.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	n, _ := res.RowsAffected()
	return stores.PurgeResult{DeletedRows: n}, nil
}

func isProbablyJSON(b []byte) bool {
	for _, c := range b {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		return c == '{' || c == '[' || c == '"' || (c >= '0' && c <= '9') || c == '-' || c == 't' || c == 'f' || c == 'n'
	}
	return false
}
