package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"monstermq.io/edge/internal/stores"
)

const queueTable = "messagequeue"

type QueueStore struct {
	db                *DB
	visibilityTimeout time.Duration
}

func NewQueueStore(db *DB, visibilityTimeout time.Duration) *QueueStore {
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30 * time.Second
	}
	return &QueueStore{db: db, visibilityTimeout: visibilityTimeout}
}

func (q *QueueStore) Close() error { return nil }

func (q *QueueStore) EnsureTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + queueTable + ` (
            msg_id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_uuid TEXT NOT NULL,
            client_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            payload BLOB,
            qos INTEGER NOT NULL,
            retained INTEGER NOT NULL DEFAULT 0,
            publisher_id TEXT,
            creation_time INTEGER NOT NULL,
            message_expiry_interval INTEGER,
            vt INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            read_ct INTEGER NOT NULL DEFAULT 0
        )`,
		`CREATE INDEX IF NOT EXISTS ` + queueTable + `_fetch_idx ON ` + queueTable + ` (client_id, vt)`,
		`CREATE INDEX IF NOT EXISTS ` + queueTable + `_client_uuid_idx ON ` + queueTable + ` (client_id, message_uuid)`,
	}
	for _, s := range stmts {
		if _, err := q.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func (q *QueueStore) Enqueue(ctx context.Context, clientID string, msg stores.BrokerMessage) error {
	return q.EnqueueMulti(ctx, msg, []string{clientID})
}

func (q *QueueStore) EnqueueMulti(ctx context.Context, msg stores.BrokerMessage, clientIDs []string) error {
	if len(clientIDs) == 0 {
		return nil
	}
	insert := fmt.Sprintf(`INSERT INTO %s
        (message_uuid, client_id, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, queueTable)
	q.db.Lock()
	defer q.db.Unlock()
	tx, err := q.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, insert)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, cid := range clientIDs {
		retained := 0
		if msg.IsRetain {
			retained = 1
		}
		var expiry sql.NullInt64
		if msg.MessageExpiryInterval != nil {
			expiry = sql.NullInt64{Int64: int64(*msg.MessageExpiryInterval), Valid: true}
		}
		if _, err := stmt.ExecContext(ctx, msg.MessageUUID, cid, msg.TopicName, msg.Payload, int(msg.QoS), retained, msg.ClientID, msg.Time.UnixMilli(), expiry); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (q *QueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]stores.BrokerMessage, error) {
	if batchSize <= 0 {
		batchSize = 10
	}
	now := time.Now().Unix()
	newVT := now + int64(q.visibilityTimeout.Seconds())

	q.db.Lock()
	defer q.db.Unlock()
	tx, err := q.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx,
		fmt.Sprintf(`SELECT msg_id, message_uuid, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval
                     FROM %s WHERE client_id = ? AND vt <= ? ORDER BY msg_id LIMIT ?`, queueTable),
		clientID, now, batchSize)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	type rec struct {
		msgID int64
		msg   stores.BrokerMessage
	}
	recs := []rec{}
	for rows.Next() {
		var (
			r        rec
			retained int
			pubID    sql.NullString
			created  int64
			expiry   sql.NullInt64
			qos      int
		)
		if err := rows.Scan(&r.msgID, &r.msg.MessageUUID, &r.msg.TopicName, &r.msg.Payload, &qos, &retained, &pubID, &created, &expiry); err != nil {
			rows.Close()
			_ = tx.Rollback()
			return nil, err
		}
		r.msg.QoS = byte(qos)
		r.msg.IsRetain = retained == 1
		r.msg.IsQueued = true
		r.msg.ClientID = pubID.String
		r.msg.Time = time.UnixMilli(created)
		if expiry.Valid {
			v := uint32(expiry.Int64)
			r.msg.MessageExpiryInterval = &v
		}
		recs = append(recs, r)
	}
	rows.Close()

	if len(recs) > 0 {
		stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("UPDATE %s SET vt = ?, read_ct = read_ct + 1 WHERE msg_id = ?", queueTable))
		if err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		for _, r := range recs {
			if _, err := stmt.ExecContext(ctx, newVT, r.msgID); err != nil {
				stmt.Close()
				_ = tx.Rollback()
				return nil, err
			}
		}
		stmt.Close()
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	out := make([]stores.BrokerMessage, len(recs))
	for i, r := range recs {
		out[i] = r.msg
	}
	return out, nil
}

func (q *QueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	_, err := q.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE client_id = ? AND message_uuid = ?", queueTable), clientID, messageUUID)
	return err
}

func (q *QueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	res, err := q.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE client_id = ?", queueTable), clientID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (q *QueueStore) Count(ctx context.Context, clientID string) (int64, error) {
	row := q.db.Conn().QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE client_id = ?", queueTable), clientID)
	var n int64
	err := row.Scan(&n)
	return n, err
}

func (q *QueueStore) CountAll(ctx context.Context) (int64, error) {
	row := q.db.Conn().QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", queueTable))
	var n int64
	err := row.Scan(&n)
	return n, err
}
