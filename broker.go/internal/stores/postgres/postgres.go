// Package postgres implements all seven storage interfaces against PostgreSQL
// using jackc/pgx/v5. DDL/column names mirror the Kotlin MonsterMQ broker so
// the same database can be opened by either implementation.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

// DB wraps a pgx connection pool.
type DB struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect pg: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}
	return &DB{pool: pool}, nil
}

func (d *DB) Close() error { d.pool.Close(); return nil }

// Build constructs a Storage backed by Postgres.
func Build(ctx context.Context, cfg *config.Config) (*stores.Storage, error) {
	dsn := cfg.Postgres.URL
	if cfg.Postgres.User != "" || cfg.Postgres.Pass != "" {
		dsn = fmt.Sprintf("%s?user=%s&password=%s", dsn, cfg.Postgres.User, cfg.Postgres.Pass)
	}
	db, err := Open(ctx, dsn)
	if err != nil {
		return nil, err
	}
	retained := &MessageStore{name: "retainedmessages", db: db}
	sessions := &SessionStore{db: db}
	queue := &QueueStore{db: db, visibility: 30 * time.Second}
	users := &UserStore{db: db}
	archives := &ArchiveConfigStore{db: db}
	devices := &DeviceConfigStore{db: db}
	metricsStore := &MetricsStore{db: db}
	for _, t := range []interface{ EnsureTable(context.Context) error }{
		retained, sessions, queue, users, archives, devices, metricsStore,
	} {
		if err := t.EnsureTable(ctx); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &stores.Storage{
		Backend:       config.StorePostgres,
		Sessions:      sessions, Subscriptions: sessions,
		Queue: queue, Retained: retained, Users: users,
		ArchiveConfig: archives, DeviceConfig: devices, Metrics: metricsStore,
		Closer: db.Close,
	}, nil
}

// MessageStore -------------------------------------------------------------

type MessageStore struct {
	name string
	db   *DB
}

func NewMessageStore(name string, db *DB) *MessageStore { return &MessageStore{name: name, db: db} }

func (s *MessageStore) Name() string                  { return s.name }
func (s *MessageStore) Type() stores.MessageStoreType { return stores.MessageStorePostgres }
func (s *MessageStore) Close() error                  { return nil }

func (s *MessageStore) tableName() string { return strings.ToLower(s.name) }

func (s *MessageStore) EnsureTable(ctx context.Context) error {
	t := s.tableName()
	_, err := s.db.pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
        topic TEXT PRIMARY KEY,
        time TIMESTAMPTZ,
        payload_blob BYTEA,
        payload_json JSONB,
        qos INTEGER,
        retained BOOLEAN,
        client_id TEXT,
        message_uuid TEXT,
        creation_time BIGINT,
        message_expiry_interval BIGINT
    )`, t))
	return err
}

func (s *MessageStore) Get(ctx context.Context, topic string) (*stores.BrokerMessage, error) {
	row := s.db.pool.QueryRow(ctx, fmt.Sprintf(`SELECT payload_blob, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval FROM %s WHERE topic = $1`, s.tableName()), topic)
	var (
		payload []byte
		qos     int
		retain  bool
		cid     *string
		uid     *string
		creat   *int64
		expiry  *int64
	)
	if err := row.Scan(&payload, &qos, &retain, &cid, &uid, &creat, &expiry); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	msg := &stores.BrokerMessage{TopicName: topic, Payload: payload, QoS: byte(qos), IsRetain: retain}
	if cid != nil {
		msg.ClientID = *cid
	}
	if uid != nil {
		msg.MessageUUID = *uid
	}
	if creat != nil {
		msg.Time = time.UnixMilli(*creat)
	}
	if expiry != nil {
		v := uint32(*expiry)
		msg.MessageExpiryInterval = &v
	}
	return msg, nil
}

func (s *MessageStore) AddAll(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	t := s.tableName()
	q := fmt.Sprintf(`INSERT INTO %s (topic, time, payload_blob, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (topic) DO UPDATE SET
            time = EXCLUDED.time, payload_blob = EXCLUDED.payload_blob,
            qos = EXCLUDED.qos, retained = EXCLUDED.retained,
            client_id = EXCLUDED.client_id, message_uuid = EXCLUDED.message_uuid,
            creation_time = EXCLUDED.creation_time, message_expiry_interval = EXCLUDED.message_expiry_interval`, t)
	tx, err := s.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		var expiry *int64
		if m.MessageExpiryInterval != nil {
			v := int64(*m.MessageExpiryInterval)
			expiry = &v
		}
		if _, err := tx.Exec(ctx, q, m.TopicName, m.Time.UTC(), m.Payload, int(m.QoS), m.IsRetain, m.ClientID, m.MessageUUID, m.Time.UnixMilli(), expiry); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *MessageStore) DelAll(ctx context.Context, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	_, err := s.db.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE topic = ANY($1)`, s.tableName()), topics)
	return err
}

func (s *MessageStore) FindMatchingMessages(ctx context.Context, pattern string, yield func(stores.BrokerMessage) bool) error {
	rows, err := s.db.pool.Query(ctx, fmt.Sprintf(`SELECT topic, payload_blob, qos, client_id, message_uuid, creation_time, message_expiry_interval FROM %s`, s.tableName()))
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now().UnixMilli()
	for rows.Next() {
		var (
			topic  string
			payload []byte
			qos    int
			cid    *string
			uid    *string
			creat  *int64
			expiry *int64
		)
		if err := rows.Scan(&topic, &payload, &qos, &cid, &uid, &creat, &expiry); err != nil {
			return err
		}
		if !matchTopic(pattern, topic) {
			continue
		}
		if expiry != nil && *expiry >= 0 && creat != nil && (now-*creat)/1000 >= *expiry {
			continue
		}
		msg := stores.BrokerMessage{TopicName: topic, Payload: payload, QoS: byte(qos), IsRetain: true}
		if cid != nil {
			msg.ClientID = *cid
		}
		if uid != nil {
			msg.MessageUUID = *uid
		}
		if creat != nil {
			msg.Time = time.UnixMilli(*creat)
		}
		if !yield(msg) {
			return nil
		}
	}
	return rows.Err()
}

func (s *MessageStore) FindMatchingTopics(ctx context.Context, pattern string, yield func(string) bool) error {
	rows, err := s.db.pool.Query(ctx, fmt.Sprintf(`SELECT topic FROM %s`, s.tableName()))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		if matchTopic(pattern, t) {
			if !yield(t) {
				return nil
			}
		}
	}
	return rows.Err()
}

func (s *MessageStore) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	res, err := s.db.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE time < $1`, s.tableName()), t.UTC())
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	return stores.PurgeResult{DeletedRows: res.RowsAffected()}, nil
}

// MessageArchive -----------------------------------------------------------

type MessageArchive struct {
	name string
	db   *DB
	fmt  stores.PayloadFormat
}

func NewMessageArchive(name string, db *DB, fmt stores.PayloadFormat) *MessageArchive {
	return &MessageArchive{name: name, db: db, fmt: fmt}
}

func (a *MessageArchive) Name() string                    { return a.name }
func (a *MessageArchive) Type() stores.MessageArchiveType { return stores.ArchivePostgres }
func (a *MessageArchive) Close() error                    { return nil }
func (a *MessageArchive) tableName() string               { return strings.ToLower(a.name) }

func (a *MessageArchive) EnsureTable(ctx context.Context) error {
	t := a.tableName()
	for _, q := range []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
            topic TEXT NOT NULL,
            time TIMESTAMPTZ NOT NULL,
            payload_blob BYTEA,
            payload_json JSONB,
            qos INTEGER,
            retained BOOLEAN,
            client_id TEXT,
            message_uuid TEXT,
            PRIMARY KEY (topic, time)
        )`, t),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_time_idx ON %s (time)`, t, t),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_topic_time_idx ON %s (topic, time)`, t, t),
	} {
		if _, err := a.db.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (a *MessageArchive) AddHistory(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	t := a.tableName()
	q := fmt.Sprintf(`INSERT INTO %s (topic, time, payload_blob, qos, retained, client_id, message_uuid)
        VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (topic, time) DO NOTHING`, t)
	tx, err := a.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if _, err := tx.Exec(ctx, q, m.TopicName, m.Time.UTC(), m.Payload, int(m.QoS), m.IsRetain, m.ClientID, m.MessageUUID); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *MessageArchive) GetHistory(ctx context.Context, topic string, from, to *time.Time, limit int) ([]stores.ArchivedMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	pattern := strings.ReplaceAll(strings.ReplaceAll(topic, "#", "%"), "+", "%")
	q := fmt.Sprintf(`SELECT topic, time, payload_blob, qos, client_id FROM %s WHERE topic LIKE $1`, a.tableName())
	args := []any{pattern}
	if from != nil {
		q += fmt.Sprintf(` AND time >= $%d`, len(args)+1)
		args = append(args, from.UTC())
	}
	if to != nil {
		q += fmt.Sprintf(` AND time <= $%d`, len(args)+1)
		args = append(args, to.UTC())
	}
	q += fmt.Sprintf(` ORDER BY time DESC LIMIT $%d`, len(args)+1)
	args = append(args, limit)
	rows, err := a.db.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.ArchivedMessage{}
	for rows.Next() {
		var (
			topic   string
			ts      time.Time
			payload []byte
			qos     int
			cid     *string
		)
		if err := rows.Scan(&topic, &ts, &payload, &qos, &cid); err != nil {
			return nil, err
		}
		am := stores.ArchivedMessage{Topic: topic, Timestamp: ts, Payload: payload, QoS: byte(qos)}
		if cid != nil {
			am.ClientID = *cid
		}
		out = append(out, am)
	}
	return out, rows.Err()
}

func (a *MessageArchive) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	res, err := a.db.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE time < $1`, a.tableName()), t.UTC())
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	return stores.PurgeResult{DeletedRows: res.RowsAffected()}, nil
}

// SessionStore -------------------------------------------------------------

type SessionStore struct{ db *DB }

func (s *SessionStore) Close() error { return nil }
func (s *SessionStore) EnsureTable(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS sessions (
            client_id TEXT PRIMARY KEY,
            node_id TEXT,
            clean_session BOOLEAN,
            connected BOOLEAN,
            update_time TIMESTAMPTZ DEFAULT NOW(),
            information TEXT,
            last_will_topic TEXT,
            last_will_message BYTEA,
            last_will_qos INTEGER,
            last_will_retain BOOLEAN
        )`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
            client_id TEXT,
            topic TEXT,
            qos INTEGER,
            wildcard BOOLEAN,
            no_local INTEGER DEFAULT 0,
            retain_handling INTEGER DEFAULT 0,
            retain_as_published INTEGER DEFAULT 0,
            PRIMARY KEY (client_id, topic)
        )`,
		`CREATE INDEX IF NOT EXISTS subscriptions_topic_idx ON subscriptions (topic)`,
		`CREATE INDEX IF NOT EXISTS subscriptions_wildcard_idx ON subscriptions (wildcard) WHERE wildcard = true`,
	} {
		if _, err := s.db.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *SessionStore) SetClient(ctx context.Context, info stores.SessionInfo) error {
	_, err := s.db.pool.Exec(ctx,
		`INSERT INTO sessions (client_id, node_id, clean_session, connected, update_time, information)
         VALUES ($1, $2, $3, $4, NOW(), $5)
         ON CONFLICT (client_id) DO UPDATE SET
            node_id = EXCLUDED.node_id, clean_session = EXCLUDED.clean_session,
            connected = EXCLUDED.connected, update_time = EXCLUDED.update_time,
            information = EXCLUDED.information`,
		info.ClientID, info.NodeID, info.CleanSession, info.Connected, info.Information)
	return err
}

func (s *SessionStore) SetConnected(ctx context.Context, clientID string, connected bool) error {
	_, err := s.db.pool.Exec(ctx, `UPDATE sessions SET connected=$1, update_time=NOW() WHERE client_id=$2`, connected, clientID)
	return err
}

func (s *SessionStore) SetLastWill(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool) error {
	_, err := s.db.pool.Exec(ctx, `UPDATE sessions SET last_will_topic=$1, last_will_message=$2, last_will_qos=$3, last_will_retain=$4 WHERE client_id=$5`,
		topic, payload, int(qos), retain, clientID)
	return err
}

func (s *SessionStore) IsConnected(ctx context.Context, clientID string) (bool, error) {
	var connected bool
	err := s.db.pool.QueryRow(ctx, `SELECT connected FROM sessions WHERE client_id=$1`, clientID).Scan(&connected)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return connected, err
}

func (s *SessionStore) IsPresent(ctx context.Context, clientID string) (bool, error) {
	var x int
	err := s.db.pool.QueryRow(ctx, `SELECT 1 FROM sessions WHERE client_id=$1`, clientID).Scan(&x)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *SessionStore) GetSession(ctx context.Context, clientID string) (*stores.SessionInfo, error) {
	row := s.db.pool.QueryRow(ctx,
		`SELECT client_id, node_id, clean_session, connected, update_time, information,
                last_will_topic, last_will_message, last_will_qos, last_will_retain
         FROM sessions WHERE client_id=$1`, clientID)
	return scanSession(row)
}

func scanSession(scanner pgx.Row) (*stores.SessionInfo, error) {
	var (
		info      stores.SessionInfo
		nodeID    *string
		clean     *bool
		connected *bool
		updTime   *time.Time
		infoStr   *string
		lwTopic   *string
		lwMsg     []byte
		lwQoS     *int
		lwRetain  *bool
	)
	if err := scanner.Scan(&info.ClientID, &nodeID, &clean, &connected, &updTime, &infoStr, &lwTopic, &lwMsg, &lwQoS, &lwRetain); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if nodeID != nil {
		info.NodeID = *nodeID
	}
	if clean != nil {
		info.CleanSession = *clean
	}
	if connected != nil {
		info.Connected = *connected
	}
	if updTime != nil {
		info.UpdateTime = *updTime
	}
	if infoStr != nil {
		info.Information = *infoStr
	}
	if lwTopic != nil {
		info.LastWillTopic = *lwTopic
	}
	info.LastWillPayload = lwMsg
	if lwQoS != nil {
		info.LastWillQoS = byte(*lwQoS)
	}
	if lwRetain != nil {
		info.LastWillRetain = *lwRetain
	}
	return &info, nil
}

func (s *SessionStore) IterateSessions(ctx context.Context, yield func(stores.SessionInfo) bool) error {
	rows, err := s.db.pool.Query(ctx,
		`SELECT client_id, node_id, clean_session, connected, update_time, information,
                last_will_topic, last_will_message, last_will_qos, last_will_retain FROM sessions`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		info, err := scanSession(rows)
		if err != nil || info == nil {
			return err
		}
		if !yield(*info) {
			return nil
		}
	}
	return rows.Err()
}

func (s *SessionStore) IterateSubscriptions(ctx context.Context, yield func(stores.MqttSubscription) bool) error {
	rows, err := s.db.pool.Query(ctx, `SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM subscriptions`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			sub stores.MqttSubscription
			qos int
			nl  int
			rh  int
			rap int
		)
		if err := rows.Scan(&sub.ClientID, &sub.TopicFilter, &qos, &nl, &rh, &rap); err != nil {
			return err
		}
		sub.QoS = byte(qos)
		sub.NoLocal = nl == 1
		sub.RetainHandling = byte(rh)
		sub.RetainAsPublished = rap == 1
		if !yield(sub) {
			return nil
		}
	}
	return rows.Err()
}

func (s *SessionStore) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]stores.MqttSubscription, error) {
	rows, err := s.db.pool.Query(ctx,
		`SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM subscriptions WHERE client_id=$1`, clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.MqttSubscription{}
	for rows.Next() {
		var (
			sub stores.MqttSubscription
			qos int
			nl  int
			rh  int
			rap int
		)
		if err := rows.Scan(&sub.ClientID, &sub.TopicFilter, &qos, &nl, &rh, &rap); err != nil {
			return nil, err
		}
		sub.QoS = byte(qos)
		sub.NoLocal = nl == 1
		sub.RetainHandling = byte(rh)
		sub.RetainAsPublished = rap == 1
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *SessionStore) AddSubscriptions(ctx context.Context, subs []stores.MqttSubscription) error {
	if len(subs) == 0 {
		return nil
	}
	tx, err := s.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		nl := 0
		if sub.NoLocal {
			nl = 1
		}
		rap := 0
		if sub.RetainAsPublished {
			rap = 1
		}
		wildcard := strings.ContainsAny(sub.TopicFilter, "+#")
		if _, err := tx.Exec(ctx,
			`INSERT INTO subscriptions (client_id, topic, qos, wildcard, no_local, retain_handling, retain_as_published)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (client_id, topic) DO UPDATE SET
                qos = EXCLUDED.qos, wildcard = EXCLUDED.wildcard,
                no_local = EXCLUDED.no_local, retain_handling = EXCLUDED.retain_handling,
                retain_as_published = EXCLUDED.retain_as_published`,
			sub.ClientID, sub.TopicFilter, int(sub.QoS), wildcard, nl, int(sub.RetainHandling), rap); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *SessionStore) DelSubscriptions(ctx context.Context, subs []stores.MqttSubscription) error {
	if len(subs) == 0 {
		return nil
	}
	tx, err := s.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		if _, err := tx.Exec(ctx, `DELETE FROM subscriptions WHERE client_id=$1 AND topic=$2`, sub.ClientID, sub.TopicFilter); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *SessionStore) DelClient(ctx context.Context, clientID string) error {
	tx, err := s.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM subscriptions WHERE client_id=$1`, clientID); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM sessions WHERE client_id=$1`, clientID); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (s *SessionStore) PurgeSessions(ctx context.Context) error {
	_, err := s.db.pool.Exec(ctx, `DELETE FROM sessions WHERE clean_session = TRUE AND connected = FALSE`)
	return err
}

// QueueStore --------------------------------------------------------------

type QueueStore struct {
	db         *DB
	visibility time.Duration
}

func (q *QueueStore) Close() error { return nil }
func (q *QueueStore) EnsureTable(ctx context.Context) error {
	for _, s := range []string{
		`CREATE TABLE IF NOT EXISTS messagequeue (
            msg_id BIGSERIAL PRIMARY KEY,
            message_uuid TEXT NOT NULL,
            client_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            payload BYTEA,
            qos INTEGER NOT NULL,
            retained INTEGER NOT NULL DEFAULT 0,
            publisher_id TEXT,
            creation_time BIGINT NOT NULL,
            message_expiry_interval BIGINT,
            vt BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
            read_ct INTEGER NOT NULL DEFAULT 0
        )`,
		`CREATE INDEX IF NOT EXISTS messagequeue_fetch_idx ON messagequeue (client_id, vt)`,
		`CREATE INDEX IF NOT EXISTS messagequeue_client_uuid_idx ON messagequeue (client_id, message_uuid)`,
	} {
		if _, err := q.db.pool.Exec(ctx, s); err != nil {
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
	tx, err := q.db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	for _, cid := range clientIDs {
		retained := 0
		if msg.IsRetain {
			retained = 1
		}
		var expiry *int64
		if msg.MessageExpiryInterval != nil {
			v := int64(*msg.MessageExpiryInterval)
			expiry = &v
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO messagequeue (message_uuid, client_id, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			msg.MessageUUID, cid, msg.TopicName, msg.Payload, int(msg.QoS), retained, msg.ClientID, msg.Time.UnixMilli(), expiry); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

func (q *QueueStore) Dequeue(ctx context.Context, clientID string, batchSize int) ([]stores.BrokerMessage, error) {
	if batchSize <= 0 {
		batchSize = 10
	}
	now := time.Now().Unix()
	newVT := now + int64(q.visibility.Seconds())
	rows, err := q.db.pool.Query(ctx,
		`UPDATE messagequeue SET vt=$3, read_ct = read_ct+1
         WHERE msg_id IN (SELECT msg_id FROM messagequeue WHERE client_id=$1 AND vt<=$2 ORDER BY msg_id LIMIT $4 FOR UPDATE SKIP LOCKED)
         RETURNING message_uuid, topic, payload, qos, retained, publisher_id, creation_time, message_expiry_interval`,
		clientID, now, newVT, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.BrokerMessage{}
	for rows.Next() {
		var (
			msg      stores.BrokerMessage
			retained int
			pubID    *string
			creat    int64
			expiry   *int64
			qos      int
		)
		if err := rows.Scan(&msg.MessageUUID, &msg.TopicName, &msg.Payload, &qos, &retained, &pubID, &creat, &expiry); err != nil {
			return nil, err
		}
		msg.QoS = byte(qos)
		msg.IsRetain = retained == 1
		msg.IsQueued = true
		if pubID != nil {
			msg.ClientID = *pubID
		}
		msg.Time = time.UnixMilli(creat)
		if expiry != nil {
			v := uint32(*expiry)
			msg.MessageExpiryInterval = &v
		}
		out = append(out, msg)
	}
	return out, rows.Err()
}

func (q *QueueStore) Ack(ctx context.Context, clientID, messageUUID string) error {
	_, err := q.db.pool.Exec(ctx, `DELETE FROM messagequeue WHERE client_id=$1 AND message_uuid=$2`, clientID, messageUUID)
	return err
}
func (q *QueueStore) PurgeForClient(ctx context.Context, clientID string) (int64, error) {
	res, err := q.db.pool.Exec(ctx, `DELETE FROM messagequeue WHERE client_id=$1`, clientID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}
func (q *QueueStore) Count(ctx context.Context, clientID string) (int64, error) {
	var n int64
	err := q.db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM messagequeue WHERE client_id=$1`, clientID).Scan(&n)
	return n, err
}
func (q *QueueStore) CountAll(ctx context.Context) (int64, error) {
	var n int64
	err := q.db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM messagequeue`).Scan(&n)
	return n, err
}

// UserStore ---------------------------------------------------------------

type UserStore struct{ db *DB }

func (u *UserStore) Close() error { return nil }
func (u *UserStore) EnsureTable(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            can_subscribe BOOLEAN DEFAULT TRUE,
            can_publish BOOLEAN DEFAULT TRUE,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )`,
		`CREATE TABLE IF NOT EXISTS usersacl (
            id BIGSERIAL PRIMARY KEY,
            username TEXT REFERENCES users(username) ON DELETE CASCADE,
            topic_pattern TEXT NOT NULL,
            can_subscribe BOOLEAN DEFAULT FALSE,
            can_publish BOOLEAN DEFAULT FALSE,
            priority INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )`,
		`CREATE INDEX IF NOT EXISTS usersacl_username_idx ON usersacl (username)`,
		`CREATE INDEX IF NOT EXISTS usersacl_priority_idx ON usersacl (priority)`,
	} {
		if _, err := u.db.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (u *UserStore) CreateUser(ctx context.Context, user stores.User) error {
	_, err := u.db.pool.Exec(ctx,
		`INSERT INTO users (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES ($1, $2, $3, $4, $5, $6)`,
		user.Username, user.PasswordHash, user.Enabled, user.CanSubscribe, user.CanPublish, user.IsAdmin)
	return err
}
func (u *UserStore) UpdateUser(ctx context.Context, user stores.User) error {
	_, err := u.db.pool.Exec(ctx,
		`UPDATE users SET password_hash=$1, enabled=$2, can_subscribe=$3, can_publish=$4, is_admin=$5, updated_at=NOW() WHERE username=$6`,
		user.PasswordHash, user.Enabled, user.CanSubscribe, user.CanPublish, user.IsAdmin, user.Username)
	return err
}
func (u *UserStore) DeleteUser(ctx context.Context, username string) error {
	_, err := u.db.pool.Exec(ctx, `DELETE FROM users WHERE username=$1`, username)
	return err
}
func (u *UserStore) GetUser(ctx context.Context, username string) (*stores.User, error) {
	row := u.db.pool.QueryRow(ctx,
		`SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM users WHERE username=$1`, username)
	var (
		user                                              stores.User
		enabled, canSub, canPub, admin                    bool
		createdAt, updatedAt                              time.Time
	)
	if err := row.Scan(&user.Username, &user.PasswordHash, &enabled, &canSub, &canPub, &admin, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	user.Enabled, user.CanSubscribe, user.CanPublish, user.IsAdmin = enabled, canSub, canPub, admin
	user.CreatedAt, user.UpdatedAt = createdAt, updatedAt
	return &user, nil
}
func (u *UserStore) GetAllUsers(ctx context.Context) ([]stores.User, error) {
	rows, err := u.db.pool.Query(ctx,
		`SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM users ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.User{}
	for rows.Next() {
		var (
			user                                              stores.User
			enabled, canSub, canPub, admin                    bool
			createdAt, updatedAt                              time.Time
		)
		if err := rows.Scan(&user.Username, &user.PasswordHash, &enabled, &canSub, &canPub, &admin, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		user.Enabled, user.CanSubscribe, user.CanPublish, user.IsAdmin = enabled, canSub, canPub, admin
		user.CreatedAt, user.UpdatedAt = createdAt, updatedAt
		out = append(out, user)
	}
	return out, rows.Err()
}
func (u *UserStore) ValidateCredentials(ctx context.Context, username, password string) (*stores.User, error) {
	user, err := u.GetUser(ctx, username)
	if err != nil || user == nil || !user.Enabled {
		return nil, err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, nil
	}
	return user, nil
}
func (u *UserStore) CreateAclRule(ctx context.Context, r stores.AclRule) error {
	_, err := u.db.pool.Exec(ctx,
		`INSERT INTO usersacl (username, topic_pattern, can_subscribe, can_publish, priority) VALUES ($1, $2, $3, $4, $5)`,
		r.Username, r.TopicPattern, r.CanSubscribe, r.CanPublish, r.Priority)
	return err
}
func (u *UserStore) UpdateAclRule(ctx context.Context, r stores.AclRule) error {
	_, err := u.db.pool.Exec(ctx,
		`UPDATE usersacl SET username=$1, topic_pattern=$2, can_subscribe=$3, can_publish=$4, priority=$5 WHERE id=$6`,
		r.Username, r.TopicPattern, r.CanSubscribe, r.CanPublish, r.Priority, r.ID)
	return err
}
func (u *UserStore) DeleteAclRule(ctx context.Context, id string) error {
	_, err := u.db.pool.Exec(ctx, `DELETE FROM usersacl WHERE id=$1`, id)
	return err
}
func (u *UserStore) GetUserAclRules(ctx context.Context, username string) ([]stores.AclRule, error) {
	return u.queryAcl(ctx, `SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM usersacl WHERE username=$1 ORDER BY priority DESC`, username)
}
func (u *UserStore) GetAllAclRules(ctx context.Context) ([]stores.AclRule, error) {
	return u.queryAcl(ctx, `SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM usersacl ORDER BY username, priority DESC`)
}
func (u *UserStore) queryAcl(ctx context.Context, q string, args ...any) ([]stores.AclRule, error) {
	rows, err := u.db.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.AclRule{}
	for rows.Next() {
		var (
			r       stores.AclRule
			id      int64
			canSub  bool
			canPub  bool
			created time.Time
		)
		if err := rows.Scan(&id, &r.Username, &r.TopicPattern, &canSub, &canPub, &r.Priority, &created); err != nil {
			return nil, err
		}
		r.ID = fmt.Sprintf("%d", id)
		r.CanSubscribe, r.CanPublish, r.CreatedAt = canSub, canPub, created
		out = append(out, r)
	}
	return out, rows.Err()
}
func (u *UserStore) LoadAll(ctx context.Context) ([]stores.User, []stores.AclRule, error) {
	users, err := u.GetAllUsers(ctx)
	if err != nil {
		return nil, nil, err
	}
	rules, err := u.GetAllAclRules(ctx)
	if err != nil {
		return nil, nil, err
	}
	return users, rules, nil
}

// ArchiveConfigStore ------------------------------------------------------

type ArchiveConfigStore struct{ db *DB }

func (a *ArchiveConfigStore) Close() error { return nil }
func (a *ArchiveConfigStore) EnsureTable(ctx context.Context) error {
	_, err := a.db.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS archiveconfigs (
        name TEXT PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0,
        topic_filter TEXT NOT NULL,
        retained_only INTEGER NOT NULL DEFAULT 0,
        last_val_type TEXT NOT NULL,
        archive_type TEXT NOT NULL,
        last_val_retention TEXT,
        archive_retention TEXT,
        purge_interval TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        payload_format TEXT DEFAULT 'DEFAULT'
    )`)
	return err
}
func (a *ArchiveConfigStore) GetAll(ctx context.Context) ([]stores.ArchiveGroupConfig, error) {
	rows, err := a.db.pool.Query(ctx,
		`SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format FROM archiveconfigs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.ArchiveGroupConfig{}
	for rows.Next() {
		c, err := scanArchiveCfg(rows)
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
	row := a.db.pool.QueryRow(ctx,
		`SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format FROM archiveconfigs WHERE name=$1`, name)
	return scanArchiveCfg(row)
}
func scanArchiveCfg(scanner pgx.Row) (*stores.ArchiveGroupConfig, error) {
	var (
		cfg                                         stores.ArchiveGroupConfig
		enabled, retainedOnly                       int
		topicFilter, lvType, arType                 string
		lvRet, arRet, purgeInt, payloadFormat       *string
	)
	if err := scanner.Scan(&cfg.Name, &enabled, &topicFilter, &retainedOnly, &lvType, &arType, &lvRet, &arRet, &purgeInt, &payloadFormat); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	cfg.Enabled = enabled == 1
	cfg.RetainedOnly = retainedOnly == 1
	cfg.TopicFilters = strings.Split(topicFilter, ",")
	cfg.LastValType = stores.MessageStoreType(lvType)
	cfg.ArchiveType = stores.MessageArchiveType(arType)
	if lvRet != nil {
		cfg.LastValRetention = *lvRet
	}
	if arRet != nil {
		cfg.ArchiveRetention = *arRet
	}
	if purgeInt != nil {
		cfg.PurgeInterval = *purgeInt
	}
	cfg.PayloadFormat = stores.PayloadDefault
	if payloadFormat != nil {
		cfg.PayloadFormat = stores.PayloadFormat(*payloadFormat)
	}
	return &cfg, nil
}
func (a *ArchiveConfigStore) Save(ctx context.Context, cfg stores.ArchiveGroupConfig) error {
	enabled := 0
	if cfg.Enabled {
		enabled = 1
	}
	retainedOnly := 0
	if cfg.RetainedOnly {
		retainedOnly = 1
	}
	_, err := a.db.pool.Exec(ctx, `INSERT INTO archiveconfigs (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (name) DO UPDATE SET
            enabled=EXCLUDED.enabled, topic_filter=EXCLUDED.topic_filter, retained_only=EXCLUDED.retained_only,
            last_val_type=EXCLUDED.last_val_type, archive_type=EXCLUDED.archive_type,
            last_val_retention=EXCLUDED.last_val_retention, archive_retention=EXCLUDED.archive_retention,
            purge_interval=EXCLUDED.purge_interval, payload_format=EXCLUDED.payload_format,
            updated_at=NOW()`,
		cfg.Name, enabled, strings.Join(cfg.TopicFilters, ","), retainedOnly,
		string(cfg.LastValType), string(cfg.ArchiveType),
		nullStr(cfg.LastValRetention), nullStr(cfg.ArchiveRetention), nullStr(cfg.PurgeInterval),
		string(cfg.PayloadFormat))
	return err
}
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func (a *ArchiveConfigStore) Update(ctx context.Context, cfg stores.ArchiveGroupConfig) error { return a.Save(ctx, cfg) }
func (a *ArchiveConfigStore) Delete(ctx context.Context, name string) error {
	_, err := a.db.pool.Exec(ctx, `DELETE FROM archiveconfigs WHERE name=$1`, name)
	return err
}

// DeviceConfigStore -------------------------------------------------------

type DeviceConfigStore struct{ db *DB }

func (d *DeviceConfigStore) Close() error { return nil }
func (d *DeviceConfigStore) EnsureTable(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS deviceconfigs (
            name TEXT PRIMARY KEY,
            namespace TEXT NOT NULL,
            node_id TEXT NOT NULL,
            config TEXT NOT NULL,
            enabled INTEGER DEFAULT 1,
            type TEXT DEFAULT 'MQTT_CLIENT',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_node_id ON deviceconfigs (node_id)`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_enabled ON deviceconfigs (enabled)`,
		`CREATE INDEX IF NOT EXISTS idx_deviceconfigs_namespace ON deviceconfigs (namespace)`,
	} {
		if _, err := d.db.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
func (d *DeviceConfigStore) GetAll(ctx context.Context) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs ORDER BY name`)
}
func (d *DeviceConfigStore) GetByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE node_id=$1 ORDER BY name`, nodeID)
}
func (d *DeviceConfigStore) GetEnabledByNode(ctx context.Context, nodeID string) ([]stores.DeviceConfig, error) {
	return d.query(ctx, `SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE node_id=$1 AND enabled=1 ORDER BY name`, nodeID)
}
func (d *DeviceConfigStore) query(ctx context.Context, q string, args ...any) ([]stores.DeviceConfig, error) {
	rows, err := d.db.pool.Query(ctx, q, args...)
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
func scanDevice(scanner pgx.Row) (*stores.DeviceConfig, error) {
	var (
		dc        stores.DeviceConfig
		enabled   int
		createdAt time.Time
		updatedAt time.Time
	)
	if err := scanner.Scan(&dc.Name, &dc.Namespace, &dc.NodeID, &dc.Config, &enabled, &dc.Type, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	dc.Enabled = enabled == 1
	dc.CreatedAt, dc.UpdatedAt = createdAt, updatedAt
	return &dc, nil
}
func (d *DeviceConfigStore) Get(ctx context.Context, name string) (*stores.DeviceConfig, error) {
	row := d.db.pool.QueryRow(ctx,
		`SELECT name, namespace, node_id, config, enabled, type, created_at, updated_at FROM deviceconfigs WHERE name=$1`, name)
	return scanDevice(row)
}
func (d *DeviceConfigStore) Save(ctx context.Context, dc stores.DeviceConfig) error {
	enabled := 0
	if dc.Enabled {
		enabled = 1
	}
	_, err := d.db.pool.Exec(ctx, `INSERT INTO deviceconfigs (name, namespace, node_id, config, enabled, type)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (name) DO UPDATE SET
            namespace=EXCLUDED.namespace, node_id=EXCLUDED.node_id, config=EXCLUDED.config,
            enabled=EXCLUDED.enabled, type=EXCLUDED.type, updated_at=NOW()`,
		dc.Name, dc.Namespace, dc.NodeID, dc.Config, enabled, dc.Type)
	return err
}
func (d *DeviceConfigStore) Delete(ctx context.Context, name string) error {
	_, err := d.db.pool.Exec(ctx, `DELETE FROM deviceconfigs WHERE name=$1`, name)
	return err
}
func (d *DeviceConfigStore) Toggle(ctx context.Context, name string, enabled bool) (*stores.DeviceConfig, error) {
	v := 0
	if enabled {
		v = 1
	}
	if _, err := d.db.pool.Exec(ctx, `UPDATE deviceconfigs SET enabled=$1, updated_at=NOW() WHERE name=$2`, v, name); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}
func (d *DeviceConfigStore) Reassign(ctx context.Context, name, nodeID string) (*stores.DeviceConfig, error) {
	if _, err := d.db.pool.Exec(ctx, `UPDATE deviceconfigs SET node_id=$1, updated_at=NOW() WHERE name=$2`, nodeID, name); err != nil {
		return nil, err
	}
	return d.Get(ctx, name)
}

// MetricsStore ------------------------------------------------------------

type MetricsStore struct{ db *DB }

func (m *MetricsStore) Close() error { return nil }
func (m *MetricsStore) EnsureTable(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS metrics (
            timestamp TIMESTAMPTZ NOT NULL,
            metric_type TEXT NOT NULL,
            identifier TEXT NOT NULL,
            metrics JSONB NOT NULL,
            PRIMARY KEY (timestamp, metric_type, identifier)
        )`,
		`CREATE INDEX IF NOT EXISTS metrics_timestamp_idx ON metrics (timestamp)`,
		`CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx ON metrics (metric_type, identifier, timestamp)`,
	} {
		if _, err := m.db.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
func (m *MetricsStore) StoreMetrics(ctx context.Context, kind stores.MetricKind, ts time.Time, identifier, payload string) error {
	_, err := m.db.pool.Exec(ctx,
		`INSERT INTO metrics (timestamp, metric_type, identifier, metrics) VALUES ($1, $2, $3, $4::jsonb)
         ON CONFLICT (timestamp, metric_type, identifier) DO UPDATE SET metrics = EXCLUDED.metrics`,
		ts.UTC(), string(kind), identifier, payload)
	return err
}
func (m *MetricsStore) GetLatest(ctx context.Context, kind stores.MetricKind, identifier string) (time.Time, string, error) {
	row := m.db.pool.QueryRow(ctx,
		`SELECT timestamp, metrics::text FROM metrics WHERE metric_type=$1 AND identifier=$2 ORDER BY timestamp DESC LIMIT 1`,
		string(kind), identifier)
	var (
		ts      time.Time
		payload string
	)
	if err := row.Scan(&ts, &payload); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, "", nil
		}
		return time.Time{}, "", err
	}
	return ts, payload, nil
}
func (m *MetricsStore) GetHistory(ctx context.Context, kind stores.MetricKind, identifier string, from, to time.Time, limit int) ([]stores.MetricsRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := m.db.pool.Query(ctx,
		`SELECT timestamp, metrics::text FROM metrics WHERE metric_type=$1 AND identifier=$2 AND timestamp >= $3 AND timestamp <= $4 ORDER BY timestamp ASC LIMIT $5`,
		string(kind), identifier, from.UTC(), to.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.MetricsRow{}
	for rows.Next() {
		var (
			ts      time.Time
			payload string
		)
		if err := rows.Scan(&ts, &payload); err != nil {
			return nil, err
		}
		out = append(out, stores.MetricsRow{Timestamp: ts, Payload: payload})
	}
	return out, rows.Err()
}
func (m *MetricsStore) PurgeOlderThan(ctx context.Context, t time.Time) (int64, error) {
	res, err := m.db.pool.Exec(ctx, `DELETE FROM metrics WHERE timestamp < $1`, t.UTC())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

// helpers ----------------------------------------------------------------

func matchTopic(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tt := strings.Split(topic, "/")
	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tt) {
			return false
		}
		if p == "+" {
			continue
		}
		if p != tt[i] {
			return false
		}
	}
	return len(pp) == len(tt)
}

// Compile-time assertions ------------------------------------------------

var (
	_ stores.MessageStore       = (*MessageStore)(nil)
	_ stores.MessageArchive     = (*MessageArchive)(nil)
	_ stores.SessionStore       = (*SessionStore)(nil)
	_ stores.QueueStore         = (*QueueStore)(nil)
	_ stores.UserStore          = (*UserStore)(nil)
	_ stores.ArchiveConfigStore = (*ArchiveConfigStore)(nil)
	_ stores.DeviceConfigStore  = (*DeviceConfigStore)(nil)
	_ stores.MetricsStore       = (*MetricsStore)(nil)
)

// keep encoding/json + uuid used to silence unused-import linters in case of conditional code paths
var _ = json.Marshal
var _ = uuid.New
