package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"monstermq.io/edge/internal/stores"
)

// MessageStore is the byte-compatible Go port of MessageStoreSQLite.kt.
// Schema matches: topic PK, topic_1..topic_9, topic_r (JSON), topic_l, time,
// payload_blob, payload_json, qos, retained, client_id, message_uuid,
// creation_time, message_expiry_interval.
type MessageStore struct {
	name      string
	tableName string
	db        *DB
}

func NewMessageStore(name string, db *DB) *MessageStore {
	return &MessageStore{
		name:      name,
		tableName: strings.ToLower(name),
		db:        db,
	}
}

func (s *MessageStore) Name() string                    { return s.name }
func (s *MessageStore) Type() stores.MessageStoreType   { return stores.MessageStoreSQLite }
func (s *MessageStore) Close() error                    { return nil }

func (s *MessageStore) EnsureTable(ctx context.Context) error {
	cols := make([]string, 0, MaxFixedTopicLevels)
	for i := 1; i <= MaxFixedTopicLevels; i++ {
		cols = append(cols, fmt.Sprintf("topic_%d TEXT NOT NULL DEFAULT ''", i))
	}
	idxCols := make([]string, 0, MaxFixedTopicLevels+1)
	for i := 1; i <= MaxFixedTopicLevels; i++ {
		idxCols = append(idxCols, fmt.Sprintf("topic_%d", i))
	}
	idxCols = append(idxCols, "topic_r")

	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
            topic TEXT PRIMARY KEY,
            %s,
            topic_r TEXT,
            topic_l TEXT NOT NULL,
            time TEXT,
            payload_blob BLOB,
            payload_json TEXT,
            qos INTEGER,
            retained BOOLEAN,
            client_id TEXT,
            message_uuid TEXT,
            creation_time INTEGER,
            message_expiry_interval INTEGER
        )`, s.tableName, strings.Join(cols, ", ")),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_topic ON %s (%s)",
			s.tableName, s.tableName, strings.Join(idxCols, ", ")),
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("create %s: %w", s.tableName, err)
		}
	}
	return nil
}

func (s *MessageStore) Get(ctx context.Context, topic string) (*stores.BrokerMessage, error) {
	q := fmt.Sprintf(`SELECT payload_blob, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval
                      FROM %s WHERE topic = ?`, s.tableName)
	row := s.db.Conn().QueryRowContext(ctx, q, topic)
	var (
		payload     []byte
		qos         int
		retained    bool
		clientID    sql.NullString
		messageUUID sql.NullString
		creation    sql.NullInt64
		expiry      sql.NullInt64
	)
	if err := row.Scan(&payload, &qos, &retained, &clientID, &messageUUID, &creation, &expiry); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	msg := &stores.BrokerMessage{
		MessageUUID: messageUUID.String,
		TopicName:   topic,
		Payload:     payload,
		QoS:         byte(qos),
		IsRetain:    retained,
		ClientID:    clientID.String,
		Time:        time.UnixMilli(creation.Int64),
	}
	if expiry.Valid {
		v := uint32(expiry.Int64)
		msg.MessageExpiryInterval = &v
	}
	return msg, nil
}

func (s *MessageStore) AddAll(ctx context.Context, msgs []stores.BrokerMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	cols := make([]string, 0, MaxFixedTopicLevels)
	for i := 1; i <= MaxFixedTopicLevels; i++ {
		cols = append(cols, fmt.Sprintf("topic_%d", i))
	}
	placeholders := strings.Repeat("?, ", MaxFixedTopicLevels)
	placeholders = strings.TrimRight(placeholders, ", ")
	q := fmt.Sprintf(`INSERT INTO %s (topic, %s, topic_r, topic_l, time, payload_blob, payload_json, qos, retained, client_id, message_uuid, creation_time, message_expiry_interval)
        VALUES (?, %s, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (topic) DO UPDATE SET
            time = excluded.time,
            payload_blob = excluded.payload_blob,
            payload_json = excluded.payload_json,
            qos = excluded.qos,
            retained = excluded.retained,
            client_id = excluded.client_id,
            message_uuid = excluded.message_uuid,
            creation_time = excluded.creation_time,
            message_expiry_interval = excluded.message_expiry_interval`,
		s.tableName, strings.Join(cols, ", "), placeholders)

	s.db.Lock()
	defer s.db.Unlock()
	tx, err := s.db.Conn().BeginTx(ctx, nil)
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
		fixed, restJSON, last := SplitTopic(m.TopicName)
		args := make([]any, 0, 1+MaxFixedTopicLevels+11)
		args = append(args, m.TopicName)
		for i := 0; i < MaxFixedTopicLevels; i++ {
			args = append(args, fixed[i])
		}
		args = append(args,
			restJSON,
			last,
			m.Time.UTC().Format(time.RFC3339Nano),
			m.Payload,
			nil, // payload_json — populated only for JSON-format archives
			int(m.QoS),
			m.IsRetain,
			m.ClientID,
			m.MessageUUID,
			m.Time.UnixMilli(),
			m.MessageExpiryInterval,
		)
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *MessageStore) DelAll(ctx context.Context, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE topic = ?", s.tableName)
	s.db.Lock()
	defer s.db.Unlock()
	tx, err := s.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, t := range topics {
		if _, err := stmt.ExecContext(ctx, t); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// FindMatchingMessages walks all retained rows and yields those matching the pattern.
// We do MQTT-style matching in Go to keep the SQL portable; performance can be tuned
// later by leveraging the topic_1..topic_9 columns for prefix prefiltering.
func (s *MessageStore) FindMatchingMessages(ctx context.Context, pattern string, yield func(stores.BrokerMessage) bool) error {
	q := fmt.Sprintf(`SELECT topic, payload_blob, qos, client_id, message_uuid, creation_time, message_expiry_interval
                      FROM %s`, s.tableName)
	rows, err := s.db.Conn().QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now().UnixMilli()
	for rows.Next() {
		var (
			topic       string
			payload     []byte
			qos         int
			clientID    sql.NullString
			messageUUID sql.NullString
			creation    sql.NullInt64
			expiry      sql.NullInt64
		)
		if err := rows.Scan(&topic, &payload, &qos, &clientID, &messageUUID, &creation, &expiry); err != nil {
			return err
		}
		if !MatchTopic(pattern, topic) {
			continue
		}
		// Drop expired messages (MQTT v5 message_expiry_interval, in seconds).
		if expiry.Valid && expiry.Int64 >= 0 {
			ageSec := (now - creation.Int64) / 1000
			if ageSec >= expiry.Int64 {
				continue
			}
		}
		msg := stores.BrokerMessage{
			MessageUUID: messageUUID.String,
			TopicName:   topic,
			Payload:     payload,
			QoS:         byte(qos),
			IsRetain:    true,
			ClientID:    clientID.String,
			Time:        time.UnixMilli(creation.Int64),
		}
		if expiry.Valid {
			v := uint32(expiry.Int64)
			msg.MessageExpiryInterval = &v
		}
		if !yield(msg) {
			return nil
		}
	}
	return rows.Err()
}

func (s *MessageStore) FindMatchingTopics(ctx context.Context, pattern string, yield func(string) bool) error {
	q := fmt.Sprintf("SELECT topic FROM %s", s.tableName)
	rows, err := s.db.Conn().QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		if MatchTopic(pattern, t) {
			if !yield(t) {
				return nil
			}
		}
	}
	return rows.Err()
}

func (s *MessageStore) PurgeOlderThan(ctx context.Context, t time.Time) (stores.PurgeResult, error) {
	q := fmt.Sprintf("DELETE FROM %s WHERE time < ?", s.tableName)
	res, err := s.db.Exec(q, t.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return stores.PurgeResult{Err: err}, err
	}
	n, _ := res.RowsAffected()
	return stores.PurgeResult{DeletedRows: n}, nil
}
