package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"monstermq.io/edge/internal/stores"
)

const (
	sessionsTable      = "sessions"
	subscriptionsTable = "subscriptions"
)

type SessionStore struct {
	db *DB
}

func NewSessionStore(db *DB) *SessionStore { return &SessionStore{db: db} }

func (s *SessionStore) Close() error { return nil }

func (s *SessionStore) EnsureTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + sessionsTable + ` (
            client_id TEXT PRIMARY KEY,
            node_id TEXT,
            clean_session BOOLEAN,
            connected BOOLEAN,
            update_time TEXT DEFAULT CURRENT_TIMESTAMP,
            information TEXT,
            last_will_topic TEXT,
            last_will_message BLOB,
            last_will_qos INTEGER,
            last_will_retain BOOLEAN
        )`,
		`CREATE TABLE IF NOT EXISTS ` + subscriptionsTable + ` (
            client_id TEXT,
            topic TEXT,
            qos INTEGER,
            wildcard BOOLEAN,
            no_local INTEGER DEFAULT 0,
            retain_handling INTEGER DEFAULT 0,
            retain_as_published INTEGER DEFAULT 0,
            PRIMARY KEY (client_id, topic)
        )`,
		`CREATE INDEX IF NOT EXISTS ` + subscriptionsTable + `_topic_idx ON ` + subscriptionsTable + ` (topic)`,
		`CREATE INDEX IF NOT EXISTS ` + subscriptionsTable + `_wildcard_idx ON ` + subscriptionsTable + ` (wildcard) WHERE wildcard = 1`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (s *SessionStore) SetClient(ctx context.Context, info stores.SessionInfo) error {
	q := `INSERT INTO ` + sessionsTable + ` (client_id, node_id, clean_session, connected, update_time, information)
          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
          ON CONFLICT (client_id) DO UPDATE SET
            node_id = excluded.node_id,
            clean_session = excluded.clean_session,
            connected = excluded.connected,
            update_time = excluded.update_time,
            information = excluded.information`
	_, err := s.db.Exec(q, info.ClientID, info.NodeID, info.CleanSession, info.Connected, info.Information)
	return err
}

func (s *SessionStore) SetConnected(ctx context.Context, clientID string, connected bool) error {
	_, err := s.db.Exec(`UPDATE `+sessionsTable+` SET connected = ?, update_time = CURRENT_TIMESTAMP WHERE client_id = ?`, connected, clientID)
	return err
}

func (s *SessionStore) SetLastWill(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool) error {
	_, err := s.db.Exec(`UPDATE `+sessionsTable+` SET last_will_topic = ?, last_will_message = ?, last_will_qos = ?, last_will_retain = ? WHERE client_id = ?`,
		topic, payload, int(qos), retain, clientID)
	return err
}

func (s *SessionStore) IsConnected(ctx context.Context, clientID string) (bool, error) {
	row := s.db.Conn().QueryRowContext(ctx, `SELECT connected FROM `+sessionsTable+` WHERE client_id = ?`, clientID)
	var c bool
	if err := row.Scan(&c); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return c, nil
}

func (s *SessionStore) IsPresent(ctx context.Context, clientID string) (bool, error) {
	row := s.db.Conn().QueryRowContext(ctx, `SELECT 1 FROM `+sessionsTable+` WHERE client_id = ?`, clientID)
	var x int
	if err := row.Scan(&x); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *SessionStore) GetSession(ctx context.Context, clientID string) (*stores.SessionInfo, error) {
	row := s.db.Conn().QueryRowContext(ctx,
		`SELECT client_id, node_id, clean_session, connected, update_time, information,
                last_will_topic, last_will_message, last_will_qos, last_will_retain
         FROM `+sessionsTable+` WHERE client_id = ?`, clientID)
	return scanSession(row)
}

func scanSession(scanner interface{ Scan(...any) error }) (*stores.SessionInfo, error) {
	var (
		info        stores.SessionInfo
		nodeID      sql.NullString
		clean       sql.NullBool
		connected   sql.NullBool
		updateTime  sql.NullString
		information sql.NullString
		lwTopic     sql.NullString
		lwMessage   []byte
		lwQoS       sql.NullInt64
		lwRetain    sql.NullBool
	)
	if err := scanner.Scan(&info.ClientID, &nodeID, &clean, &connected, &updateTime, &information,
		&lwTopic, &lwMessage, &lwQoS, &lwRetain); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	info.NodeID = nodeID.String
	info.CleanSession = clean.Bool
	info.Connected = connected.Bool
	info.Information = information.String
	if updateTime.Valid {
		if t, err := time.Parse(time.RFC3339Nano, updateTime.String); err == nil {
			info.UpdateTime = t
		} else if t, err := time.Parse("2006-01-02 15:04:05", strings.ReplaceAll(updateTime.String, "T", " ")); err == nil {
			info.UpdateTime = t
		}
	}
	info.LastWillTopic = lwTopic.String
	info.LastWillPayload = lwMessage
	if lwQoS.Valid {
		info.LastWillQoS = byte(lwQoS.Int64)
	}
	info.LastWillRetain = lwRetain.Bool
	return &info, nil
}

func (s *SessionStore) IterateSessions(ctx context.Context, yield func(stores.SessionInfo) bool) error {
	rows, err := s.db.Conn().QueryContext(ctx,
		`SELECT client_id, node_id, clean_session, connected, update_time, information,
                last_will_topic, last_will_message, last_will_qos, last_will_retain
         FROM `+sessionsTable)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		info, err := scanSession(rows)
		if err != nil {
			return err
		}
		if info == nil {
			continue
		}
		if !yield(*info) {
			return nil
		}
	}
	return rows.Err()
}

func (s *SessionStore) IterateSubscriptions(ctx context.Context, yield func(stores.MqttSubscription) bool) error {
	rows, err := s.db.Conn().QueryContext(ctx,
		`SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM `+subscriptionsTable)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			sub      stores.MqttSubscription
			qos      int
			noLocal  int
			rh       int
			rap      int
		)
		if err := rows.Scan(&sub.ClientID, &sub.TopicFilter, &qos, &noLocal, &rh, &rap); err != nil {
			return err
		}
		sub.QoS = byte(qos)
		sub.NoLocal = noLocal == 1
		sub.RetainHandling = byte(rh)
		sub.RetainAsPublished = rap == 1
		if !yield(sub) {
			return nil
		}
	}
	return rows.Err()
}

func (s *SessionStore) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]stores.MqttSubscription, error) {
	rows, err := s.db.Conn().QueryContext(ctx,
		`SELECT client_id, topic, qos, no_local, retain_handling, retain_as_published FROM `+subscriptionsTable+` WHERE client_id = ?`, clientID)
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
	q := `INSERT INTO ` + subscriptionsTable + ` (client_id, topic, qos, wildcard, no_local, retain_handling, retain_as_published)
          VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (client_id, topic) DO UPDATE SET
            qos = excluded.qos,
            wildcard = excluded.wildcard,
            no_local = excluded.no_local,
            retain_handling = excluded.retain_handling,
            retain_as_published = excluded.retain_as_published`
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
	for _, sub := range subs {
		wildcard := strings.ContainsAny(sub.TopicFilter, "+#")
		nl := 0
		if sub.NoLocal {
			nl = 1
		}
		rap := 0
		if sub.RetainAsPublished {
			rap = 1
		}
		if _, err := stmt.ExecContext(ctx, sub.ClientID, sub.TopicFilter, int(sub.QoS), wildcard, nl, int(sub.RetainHandling), rap); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *SessionStore) DelSubscriptions(ctx context.Context, subs []stores.MqttSubscription) error {
	if len(subs) == 0 {
		return nil
	}
	q := `DELETE FROM ` + subscriptionsTable + ` WHERE client_id = ? AND topic = ?`
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
	for _, sub := range subs {
		if _, err := stmt.ExecContext(ctx, sub.ClientID, sub.TopicFilter); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *SessionStore) DelClient(ctx context.Context, clientID string) error {
	s.db.Lock()
	defer s.db.Unlock()
	tx, err := s.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+subscriptionsTable+` WHERE client_id = ?`, clientID); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+sessionsTable+` WHERE client_id = ?`, clientID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SessionStore) PurgeSessions(ctx context.Context) error {
	s.db.Lock()
	defer s.db.Unlock()
	tx, err := s.db.Conn().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+subscriptionsTable+` WHERE client_id IN (SELECT client_id FROM `+sessionsTable+` WHERE clean_session = 1 AND connected = 0)`); err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+sessionsTable+` WHERE clean_session = 1 AND connected = 0`); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
