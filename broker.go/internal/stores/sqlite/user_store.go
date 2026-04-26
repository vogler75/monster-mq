package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"monstermq.io/edge/internal/stores"
)

const (
	usersTable    = "users"
	usersAclTable = "usersacl"
)

type UserStore struct {
	db *DB
}

func NewUserStore(db *DB) *UserStore { return &UserStore{db: db} }

func (u *UserStore) Close() error { return nil }

func (u *UserStore) EnsureTable(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + usersTable + ` (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            enabled BOOLEAN DEFAULT 1,
            can_subscribe BOOLEAN DEFAULT 1,
            can_publish BOOLEAN DEFAULT 1,
            is_admin BOOLEAN DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`,
		`CREATE TABLE IF NOT EXISTS ` + usersAclTable + ` (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT REFERENCES ` + usersTable + `(username) ON DELETE CASCADE,
            topic_pattern TEXT NOT NULL,
            can_subscribe BOOLEAN DEFAULT 0,
            can_publish BOOLEAN DEFAULT 0,
            priority INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`,
		`CREATE INDEX IF NOT EXISTS ` + usersAclTable + `_username_idx ON ` + usersAclTable + ` (username)`,
		`CREATE INDEX IF NOT EXISTS ` + usersAclTable + `_priority_idx ON ` + usersAclTable + ` (priority)`,
	}
	for _, q := range stmts {
		if _, err := u.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (u *UserStore) CreateUser(ctx context.Context, user stores.User) error {
	q := `INSERT INTO ` + usersTable + ` (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := u.db.Exec(q, user.Username, user.PasswordHash, boolToInt(user.Enabled), boolToInt(user.CanSubscribe), boolToInt(user.CanPublish), boolToInt(user.IsAdmin))
	return err
}

func (u *UserStore) UpdateUser(ctx context.Context, user stores.User) error {
	q := `UPDATE ` + usersTable + ` SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?`
	_, err := u.db.Exec(q, user.PasswordHash, boolToInt(user.Enabled), boolToInt(user.CanSubscribe), boolToInt(user.CanPublish), boolToInt(user.IsAdmin), user.Username)
	return err
}

func (u *UserStore) DeleteUser(ctx context.Context, username string) error {
	_, err := u.db.Exec(`DELETE FROM `+usersTable+` WHERE username = ?`, username)
	return err
}

func (u *UserStore) GetUser(ctx context.Context, username string) (*stores.User, error) {
	row := u.db.Conn().QueryRowContext(ctx,
		`SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM `+usersTable+` WHERE username = ?`, username)
	return scanUser(row)
}

func scanUser(scanner interface{ Scan(...any) error }) (*stores.User, error) {
	var (
		user      stores.User
		enabled   bool
		canSub    bool
		canPub    bool
		admin     bool
		createdAt sql.NullString
		updatedAt sql.NullString
	)
	if err := scanner.Scan(&user.Username, &user.PasswordHash, &enabled, &canSub, &canPub, &admin, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	user.Enabled = enabled
	user.CanSubscribe = canSub
	user.CanPublish = canPub
	user.IsAdmin = admin
	user.CreatedAt = parseTime(createdAt)
	user.UpdatedAt = parseTime(updatedAt)
	return &user, nil
}

func parseTime(s sql.NullString) time.Time {
	if !s.Valid {
		return time.Time{}
	}
	v := strings.ReplaceAll(s.String, " ", "T")
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t
	}
	if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
		return t
	}
	return time.Time{}
}

func (u *UserStore) GetAllUsers(ctx context.Context) ([]stores.User, error) {
	rows, err := u.db.Conn().QueryContext(ctx,
		`SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM `+usersTable+` ORDER BY username`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.User{}
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		if user != nil {
			out = append(out, *user)
		}
	}
	return out, rows.Err()
}

func (u *UserStore) ValidateCredentials(ctx context.Context, username, password string) (*stores.User, error) {
	user, err := u.GetUser(ctx, username)
	if err != nil || user == nil {
		return nil, err
	}
	if !user.Enabled {
		return nil, nil
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, nil
	}
	return user, nil
}

func (u *UserStore) CreateAclRule(ctx context.Context, r stores.AclRule) error {
	q := `INSERT INTO ` + usersAclTable + ` (username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?)`
	_, err := u.db.Exec(q, r.Username, r.TopicPattern, boolToInt(r.CanSubscribe), boolToInt(r.CanPublish), r.Priority)
	return err
}

func (u *UserStore) UpdateAclRule(ctx context.Context, r stores.AclRule) error {
	q := `UPDATE ` + usersAclTable + ` SET username = ?, topic_pattern = ?, can_subscribe = ?, can_publish = ?, priority = ? WHERE id = ?`
	_, err := u.db.Exec(q, r.Username, r.TopicPattern, boolToInt(r.CanSubscribe), boolToInt(r.CanPublish), r.Priority, r.ID)
	return err
}

func (u *UserStore) DeleteAclRule(ctx context.Context, id string) error {
	_, err := u.db.Exec(`DELETE FROM `+usersAclTable+` WHERE id = ?`, id)
	return err
}

func (u *UserStore) GetUserAclRules(ctx context.Context, username string) ([]stores.AclRule, error) {
	return u.queryAclRules(ctx, `SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM `+usersAclTable+` WHERE username = ? ORDER BY priority DESC`, username)
}

func (u *UserStore) GetAllAclRules(ctx context.Context) ([]stores.AclRule, error) {
	return u.queryAclRules(ctx, `SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM `+usersAclTable+` ORDER BY username, priority DESC`)
}

func (u *UserStore) queryAclRules(ctx context.Context, q string, args ...any) ([]stores.AclRule, error) {
	rows, err := u.db.Conn().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []stores.AclRule{}
	for rows.Next() {
		var (
			r         stores.AclRule
			id        int64
			canSub    bool
			canPub    bool
			createdAt sql.NullString
		)
		if err := rows.Scan(&id, &r.Username, &r.TopicPattern, &canSub, &canPub, &r.Priority, &createdAt); err != nil {
			return nil, err
		}
		r.ID = fmt.Sprintf("%d", id)
		r.CanSubscribe = canSub
		r.CanPublish = canPub
		r.CreatedAt = parseTime(createdAt)
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
