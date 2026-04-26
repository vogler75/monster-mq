package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"
)

// Open opens a single shared SQLite database file. SQLite is single-writer; we
// gate writes through a mutex per *DB to avoid SQLITE_BUSY under concurrent
// writers. Reads run unsynchronized.
type DB struct {
	conn *sql.DB
	mu   sync.Mutex // guards write transactions
	path string
}

func Open(path string) (*DB, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	// SQLite is single-writer but WAL mode supports multiple concurrent readers.
	// Writes are serialized via DB.mu (Lock/Unlock around tx). Allow several connections
	// so a long-running read iterator (e.g. FindMatchingTopics) does not deadlock with a
	// nested Get call on the same logical operation.
	conn.SetMaxOpenConns(8)
	conn.SetMaxIdleConns(8)
	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping sqlite %s: %w", path, err)
	}
	return &DB{conn: conn, path: path}, nil
}

func (d *DB) Conn() *sql.DB { return d.conn }

func (d *DB) Lock()   { d.mu.Lock() }
func (d *DB) Unlock() { d.mu.Unlock() }

func (d *DB) Close() error {
	return d.conn.Close()
}

// Exec runs DDL or simple statements with the write lock held.
func (d *DB) Exec(query string, args ...any) (sql.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conn.Exec(query, args...)
}
