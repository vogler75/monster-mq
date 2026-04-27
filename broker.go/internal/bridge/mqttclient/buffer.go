package mqttclient

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// BufferItem is one queued publish destined for the remote broker.
type BufferItem struct {
	Topic   string
	QoS     byte
	Retain  bool
	Payload []byte
}

// Buffer is the interface implemented by the in-memory and disk-backed
// outbound buffers. Push appends one item; Drain pulls items in FIFO order
// and only removes them after `send` returns nil.
type Buffer interface {
	Push(item BufferItem) error
	Drain(send func(BufferItem) error) error
	Len() int
	Close() error
}

// ErrBufferFull is returned by Push when the buffer is at capacity and the
// policy is "reject new" (DeleteOldestMessages = false).
var ErrBufferFull = errors.New("bridge buffer full")

// memoryBuffer is a bounded FIFO slice protected by a mutex.
type memoryBuffer struct {
	mu               sync.Mutex
	items            []BufferItem
	cap              int
	deleteOldestOnFull bool
}

func newMemoryBuffer(capacity int, deleteOldestOnFull bool) *memoryBuffer {
	if capacity <= 0 {
		capacity = 100_000
	}
	return &memoryBuffer{cap: capacity, deleteOldestOnFull: deleteOldestOnFull}
}

func (b *memoryBuffer) Push(item BufferItem) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) >= b.cap {
		if !b.deleteOldestOnFull {
			return ErrBufferFull
		}
		b.items = b.items[1:]
	}
	b.items = append(b.items, item)
	return nil
}

func (b *memoryBuffer) Drain(send func(BufferItem) error) error {
	for {
		b.mu.Lock()
		if len(b.items) == 0 {
			b.mu.Unlock()
			return nil
		}
		next := b.items[0]
		b.mu.Unlock()
		if err := send(next); err != nil {
			return err
		}
		b.mu.Lock()
		// Drop the head we just sent. We assume single-consumer (Drain is
		// called only from the OnConnect goroutine), so the head we
		// observed is still the head.
		if len(b.items) > 0 {
			b.items = b.items[1:]
		}
		b.mu.Unlock()
	}
}

func (b *memoryBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

func (b *memoryBuffer) Close() error { return nil }

// sqliteBuffer persists outbound publishes to a small per-bridge SQLite
// file. Survives broker restarts. Schema:
//
//   CREATE TABLE bridge_buffer (
//       id INTEGER PRIMARY KEY AUTOINCREMENT,
//       enqueued_at INTEGER NOT NULL,        -- Unix milliseconds
//       topic TEXT NOT NULL,
//       qos INTEGER NOT NULL,
//       retain INTEGER NOT NULL,
//       payload BLOB
//   );
//
// FIFO order = ORDER BY id ASC.
type sqliteBuffer struct {
	mu                 sync.Mutex
	db                 *sql.DB
	path               string
	cap                int
	deleteOldestOnFull bool
}

func newSqliteBuffer(path string, capacity int, deleteOldestOnFull bool) (*sqliteBuffer, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open bridge buffer %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // single writer
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS bridge_buffer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        enqueued_at INTEGER NOT NULL,
        topic TEXT NOT NULL,
        qos INTEGER NOT NULL,
        retain INTEGER NOT NULL,
        payload BLOB
    )`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create bridge_buffer: %w", err)
	}
	if capacity <= 0 {
		capacity = 100_000
	}
	return &sqliteBuffer{db: db, path: path, cap: capacity, deleteOldestOnFull: deleteOldestOnFull}, nil
}

func (b *sqliteBuffer) Push(item BufferItem) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.lenLocked()
	if n >= b.cap {
		if !b.deleteOldestOnFull {
			return ErrBufferFull
		}
		// Drop oldest (id-asc).
		if _, err := b.db.Exec(`DELETE FROM bridge_buffer WHERE id = (SELECT id FROM bridge_buffer ORDER BY id ASC LIMIT 1)`); err != nil {
			return fmt.Errorf("evict oldest: %w", err)
		}
	}
	retain := 0
	if item.Retain {
		retain = 1
	}
	_, err := b.db.Exec(
		`INSERT INTO bridge_buffer (enqueued_at, topic, qos, retain, payload) VALUES (?, ?, ?, ?, ?)`,
		time.Now().UnixMilli(), item.Topic, int(item.QoS), retain, item.Payload,
	)
	return err
}

func (b *sqliteBuffer) Drain(send func(BufferItem) error) error {
	for {
		b.mu.Lock()
		row := b.db.QueryRow(`SELECT id, topic, qos, retain, payload FROM bridge_buffer ORDER BY id ASC LIMIT 1`)
		var (
			id      int64
			topic   string
			qos     int
			retain  int
			payload []byte
		)
		if err := row.Scan(&id, &topic, &qos, &retain, &payload); err != nil {
			b.mu.Unlock()
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		b.mu.Unlock()
		item := BufferItem{Topic: topic, QoS: byte(qos), Retain: retain == 1, Payload: payload}
		if err := send(item); err != nil {
			return err
		}
		b.mu.Lock()
		_, _ = b.db.Exec(`DELETE FROM bridge_buffer WHERE id = ?`, id)
		b.mu.Unlock()
	}
}

func (b *sqliteBuffer) lenLocked() int {
	var n int
	_ = b.db.QueryRow(`SELECT COUNT(*) FROM bridge_buffer`).Scan(&n)
	return n
}

func (b *sqliteBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lenLocked()
}

func (b *sqliteBuffer) Close() error { return b.db.Close() }
