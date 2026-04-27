package mqttclient

import (
	"errors"
	"path/filepath"
	"testing"
)

// runBufferContract exercises the contract every Buffer implementation must
// satisfy: FIFO push/drain, capacity-with-reject, capacity-with-evict-oldest,
// drain-stops-on-error, restart-survival (only when persistent).
func runBufferContract(t *testing.T, name string, makeBuffer func(cap int, deleteOldest bool) Buffer) {
	t.Helper()

	t.Run(name+"/FIFO push then drain", func(t *testing.T) {
		b := makeBuffer(100, false)
		defer b.Close()
		for i := 0; i < 5; i++ {
			if err := b.Push(BufferItem{Topic: "t", Payload: []byte{byte(i)}}); err != nil {
				t.Fatalf("push %d: %v", i, err)
			}
		}
		if b.Len() != 5 {
			t.Fatalf("len=%d want 5", b.Len())
		}
		var got []byte
		if err := b.Drain(func(item BufferItem) error {
			got = append(got, item.Payload[0])
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if string(got) != "\x00\x01\x02\x03\x04" {
			t.Fatalf("FIFO order broken: %v", got)
		}
		if b.Len() != 0 {
			t.Fatalf("len after drain = %d, want 0", b.Len())
		}
	})

	t.Run(name+"/reject on overflow", func(t *testing.T) {
		b := makeBuffer(3, false /* deleteOldestOnFull */)
		defer b.Close()
		for i := 0; i < 3; i++ {
			if err := b.Push(BufferItem{Topic: "t", Payload: []byte{byte(i)}}); err != nil {
				t.Fatalf("push %d: %v", i, err)
			}
		}
		// 4th push must fail with ErrBufferFull.
		err := b.Push(BufferItem{Topic: "t", Payload: []byte{0xFF}})
		if !errors.Is(err, ErrBufferFull) {
			t.Fatalf("4th push: got err=%v, want ErrBufferFull", err)
		}
		// Buffer still holds the original 3.
		if b.Len() != 3 {
			t.Fatalf("len=%d want 3", b.Len())
		}
		var got []byte
		_ = b.Drain(func(item BufferItem) error {
			got = append(got, item.Payload[0])
			return nil
		})
		if string(got) != "\x00\x01\x02" {
			t.Fatalf("kept oldest on reject: got %v want 00 01 02", got)
		}
	})

	t.Run(name+"/evict oldest on overflow", func(t *testing.T) {
		b := makeBuffer(3, true /* deleteOldestOnFull */)
		defer b.Close()
		for i := 0; i < 5; i++ {
			if err := b.Push(BufferItem{Topic: "t", Payload: []byte{byte(i)}}); err != nil {
				t.Fatalf("push %d: %v", i, err)
			}
		}
		if b.Len() != 3 {
			t.Fatalf("len=%d want 3 after evict", b.Len())
		}
		var got []byte
		_ = b.Drain(func(item BufferItem) error {
			got = append(got, item.Payload[0])
			return nil
		})
		// Should have kept the LAST 3: 02 03 04.
		if string(got) != "\x02\x03\x04" {
			t.Fatalf("evict-oldest order: got %v want 02 03 04", got)
		}
	})

	t.Run(name+"/drain stops on send error and leaves rest in buffer", func(t *testing.T) {
		b := makeBuffer(100, false)
		defer b.Close()
		for i := 0; i < 5; i++ {
			b.Push(BufferItem{Topic: "t", Payload: []byte{byte(i)}})
		}
		stopErr := errors.New("simulated remote down")
		var sent []byte
		err := b.Drain(func(item BufferItem) error {
			if len(sent) == 2 {
				return stopErr
			}
			sent = append(sent, item.Payload[0])
			return nil
		})
		if !errors.Is(err, stopErr) {
			t.Fatalf("drain returned %v, want stopErr", err)
		}
		if string(sent) != "\x00\x01" {
			t.Fatalf("sent %v want 00 01", sent)
		}
		if b.Len() != 3 {
			t.Fatalf("len after partial drain = %d, want 3 (untouched: 02 03 04)", b.Len())
		}
	})
}

func TestMemoryBuffer(t *testing.T) {
	runBufferContract(t, "memory", func(cap int, deleteOldest bool) Buffer {
		return newMemoryBuffer(cap, deleteOldest)
	})
}

func TestSqliteBuffer(t *testing.T) {
	dir := t.TempDir()
	counter := 0
	runBufferContract(t, "sqlite", func(cap int, deleteOldest bool) Buffer {
		counter++
		path := filepath.Join(dir, "buf.db")
		// Each subtest gets its own file so they don't share state.
		path = filepath.Join(dir, "buf"+itoa(counter)+".db")
		b, err := newSqliteBuffer(path, cap, deleteOldest)
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}

// TestSqliteBufferSurvivesProcessRestart pushes items, closes the buffer (as
// if the broker had crashed), opens a fresh handle on the same file, and
// confirms the items come back in FIFO order. Proves real disk persistence.
func TestSqliteBufferSurvivesProcessRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	b1, err := newSqliteBuffer(path, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := b1.Push(BufferItem{Topic: "t", Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen on the same file; items must still be there.
	b2, err := newSqliteBuffer(path, 100, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if b2.Len() != 5 {
		t.Fatalf("len after reopen = %d, want 5", b2.Len())
	}
	var got []byte
	_ = b2.Drain(func(item BufferItem) error {
		got = append(got, item.Payload[0])
		return nil
	})
	if string(got) != "\x00\x01\x02\x03\x04" {
		t.Fatalf("FIFO order after restart: %v", got)
	}
}

// itoa is a tiny stdlib-free int-to-string for the test helper above.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
