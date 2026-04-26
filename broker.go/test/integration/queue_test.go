package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "modernc.org/sqlite"

	"monstermq.io/edge/internal/config"
)

// persistentOpts builds paho options for a clean=false (persistent) session.
func persistentOpts(port int, clientID string) *mqtt.ClientOptions {
	o := mqtt.NewClientOptions()
	o.AddBroker(fmt.Sprintf("tcp://localhost:%d", port))
	o.SetClientID(clientID)
	o.SetConnectTimeout(2 * time.Second)
	o.SetCleanSession(false)
	o.SetAutoReconnect(false)
	return o
}

// TestQueuedMessagesPersistAcrossRestart subscribes a clean=false client,
// disconnects it, publishes while offline, restarts the broker on the same
// SQLite file, reconnects the persistent client and confirms it receives the
// queued messages.
func TestQueuedMessagesPersistAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	port := 25001

	srv := startWithDB(t, port, dbPath, nil)

	// 1) subscribe with persistent session
	sub := mqtt.NewClient(persistentOpts(port, "persistent-sub"))
	if tok := sub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := sub.Subscribe("queue/+", 1, nil); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	sub.Disconnect(100)
	time.Sleep(150 * time.Millisecond) // let session row update to connected=false

	// 2) publish while subscriber is offline (persistent session is recorded in SQLite)
	pub := mqtt.NewClient(mqttOpts(port, "publisher"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for _, payload := range []string{"m1", "m2", "m3"} {
		if tok := pub.Publish("queue/topic", 1, false, payload); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
	}
	pub.Disconnect(100)
	time.Sleep(200 * time.Millisecond) // let queue hook flush enqueues

	// 3) verify rows landed in messagequeue
	conn, _ := sql.Open("sqlite", dbPath)
	defer conn.Close()
	var rows int
	if err := conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messagequeue WHERE client_id = ?`, "persistent-sub").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("expected 3 enqueued rows for persistent-sub, got %d", rows)
	}

	// 4) restart broker on the same file
	srv.Close()
	time.Sleep(150 * time.Millisecond)
	srv2 := startWithDB(t, port, dbPath, nil)
	defer srv2.Close()

	// 5) reconnect persistent session, expect all three queued messages
	var got atomic.Int32
	received := make(chan string, 3)

	opts := persistentOpts(port, "persistent-sub")
	opts.SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
		got.Add(1)
		select {
		case received <- string(m.Payload()):
		default:
		}
	})
	cl := mqtt.NewClient(opts)
	if tok := cl.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer cl.Disconnect(100)

	deadline := time.After(3 * time.Second)
	collected := map[string]bool{}
	for len(collected) < 3 {
		select {
		case p := <-received:
			collected[p] = true
		case <-deadline:
			t.Fatalf("only got %d/3 queued messages: %v", len(collected), collected)
		}
	}
	for _, want := range []string{"m1", "m2", "m3"} {
		if !collected[want] {
			t.Fatalf("missing payload %q", want)
		}
	}

	// 6) queue should be drained
	time.Sleep(200 * time.Millisecond)
	conn2, _ := sql.Open("sqlite", dbPath)
	defer conn2.Close()
	if err := conn2.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messagequeue WHERE client_id = ?`, "persistent-sub").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("expected queue drained, got %d rows", rows)
	}
}

// TestQueuedNoDuplicatesOnInProcessReconnect: when the client briefly disconnects
// and reconnects within the same broker process, mochi's in-memory inflight buffer
// already replays the offline messages. The hook must NOT also replay them from
// the DB queue, or each message arrives twice.
func TestQueuedNoDuplicatesOnInProcessReconnect(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "qdup.db")
	port := 25003
	srv := startWithDB(t, port, dbPath, nil)
	defer srv.Close()

	// 1) subscribe persistent
	sub := mqtt.NewClient(persistentOpts(port, "persist-no-dup"))
	if tok := sub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := sub.Subscribe("nodup/+", 1, nil); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	sub.Disconnect(100)
	time.Sleep(150 * time.Millisecond)

	// 2) publish 3 messages while offline
	pub := mqtt.NewClient(mqttOpts(port, "pub-no-dup"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for _, p := range []string{"a", "b", "c"} {
		if tok := pub.Publish("nodup/x", 1, false, p); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
	}
	pub.Disconnect(100)
	time.Sleep(200 * time.Millisecond)

	// 3) reconnect persistent — broker did NOT restart, so mochi's in-memory
	//    inflight buffer still has all three messages. Our hook must purge the
	//    DB queue rather than re-replay.
	count := atomic.Int32{}
	received := make(chan string, 10)
	opts := persistentOpts(port, "persist-no-dup")
	opts.SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
		count.Add(1)
		select {
		case received <- string(m.Payload()):
		default:
		}
	})
	cl := mqtt.NewClient(opts)
	if tok := cl.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer cl.Disconnect(100)

	// Collect for a fixed window. We expect exactly 3 unique payloads, no duplicates.
	deadline := time.After(2 * time.Second)
	got := map[string]int{}
	for {
		select {
		case p := <-received:
			got[p]++
		case <-deadline:
			goto done
		}
	}
done:
	if len(got) != 3 || got["a"] != 1 || got["b"] != 1 || got["c"] != 1 {
		t.Fatalf("expected each of a/b/c exactly once, got %v (total %d)", got, count.Load())
	}
}

// TestQueueDisabledFallsBackToMochi verifies that when QueuedMessagesEnabled
// is false, no rows are written to messagequeue (mochi keeps state in memory).
func TestQueueDisabledFallsBackToMochi(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "noqueue.db")
	port := 25002
	srv := startWithDB(t, port, dbPath, func(c *config.Config) { c.QueuedMessagesEnabled = false })
	defer srv.Close()

	sub := mqtt.NewClient(persistentOpts(port, "p2"))
	if tok := sub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := sub.Subscribe("q2/+", 1, nil); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	sub.Disconnect(100)
	time.Sleep(100 * time.Millisecond)

	pub := mqtt.NewClient(mqttOpts(port, "p2-pub"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := pub.Publish("q2/x", 1, false, "skip"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	pub.Disconnect(100)
	time.Sleep(150 * time.Millisecond)

	conn, _ := sql.Open("sqlite", dbPath)
	defer conn.Close()
	var rows int
	_ = conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM messagequeue`).Scan(&rows)
	if rows != 0 {
		t.Fatalf("queue disabled but %d rows landed in messagequeue", rows)
	}
}
