package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"monstermq.io/edge/internal/stores"
)

func tempDB(t *testing.T) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMessageStoreRoundTrip(t *testing.T) {
	db := tempDB(t)
	store := NewMessageStore("retainedmessages", db)
	ctx := context.Background()
	if err := store.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	msg := stores.BrokerMessage{
		MessageUUID: "uuid-1",
		TopicName:   "a/b/c",
		Payload:     []byte("hello"),
		QoS:         1,
		IsRetain:    true,
		ClientID:    "client-1",
		Time:        time.Now(),
	}
	if err := store.AddAll(ctx, []stores.BrokerMessage{msg}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "a/b/c")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || string(got.Payload) != "hello" {
		t.Fatalf("expected hello, got %+v", got)
	}
	// Wildcard match
	count := 0
	if err := store.FindMatchingMessages(ctx, "a/+/c", func(m stores.BrokerMessage) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 wildcard match, got %d", count)
	}
}

func TestSessionStoreRoundTrip(t *testing.T) {
	db := tempDB(t)
	ss := NewSessionStore(db)
	ctx := context.Background()
	if err := ss.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	info := stores.SessionInfo{ClientID: "c1", NodeID: "n1", CleanSession: false, Connected: true}
	if err := ss.SetClient(ctx, info); err != nil {
		t.Fatal(err)
	}
	got, err := ss.GetSession(ctx, "c1")
	if err != nil || got == nil || got.ClientID != "c1" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if err := ss.AddSubscriptions(ctx, []stores.MqttSubscription{{ClientID: "c1", TopicFilter: "a/+", QoS: 1}}); err != nil {
		t.Fatal(err)
	}
	subs, err := ss.GetSubscriptionsForClient(ctx, "c1")
	if err != nil || len(subs) != 1 || subs[0].TopicFilter != "a/+" {
		t.Fatalf("subs=%+v err=%v", subs, err)
	}
}

func TestUserStoreCreateAndValidate(t *testing.T) {
	db := tempDB(t)
	us := NewUserStore(db)
	ctx := context.Background()
	if err := us.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	hash, err := HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if err := us.CreateUser(ctx, stores.User{Username: "alice", PasswordHash: hash, Enabled: true, CanPublish: true, CanSubscribe: true}); err != nil {
		t.Fatal(err)
	}
	u, err := us.ValidateCredentials(ctx, "alice", "s3cret")
	if err != nil || u == nil {
		t.Fatalf("validate=%+v err=%v", u, err)
	}
	bad, err := us.ValidateCredentials(ctx, "alice", "wrong")
	if err != nil || bad != nil {
		t.Fatalf("expected nil for wrong password, got %+v err=%v", bad, err)
	}
}

func TestQueueStoreEnqueueDequeueAck(t *testing.T) {
	db := tempDB(t)
	qs := NewQueueStore(db, 30*time.Second)
	ctx := context.Background()
	if err := qs.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	msg := stores.BrokerMessage{MessageUUID: "u1", TopicName: "x", Payload: []byte("hi"), QoS: 1, Time: time.Now()}
	if err := qs.Enqueue(ctx, "c1", msg); err != nil {
		t.Fatal(err)
	}
	got, err := qs.Dequeue(ctx, "c1", 10)
	if err != nil || len(got) != 1 || got[0].MessageUUID != "u1" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	// Re-dequeue should return nothing because vt is in the future
	more, err := qs.Dequeue(ctx, "c1", 10)
	if err != nil || len(more) != 0 {
		t.Fatalf("expected zero on re-dequeue, got %+v", more)
	}
	if err := qs.Ack(ctx, "c1", "u1"); err != nil {
		t.Fatal(err)
	}
	n, _ := qs.Count(ctx, "c1")
	if n != 0 {
		t.Fatalf("expected count 0 after ack, got %d", n)
	}
}

func TestMetricsStoreLatestAndHistory(t *testing.T) {
	db := tempDB(t)
	ms := NewMetricsStore(db)
	ctx := context.Background()
	if err := ms.EnsureTable(ctx); err != nil {
		t.Fatal(err)
	}
	t1 := time.Now().Add(-2 * time.Minute)
	t2 := time.Now().Add(-1 * time.Minute)
	t3 := time.Now()
	for _, ts := range []time.Time{t1, t2, t3} {
		if err := ms.StoreMetrics(ctx, stores.MetricBroker, ts, "node-1", `{"messagesIn":1.0}`); err != nil {
			t.Fatal(err)
		}
	}
	when, payload, err := ms.GetLatest(ctx, stores.MetricBroker, "node-1")
	if err != nil || payload == "" {
		t.Fatalf("latest: %v %q", err, payload)
	}
	if when.Before(t3.Add(-1 * time.Second)) {
		t.Fatalf("latest ts older than expected: %v", when)
	}
	h, err := ms.GetHistory(ctx, stores.MetricBroker, "node-1", t1.Add(-time.Second), t3.Add(time.Second), 100)
	if err != nil || len(h) != 3 {
		t.Fatalf("history: %d entries err=%v", len(h), err)
	}
}
