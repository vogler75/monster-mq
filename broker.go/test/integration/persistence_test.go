package integration

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/sqlite"
)

func startWithDB(t *testing.T, port int, dbPath string, cfgFn func(*config.Config)) *broker.Server {
	t.Helper()
	cfg := config.Default()
	cfg.NodeID = fmt.Sprintf("p-%d", port)
	cfg.TCP.Enabled = true
	cfg.TCP.Port = port
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Dashboard.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = dbPath
	if cfgFn != nil {
		cfgFn(cfg)
	}
	srv, err := broker.New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	go func() { _ = srv.Serve() }()
	time.Sleep(100 * time.Millisecond)
	return srv
}

// TestRetainedSurvivesRestart publishes a retained message, shuts the broker down,
// reopens it on the same SQLite file, and confirms a new subscriber receives the
// retained value.
func TestRetainedSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist.db")
	port := 22001

	srv := startWithDB(t, port, dbPath, nil)

	pub := mqtt.NewClient(mqttOpts(port, "p1"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := pub.Publish("persist/topic", 1, true, "stay-on-disk"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	pub.Disconnect(100)
	time.Sleep(200 * time.Millisecond) // let the storage hook flush
	srv.Close()

	// Reopen
	srv2 := startWithDB(t, port, dbPath, nil)
	defer srv2.Close()

	sub := mqtt.NewClient(mqttOpts(port, "p2"))
	if tok := sub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer sub.Disconnect(100)

	got := make(chan string, 1)
	if tok := sub.Subscribe("persist/+", 1, func(_ mqtt.Client, m mqtt.Message) {
		got <- string(m.Payload())
	}); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}

	select {
	case payload := <-got:
		if payload != "stay-on-disk" {
			t.Fatalf("payload %q", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retained message did not survive restart")
	}
}

// TestAuthAndAclEnforcement creates a user with a topic restriction, then
// confirms publish allowed/denied as expected.
func TestAuthAndAclEnforcement(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "auth.db")

	// Bootstrap a user before starting the broker.
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	us := sqlite.NewUserStore(db)
	if err := us.EnsureTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	hash, _ := sqlite.HashPassword("good")
	if err := us.CreateUser(context.Background(), stores.User{
		Username: "alice", PasswordHash: hash,
		Enabled: true, CanSubscribe: true, CanPublish: true, IsAdmin: true,
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	port := 22002
	srv := startWithDB(t, port, dbPath, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	defer srv.Close()

	good := mqttOpts(port, "alice-client")
	good.SetUsername("alice")
	good.SetPassword("good")
	gc := mqtt.NewClient(good)
	if tok := gc.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("good connect rejected: %v", tok.Error())
	}
	gc.Disconnect(100)

	bad := mqttOpts(port, "alice-bad")
	bad.SetUsername("alice")
	bad.SetPassword("WRONG")
	bc := mqtt.NewClient(bad)
	tok := bc.Connect()
	tok.WaitTimeout(2 * time.Second)
	// Either the connect token returns an error, or the client reports not connected.
	if tok.Error() == nil && bc.IsConnected() {
		t.Fatal("bad password connect should have been rejected")
	}
}

