package integration

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
)

// startEphemeral spins up a broker on an ephemeral set of ports. Returns the TCP address.
func startEphemeral(t *testing.T, port int) (*broker.Server, string) {
	t.Helper()
	cfg := config.Default()
	cfg.NodeID = fmt.Sprintf("test-%d", port)
	cfg.TCP.Enabled = true
	cfg.TCP.Port = port
	cfg.WS.Enabled = false
	cfg.GraphQL.Enabled = false
	cfg.Dashboard.Enabled = false
	cfg.Metrics.Enabled = false
	cfg.SQLite.Path = t.TempDir() + "/test.db"

	logger := slog.New(slog.DiscardHandler)
	srv, err := broker.New(cfg, logger)
	if err != nil {
		t.Fatalf("broker init: %v", err)
	}
	go func() { _ = srv.Serve() }()
	time.Sleep(100 * time.Millisecond)
	return srv, fmt.Sprintf("tcp://localhost:%d", port)
}

func mqttOpts(port int, clientID string) *mqtt.ClientOptions {
	o := mqtt.NewClientOptions()
	o.AddBroker(fmt.Sprintf("tcp://localhost:%d", port))
	o.SetClientID(clientID)
	o.SetConnectTimeout(2 * time.Second)
	o.SetCleanSession(true)
	return o
}

func TestPublishSubscribeQoS0(t *testing.T) {
	srv, _ := startEphemeral(t, 21883)
	defer srv.Close()

	subClient := mqtt.NewClient(mqttOpts(21883, "sub-q0"))
	if tok := subClient.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	defer subClient.Disconnect(100)

	var received atomic.Int32
	receivedMsg := make(chan string, 1)

	if tok := subClient.Subscribe("test/q0", 0, func(_ mqtt.Client, m mqtt.Message) {
		received.Add(1)
		select {
		case receivedMsg <- string(m.Payload()):
		default:
		}
	}); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pubClient := mqtt.NewClient(mqttOpts(21883, "pub-q0"))
	if tok := pubClient.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("pub connect: %v", tok.Error())
	}
	defer pubClient.Disconnect(100)

	if tok := pubClient.Publish("test/q0", 0, false, "hello"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	select {
	case got := <-receivedMsg:
		if got != "hello" {
			t.Fatalf("payload mismatch: got %q want %q", got, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive message in time")
	}
}

func TestRetainedMessageDelivery(t *testing.T) {
	srv, _ := startEphemeral(t, 21884)
	defer srv.Close()

	pub := mqtt.NewClient(mqttOpts(21884, "pub-ret"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("pub connect: %v", tok.Error())
	}
	if tok := pub.Publish("retained/topic", 1, true, "stay"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("publish retained: %v", tok.Error())
	}
	pub.Disconnect(100)

	sub := mqtt.NewClient(mqttOpts(21884, "sub-ret"))
	if tok := sub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("sub connect: %v", tok.Error())
	}
	defer sub.Disconnect(100)

	got := make(chan string, 1)
	if tok := sub.Subscribe("retained/+", 1, func(_ mqtt.Client, m mqtt.Message) {
		got <- string(m.Payload())
	}); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	select {
	case payload := <-got:
		if payload != "stay" {
			t.Fatalf("retained payload mismatch: %q", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retained message not delivered")
	}
}
