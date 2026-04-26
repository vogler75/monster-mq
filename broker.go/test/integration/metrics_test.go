package integration

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestMetricsCollectorAndPersistence publishes a few messages and confirms
// the broker records broker-level metrics in SQLite and surfaces them via GraphQL.
func TestMetricsCollectorAndPersistence(t *testing.T) {
	srv, url := startWithGraphQL(t, 23010, 28010)
	defer srv.Close()

	pub := mqtt.NewClient(mqttOpts(23010, "metrics-pub"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for i := 0; i < 10; i++ {
		_ = pub.Publish("m/x", 0, false, "x").WaitTimeout(2 * time.Second)
	}
	pub.Disconnect(100)

	// One full collector tick (default 1s) plus margin.
	time.Sleep(1500 * time.Millisecond)

	data := gqlQuery(t, url, `{ broker { metrics { messagesIn nodeSessionCount } metricsHistory(lastMinutes: 1) { messagesIn timestamp } } }`, nil)
	br := data["broker"].(map[string]any)
	metrics := br["metrics"].([]any)
	if len(metrics) == 0 {
		t.Fatal("expected current metrics")
	}
	hist := br["metricsHistory"].([]any)
	if len(hist) == 0 {
		t.Fatal("expected at least one metrics history row in SQLite")
	}
}
