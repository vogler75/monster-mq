package integration

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"monstermq.io/edge/internal/config"
)

// TestBridgeDynamicDeploy: start two brokers (B is "remote", A is "edge"),
// then via A's GraphQL mutation create an MQTT bridge with a SUBSCRIBE
// address pointing at B. Publish on B, expect the message to land on A's
// local topic (proves: dynamic Reload works AND removePath/local-mapping
// is wired correctly).
func TestBridgeDynamicDeploy(t *testing.T) {
	dbA := filepath.Join(t.TempDir(), "a.db")
	dbB := filepath.Join(t.TempDir(), "b.db")

	// Remote broker B
	srvB := startWithDB(t, 26101, dbB, func(c *config.Config) {
		c.Bridges.Mqtt.Enabled = false
	})
	defer srvB.Close()

	// Edge broker A — has GraphQL on 28101 and MQTT on 26100, bridge enabled.
	cfgA := config.Default()
	cfgA.NodeID = "edge-a"
	cfgA.TCP.Port = 26100
	cfgA.GraphQL.Enabled = true
	cfgA.GraphQL.Port = 28101
	cfgA.Dashboard.Enabled = false
	cfgA.SQLite.Path = dbA
	cfgA.Bridges.Mqtt.Enabled = true
	srvA, urlA := startWithGraphQL(t, 26100, 28101, func(c *config.Config) {
		*c = *cfgA
	})
	_ = srvA
	defer srvA.Close()

	// Sanity: subscribe on A so we can observe any inbound republish.
	subA := mqtt.NewClient(mqttOpts(26100, "subA"))
	if tok := subA.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	defer subA.Disconnect(100)
	var got atomic.Int32
	rcv := make(chan string, 8)
	if tok := subA.Subscribe("from-cloud/#", 0, func(_ mqtt.Client, m mqtt.Message) {
		got.Add(1)
		select {
		case rcv <- fmt.Sprintf("%s=%s", m.Topic(), string(m.Payload())):
		default:
		}
	}); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}

	// Create the bridge via GraphQL.
	gqlQuery(t, urlA,
		`mutation C($i:MqttClientInput!){ mqttClient{ create(input:$i){ success errors } } }`,
		map[string]any{
			"i": map[string]any{
				"name":      "BridgeToB",
				"namespace": "bridge/b",
				"nodeId":    "edge-a",
				"enabled":   true,
				"config": map[string]any{
					"brokerUrl":    fmt.Sprintf("tcp://localhost:%d", 26101),
					"clientId":     "edge-bridge-client",
					"cleanSession": true,
					"keepAlive":    10,
					"addresses":    []map[string]any{},
				},
			},
		})

	// Add an inbound address: subscribe on B's "remote/#" → republish under "from-cloud" on A.
	gqlQuery(t, urlA,
		`mutation A($d:String!,$i:MqttClientAddressInput!){ mqttClient{ addAddress(deviceName:$d,input:$i){ success errors } } }`,
		map[string]any{
			"d": "BridgeToB",
			"i": map[string]any{
				"mode":        "SUBSCRIBE",
				"remoteTopic": "remote/#",
				"localTopic":  "from-cloud",
				"removePath":  true,
				"qos":         0,
			},
		})

	// Wait for the bridge to actually connect to B.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		probe := mqtt.NewClient(mqttOpts(26101, "probe"))
		if tok := probe.Connect(); tok.WaitTimeout(500*time.Millisecond) && tok.Error() == nil {
			probe.Disconnect(50)
			break
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Publish on B → should arrive on A under from-cloud/sensor/temp.
	pubB := mqtt.NewClient(mqttOpts(26101, "pubB"))
	if tok := pubB.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	if tok := pubB.Publish("remote/sensor/temp", 0, false, "21"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	pubB.Disconnect(100)

	select {
	case msg := <-rcv:
		// removePath=true → "remote/" stripped → localTopic "from-cloud" + "sensor/temp"
		if msg != "from-cloud/sensor/temp=21" {
			t.Fatalf("expected from-cloud/sensor/temp=21, got %s", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("bridge did not deliver after dashboard create+addAddress (got=%d)", got.Load())
	}
}
