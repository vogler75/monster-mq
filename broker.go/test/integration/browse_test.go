package integration

import (
	"sort"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TestBrowseTopicsLevels covers the contract the dashboard's topic browser
// relies on:
//   - root pattern ("+") returns distinct first-level prefixes including those
//     whose only stored value is at a deeper level
//   - sub-level pattern ("a/+") returns distinct prefixes truncated at the
//     wildcard depth
//   - exact pattern returns the topic iff a value exists
//   - leaves are flagged when a stored value sits exactly at the queried depth
func TestBrowseTopicsLevels(t *testing.T) {
	srv, url := startWithGraphQL(t, 23020, 28020)
	defer srv.Close()

	pub := mqtt.NewClient(mqttOpts(23020, "browse-pub"))
	if tok := pub.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for _, topic := range []string{
		"sensor/temp",
		"sensor/temp/celsius",
		"sensor/humid",
		"alarm",
	} {
		if tok := pub.Publish(topic, 0, false, "v"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
	}
	pub.Disconnect(100)
	time.Sleep(500 * time.Millisecond) // archive group flush

	got := func(pattern string) []map[string]any {
		data := gqlQuery(t, url, `query Q($t:String!){ browseTopics(topic:$t, archiveGroup:"Default"){ name isLeaf } }`,
			map[string]any{"t": pattern})
		raw := data["browseTopics"].([]any)
		out := make([]map[string]any, len(raw))
		for i, r := range raw {
			out[i] = r.(map[string]any)
		}
		sort.Slice(out, func(i, j int) bool { return out[i]["name"].(string) < out[j]["name"].(string) })
		return out
	}

	// Root: { "alarm" (leaf), "sensor" (intermediate) }
	root := got("+")
	if len(root) != 2 {
		t.Fatalf("root: expected 2 entries, got %v", root)
	}
	if root[0]["name"] != "alarm" || root[0]["isLeaf"] != true {
		t.Fatalf("root[0] expected alarm leaf, got %v", root[0])
	}
	if root[1]["name"] != "sensor" || root[1]["isLeaf"] != false {
		t.Fatalf("root[1] expected sensor non-leaf, got %v", root[1])
	}

	// One level below "sensor": { "sensor/humid" (leaf), "sensor/temp" (also leaf because value at that depth) }
	sub := got("sensor/+")
	if len(sub) != 2 {
		t.Fatalf("sensor/+: expected 2 entries, got %v", sub)
	}
	if sub[0]["name"] != "sensor/humid" || sub[0]["isLeaf"] != true {
		t.Fatalf("expected sensor/humid leaf, got %v", sub[0])
	}
	if sub[1]["name"] != "sensor/temp" || sub[1]["isLeaf"] != true {
		t.Fatalf("expected sensor/temp leaf, got %v", sub[1])
	}

	// Two levels below sensor/temp/+ : { "sensor/temp/celsius" (leaf) }
	deep := got("sensor/temp/+")
	if len(deep) != 1 || deep[0]["name"] != "sensor/temp/celsius" || deep[0]["isLeaf"] != true {
		t.Fatalf("sensor/temp/+: expected sensor/temp/celsius leaf, got %v", deep)
	}

	// Exact match
	exact := got("alarm")
	if len(exact) != 1 || exact[0]["name"] != "alarm" || exact[0]["isLeaf"] != true {
		t.Fatalf("exact alarm: %v", exact)
	}
	none := got("nonexistent")
	if len(none) != 0 {
		t.Fatalf("nonexistent: expected empty, got %v", none)
	}
}
