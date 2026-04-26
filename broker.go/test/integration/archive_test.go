package integration

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "modernc.org/sqlite"

	"monstermq.io/edge/internal/archive"
)

// TestArchiveGroupWrites confirms the Default archive group is created on
// startup and that published messages reach both its last-value and history
// tables. Table names are derived from the group name via the same helpers
// production uses (archive.LastValName / archive.ArchiveName), so this test
// breaks loudly if the naming convention changes.
func TestArchiveGroupWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "archive.db")
	port := 22003
	srv := startWithDB(t, port, dbPath, nil)

	client := mqtt.NewClient(mqttOpts(port, "ar-pub"))
	if tok := client.Connect(); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
		t.Fatal(tok.Error())
	}
	for i := 0; i < 5; i++ {
		if tok := client.Publish("sensor/temp", 0, false, "21.5"); tok.WaitTimeout(2*time.Second) && tok.Error() != nil {
			t.Fatal(tok.Error())
		}
	}
	client.Disconnect(100)

	// Wait long enough for the archive group ticker (250ms) to flush.
	time.Sleep(600 * time.Millisecond)
	srv.Close()

	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()

	var lvCount int
	lvTable := strings.ToLower(archive.LastValName("Default"))  // "defaultlastval"
	arTable := strings.ToLower(archive.ArchiveName("Default"))  // "defaultarchive"

	if err := conn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE topic = ?", lvTable),
		"sensor/temp").Scan(&lvCount); err != nil {
		t.Fatal(err)
	}
	if lvCount != 1 {
		t.Fatalf("%s expected 1 row, got %d", lvTable, lvCount)
	}

	var arCount int
	if err := conn.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE topic = ?", arTable),
		"sensor/temp").Scan(&arCount); err != nil {
		t.Fatal(err)
	}
	if arCount < 1 {
		t.Fatalf("%s expected >= 1 row, got %d", arTable, arCount)
	}
}
