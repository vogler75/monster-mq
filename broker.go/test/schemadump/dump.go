package main

import (
  "context"
  "database/sql"
  "fmt"
  "os"

  _ "modernc.org/sqlite"

  "monstermq.io/edge/internal/stores/sqlite"
)

func main() {
  path := "/tmp/test-schema.db"
  db, err := sqlite.Open(path)
  if err != nil { fmt.Println(err); os.Exit(1) }
  ctx := context.Background()
  for name, fn := range map[string]func(context.Context) error{
    "messages":  sqlite.NewMessageStore("messages", db).EnsureTable,
    "history":   sqlite.NewMessageArchive("history", db, "DEFAULT").EnsureTable,
    "sessions":  sqlite.NewSessionStore(db).EnsureTable,
    "queue":     sqlite.NewQueueStore(db, 0).EnsureTable,
    "users":     sqlite.NewUserStore(db).EnsureTable,
    "archives":  sqlite.NewArchiveConfigStore(db).EnsureTable,
    "devices":   sqlite.NewDeviceConfigStore(db).EnsureTable,
    "metrics":   sqlite.NewMetricsStore(db).EnsureTable,
  } {
    if err := fn(ctx); err != nil { fmt.Println("err", name, err); os.Exit(1) }
  }
  raw, _ := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
  rows, _ := raw.Query("SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') ORDER BY name")
  for rows.Next() {
    var n, s sql.NullString
    rows.Scan(&n, &s)
    fmt.Printf("%-40s :: %s\n", n.String, s.String)
  }
  raw.Close()
  db.Close()
}
