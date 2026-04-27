package log

import (
	"log/slog"
	"os"
	"strings"
)

// Setup builds a logger that writes to stderr AND fans out to bus (if non-nil)
// so the GraphQL systemLogs subscription can stream live entries.
func Setup(levelStr string, bus *Bus, nodeID string) *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	inner := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	var h slog.Handler = inner
	if bus != nil {
		h = NewHandler(bus, inner, nodeID)
	}
	logger := slog.New(h)
	slog.SetDefault(logger)
	return logger
}
