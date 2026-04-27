package archive

import (
	"strings"
	"time"
)

// parseDuration accepts MonsterMQ-style retention strings like "30d", "12h",
// "1500m", "45s" and returns the duration; 0 if unparseable or empty.
func parseDuration(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var n int
		_, err := fmtSscanf(s, "%dd", &n)
		if err == nil {
			return time.Duration(n) * 24 * time.Hour
		}
	}
	return 0
}

var fmtSscanf = func(s, format string, a ...any) (int, error) {
	return _sscan(s, format, a...)
}
