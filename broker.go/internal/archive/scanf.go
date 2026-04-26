package archive

import "fmt"

func _sscan(s, format string, a ...any) (int, error) {
	return fmt.Sscanf(s, format, a...)
}
