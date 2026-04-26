package resolvers

import "monstermq.io/edge/internal/stores/sqlite"

func hashPassword(p string) (string, error) {
	return sqlite.HashPassword(p)
}
