package graphql

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
)

//go:embed dashboard_placeholder/*
var placeholderFS embed.FS

// DashboardHandler returns an http.Handler serving dashboard/dist if present,
// or a built-in placeholder page that links to the GraphQL playground.
func DashboardHandler(externalDir string) http.Handler {
	if externalDir != "" {
		if info, err := os.Stat(externalDir); err == nil && info.IsDir() {
			return http.FileServer(http.Dir(externalDir))
		}
	}
	sub, err := fs.Sub(placeholderFS, "dashboard_placeholder")
	if err != nil {
		return http.NotFoundHandler()
	}
	return http.FileServer(http.FS(sub))
}
