package api

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"monstermq-setup/internal/github"
	"monstermq-setup/internal/install"
	"monstermq-setup/internal/system"
)

// Server coordinates the installer HTTP interface.
type Server struct {
	WebFS      fs.FS
	SchemaJSON []byte
	ghClient   *github.Client
	mu         sync.Mutex
	onComplete func()
}

// NewServer creates a new API and Web server.
func NewServer(webFS fs.FS, schemaJSON []byte, onComplete func()) *Server {
	return &Server{
		WebFS:      webFS,
		SchemaJSON: schemaJSON,
		ghClient:   github.NewClient(""),
		onComplete: onComplete,
	}
}

// RegisterRoutes sets up REST endpoints and web file handler.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/system", s.handleSystem)
	mux.HandleFunc("/api/releases", s.handleReleases)
	mux.HandleFunc("/api/schema", s.handleSchema)
	mux.HandleFunc("/api/validate-dir", s.handleValidateDir)
	mux.HandleFunc("/api/install", s.handleInstall)
	mux.HandleFunc("/api/start-broker", s.handleStartBroker)
	mux.HandleFunc("/api/stop-broker", s.handleStopBroker)
	mux.HandleFunc("/api/broker-status", s.handleBrokerStatus)
	mux.HandleFunc("/api/open-dashboard", s.handleOpenDashboard)
	mux.HandleFunc("/api/open-folder", s.handleOpenFolder)
	mux.HandleFunc("/api/exit", s.handleExit)

	// Static web assets
	fileServer := http.FileServer(http.FS(s.WebFS))
	mux.Handle("/", fileServer)
}

func (s *Server) handleSystem(w http.ResponseWriter, r *http.Request) {
	info := system.Detect()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}

func (s *Server) handleReleases(w http.ResponseWriter, r *http.Request) {
	releases, err := s.ghClient.FetchReleases()
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(releases)
}

func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(s.SchemaJSON)
}

func (s *Server) handleValidateDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	valid, absPath, err := system.ValidateInstallDir(req.Path)
	res := map[string]interface{}{
		"valid":   valid,
		"absPath": absPath,
	}
	if err != nil {
		res["error"] = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func (s *Server) handleInstall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Prepare SSE response
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	var opts install.Options
	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
		sendSSE(w, flusher, "error", map[string]interface{}{
			"error": fmt.Sprintf("invalid options: %s", err.Error()),
		})
		return
	}

	opts.SchemaJSON = s.SchemaJSON

	sendProgress := func(stage string, pct float64, msg string) {
		sendSSE(w, flusher, "progress", map[string]interface{}{
			"stage":   stage,
			"percent": pct,
			"message": msg,
		})
	}

	result, err := install.Perform(opts, sendProgress)
	if err != nil {
		sendSSE(w, flusher, "error", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	sendSSE(w, flusher, "done", result)
}

func (s *Server) handleStartBroker(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TargetDir string `json:"targetDir"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// SSE stream for log lines
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	logChan := make(chan string, 200)

	pid, err := install.StartBroker(req.TargetDir, func(line string) {
		select {
		case logChan <- line:
		default:
		}
	})
	if err != nil {
		sendSSE(w, flusher, "error", map[string]string{"error": err.Error()})
		return
	}

	sendSSE(w, flusher, "started", map[string]interface{}{"status": "running", "pid": pid})

	// Keep stream open to pump logs until client disconnects or process finishes
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-logChan:
			if !ok {
				return
			}
			sendSSE(w, flusher, "log", map[string]string{"line": line})
			if strings.Contains(line, "[SYSTEM] Broker process exited") {
				sendSSE(w, flusher, "stopped", map[string]string{"status": "stopped"})
			}
		}
	}
}

func (s *Server) handleStopBroker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := install.StopBroker()
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

func (s *Server) handleBrokerStatus(w http.ResponseWriter, r *http.Request) {
	status := install.GetBrokerStatus()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func (s *Server) handleOpenDashboard(w http.ResponseWriter, r *http.Request) {
	url := "http://localhost:4000/"
	_ = openURL(url)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *Server) handleOpenFolder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	target := req.Path
	if target == "" {
		target = "."
	}
	absTarget, _ := filepath.Abs(target)

	switch runtime.GOOS {
	case "windows":
		_ = exec.Command("explorer", absTarget).Start()
	case "darwin":
		_ = exec.Command("open", absTarget).Start()
	default:
		_ = exec.Command("xdg-open", absTarget).Start()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *Server) handleExit(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})

	go func() {
		time.Sleep(500 * time.Millisecond)
		if s.onComplete != nil {
			s.onComplete()
		}
	}()
}

func sendSSE(w http.ResponseWriter, flusher http.Flusher, event string, data interface{}) {
	defer func() {
		_ = recover()
	}()
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, string(dataBytes))
	if flusher != nil {
		flusher.Flush()
	}
}

func openURL(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", strings.ReplaceAll(url, "&", "^&"))
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}
