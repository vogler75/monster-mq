package install

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ProgressCallback receives updates during installation.
type ProgressCallback func(stage string, percent float64, message string)

// Options holds parameters for installation.
type Options struct {
	TargetDir    string                 `json:"targetDir"`
	DownloadURL  string                 `json:"downloadUrl"`
	Version      string                 `json:"version"`
	ConfigValues map[string]interface{} `json:"configValues"`
	RawConfig    string                 `json:"rawConfig"`
	SchemaJSON   []byte                 `json:"-"`
}

// Result holds the installation output.
type Result struct {
	Success      bool     `json:"success"`
	TargetDir    string   `json:"targetDir"`
	InstalledJar string   `json:"installedJar"`
	RunScript    string   `json:"runScript"`
	Endpoints    []string `json:"endpoints"`
	Message      string   `json:"message"`
	Error        string   `json:"error,omitempty"`
}

// Perform executes the download, extraction, config writing, and permission setup.
func Perform(opts Options, progress ProgressCallback) (*Result, error) {
	if progress == nil {
		progress = func(stage string, percent float64, message string) {}
	}

	absTarget, err := filepath.Abs(opts.TargetDir)
	if err != nil {
		return nil, fmt.Errorf("invalid target directory: %w", err)
	}

	// 1. Prepare target directory
	progress("prepare", 5, "Preparing target directory...")
	if err := os.MkdirAll(absTarget, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// 2. Download zip bundle
	progress("download", 10, fmt.Sprintf("Downloading MonsterMQ %s...", opts.Version))
	tempZip, err := downloadFileWithProgress(opts.DownloadURL, func(pct float64, speed string) {
		scaled := 10.0 + (pct * 0.50) // 10% to 60%
		progress("download", scaled, fmt.Sprintf("Downloading... %.1f%% (%s)", pct*100, speed))
	})
	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
	}
	defer os.Remove(tempZip)

	// 3. Extract zip bundle
	progress("extract", 65, "Extracting package contents...")
	jarFile, err := extractZip(tempZip, absTarget, func(pct float64) {
		scaled := 65.0 + (pct * 0.20) // 65% to 85%
		progress("extract", scaled, fmt.Sprintf("Extracting files... %.0f%%", pct*100))
	})
	if err != nil {
		return nil, fmt.Errorf("extraction failed: %w", err)
	}

	// 4. Create necessary subfolders
	progress("configure", 88, "Setting up workspace directories...")
	_ = os.MkdirAll(filepath.Join(absTarget, "sqlite"), 0755)
	_ = os.MkdirAll(filepath.Join(absTarget, "log"), 0755)

	// 5. Write config.yaml
	progress("configure", 92, "Writing config.yaml...")
	configPath := filepath.Join(absTarget, "config.yaml")

	var configContent []byte
	if strings.TrimSpace(opts.RawConfig) != "" {
		configContent = []byte(opts.RawConfig)
	} else if opts.ConfigValues != nil {
		data, err := yaml.Marshal(opts.ConfigValues)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize config.yaml: %w", err)
		}
		configContent = data
	}

	if len(configContent) > 0 {
		if err := os.WriteFile(configPath, configContent, 0644); err != nil {
			return nil, fmt.Errorf("failed to write config.yaml: %w", err)
		}
	}

	// 6. Write schema file if provided
	if len(opts.SchemaJSON) > 0 {
		schemaPath := filepath.Join(absTarget, "yaml-json-schema.json")
		_ = os.WriteFile(schemaPath, opts.SchemaJSON, 0644)
	}

	// 7. Ensure run scripts and permissions
	progress("finalize", 96, "Finalizing installation and scripts...")
	runScript := setupRunScripts(absTarget, jarFile)

	progress("complete", 100, "Installation completed successfully!")

	// Determine endpoints
	endpoints := []string{
		"Web Dashboard & GraphQL: http://localhost:4000/",
		"MQTT TCP Broker: mqtt://localhost:1883",
		"MCP AI Server: http://localhost:3000/",
	}

	return &Result{
		Success:      true,
		TargetDir:    absTarget,
		InstalledJar: jarFile,
		RunScript:    runScript,
		Endpoints:    endpoints,
		Message:      fmt.Sprintf("MonsterMQ %s successfully installed in %s", opts.Version, absTarget),
	}, nil
}

func downloadFileWithProgress(url string, update func(pct float64, speed string)) (string, error) {
	tempFile, err := os.CreateTemp("", "monstermq-*.zip")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "MonsterMQ-Installer/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download server returned %s", resp.Status)
	}

	total := resp.ContentLength
	var downloaded int64
	buf := make([]byte, 64*1024)
	startTime := time.Now()
	lastUpdate := time.Now()

	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			_, wErr := tempFile.Write(buf[:n])
			if wErr != nil {
				return "", wErr
			}
			downloaded += int64(n)

			if time.Since(lastUpdate) > 200*time.Millisecond || err == io.EOF {
				lastUpdate = time.Now()
				elapsed := time.Since(startTime).Seconds()
				var speedStr string
				if elapsed > 0 {
					mbps := (float64(downloaded) / (1024 * 1024)) / elapsed
					speedStr = fmt.Sprintf("%.2f MB/s", mbps)
				}
				pct := 0.0
				if total > 0 {
					pct = float64(downloaded) / float64(total)
				}
				update(pct, speedStr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	return tempFile.Name(), nil
}

func extractZip(zipPath, destDir string, update func(pct float64)) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", err
	}
	defer r.Close()

	// Check if all entries share a single top-level folder (e.g. monstermq-broker-1.8.28/)
	var topLevelDir string
	if len(r.File) > 0 {
		first := r.File[0].Name
		parts := strings.Split(filepath.ToSlash(first), "/")
		if len(parts) > 1 && parts[0] != "" {
			prefix := parts[0] + "/"
			allMatch := true
			for _, f := range r.File {
				if !strings.HasPrefix(filepath.ToSlash(f.Name), prefix) && filepath.ToSlash(f.Name) != parts[0] {
					allMatch = false
					break
				}
			}
			if allMatch {
				topLevelDir = prefix
			}
		}
	}

	totalFiles := len(r.File)
	var jarName string

	for i, f := range r.File {
		relPath := f.Name
		if topLevelDir != "" && strings.HasPrefix(filepath.ToSlash(relPath), topLevelDir) {
			relPath = strings.TrimPrefix(filepath.ToSlash(relPath), topLevelDir)
		}
		if relPath == "" {
			continue
		}

		targetPath := filepath.Join(destDir, relPath)

		// Guard against zip slip
		if !strings.HasPrefix(filepath.Clean(targetPath), filepath.Clean(destDir)) {
			continue
		}

		if f.FileInfo().IsDir() {
			_ = os.MkdirAll(targetPath, f.Mode())
			continue
		}

		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return "", err
		}

		outFile, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return "", err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return "", err
		}

		_, err = io.Copy(outFile, rc)
		rc.Close()
		outFile.Close()
		if err != nil {
			return "", err
		}

		if strings.HasSuffix(f.Name, ".jar") && strings.Contains(f.Name, "monstermq-broker") {
			jarName = filepath.Base(targetPath)
		}

		if (i % 20) == 0 {
			update(float64(i) / float64(totalFiles))
		}
	}

	return jarName, nil
}

func setupRunScripts(destDir, jarName string) string {
	runSh := filepath.Join(destDir, "run.sh")
	runBat := filepath.Join(destDir, "run.bat")

	if _, err := os.Stat(runSh); os.IsNotExist(err) {
		shContent := `#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
JAR_FILE=$(ls monstermq-broker-*.jar 2>/dev/null | head -n 1)
exec java -classpath "${JAR_FILE}:dependencies/*" at.rocworks.MonsterKt "$@"
`
		_ = os.WriteFile(runSh, []byte(shContent), 0755)
	} else {
		_ = os.Chmod(runSh, 0755)
	}

	batContent := `@echo off
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
set "JAR_FILE="
for %%f in ("monstermq-broker-*.jar") do set "JAR_FILE=%%f"
if not defined JAR_FILE (
    echo Error: monstermq-broker-*.jar not found.
    exit /b 1
)
java -classpath "%JAR_FILE%;dependencies/*" at.rocworks.MonsterKt %*
`
	_ = os.WriteFile(runBat, []byte(batContent), 0644)

	if runtime.GOOS == "windows" {
		return runBat
	}
	return runSh
}

// BrokerProcess represents a running MonsterMQ instance managed by the setup tool.
type BrokerProcess struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	dir    string
	logs   []string
	active bool
}

var globalProcess = &BrokerProcess{}

// StartBroker launches the broker process in the background.
func StartBroker(dir string, logCallback func(line string)) error {
	globalProcess.mu.Lock()
	defer globalProcess.mu.Unlock()

	if globalProcess.active && globalProcess.cmd != nil && globalProcess.cmd.Process != nil {
		return fmt.Errorf("broker is already running")
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "run.bat")
	} else {
		cmd = exec.Command("./run.sh")
	}
	cmd.Dir = dir

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start broker process: %w", err)
	}

	globalProcess.cmd = cmd
	globalProcess.dir = dir
	globalProcess.active = true
	globalProcess.logs = nil

	go func() {
		buf := make([]byte, 1024)
		var lineBuf bytes.Buffer
		for {
			n, err := stdoutPipe.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				for _, b := range chunk {
					if b == '\n' {
						line := strings.TrimSpace(lineBuf.String())
						lineBuf.Reset()
						if line != "" {
							globalProcess.mu.Lock()
							globalProcess.logs = append(globalProcess.logs, line)
							if len(globalProcess.logs) > 200 {
								globalProcess.logs = globalProcess.logs[1:]
							}
							globalProcess.mu.Unlock()
							if logCallback != nil {
								logCallback(line)
							}
						}
					} else {
						lineBuf.WriteByte(b)
					}
				}
			}
			if err != nil {
				break
			}
		}
		_ = cmd.Wait()
		globalProcess.mu.Lock()
		globalProcess.active = false
		globalProcess.mu.Unlock()
	}()

	return nil
}
