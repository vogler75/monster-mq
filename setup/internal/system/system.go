package system

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// Info holds system detection results.
type Info struct {
	OS            string   `json:"os"`
	Arch          string   `json:"arch"`
	DefaultDir    string   `json:"defaultDir"`
	UserHome      string   `json:"userHome"`
	JavaInstalled bool     `json:"javaInstalled"`
	JavaPath      string   `json:"javaPath"`
	JavaVersion   string   `json:"javaVersion"`
	JavaMajor     int      `json:"javaMajor"`
	JavaSupported bool     `json:"javaSupported"`
	JavaDownload  string   `json:"javaDownload"`
	JavaHelp      []string `json:"javaHelp"`
}

// Detect inspects the local machine environment.
func Detect() Info {
	info := Info{
		OS:   runtime.GOOS,
		Arch: runtime.GOARCH,
	}

	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	info.UserHome = home

	// Default installation directory based on OS
	switch runtime.GOOS {
	case "windows":
		if _, err := os.Stat("C:\\"); err == nil {
			info.DefaultDir = "C:\\MonsterMQ"
		} else {
			info.DefaultDir = filepath.Join(home, "MonsterMQ")
		}
	case "darwin":
		info.DefaultDir = filepath.Join(home, "MonsterMQ")
	default:
		info.DefaultDir = filepath.Join(home, "monstermq")
	}

	// Check if running from within an existing installation directory
	if exePath, err := os.Executable(); err == nil {
		if evalPath, err := filepath.EvalSymlinks(exePath); err == nil {
			exeDir := filepath.Dir(evalPath)
			if _, err := os.Stat(filepath.Join(exeDir, "config.yaml")); err == nil {
				info.DefaultDir = exeDir
			}
		}
	} else if cwd, err := os.Getwd(); err == nil {
		if _, err := os.Stat(filepath.Join(cwd, "config.yaml")); err == nil {
			info.DefaultDir = cwd
		}
	}

	// Java 21+ inspection
	info.CheckJava()

	return info
}

// CheckJava probes the system for Java 21+ installations.
func (info *Info) CheckJava() {
	candidateBinaries := findJavaCandidates()

	for _, binPath := range candidateBinaries {
		cmd := exec.Command(binPath, "-version")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Run(); err == nil {
			raw := outBuf.String()
			versionStr := extractJavaVersion(raw)
			major := parseJavaMajor(versionStr)

			if major > 0 {
				info.JavaInstalled = true
				info.JavaPath = binPath
				info.JavaVersion = versionStr
				info.JavaMajor = major
				info.JavaSupported = major >= 21

				if info.JavaSupported {
					info.JavaDownload = getJavaDownloadURL(info.OS, info.Arch)
					info.JavaHelp = nil
					return
				}
			}
		}
	}

	// If no supported Java 21+ found:
	info.JavaDownload = getJavaDownloadURL(info.OS, info.Arch)
	info.JavaHelp = getJavaInstallHelp(info.OS)
}

func findJavaCandidates() []string {
	var candidates []string

	// 1. PATH lookup
	if path, err := exec.LookPath("java"); err == nil {
		candidates = append(candidates, path)
	}

	// 2. JAVA_HOME environment variable
	if javaHome := os.Getenv("JAVA_HOME"); javaHome != "" {
		bin := filepath.Join(javaHome, "bin", "java")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		if _, err := os.Stat(bin); err == nil {
			candidates = append(candidates, bin)
		}
	}

	// 3. Platform standard search paths
	switch runtime.GOOS {
	case "windows":
		roots := []string{
			`C:\Program Files\Eclipse Adoptium`,
			`C:\Program Files\Java`,
			`C:\Program Files\Microsoft`,
			`C:\Program Files\Amazon Corretto`,
		}
		for _, r := range roots {
			matches, _ := filepath.Glob(filepath.Join(r, "*", "bin", "java.exe"))
			candidates = append(candidates, matches...)
		}

	case "darwin":
		macMatches, _ := filepath.Glob("/Library/Java/JavaVirtualMachines/*/Contents/Home/bin/java")
		candidates = append(candidates, macMatches...)
		homebrewPaths := []string{
			"/opt/homebrew/opt/openjdk@21/bin/java",
			"/opt/homebrew/opt/openjdk/bin/java",
			"/usr/local/opt/openjdk@21/bin/java",
			"/usr/local/opt/openjdk/bin/java",
		}
		for _, p := range homebrewPaths {
			if _, err := os.Stat(p); err == nil {
				candidates = append(candidates, p)
			}
		}

	case "linux":
		linuxMatches, _ := filepath.Glob("/usr/lib/jvm/*/bin/java")
		candidates = append(candidates, linuxMatches...)
		if _, err := os.Stat("/usr/bin/java"); err == nil {
			candidates = append(candidates, "/usr/bin/java")
		}
	}

	return uniqueStrings(candidates)
}

func uniqueStrings(items []string) []string {
	seen := make(map[string]bool)
	var res []string
	for _, item := range items {
		if item != "" && !seen[item] {
			seen[item] = true
			res = append(res, item)
		}
	}
	return res
}

// extractJavaVersion extracts version string from 'java -version' output
func extractJavaVersion(output string) string {
	// Matches: version "21.0.2" or version "25" or version "1.8.0_292"
	re := regexp.MustCompile(`version "([^"]+)"`)
	matches := re.FindStringSubmatch(output)
	if len(matches) > 1 {
		return matches[1]
	}

	// Fallback to first line
	lines := strings.Split(output, "\n")
	if len(lines) > 0 {
		return strings.TrimSpace(lines[0])
	}
	return "unknown"
}

// parseJavaMajor extracts major version number (e.g. "21.0.2" -> 21, "25" -> 25, "1.8.0" -> 8)
func parseJavaMajor(verStr string) int {
	clean := strings.TrimPrefix(verStr, "1.")
	re := regexp.MustCompile(`^([0-9]+)`)
	matches := re.FindStringSubmatch(clean)
	if len(matches) > 1 {
		val, err := strconv.Atoi(matches[1])
		if err == nil {
			return val
		}
	}
	return 0
}

func getJavaDownloadURL(osName, arch string) string {
	return "https://adoptium.net/temurin/releases/?version=21"
}

func getJavaInstallHelp(osName string) []string {
	switch osName {
	case "darwin":
		return []string{
			"MonsterMQ requires Java 21 or higher (OpenJDK / Eclipse Temurin).",
			"Install via Homebrew: brew install openjdk@21",
			"Or download macOS installer (.pkg): https://adoptium.net/temurin/releases/?version=21",
		}
	case "windows":
		return []string{
			"MonsterMQ requires Java 21 or higher (OpenJDK / Eclipse Temurin).",
			"Install via winget: winget install EclipseAdoptium.Temurin.21.JRE",
			"Or download Windows installer (.msi): https://adoptium.net/temurin/releases/?version=21",
		}
	case "linux":
		return []string{
			"MonsterMQ requires Java 21 or higher.",
			"Ubuntu/Debian: sudo apt update && sudo apt install openjdk-21-jre-headless",
			"Fedora/RHEL: sudo dnf install java-21-openjdk-headless",
			"Arch Linux: sudo pacman -S jre21-openjdk-headless",
			"Or install via SDKMAN: sdk install java 21-tem",
		}
	default:
		return []string{
			"MonsterMQ requires Java 21 or higher.",
			"Download OpenJDK 21 from: https://adoptium.net/temurin/releases/?version=21",
		}
	}
}

// DirValidationResult contains the validation status and any existing configuration.
type DirValidationResult struct {
	Valid     bool   `json:"valid"`
	AbsPath   string `json:"absPath"`
	HasConfig bool   `json:"hasConfig"`
	RawConfig string `json:"rawConfig,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ValidateInstallDir checks whether the given path can be written to and checks for an existing config.yaml.
func ValidateInstallDir(path string) DirValidationResult {
	if strings.TrimSpace(path) == "" {
		return DirValidationResult{Valid: false, Error: "path cannot be empty"}
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return DirValidationResult{Valid: false, Error: fmt.Sprintf("invalid path: %v", err)}
	}

	// Check if already exists
	stat, err := os.Stat(absPath)
	if err == nil {
		if !stat.IsDir() {
			return DirValidationResult{Valid: false, AbsPath: absPath, Error: "target path is an existing file, not a directory"}
		}
		// Test write permission
		testFile := filepath.Join(absPath, ".tmp_monstermq_write_test")
		if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
			return DirValidationResult{Valid: false, AbsPath: absPath, Error: fmt.Sprintf("directory is not writable: %v", err)}
		}
		_ = os.Remove(testFile)

		hasConfig := false
		var rawConfig string
		configPath := filepath.Join(absPath, "config.yaml")
		if content, err := os.ReadFile(configPath); err == nil {
			hasConfig = true
			rawConfig = string(content)
		}

		return DirValidationResult{
			Valid:     true,
			AbsPath:   absPath,
			HasConfig: hasConfig,
			RawConfig: rawConfig,
		}
	}

	// Try creating parent / directory
	if err := os.MkdirAll(absPath, 0755); err != nil {
		return DirValidationResult{Valid: false, AbsPath: absPath, Error: fmt.Sprintf("unable to create directory: %v", err)}
	}

	testFile := filepath.Join(absPath, ".tmp_monstermq_write_test")
	if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
		return DirValidationResult{Valid: false, AbsPath: absPath, Error: fmt.Sprintf("directory is not writable: %v", err)}
	}
	_ = os.Remove(testFile)

	return DirValidationResult{
		Valid:   true,
		AbsPath: absPath,
	}
}
