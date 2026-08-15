package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"monstermq-installer/internal/api"
	"monstermq-installer/internal/github"
	"monstermq-installer/internal/install"
	"monstermq-installer/internal/system"
)

//go:embed web/*
var embeddedWebFS embed.FS

//go:embed schema.json
var embeddedSchemaJSON []byte

func main() {
	portFlag := flag.Int("port", 0, "Local HTTP port to bind (0 for automatic free port)")
	dirFlag := flag.String("dir", "", "Target installation directory")
	versionFlag := flag.String("version", "latest", "MonsterMQ release version tag to install")
	cliFlag := flag.Bool("cli", false, "Run in terminal CLI mode instead of browser wizard")
	unattendedFlag := flag.Bool("unattended", false, "Run non-interactive automatic installation")
	noBrowserFlag := flag.Bool("no-browser", false, "Do not auto-launch system web browser")
	flag.Parse()

	printBanner()

	// 1. Check if CLI or unattended mode
	if *unattendedFlag || *cliFlag {
		runCLIMode(*dirFlag, *versionFlag, *unattendedFlag)
		return
	}

	// 2. Browser GUI Wizard Mode
	webSubFS, err := fs.Sub(embeddedWebFS, "web")
	if err != nil {
		fmt.Printf("Error accessing embedded web assets: %v\n", err)
		os.Exit(1)
	}

	serverCtx, cancelServer := context.WithCancel(context.Background())

	apiServer := api.NewServer(webSubFS, embeddedSchemaJSON, func() {
		cancelServer()
	})

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	// Listen on 127.0.0.1
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *portFlag))
	if err != nil {
		fmt.Printf("Failed to bind local listener: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	url := fmt.Sprintf("http://%s/", addr)

	fmt.Printf("====================================================\n")
	fmt.Printf(" MonsterMQ Setup Wizard started!\n")
	fmt.Printf(" Open in browser: %s\n", url)
	fmt.Printf("====================================================\n\n")

	// Auto-launch browser
	if !*noBrowserFlag {
		go func() {
			time.Sleep(300 * time.Millisecond)
			_ = openBrowser(url)
		}()
	}

	httpServer := &http.Server{
		Handler: mux,
	}

	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
		}
	}()

	// Wait for exit signal or web completion
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigChan:
		fmt.Println("\nSetup cancelled by user signal.")
	case <-serverCtx.Done():
		fmt.Println("\nSetup completed. Exiting installer.")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

func runCLIMode(targetDir, version string, unattended bool) {
	fmt.Println("--- MonsterMQ Terminal Installer ---")

	sysInfo := system.Detect()
	fmt.Printf("Platform: %s (%s)\n", sysInfo.OS, sysInfo.Arch)
	if sysInfo.JavaInstalled {
		fmt.Printf("Java: %s (Major: %d, Supported: %t)\n", sysInfo.JavaVersion, sysInfo.JavaMajor, sysInfo.JavaSupported)
	} else {
		fmt.Println("Warning: Java 21+ is not detected on this machine.")
	}

	if targetDir == "" {
		targetDir = sysInfo.DefaultDir
	}

	if !unattended {
		fmt.Printf("\nTarget directory [%s]: ", targetDir)
		var input string
		_, _ = fmt.Scanln(&input)
		if strings.TrimSpace(input) != "" {
			targetDir = strings.TrimSpace(input)
		}
	}

	absDir, err := filepath.Abs(targetDir)
	if err != nil {
		fmt.Printf("Invalid directory path: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Resolving release %s from GitHub...\n", version)
	ghClient := github.NewClient("")
	releases, err := ghClient.FetchReleases()
	if err != nil || len(releases) == 0 {
		fmt.Printf("Error fetching releases: %v\n", err)
		os.Exit(1)
	}

	var chosenRel *github.Release
	if version == "latest" {
		chosenRel = &releases[0]
	} else {
		for i := range releases {
			if releases[i].TagName == version || strings.TrimPrefix(releases[i].TagName, "v") == strings.TrimPrefix(version, "v") {
				chosenRel = &releases[i]
				break
			}
		}
	}

	if chosenRel == nil || chosenRel.BrokerZip == nil {
		fmt.Printf("Could not find release package for version '%s'\n", version)
		os.Exit(1)
	}

	fmt.Printf("Selected: %s (Zip: %s)\n", chosenRel.Name, chosenRel.BrokerZip.Name)

	opts := install.Options{
		TargetDir:   absDir,
		DownloadURL: chosenRel.BrokerZip.DownloadURL,
		Version:     chosenRel.TagName,
		SchemaJSON:  embeddedSchemaJSON,
	}

	result, err := install.Perform(opts, func(stage string, percent float64, message string) {
		fmt.Printf("  [%s] %.0f%% - %s\n", stage, percent, message)
	})

	if err != nil {
		fmt.Printf("\nInstallation failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n----------------------------------------------------")
	fmt.Printf("✓ %s\n", result.Message)
	fmt.Printf("Installation Directory: %s\n", result.TargetDir)
	fmt.Printf("Run script: %s\n", result.RunScript)
	fmt.Println("----------------------------------------------------")
}

func printBanner() {
	fmt.Println(`
  __  __                  _              __  __  ____ 
 |  \/  | ___  _ __  ___ | |_  ___  _ __|  \/  |/ __ \
 | |\/| |/ _ \| '_ \/ __|| __|/ _ \| '__| |\/| | / / |
 | |  | | (_) | | | \__ \| |_|  __/| |  | |  | | \ \_|
 |_|  |_|\___/|_| |_|___/ \__|\___||_|  |_|  |_|\___\_\
                                      Setup & Config`)
}

func openBrowser(url string) error {
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
