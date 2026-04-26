package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	mlog "monstermq.io/edge/internal/log"
	"monstermq.io/edge/internal/version"
)

func main() {
	configPath := flag.String("config", "", "Path to config.yaml (defaults to built-in defaults if empty)")
	logLevel := flag.String("log-level", "", "Override log level (DEBUG|INFO|WARN|ERROR)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}

	logger := mlog.Setup(cfg.Logging.Level)
	logger.Info("starting", "name", version.Name, "version", version.Version, "node", cfg.NodeID)

	srv, err := broker.New(cfg, logger)
	if err != nil {
		logger.Error("broker init failed", "err", err)
		os.Exit(1)
	}

	go func() {
		if err := srv.Serve(); err != nil {
			logger.Error("broker serve error", "err", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	logger.Info("shutting down")
	if err := srv.Close(); err != nil {
		logger.Error("shutdown error", "err", err)
	}
}
