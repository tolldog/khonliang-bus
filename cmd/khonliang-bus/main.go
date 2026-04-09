// Command khonliang-bus runs the cross-app event bus and service registry.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tolldog/khonliang-bus/internal/config"
	"github.com/tolldog/khonliang-bus/internal/registry"
	"github.com/tolldog/khonliang-bus/internal/server"
	"github.com/tolldog/khonliang-bus/internal/storage"
	"github.com/tolldog/khonliang-bus/internal/storage/memory"
	"github.com/tolldog/khonliang-bus/internal/storage/sqlite"
)

func main() {
	cfgPath := flag.String("config", "", "path to config YAML (optional)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Allow env override for listen address (handy for tests).
	if v := os.Getenv("KHONLIANG_BUS_LISTEN"); v != "" {
		cfg.Listen = v
	}

	backend, err := buildBackend(cfg.Backend)
	if err != nil {
		log.Fatalf("build backend: %v", err)
	}
	defer backend.Close()

	reg := registry.New(5 * cfg.Registry.HeartbeatInterval)
	srv := server.New(backend, reg, server.WithAllowedOrigins(cfg.AllowedOrigins...))

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Background pruning loop for the registry.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pruneLoop(ctx, reg, cfg.Registry.HeartbeatInterval)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("khonliang-bus listening on %s (backend=%s)", cfg.Listen, cfg.Backend.Type)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-sigCh
	log.Println("shutdown signal received")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Println("khonliang-bus stopped")
}

func buildBackend(cfg config.BackendConfig) (storage.Backend, error) {
	switch cfg.Type {
	case "", "memory":
		return memory.New(), nil
	case "sqlite":
		path := cfg.SQLite.Path
		if path == "" {
			path = "data/bus.db"
		}
		return sqlite.New(path)
	case "redis":
		return nil, fmt.Errorf("redis backend not yet implemented (planned v0.2)")
	default:
		return nil, fmt.Errorf("unknown backend type: %q", cfg.Type)
	}
}

func pruneLoop(ctx context.Context, reg *registry.Registry, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := reg.Prune(); n > 0 {
				log.Printf("pruned %d stale services from registry", n)
			}
		}
	}
}
