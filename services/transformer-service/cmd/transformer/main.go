package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "transformer-service")
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// This composition root will wire service-owned ports and adapters as they are added.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})).With("service", "transformer-service")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("service skeleton started", "etl_enabled", false)
	<-ctx.Done()
	logger.Info("service skeleton stopped")
}
