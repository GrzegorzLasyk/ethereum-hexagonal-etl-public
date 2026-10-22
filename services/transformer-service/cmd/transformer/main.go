package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	receiver "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/postgres"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "transformer-service")
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "transformer-service")
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("transformer stopped", "error", err)
		os.Exit(1)
	}
}

// run is the composition root: all workflow decisions live in use cases.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	pc.MaxConns = int32(cfg.Workers + 4)
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return errors.New("cannot create database pool")
	}
	defer pool.Close()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	var role, db string
	err = pool.QueryRow(startup, "SELECT current_user,current_database()").Scan(&role, &db)
	cancel()
	if err != nil || role != "transformer_runtime" || db != "ethereum_etl" {
		return errors.New("database requires transformer_runtime in ethereum_etl")
	}
	store, err := postgres.New(pool, cfg.ChainID, cfg.Lease, cfg.MaxAttempts)
	if err != nil {
		return err
	}
	accept, err := usecases.NewAccept(cfg.ChainID, cfg.Limits.PayloadBytes, store)
	if err != nil {
		return err
	}
	encoder, err := jsondocument.New(cfg.Limits.PayloadBytes)
	if err != nil {
		return err
	}
	process, err := usecases.NewProcess(store, ethereumjson.Decoder{}, encoder, cfg.InitialBackoff, cfg.MaxBackoff)
	if err != nil {
		return err
	}
	runner, err := worker.New(process, logger, cfg.Workers, cfg.PollInterval, cfg.ProcessTimeout)
	if err != nil {
		return err
	}
	server, err := receiver.NewServer(string(cfg.ChainID), cfg.Limits, accept)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return errors.New("cannot open gRPC listener")
	}
	defer listener.Close()
	working, stop := context.WithCancel(ctx)
	defer stop()
	workerDone := make(chan struct{})
	go func() { runner.Run(working); close(workerDone) }()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	logger.Info("transformer started", "address", cfg.ListenAddress, "chain_id", cfg.ChainID, "workers", cfg.Workers)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serving:
	}
	stop()
	graceful := make(chan struct{})
	go func() { server.GracefulStop(); close(graceful) }()
	timer := time.NewTimer(cfg.ShutdownTimeout)
	select {
	case <-graceful:
		timer.Stop()
	case <-timer.C:
		server.Stop()
		<-graceful
	}
	<-workerDone
	logger.Info("transformer stopped")
	if serveErr != nil {
		return errors.New("gRPC serving failed")
	}
	return nil
}
