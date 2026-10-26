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

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/health"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/dispatcher"
	receiver "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/grpc"
	servicemetrics "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/ethereumjson"
	publisher "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/postgres"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	started := time.Now()
	cfg, err := config.Load()
	if err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "transformer-service", Operation: "configure", Outcome: "failed", ErrorCode: "invalid_configuration"}, started)
		os.Exit(1)
	}
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	if err := run(ctx, cfg, logger); err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "transformer-service", Operation: "run", Outcome: logevent.FailureOutcome(logging.ErrorCode(err)), ErrorCode: logging.ErrorCode(err)}, started)
		os.Exit(1)
	}
}

// run is the composition root: all workflow decisions live in use cases.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	started := time.Now()
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	pc.MaxConns = int32(cfg.Workers + cfg.Dispatch.Workers + 4)
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
	registry := metrics.New("process_inbox", "claim_inbox", "dispatch_outbox", "claim_outbox")
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
	runner, err := worker.New(process, logger, cfg.Workers, cfg.PollInterval, cfg.ProcessTimeout, registry)
	if err != nil {
		return err
	}
	queue, err := postgres.NewOutbox(pool, string(cfg.ChainID), cfg.Dispatch.Lease, cfg.Dispatch.MaxAttempts)
	if err != nil {
		return err
	}
	client, err := publisher.New(cfg.Dispatch.Target, string(cfg.ChainID), cfg.Limits)
	if err != nil {
		return err
	}
	defer client.Close()
	dispatch, err := usecases.NewDispatch(queue, client, cfg.Dispatch.RPCTimeout, cfg.Dispatch.InitialBackoff, cfg.Dispatch.MaxBackoff)
	if err != nil {
		return err
	}
	sender, err := dispatcher.New(dispatch, logger, cfg.Dispatch.Workers, cfg.Dispatch.PollInterval, cfg.Dispatch.Timeout, registry)
	if err != nil {
		return err
	}
	server, err := receiver.NewServer(string(cfg.ChainID), cfg.Limits, accept, logger)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return errors.New("cannot open gRPC listener")
	}
	defer listener.Close()
	closeHealth, err := health.Start(ctx, os.Getenv("TRANSFORMER_HEALTH_ADDRESS"), store.CheckReadiness, servicemetrics.Handler(store, registry))
	if err != nil {
		return err
	}
	defer closeHealth()
	working, stop := context.WithCancel(ctx)
	defer stop()
	workerDone := make(chan struct{})
	go func() { runner.Run(working); close(workerDone) }()
	dispatchDone := make(chan struct{})
	go func() { sender.Run(working); close(dispatchDone) }()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	logevent.Write(ctx, logger, logevent.Event{Service: "transformer-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "started"}, started)
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
	<-dispatchDone
	logevent.Write(ctx, logger, logevent.Event{Service: "transformer-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "stopped"}, started)
	if serveErr != nil {
		return errors.New("gRPC serving failed")
	}
	return nil
}
