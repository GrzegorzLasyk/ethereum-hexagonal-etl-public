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

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/baseline"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/health"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	receiver "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/inbound/grpc"
	servicemetrics "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/inbound/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	persistmetrics "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/postgres"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	started := time.Now()
	cfg, err := config.Load()
	if err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "loader-service", Operation: "configure", Outcome: "failed", ErrorCode: "invalid_configuration"}, started)
		os.Exit(1)
	}
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	if err := run(ctx, cfg, logger); err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "loader-service", Operation: "run", Outcome: logevent.FailureOutcome(logging.ErrorCode(err)), ErrorCode: logging.ErrorCode(err)}, started)
		os.Exit(1)
	}
}

// run is the composition root: all workflow decisions live in use cases.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	started := time.Now()
	if cfg.Repair != nil {
		limited, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		ctx = limited
	}
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	pc.MaxConns = int32(cfg.Resources.PoolConnections)
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	probe, err := baseline.Configure(pc)
	if err != nil {
		return err
	}
	defer probe.Close()
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return errors.New("cannot create database pool")
	}
	defer pool.Close()
	probe.Attach(pool)
	defer probe.Close()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	var role, db string
	err = pool.QueryRow(startup, "SELECT current_user,current_database()").Scan(&role, &db)
	cancel()
	if err != nil || role != "loader_runtime" || db != "ethereum_etl" {
		return errors.New("database requires loader_runtime in ethereum_etl")
	}
	registry := metrics.New("process_inbox", "claim_inbox", "persist_block")
	store, err := postgres.NewWithBatchSize(pool, cfg.ChainID, cfg.Lease, cfg.MaxAttempts, cfg.AdmissionCapacity, cfg.SQLBatchSize)
	if err != nil {
		return err
	}
	if cfg.Repair != nil {
		err := usecases.Repair(ctx, string(cfg.ChainID), store, *cfg.Repair)
		outcome, code := "requeued", ""
		if err != nil {
			outcome = "failed"
			code = logging.ErrorCode(err)
		}
		logevent.Write(ctx, logger, logevent.Event{Service: "loader-service", Operation: "repair_" + cfg.Repair.Queue, EventID: cfg.Repair.EventID, ChainID: string(cfg.ChainID), Outcome: outcome, ErrorCode: code}, started)
		return err
	}
	accept, err := usecases.NewAccept(cfg.ChainID, cfg.Limits.PayloadBytes, store)
	if err != nil {
		return err
	}
	process, err := usecases.NewProcess(persistmetrics.Queue{Queue: store, Registry: registry}, jsondocument.Decoder{}, cfg.InitialBackoff, cfg.MaxBackoff)
	if err != nil {
		return err
	}
	runner, err := worker.New(process, logger, cfg.Workers, cfg.PollInterval, cfg.ProcessTimeout, registry)
	if err != nil {
		return err
	}
	gate, err := grpccontract.NewAdmission(cfg.Resources, cfg.Limits, registry)
	if err != nil {
		return err
	}
	server, err := receiver.NewServerWithAdmission(string(cfg.ChainID), cfg.Limits, accept, gate, logger)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return errors.New("cannot open gRPC listener")
	}
	listener = gate.LimitListener(listener)
	defer listener.Close()
	closeHealth, err := health.Start(ctx, os.Getenv("LOADER_HEALTH_ADDRESS"), store.CheckReadiness, servicemetrics.Handler(store, registry))
	if err != nil {
		return err
	}
	defer closeHealth()
	working, stop := context.WithCancel(ctx)
	defer stop()
	workerDone := make(chan struct{})
	go func() { runner.Run(working); close(workerDone) }()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	logevent.Write(ctx, logger, logevent.Event{Service: "loader-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "started"}, started)
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
	logevent.Write(ctx, logger, logevent.Event{Service: "loader-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "stopped"}, started)
	if serveErr != nil {
		return errors.New("gRPC serving failed")
	}
	return nil
}
