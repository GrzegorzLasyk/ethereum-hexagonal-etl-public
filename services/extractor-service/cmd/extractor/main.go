package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/health"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/dispatcher"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/indexer"
	publisher "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/postgres"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("service", "extractor-service")
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "extractor-service")
	if err = run(ctx, cfg, logger); err != nil {
		logger.Error("extractor stopped", "error", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	pc.MaxConns = int32(cfg.Dispatch.Workers + 4)
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
	if err != nil || role != "extractor_runtime" || db != "ethereum_etl" {
		return errors.New("database requires extractor_runtime in ethereum_etl")
	}
	store, err := postgres.New(pool, cfg.ChainID, cfg.Indexing.Options.Start)
	if err != nil {
		return err
	}
	if cfg.Indexing.Mode == "replay" {
		return usecases.Replay(ctx, store, cfg.Indexing.Replay)
	}
	closeHealth, err := health.Start(ctx, os.Getenv("EXTRACTOR_HEALTH_ADDRESS"), pool.Ping)
	if err != nil {
		return err
	}
	defer closeHealth()
	var acquisition *indexer.Worker
	if cfg.Indexing.Mode == "index" || cfg.Indexing.Mode == "reindex" {
		source, e := jsonrpc.New(cfg.Indexing.RPC, nil)
		if e != nil {
			return e
		}
		var scheduler inbound.Indexer
		if cfg.Indexing.Mode == "index" {
			scheduler, err = usecases.NewIndex(cfg.Indexing.Options, source, source, store)
		} else {
			scheduler, err = usecases.NewReindex(cfg.Indexing.Options, cfg.Indexing.OperationID, source, source, store)
		}
		if err != nil {
			return err
		}
		acquisition, err = indexer.New(scheduler, logger, cfg.Indexing.Poll, cfg.Indexing.Timeout, cfg.Indexing.MaxErrors)
		if err != nil {
			return err
		}
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
	runner, err := dispatcher.New(dispatch, logger, cfg.Dispatch.Workers, cfg.Dispatch.PollInterval, cfg.Dispatch.Timeout)
	if err != nil {
		return err
	}
	logger.Info("extractor dispatcher started", "chain_id", cfg.ChainID, "workers", cfg.Dispatch.Workers)
	if acquisition == nil {
		runner.Run(ctx)
	} else {
		work, cancel := context.WithCancel(ctx)
		var group sync.WaitGroup
		group.Go(func() { runner.Run(work) })
		err = acquisition.Run(work)
		cancel()
		group.Wait()
	}
	logger.Info("extractor dispatcher stopped")
	return err
}
