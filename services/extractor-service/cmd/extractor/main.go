// main.go implements command startup, explicit adapter wiring and bounded resource lifetime in the
// service composition/operator entry point.

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/baseline"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/health"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/dispatcher"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/indexer"
	servicemetrics "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/logging"
	publisher "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/postgres"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// main invokes the bounded service/operator entry point and exits nonzero on startup or execution
// failure.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	started := time.Now()
	cfg, err := config.Load()
	if err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: "configure", Outcome: "failed", ErrorCode: "invalid_configuration"}, started)
		os.Exit(1)
	}
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	if err = run(ctx, cfg, logger); err != nil {
		logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: "run", Outcome: logevent.FailureOutcome(logging.ErrorCode(err)), ErrorCode: logging.ErrorCode(err)}, started)
		os.Exit(1)
	}
}

// run wires the configured service adapters/use cases, starts bounded workers/listeners and joins
// resources during shutdown.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return runWithinBudget(ctx, cfg.Indexing.RunTimeout, func(ctx context.Context) error { return runServices(ctx, cfg, logger) })
}

// The budget starts before database setup and includes waits, retries and shutdown.
// Cancellation can leave a COMMIT outcome unknown; existing durable recovery applies.
func runWithinBudget(ctx context.Context, duration time.Duration, operation func(context.Context) error) error {
	if duration == 0 {
		return operation(ctx)
	}
	limited, cancel := context.WithTimeoutCause(ctx, duration, outbound.ErrRunBudgetExceeded)
	defer cancel()
	err := operation(limited)
	if errors.Is(context.Cause(limited), outbound.ErrRunBudgetExceeded) {
		return errors.Join(err, outbound.ErrRunBudgetExceeded)
	}
	return err
}

// runServices wires the configured service adapters/use cases, starts bounded workers/listeners
// and joins resources during shutdown.
func runServices(ctx context.Context, cfg config.Config, logger *slog.Logger) (returnedErr error) {
	started := time.Now()
	// Replay names an event before any database I/O. Keep that identity even if
	// connection/configuration setup fails before the application call.
	if cfg.Indexing.Mode == "replay" {
		defer func() {
			code, outcome := logging.ErrorCode(returnedErr), "requeued"
			if returnedErr != nil {
				outcome = logevent.FailureOutcome(code)
			}
			logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: "replay_outbox", EventID: cfg.Indexing.Replay.EventID, ErrorCode: code, Outcome: outcome}, started)
		}()
	}
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	pc.MaxConns = int32(cfg.PoolConnections)
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
	if err != nil || role != "extractor_runtime" || db != "ethereum_etl" {
		return errors.New("database requires extractor_runtime in ethereum_etl")
	}
	registry := metrics.New("fetch_receipts", "index_cycle", "reindex_cycle", "index_block", "reindex_block", "dispatch_outbox", "claim_outbox")
	store, err := postgres.New(pool, cfg.ChainID, cfg.Indexing.Options.Start)
	if err != nil {
		return err
	}
	if cfg.Indexing.Mode == "replay" {
		return usecases.Replay(ctx, store, cfg.Indexing.Replay)
	}

	closeHealth, err := health.Start(ctx, os.Getenv("EXTRACTOR_HEALTH_ADDRESS"), store.CheckReadiness, servicemetrics.Handler(store, registry))
	if err != nil {
		return err
	}
	defer closeHealth()
	var acquisition *indexer.Worker
	if cfg.Indexing.Mode == "index" || cfg.Indexing.Mode == "reindex" {
		source, e := jsonrpc.New(cfg.Indexing.RPC, nil, registry)
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
		acquisition, err = indexer.New(scheduler, logger, cfg.Indexing.Poll, cfg.Indexing.Timeout, cfg.Indexing.MaxErrors, registry)
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
	runner, err := dispatcher.New(dispatch, logger, cfg.Dispatch.Workers, cfg.Dispatch.PollInterval, cfg.Dispatch.Timeout, registry)
	if err != nil {
		return err
	}
	logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "started"}, started)
	if acquisition == nil && cfg.Indexing.Mode != "drain" {
		runner.Run(ctx)
	} else {
		acquire := func(context.Context) error { return nil }
		if acquisition != nil {
			acquire = acquisition.Run
		}
		err = runAcquisition(ctx, cfg.Indexing.Options.End != nil, acquire, runner.Run, func(work context.Context) error {
			if cfg.Indexing.Options.End == nil {
				return nil
			}
			drainStarted := time.Now()
			drainErr := usecases.Drain(work, store, cfg.Indexing.Options.Start, *cfg.Indexing.Options.End, cfg.Dispatch.PollInterval, cfg.Indexing.DrainTimeout)
			code, outcome := logging.ErrorCode(drainErr), "delivered"
			if drainErr != nil {
				outcome = logevent.FailureOutcome(code)
			}
			logevent.Write(work, logger, logevent.Event{Service: "extractor-service", Operation: "drain_outbox", ChainID: string(cfg.ChainID), Outcome: outcome, ErrorCode: code}, drainStarted)
			return drainErr
		})
	}
	logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: "run", ChainID: string(cfg.ChainID), Outcome: "stopped"}, started)
	return err
}
