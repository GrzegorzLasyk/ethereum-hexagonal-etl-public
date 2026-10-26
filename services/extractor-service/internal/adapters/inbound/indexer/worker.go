// Package indexer schedules bounded application cycles. It owns no SQL or RPC.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type Worker struct {
	metrics       *metrics.Registry
	usecase       inbound.Indexer
	logger        *slog.Logger
	poll, timeout time.Duration
	maxErrors     int
}

func New(u inbound.Indexer, logger *slog.Logger, poll, timeout time.Duration, maxErrors int, registries ...*metrics.Registry) (*Worker, error) {
	if u == nil || logger == nil || poll <= 0 || timeout <= 0 || maxErrors < 1 {
		return nil, domain.ErrInvalidData
	}
	var registry *metrics.Registry
	if len(registries) > 0 {
		registry = registries[0]
	}
	return &Worker{registry, u, logger, poll, timeout, maxErrors}, nil
}
func (w *Worker) Run(ctx context.Context) error {
	failures := 0
	for ctx.Err() == nil {
		started := time.Now()
		call, cancel := context.WithTimeout(ctx, w.timeout)
		result, err := w.usecase.Cycle(call)
		cancel()
		writeProgress(ctx, w.logger, result, err, started)
		if err == nil {
			w.metrics.Cycle(result.Backpressure)
		}
		if result.Head != nil {
			w.metrics.Head(*result.Head, result.HeadObservedAt)
		}
		operation, blockOperation, outcome := "index_cycle", "index_block", "progress"
		if result.Reindex {
			operation, blockOperation = "reindex_cycle", "reindex_block"
		}
		for _, block := range result.Completed {
			w.metrics.Observe(blockOperation, "completed", block.Duration)
		}
		switch {
		case result.Backpressure:
			outcome = "backpressure"
		case result.Waiting:
			outcome = "waiting"
		case result.Done:
			outcome = "done"
		case result.Rewound:
			outcome = "rewound"
		}
		if err != nil {
			outcome = logevent.FailureOutcome(logging.ErrorCode(err))
		}
		w.metrics.Observe(operation, outcome, time.Since(started))
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, domain.ErrBackpressure) {
			failures++
			if errors.Is(err, domain.ErrRecoveryRequired) || errors.Is(err, outbound.ErrRetryDeferred) || failures >= w.maxErrors {
				return fmt.Errorf("indexing halted: %w", err)
			}
		} else {
			failures = 0
		}
		if err == nil {
			if result.Done {
				return nil
			}
		}
		timer := time.NewTimer(w.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func writeProgress(ctx context.Context, logger *slog.Logger, result inbound.IndexResult, err error, started time.Time) {
	operation := "index_block"
	if result.Reindex {
		operation = "reindex_block"
	}
	for _, committed := range result.Completed {
		logevent.Write(ctx, logger, logevent.Event{Service: "extractor-service", Operation: operation, EventID: committed.Block.EventID(), Outcome: "completed", Duration: &committed.Duration}, started)
	}
	event := logevent.Event{Service: "extractor-service", Operation: operation, ChainID: result.ChainID, BlockNumber: result.Next, Outcome: "progress", Duration: &result.Duration}
	if result.Block != nil {
		event.EventID = result.Block.EventID()
	}
	if result.Recovering {
		event.Operation = "reorg_recovery"
	}
	switch {
	case result.Rewound:
		event.Outcome = "rewound"
	case result.Backpressure:
		event.Outcome = "backpressure"
	case result.Waiting:
		event.Outcome = "waiting"
	case result.Done:
		event.Outcome = "done"
	}
	if err != nil {
		event.ErrorCode = logging.ErrorCode(err)
		event.Outcome = logevent.FailureOutcome(event.ErrorCode)
	}
	// The ancestor is a separate correlated operation; it does not declare finality.
	if result.Ancestor != nil {
		ancestor := event
		ancestor.Operation, ancestor.EventID = "rewind_checkpoint", result.Ancestor.EventID()
		logevent.Write(ctx, logger, ancestor, started)
	}
	if err != nil || len(result.Completed) == 0 || result.Recovering || result.Done || result.Waiting || result.Backpressure {
		logevent.Write(ctx, logger, event, started)
	}
}
