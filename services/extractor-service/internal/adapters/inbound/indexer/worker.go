// Package indexer schedules bounded application cycles. It owns no SQL or RPC.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type Worker struct {
	usecase       inbound.Indexer
	logger        *slog.Logger
	poll, timeout time.Duration
	maxErrors     int
}

func New(u inbound.Indexer, logger *slog.Logger, poll, timeout time.Duration, maxErrors int) (*Worker, error) {
	if u == nil || logger == nil || poll <= 0 || timeout <= 0 || maxErrors < 1 {
		return nil, domain.ErrInvalidData
	}
	return &Worker{u, logger, poll, timeout, maxErrors}, nil
}
func (w *Worker) Run(ctx context.Context) error {
	failures := 0
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, w.timeout)
		result, err := w.usecase.Cycle(call)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, domain.ErrBackpressure) {
			failures++
			w.logger.Error("indexing cycle failed", "consecutive_failures", failures, "error", err)
			if errors.Is(err, domain.ErrRecoveryRequired) || errors.Is(err, outbound.ErrRetryDeferred) || failures >= w.maxErrors {
				return fmt.Errorf("indexing halted: %w", err)
			}
		} else {
			failures = 0
		}
		if err == nil {
			w.logger.Info("indexing progress", "processed", result.Processed, "rewound", result.Rewound, "waiting", result.Waiting, "backpressure", result.Backpressure, "done", result.Done)
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
