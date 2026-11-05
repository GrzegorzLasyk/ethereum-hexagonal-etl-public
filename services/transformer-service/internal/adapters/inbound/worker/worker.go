// worker.go implements bounded application polling with cancellation, progress logging and worker
// joining in the inbound adapter layer.

// Package worker provides bounded application polling with cancellation, progress logging and
// worker joining in the inbound adapter layer.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/inbound"
)

// Worker owns bounded polling/concurrency policy around an injected application operation and its
// logger/metrics.
type Worker struct {
	metrics       *metrics.Registry
	processor     inbound.Processor
	logger        *slog.Logger
	concurrency   int
	poll, timeout time.Duration
}

// New validates worker counts and polling/deadline policy before retaining the injected
// application operation.
func New(p inbound.Processor, logger *slog.Logger, concurrency int, poll, timeout time.Duration, registries ...*metrics.Registry) (*Worker, error) {
	if p == nil || logger == nil || concurrency < 1 || concurrency > 8 || poll <= 0 || timeout <= 0 {
		return nil, fmt.Errorf("invalid worker configuration")
	}
	var registry *metrics.Registry
	if len(registries) > 0 {
		registry = registries[0]
	}
	return &Worker{registry, p, logger, concurrency, poll, timeout}, nil
}

// Run owns bounded polling goroutines and waits for all processing calls to
// return after cancellation. Persistence leases recover interrupted attempts.
func (w *Worker) Run(ctx context.Context) {
	var group sync.WaitGroup
	for range w.concurrency {
		group.Go(func() {
			for ctx.Err() == nil {
				started := time.Now()
				call, cancel := context.WithTimeout(ctx, w.timeout)
				result, err := w.processor.ProcessOne(call)
				cancel()
				if err != nil || result.Found {
					code, outcome := result.ErrorCode, result.Outcome
					if err != nil {
						code = logging.ErrorCode(err)
						outcome = logevent.FailureOutcome(code)
					}
					operation := "process_inbox"
					if !result.Found {
						operation = "claim_inbox"
					}
					w.metrics.Observe(operation, outcome, time.Since(started))
					logevent.Write(ctx, w.logger, logevent.Event{Service: "transformer-service", Operation: "process_inbox", EventID: result.EventID, SourceEventID: result.SourceEventID, Attempt: result.Attempt, Outcome: outcome, ErrorCode: code}, started)
				}
				if ctx.Err() != nil {
					return
				}
				if err == nil && result.Found {
					continue
				}
				timer := time.NewTimer(w.poll)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		})
	}
	group.Wait()
}
