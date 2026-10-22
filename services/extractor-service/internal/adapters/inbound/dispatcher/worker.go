package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
)

type Worker struct {
	processor     inbound.Dispatcher
	logger        *slog.Logger
	concurrency   int
	poll, timeout time.Duration
}

func New(p inbound.Dispatcher, logger *slog.Logger, concurrency int, poll, timeout time.Duration) (*Worker, error) {
	if p == nil || logger == nil || concurrency < 1 || concurrency > 8 || poll <= 0 || timeout <= 0 {
		return nil, fmt.Errorf("invalid worker configuration")
	}
	return &Worker{p, logger, concurrency, poll, timeout}, nil
}

// Run owns bounded polling goroutines and waits for all delivery calls to
// return after cancellation. Persistence leases recover interrupted attempts.
func (w *Worker) Run(ctx context.Context) {
	var group sync.WaitGroup
	for range w.concurrency {
		group.Go(func() {
			for ctx.Err() == nil {
				call, cancel := context.WithTimeout(ctx, w.timeout)
				result, err := w.processor.DispatchOne(call)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					w.logger.Error("outbox delivery attempt interrupted")
				} else if result.Found {
					w.logger.Info("outbox progress", "event_id", result.EventID, "attempt", result.Attempt, "outcome", result.Outcome)
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
