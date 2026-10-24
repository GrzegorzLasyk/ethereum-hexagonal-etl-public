package indexer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type cycleFunc func(context.Context) (inbound.IndexResult, error)

func (f cycleFunc) Cycle(ctx context.Context) (inbound.IndexResult, error) { return f(ctx) }
func TestWorkerBoundedFailureAndRecoveryHalt(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{{errors.New("source unavailable"), 3}, {domain.ErrRecoveryRequired, 1}, {outbound.ErrRetryDeferred, 1}} {
		calls := 0
		w, err := New(cycleFunc(func(context.Context) (inbound.IndexResult, error) { calls++; return inbound.IndexResult{}, tc.err }), slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond, time.Second, 3)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Run(t.Context()); !errors.Is(err, tc.err) || calls != tc.want {
			t.Fatal(calls, err)
		}
	}
}
func TestWorkerWaitsForCancellationAndReturnsOnDone(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	calls := 0
	w, _ := New(cycleFunc(func(context.Context) (inbound.IndexResult, error) {
		calls++
		return inbound.IndexResult{Done: true}, nil
	}), logger, time.Hour, time.Second, 2)
	if err := w.Run(t.Context()); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan struct{})
	w, _ = New(cycleFunc(func(c context.Context) (inbound.IndexResult, error) {
		cancel()
		<-c.Done()
		close(finished)
		return inbound.IndexResult{}, c.Err()
	}), logger, time.Hour, time.Second, 2)
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("worker returned before in-flight operation")
	}
}
func TestWorkerBackpressureDoesNotExhaustFailureBudget(t *testing.T) {
	calls := 0
	w, _ := New(cycleFunc(func(context.Context) (inbound.IndexResult, error) {
		calls++
		if calls < 4 {
			return inbound.IndexResult{}, domain.ErrBackpressure
		}
		return inbound.IndexResult{Done: true}, nil
	}), slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond, time.Second, 1)
	if err := w.Run(t.Context()); err != nil || calls != 4 {
		t.Fatal(err, calls)
	}
}
