// worker_test.go exercises bounded application polling with cancellation, progress logging and
// worker joining in the inbound adapter layer.

package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/adapters/inbound/worker"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
)

// processorFunc adapts a test callback to one worker/dispatcher application operation.
type processorFunc func(context.Context) (inbound.ProcessResult, error)

// ProcessOne delegates to the injected test callback, allowing controlled completion, blocking or
// failure at the application port.
func (f processorFunc) ProcessOne(ctx context.Context) (inbound.ProcessResult, error) { return f(ctx) }

// await waits within the test's bounded deadline for asynchronous progress; timeout is a test
// failure.
func await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not make progress or stop")
	}
}

// TestBoundedConcurrencyAndJoinedShutdown holds worker operations in flight to verify pool bounds
// and that cancellation waits for worker exit.
func TestBoundedConcurrencyAndJoinedShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 16)
	var active, maximum atomic.Int32
	p := processorFunc(func(ctx context.Context) (inbound.ProcessResult, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-ctx.Done()
		return inbound.ProcessResult{}, ctx.Err()
	})
	w, err := worker.New(p, slog.New(slog.NewTextHandler(io.Discard, nil)), 3, time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	for range 3 {
		await(t, entered)
	}
	cancel()
	await(t, done)
	if maximum.Load() != 3 || active.Load() != 0 {
		t.Fatal("concurrency limit or joined shutdown violated", maximum.Load(), active.Load())
	}
}

// TestDeadlineIdleAndErrorPolling checks worker operation deadlines and idle/error polling prevent
// unbounded blocking or tight failure loops.
func TestDeadlineIdleAndErrorPolling(t *testing.T) {
	for _, mode := range []string{"idle", "error", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			called := make(chan struct{}, 16)
			var calls atomic.Int32
			p := processorFunc(func(ctx context.Context) (inbound.ProcessResult, error) {
				calls.Add(1)
				var err error
				if mode == "deadline" {
					<-ctx.Done()
					err = ctx.Err()
				}
				if mode == "error" {
					err = errors.New("unavailable")
				}
				called <- struct{}{}
				return inbound.ProcessResult{}, err
			})
			w, err := worker.New(p, slog.New(slog.NewTextHandler(io.Discard, nil)), 1, time.Hour, 10*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { w.Run(ctx); close(done) }()
			await(t, called)
			cancel()
			await(t, done)
			if calls.Load() != 1 {
				t.Fatal("empty/error queue busy loop", calls.Load())
			}
		})
	}
}

// TestPendingWorkDrainsWithoutPollDelay checks available work is processed again immediately while
// idle/error cases retain their polling delay.
func TestPendingWorkDrainsWithoutPollDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int32
	p := processorFunc(func(context.Context) (inbound.ProcessResult, error) {
		if calls.Add(1) == 3 {
			cancel()
		}
		return inbound.ProcessResult{Found: true, Outcome: "completed"}, nil
	})
	w, err := worker.New(p, slog.New(slog.NewTextHandler(io.Discard, nil)), 1, time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	await(t, done)
	if calls.Load() != 3 {
		t.Fatal(calls.Load())
	}
}
