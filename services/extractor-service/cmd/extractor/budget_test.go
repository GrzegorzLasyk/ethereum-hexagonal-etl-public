package main

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
)

func TestRunBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		joined := false
		err := runWithinBudget(t.Context(), time.Minute, func(ctx context.Context) error {
			done := make(chan struct{})
			go func() { defer close(done); <-ctx.Done() }()
			<-ctx.Done()
			<-done
			joined = true
			return nil // Worker shutdown itself is graceful, not range completion.
		})
		if !joined || !errors.Is(err, outbound.ErrRunBudgetExceeded) {
			t.Fatal("deadline reported as success", err)
		}
	})
	if err := runWithinBudget(t.Context(), time.Minute, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runWithinBudget(parent, time.Minute, func(ctx context.Context) error { return ctx.Err() }); !errors.Is(err, context.Canceled) || errors.Is(err, outbound.ErrRunBudgetExceeded) {
		t.Fatal(err)
	}
}
