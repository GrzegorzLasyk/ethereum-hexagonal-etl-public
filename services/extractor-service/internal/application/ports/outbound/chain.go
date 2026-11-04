// chain.go defines contracts for provider head/header observations for ordered extraction and
// recovery in the application outbound boundary.

// Package outbound defines consumer-owned acquisition, persistence or delivery dependencies with
// explicit local consistency boundaries.
package outbound

import (
	"context"
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ErrRetryDeferred prevents a scheduling loop from bypassing a provider cooldown.
var ErrRetryDeferred = errors.New("provider cooldown requires a later operator restart")

// ErrSourcePermanent requires operator correction before another acquisition run.
// Adapters retain protocol diagnostics locally; workers depend only on this category.
var ErrSourcePermanent = errors.New("source request permanently rejected")

// ErrRPCBudgetExceeded signals a terminal acquisition attempt-budget stop.
//
// Execution budgets halt acquisition; they never authorize a checkpoint reset.
var ErrRPCBudgetExceeded = errors.New("RPC request budget exhausted")

// ErrRunBudgetExceeded signals execution time budget exhausted. Callers can match this category
// with errors.Is without parsing error text.
var ErrRunBudgetExceeded = errors.New("execution time budget exhausted")

// ChainObserver reads the configured provider's current branch. Observations
// are not finality proofs or a consistent snapshot across separate calls.
// Missing blocks are errors, never a signal to rewind durable progress.
type ChainObserver interface {
	// Head observes the current provider head without asserting consensus finality.
	Head(context.Context) (domain.Header, error)
	// Header reads a numbered provider identity/parent for bounded continuity and recovery checks.
	Header(context.Context, uint64) (domain.Header, error)
}
