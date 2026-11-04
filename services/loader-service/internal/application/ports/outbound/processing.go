// processing.go defines claimed-input, completion and failure contracts for
// background workers in the application outbound boundary.

// Package outbound defines consumer-owned acquisition, persistence or delivery dependencies with
// explicit local consistency boundaries.
package outbound

import (
	"context"
	"errors"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

var (
	// ErrLeaseLost signals processing lease lost. Callers can match this category with errors.Is
	// without parsing error text.
	ErrLeaseLost = errors.New("processing lease lost")
	// ErrCommitUnknown signals an uncertain commit outcome; callers must not infer rollback
	// from a lost COMMIT acknowledgement.
	ErrCommitUnknown = errors.New("commit outcome unknown")
)

// Item pairs accepted immutable Inbox input with the claimant attempt/allowance used for fencing.
type Item struct {
	Event       inbound.AcceptTransformedBlockRequest
	Attempt     int
	MaxAttempts int
}

// Failure describes a safe processing error category, retry delay and whether the input is
// permanently invalid.
type Failure struct {
	Code      string
	Permanent bool
	Delay     time.Duration
}

// Inbox owns local durability and exact-content deduplication before ACK.
// Inbox admission must atomically serialize new events against the owning
// service capacity. Identical durable duplicates bypass capacity, conflicts do
// not ACK, and ErrOverloaded leaves no new row. Transformer completion transfers
// outstanding work to its Outbox without releasing a capacity slot.
type Inbox interface {
	// Accept commits new input or reports exact already-durable input; conflicting content under one
	// ID must fail.
	Accept(context.Context, inbound.AcceptTransformedBlockRequest) (alreadyAccepted bool, err error)
}

// Queue claims at most one item with a bounded lease and incremented attempt.
// Attempt is a fencing token: stale/expired claimants cannot Complete or Fail.
// Complete atomically stores normalized records, domain conflict/parent-link
// observations for affected local candidates, and Inbox completion. Successful
// completion never implies canonicality; all candidates remain unknown.
// Fail records a safe failure code, retry time or terminal failure.
type Queue interface {
	// Claim claims at most one Inbox item under a new lease/attempt fence; nil means no due work.
	Claim(context.Context) (*Item, error)
	// Complete atomically stores the local result and completes the matching live Inbox claim;
	// ambiguous commit remains explicit.
	Complete(context.Context, Item, domain.Document) error
	// Fail records a safe failure/retry under the claim fence without replacing immutable input.
	Fail(context.Context, Item, Failure) error
}

// Decoder defines representation conversion into service-owned values. The application remains
// responsible for domain and envelope validation after decoding.
type Decoder interface {
	// Decode parses the boundary representation into service-owned values, preserving exact
	// quantities and optional semantics; domain validation follows in the application.
	Decode(context.Context, []byte) (domain.Document, error)
}
