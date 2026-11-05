// processing.go defines claimed-input, completion and failure contracts for
// background workers in the application outbound boundary.

package outbound

import (
	"context"
	"errors"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/domain"
)

var (
	// ErrLeaseLost signals processing lease lost. Callers can match this category with errors.Is
	// without parsing error text.
	ErrLeaseLost = errors.New("processing lease lost")
	// ErrCommitUnknown signals an uncertain commit outcome; callers must not infer rollback
	// from a lost COMMIT acknowledgement.
	ErrCommitUnknown = errors.New("commit outcome unknown")
	// ErrOutputTooLarge signals transformed payload exceeds byte limit. Callers can match this
	// category with errors.Is without parsing error text.
	ErrOutputTooLarge = errors.New("transformed payload exceeds byte limit")
)

// Item pairs accepted immutable Inbox input with the claimant attempt/allowance used for fencing.
type Item struct {
	Event       inbound.AcceptRawBlockRequest
	Attempt     int
	MaxAttempts int
}

// Result carries the transformed event identity, payload version and encoded document for atomic
// Inbox/Outbox completion; the snapshot and provenance are inside the document.
type Result struct {
	EventID        string
	PayloadVersion uint32
	Payload        []byte
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
	Accept(context.Context, inbound.AcceptRawBlockRequest) (alreadyAccepted bool, err error)
}

// Queue claims at most one item with a bounded lease and incremented attempt.
// Attempt is a fencing token: stale/expired claimants cannot Complete or Fail.
// Complete atomically stores the immutable result, outbox intent and completion.
// Fail records a safe failure code, retry time or terminal failure.
type Queue interface {
	// Claim claims at most one Inbox item under a new lease/attempt fence; nil means no due work.
	Claim(context.Context) (*Item, error)
	// Complete atomically stores the local result and completes the matching live Inbox claim;
	// ambiguous commit remains explicit.
	Complete(context.Context, Item, Result) error
	// Fail records a safe failure/retry under the claim fence without replacing immutable input.
	Fail(context.Context, Item, Failure) error
}

// Decoder defines representation conversion into service-owned values. The application remains
// responsible for domain and envelope validation after decoding.
type Decoder interface {
	// Decode parses the boundary representation into service-owned values, preserving exact
	// quantities and optional semantics; domain validation follows in the application.
	Decode(context.Context, []byte, [][]byte, domain.ChainID) (domain.Snapshot, error)
}

// Encoder defines normalized document encoding at the application boundary without coupling use
// cases to JSON.
type Encoder interface {
	// Encode produces bounded versioned normalized JSON with original source provenance.
	Encode(context.Context, domain.Snapshot, domain.Provenance) ([]byte, error)
}
