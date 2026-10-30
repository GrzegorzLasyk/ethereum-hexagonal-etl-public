package outbound

import (
	"context"
	"errors"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

var (
	ErrLeaseLost     = errors.New("processing lease lost")
	ErrCommitUnknown = errors.New("commit outcome unknown")
)

type Item struct {
	Event       inbound.AcceptTransformedBlockRequest
	Attempt     int
	MaxAttempts int
}
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
	Accept(context.Context, inbound.AcceptTransformedBlockRequest) (alreadyAccepted bool, err error)
}

// Queue claims at most one item with a bounded lease and incremented attempt.
// Attempt is a fencing token: stale/expired claimants cannot Complete or Fail.
// Complete atomically stores normalized records, domain conflict/parent-link
// observations for affected local candidates, and Inbox completion. Successful
// completion never implies canonicality; all candidates remain unknown.
// Fail records a safe failure code, retry time or terminal failure.
type Queue interface {
	Claim(context.Context) (*Item, error)
	Complete(context.Context, Item, domain.Document) error
	Fail(context.Context, Item, Failure) error
}
type Decoder interface {
	Decode(context.Context, []byte) (domain.Document, error)
}
