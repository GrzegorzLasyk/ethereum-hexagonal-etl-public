package outbound

import (
	"context"
	"errors"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/domain"
)

var (
	ErrLeaseLost      = errors.New("processing lease lost")
	ErrCommitUnknown  = errors.New("commit outcome unknown")
	ErrOutputTooLarge = errors.New("transformed payload exceeds byte limit")
)

type Item struct {
	Event       inbound.AcceptRawBlockRequest
	Attempt     int
	MaxAttempts int
}
type Result struct {
	EventID        string
	PayloadVersion uint32
	Payload        []byte
}
type Failure struct {
	Code      string
	Permanent bool
	Delay     time.Duration
}

// Inbox owns local durability and exact-content deduplication before ACK.
type Inbox interface {
	Accept(context.Context, inbound.AcceptRawBlockRequest) (alreadyAccepted bool, err error)
}

// Queue claims at most one item with a bounded lease and incremented attempt.
// Attempt is a fencing token: stale/expired claimants cannot Complete or Fail.
// Complete atomically stores the immutable result, outbox intent and completion.
// Fail records a safe failure code, retry time or terminal failure.
type Queue interface {
	Claim(context.Context) (*Item, error)
	Complete(context.Context, Item, Result) error
	Fail(context.Context, Item, Failure) error
}
type Decoder interface {
	Decode(context.Context, []byte, [][]byte, domain.ChainID) (domain.Snapshot, error)
}
type Encoder interface {
	Encode(context.Context, domain.Snapshot, domain.Provenance) ([]byte, error)
}
