package outbound

import (
	"context"
	"errors"
	"time"
)

// Publication contains immutable, locally committed event content. It is owned
// by this service, independently of protobuf and the receiving service domain.
type Publication struct {
	EventID, ChainID, Provider string
	Number                     uint64
	Hash, ParentHash           [32]byte
	ExtractedAt                time.Time
	Payload                    []byte
	SourceEventID              string
	TransformedAt              time.Time
}
type Delivery struct {
	Event                Publication
	Attempt, MaxAttempts int
}
type DeliveryFailure struct {
	Code      string
	Permanent bool
	Delay     time.Duration
}

var (
	ErrDeliveryLeaseLost     = errors.New("outbox delivery lease lost")
	ErrDeliveryCommitUnknown = errors.New("outbox commit outcome unknown")
	ErrPublishInvalid        = errors.New("publication rejected")
	ErrPublishConflict       = errors.New("publication content conflict")
	ErrPublishOversize       = errors.New("publication exceeds transport budget")
	ErrPublishUnavailable    = errors.New("publication temporarily unavailable")
	ErrPublishACK            = errors.New("invalid durable acknowledgement")
)

// DeliveryQueue commits claims before returning. Attempts fence all state
// updates. Published requires an unexpired claim and a verified durable ACK.
// Unknown commit outcomes must not be followed by another state transition.
type DeliveryQueue interface {
	ClaimDelivery(context.Context) (*Delivery, error)
	Published(context.Context, Delivery) error
	RetryDelivery(context.Context, Delivery, DeliveryFailure) error
}

// Publisher returns nil ONLY for durable/already-durable acceptance with the
// matching event ID. It must never mutate content or regenerate provenance.
type Publisher interface {
	Publish(context.Context, Publication) error
}
