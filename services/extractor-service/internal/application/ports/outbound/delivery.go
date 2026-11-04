// delivery.go defines contracts for immutable publication, claim-fence and safe delivery-error
// contracts in the application outbound boundary.

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
	RawBlock                   []byte
	RawReceipts                [][]byte
}

// Delivery pairs an immutable publication with its durable attempt/allowance fence.
type Delivery struct {
	Event                Publication
	Attempt, MaxAttempts int
}

// DeliveryFailure records a safe error category, retry delay and terminal/transient decision
// without payload details.
type DeliveryFailure struct {
	Code      string
	Permanent bool
	Delay     time.Duration
}

var (
	// ErrDeliveryLeaseLost signals outbox delivery lease lost. Callers can match this category with
	// errors.Is without parsing error text.
	ErrDeliveryLeaseLost = errors.New("outbox delivery lease lost")
	// ErrDeliveryCommitUnknown signals outbox commit outcome unknown. Callers can match this category
	// with errors.Is without parsing error text.
	ErrDeliveryCommitUnknown = errors.New("outbox commit outcome unknown")
	// ErrPublishInvalid signals publication rejected. Callers can match this category with errors.Is
	// without parsing error text.
	ErrPublishInvalid = errors.New("publication rejected")
	// ErrPublishConflict signals publication content conflict. Callers can match this category with
	// errors.Is without parsing error text.
	ErrPublishConflict = errors.New("publication content conflict")
	// ErrPublishOversize signals publication exceeds transport budget. Callers can match this category
	// with errors.Is without parsing error text.
	ErrPublishOversize = errors.New("publication exceeds transport budget")
	// ErrPublishOverloaded signals receiver admission overloaded. Callers can match this category with
	// errors.Is without parsing error text.
	ErrPublishOverloaded = errors.New("receiver admission overloaded")
	// ErrPublishUnavailable signals publication temporarily unavailable. Callers can match this
	// category with errors.Is without parsing error text.
	ErrPublishUnavailable = errors.New("publication temporarily unavailable")
	// ErrPublishACK signals invalid durable acknowledgement. Callers can match this category with
	// errors.Is without parsing error text.
	ErrPublishACK = errors.New("invalid durable acknowledgement")
)

// DeliveryQueue commits claims before returning. Attempts fence all state
// updates. Published requires an unexpired claim and a verified durable ACK.
// Unknown commit outcomes must not be followed by another state transition.
type DeliveryQueue interface {
	// ClaimDelivery claims at most one due/expired Outbox item with a fresh attempt/lease; nil means
	// idle.
	ClaimDelivery(context.Context) (*Delivery, error)
	// Published marks only the matching live claim delivered after its verified durable ACK.
	Published(context.Context, Delivery) error
	// RetryDelivery retains the event and records fenced retry timing or terminal failure.
	RetryDelivery(context.Context, Delivery, DeliveryFailure) error
}

// Publisher returns nil ONLY for durable/already-durable acceptance with the
// matching event ID. It must never mutate content or regenerate provenance.
type Publisher interface {
	// Publish transports one immutable publication and requires a matching durable acceptance ACK; it
	// does not own durable retry state.
	Publish(context.Context, Publication) error
}
