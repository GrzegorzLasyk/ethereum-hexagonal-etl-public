// drain.go defines contracts for bounded verification of durable outgoing range delivery in the
// application outbound boundary.

package outbound

import (
	"context"
	"errors"
)

// ErrDeliveryIncomplete signals range delivery incomplete. Callers can match this category with
// errors.Is without parsing error text.
var ErrDeliveryIncomplete = errors.New("range delivery incomplete")

// ErrDeliveryFailed signals range contains failed delivery. Callers can match this category with
// errors.Is without parsing error text.
var ErrDeliveryFailed = errors.New("range contains failed delivery")

// RangeDelivery separates complete durable range delivery from retained terminal failure; neither
// state is inferred from readiness.
type RangeDelivery struct{ Complete, Failed bool }

// RangeDeliveryReader observes only Extractor-owned history and handoff state.
// Complete requires every height and delivery of all retained range observations.
type RangeDeliveryReader interface {
	// RangeDelivery checks durable range coverage/delivery and terminal failure without modifying
	// queues.
	RangeDelivery(context.Context, uint64, uint64) (RangeDelivery, error)
}
