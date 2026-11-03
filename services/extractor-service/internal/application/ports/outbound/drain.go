package outbound

import (
	"context"
	"errors"
)

var ErrDeliveryIncomplete = errors.New("range delivery incomplete")
var ErrDeliveryFailed = errors.New("range contains failed delivery")

type RangeDelivery struct{ Complete, Failed bool }

// RangeDeliveryReader observes only Extractor-owned history and handoff state.
// Complete requires every height and delivery of all retained range observations.
type RangeDeliveryReader interface {
	RangeDelivery(context.Context, uint64, uint64) (RangeDelivery, error)
}
