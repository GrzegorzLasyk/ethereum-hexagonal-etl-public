package usecases

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
)

type Dispatch struct {
	queue                    outbound.DeliveryQueue
	publisher                outbound.Publisher
	rpcTimeout, initial, max time.Duration
}

func NewDispatch(q outbound.DeliveryQueue, p outbound.Publisher, rpcTimeout, initial, max time.Duration) (*Dispatch, error) {
	if q == nil || p == nil || rpcTimeout <= 0 || initial <= 0 || max < initial || max > 24*time.Hour {
		return nil, fmt.Errorf("invalid dispatcher configuration")
	}
	return &Dispatch{q, p, rpcTimeout, initial, max}, nil
}
func (u *Dispatch) DispatchOne(ctx context.Context) (inbound.DispatchResult, error) {
	item, err := u.queue.ClaimDelivery(ctx)
	if err != nil {
		return inbound.DispatchResult{}, fmt.Errorf("claim outbox: %w", err)
	}
	if item == nil {
		return inbound.DispatchResult{}, nil
	}
	result := inbound.DispatchResult{Found: true, EventID: item.Event.EventID, SourceEventID: item.Event.SourceEventID, Attempt: item.Attempt}
	call, cancel := context.WithTimeout(ctx, u.rpcTimeout)
	err = u.publisher.Publish(call, item.Event)
	cancel()
	if err == nil {
		// A lost local COMMIT response leaves the claim to recovery. Do not record
		// failure after a possibly successful delivered-state transaction.
		if err = u.queue.Published(ctx, *item); err != nil {
			return result, err
		}
		result.Outcome = "delivered"
		return result, nil
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	failure := outbound.DeliveryFailure{Code: "unavailable", Delay: u.deliveryBackoff(item.Attempt)}
	switch {
	case errors.Is(err, outbound.ErrPublishInvalid):
		failure.Code = "invalid"
		failure.Permanent = true
	case errors.Is(err, outbound.ErrPublishConflict):
		failure.Code = "conflict"
		failure.Permanent = true
	case errors.Is(err, outbound.ErrPublishOversize):
		failure.Code = "oversize"
		failure.Permanent = true
	case errors.Is(err, outbound.ErrPublishOverloaded):
		failure.Code = "overloaded"
	case errors.Is(err, outbound.ErrPublishACK):
		failure.Code = "invalid_ack"
	}
	result.ErrorCode = failure.Code
	if err = u.queue.RetryDelivery(ctx, *item, failure); err != nil {
		return result, err
	}
	result.Outcome = "retry"
	if failure.Permanent || item.Attempt >= item.MaxAttempts {
		result.Outcome = "failed"
	}
	return result, nil
}
func (u *Dispatch) deliveryBackoff(attempt int) time.Duration {
	delay := u.initial
	for i := 1; i < attempt && delay < u.max; i++ {
		if delay > u.max/2 {
			delay = u.max
			break
		}
		delay *= 2
	}
	if delay > u.max {
		delay = u.max
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}
