package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// Drain waits for durable ACKs while the composition root keeps dispatch alive.
// It never changes delivery state or replenishes acquisition/retry budgets.
func Drain(ctx context.Context, reader outbound.RangeDeliveryReader, start, end uint64, poll, timeout time.Duration) error {
	if reader == nil || end < start || poll <= 0 || timeout <= 0 {
		return domain.ErrInvalidData
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(outbound.ErrDeliveryIncomplete, err)
		}
		progress, err := reader.RangeDelivery(ctx, start, end)
		if err != nil {
			return errors.Join(outbound.ErrDeliveryIncomplete, err)
		}
		if progress.Failed {
			return outbound.ErrDeliveryFailed
		}
		if progress.Complete {
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(outbound.ErrDeliveryIncomplete, ctx.Err())
		case <-timer.C:
		}
	}
}
