package usecases

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
)

type drainReader func(context.Context, uint64, uint64) (outbound.RangeDelivery, error)

func (f drainReader) RangeDelivery(c context.Context, a, b uint64) (outbound.RangeDelivery, error) {
	return f(c, a, b)
}
func TestDrain(t *testing.T) {
	for _, kind := range []string{"delayed", "timeout", "failed", "database", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if kind == "cancel" {
					cancel()
				}
				err := Drain(ctx, drainReader(func(c context.Context, a, b uint64) (outbound.RangeDelivery, error) {
					calls++
					if a != 10 || b != 11 {
						t.Fatal("wrong range")
					}
					switch kind {
					case "delayed":
						return outbound.RangeDelivery{Complete: calls == 3}, nil
					case "failed":
						return outbound.RangeDelivery{Failed: true}, nil
					case "database":
						return outbound.RangeDelivery{}, errors.New("private detail")
					}
					return outbound.RangeDelivery{}, nil
				}), 10, 11, time.Second, 3*time.Second)
				switch kind {
				case "delayed":
					if err != nil || calls != 3 {
						t.Fatal(err, calls)
					}
				case "failed":
					if !errors.Is(err, outbound.ErrDeliveryFailed) {
						t.Fatal(err)
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) || calls != 0 {
						t.Fatal(err)
					}
				case "timeout":
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				default:
					if !errors.Is(err, outbound.ErrDeliveryIncomplete) {
						t.Fatal(err)
					}
				}
			})
		})
	}
}
