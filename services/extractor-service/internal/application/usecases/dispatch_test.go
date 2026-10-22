package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
)

type deliveryFake struct {
	item                           *outbound.Delivery
	claimErr, publishErr, retryErr error
	published, retried             int
	failure                        outbound.DeliveryFailure
}

func (f *deliveryFake) ClaimDelivery(context.Context) (*outbound.Delivery, error) {
	return f.item, f.claimErr
}
func (f *deliveryFake) Published(context.Context, outbound.Delivery) error {
	f.published++
	return f.publishErr
}
func (f *deliveryFake) RetryDelivery(_ context.Context, _ outbound.Delivery, v outbound.DeliveryFailure) error {
	f.retried++
	f.failure = v
	return f.retryErr
}

type publisherFunc func(context.Context, outbound.Publication) error

func (f publisherFunc) Publish(c context.Context, p outbound.Publication) error { return f(c, p) }
func TestDispatchAcknowledgementBoundary(t *testing.T) {
	for _, tc := range []struct {
		name               string
		send, commit       error
		outcome, code      string
		published, retried int
		permanent          bool
	}{
		{name: "durable ACK", outcome: "delivered", published: 1},
		{name: "lost ACK", send: outbound.ErrPublishUnavailable, outcome: "retry", code: "unavailable", retried: 1},
		{name: "invalid ACK", send: outbound.ErrPublishACK, outcome: "retry", code: "invalid_ack", retried: 1},
		{name: "conflict is not duplicate ACK", send: outbound.ErrPublishConflict, outcome: "failed", code: "conflict", retried: 1, permanent: true},
		{name: "oversize retained", send: outbound.ErrPublishOversize, outcome: "failed", code: "oversize", retried: 1, permanent: true},
		{name: "invalid retained", send: outbound.ErrPublishInvalid, outcome: "failed", code: "invalid", retried: 1, permanent: true},
		{name: "lost local commit", commit: outbound.ErrDeliveryCommitUnknown, published: 1},
		{name: "stale worker ACK", commit: outbound.ErrDeliveryLeaseLost, published: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &deliveryFake{item: &outbound.Delivery{Event: outbound.Publication{EventID: "stable"}, Attempt: 2, MaxAttempts: 3}, publishErr: tc.commit}
			u, e := NewDispatch(f, publisherFunc(func(ctx context.Context, p outbound.Publication) error {
				if p.EventID != "stable" {
					t.Fatal("changed identity")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing RPC deadline")
				}
				return tc.send
			}), time.Second, time.Millisecond, time.Second)
			if e != nil {
				t.Fatal(e)
			}
			result, e := u.DispatchOne(t.Context())
			if !errors.Is(e, tc.commit) || result.Outcome != tc.outcome || f.published != tc.published || f.retried != tc.retried || f.failure.Code != tc.code || f.failure.Permanent != tc.permanent {
				t.Fatalf("result=%+v err=%v queue=%+v", result, e, f)
			}
			if f.retried > 0 && (f.failure.Delay < time.Millisecond || f.failure.Delay > 2*time.Millisecond) {
				t.Fatal("backoff outside attempt bounds")
			}
		})
	}
}
func TestDispatchTimeoutCancellationExhaustionAndIdle(t *testing.T) {
	f := &deliveryFake{item: &outbound.Delivery{Attempt: 3, MaxAttempts: 3}}
	u, _ := NewDispatch(f, publisherFunc(func(c context.Context, _ outbound.Publication) error { <-c.Done(); return c.Err() }), time.Millisecond, time.Millisecond, time.Second)
	result, e := u.DispatchOne(t.Context())
	if e != nil || result.Outcome != "failed" || f.retried != 1 || f.published != 0 {
		t.Fatal("RPC timeout must retain exhausted work", result, e)
	}
	c, cancel := context.WithCancel(t.Context())
	cancel()
	f.retried = 0
	if _, e = u.DispatchOne(c); !errors.Is(e, context.Canceled) || f.retried != 0 {
		t.Fatal("shutdown must leave recoverable lease", e)
	}
	f.item = nil
	if result, e = u.DispatchOne(t.Context()); e != nil || result.Found {
		t.Fatal("idle", result, e)
	}
	f.claimErr = outbound.ErrDeliveryCommitUnknown
	if _, e = u.DispatchOne(t.Context()); !errors.Is(e, f.claimErr) {
		t.Fatal(e)
	}
	for _, attempt := range []int{1, 2, 10, 1000} {
		d := u.deliveryBackoff(attempt)
		if d < 0 || d > time.Second {
			t.Fatal("backoff overflow", d)
		}
	}
}
