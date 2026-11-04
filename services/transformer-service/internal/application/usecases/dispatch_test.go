// dispatch_test.go exercises durable delivery coordination and retry policy in the application
// orchestration layer.

package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
)

// deliveryFake records delivery claims, acknowledgement mutations and retry decisions at the
// application boundary.
type deliveryFake struct {
	item                           *outbound.Delivery
	claimErr, publishErr, retryErr error
	published, retried             int
	failure                        outbound.DeliveryFailure
}

// ClaimDelivery returns a controlled durable delivery item/error for dispatch policy tests.
func (f *deliveryFake) ClaimDelivery(context.Context) (*outbound.Delivery, error) {
	return f.item, f.claimErr
}

// Published records a simulated delivered-state mutation so tests can require an actual valid ACK
// first.
func (f *deliveryFake) Published(context.Context, outbound.Delivery) error {
	f.published++
	return f.publishErr
}

// RetryDelivery captures the classified failure/delay for durable retry and exhaustion assertions.
func (f *deliveryFake) RetryDelivery(_ context.Context, _ outbound.Delivery, v outbound.DeliveryFailure) error {
	f.retried++
	f.failure = v
	return f.retryErr
}

// publisherFunc adapts an injected callback to the outbound publication port.
type publisherFunc func(context.Context, outbound.Publication) error

// Publish delegates to the injected test callback, allowing controlled completion, blocking or
// failure at the application port.
func (f publisherFunc) Publish(c context.Context, p outbound.Publication) error { return f(c, p) }

// TestDispatchAcknowledgementBoundary requires a verified remote acceptance before the durable
// queue is marked delivered.
func TestDispatchAcknowledgementBoundary(t *testing.T) {
	for _, tc := range []struct {
		name               string
		send, commit       error
		outcome, code      string
		published, retried int
		permanent          bool
	}{
		{name: "durable ACK", outcome: "delivered", published: 1},
		{name: "receiver overload", send: outbound.ErrPublishOverloaded, outcome: "retry", code: "overloaded", retried: 1},
		{name: "lost ACK", send: outbound.ErrPublishUnavailable, outcome: "retry", code: "unavailable", retried: 1},
		{name: "invalid ACK", send: outbound.ErrPublishACK, outcome: "retry", code: "invalid_ack", retried: 1},
		{name: "conflict is not duplicate ACK", send: outbound.ErrPublishConflict, outcome: "failed", code: "conflict", retried: 1, permanent: true},
		{name: "oversize retained", send: outbound.ErrPublishOversize, outcome: "failed", code: "oversize", retried: 1, permanent: true},
		{name: "invalid retained", send: outbound.ErrPublishInvalid, outcome: "failed", code: "invalid", retried: 1, permanent: true},
		{name: "lost local commit", commit: outbound.ErrDeliveryCommitUnknown, published: 1},
		{name: "stale worker ACK", commit: outbound.ErrDeliveryLeaseLost, published: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &deliveryFake{item: &outbound.Delivery{Event: outbound.Publication{EventID: "stable", SourceEventID: "source"}, Attempt: 2, MaxAttempts: 3}, publishErr: tc.commit}
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
			if result.EventID != "stable" || result.Attempt != 2 || result.ErrorCode != tc.code || result.SourceEventID != "source" {
				t.Fatalf("lost correlation: %+v", result)
			}
			if !errors.Is(e, tc.commit) || result.Outcome != tc.outcome || f.published != tc.published || f.retried != tc.retried || f.failure.Code != tc.code || f.failure.Permanent != tc.permanent {
				t.Fatalf("result=%+v err=%v queue=%+v", result, e, f)
			}
			if f.retried > 0 && (f.failure.Delay < time.Millisecond || f.failure.Delay > 2*time.Millisecond) {
				t.Fatal("backoff outside attempt bounds")
			}
		})
	}
}

// TestDispatchTimeoutCancellationExhaustionAndIdle checks durable dispatch outcomes when no work
// exists, a call fails/cancels or finite attempts run out.
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

// TestOverloadExhaustsDurableAllowance requires retryable capacity errors to obey finite Outbox
// attempts instead of creating an infinite retry loop.
func TestOverloadExhaustsDurableAllowance(t *testing.T) {
	f := &deliveryFake{item: &outbound.Delivery{Attempt: 3, MaxAttempts: 3}}
	u, e := NewDispatch(f, publisherFunc(func(context.Context, outbound.Publication) error { return outbound.ErrPublishOverloaded }), time.Second, time.Millisecond, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	result, e := u.DispatchOne(t.Context())
	if e != nil || result.Outcome != "failed" || result.ErrorCode != "overloaded" || f.failure.Permanent || f.retried != 1 || f.published != 0 || f.failure.Delay < time.Millisecond {
		t.Fatal("overload bypassed bounded durable retry", result, e)
	}
}
