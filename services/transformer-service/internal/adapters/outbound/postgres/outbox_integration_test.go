//go:build postgresintegration

// outbox_integration_test.go exercises real Outbox leases, retry limits and ambiguous-commit
// recovery in the outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/usecases"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deliveryFaultDB wraps transactions to inject Outbox delivery-commit acknowledgement failures.
type deliveryFaultDB struct {
	database
	after bool
}

// BeginTx wraps or returns the controlled test transaction so commit/failure boundaries can be
// injected without changing production code.
func (d deliveryFaultDB) BeginTx(c context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := d.database.BeginTx(c, o)
	if e != nil {
		return nil, e
	}
	return deliveryFaultTx{tx, d.after}, nil
}

// deliveryFaultTx can lose the acknowledgement of a real delivery commit to test safe retry
// decisions.
type deliveryFaultTx struct {
	pgx.Tx
	after bool
}

// Commit injects a controlled lost/failed commit acknowledgement around the real transaction to
// test explicit outcome uncertainty.
func (tx deliveryFaultTx) Commit(c context.Context) error {
	if tx.after {
		if e := tx.Tx.Commit(c); e != nil {
			return e
		}
	}
	return errors.New("lost local commit response")
}

// testOutbox exercises real delivery claims, retry exhaustion, fencing and ambiguous-commit
// recovery without a live provider.
func testOutbox(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, e := owner.Exec(ctx, q, args...); e != nil {
			t.Fatal(e)
		}
	}
	clear := func() { exec("TRUNCATE transformer.inbox_events CASCADE") }
	seed := func() string {
		t.Helper()
		s, e := New(runtime, "1", time.Minute, 3)
		if e != nil {
			t.Fatal(e)
		}
		r := testEvent(t)
		if _, e = s.Accept(ctx, r); e != nil {
			t.Fatal(e)
		}
		encoder, e := jsondocument.New(64 << 20)
		if e != nil {
			t.Fatal(e)
		}
		process, e := usecases.NewProcess(s, ethereumjson.Decoder{}, encoder, time.Millisecond, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = process.ProcessOne(ctx); e != nil {
			t.Fatal(e)
		}
		return "transformed-block" + r.EventID[len("raw-block"):]
	}
	fresh := func() *Outbox {
		t.Helper()
		s, e := NewOutbox(runtime, "1", time.Minute, 3)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	claim := func(s *Outbox) outbound.Delivery {
		t.Helper()
		item, e := s.ClaimDelivery(ctx)
		if e != nil || item == nil {
			t.Fatal("claim", e)
		}
		return *item
	}
	state := func(id, want string) {
		t.Helper()
		var got string
		if e := runtime.QueryRow(ctx, "SELECT status FROM transformer.outbox_events WHERE event_id=$1", id).Scan(&got); e != nil || got != want {
			t.Fatalf("status=%s want=%s err=%v", got, want, e)
		}
	}
	expire := func() {
		exec("UPDATE transformer.outbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE status='sending'")
	}
	t.Run("exclusive concurrent claim exact persisted publication and delivered replay", func(t *testing.T) {
		clear()
		id := seed()
		s := fresh()
		items := make(chan *outbound.Delivery, 8)
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { i, e := s.ClaimDelivery(ctx); items <- i; errs <- e })
		}
		wg.Wait()
		close(items)
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		var item outbound.Delivery
		n := 0
		for i := range items {
			if i != nil {
				n++
				item = *i
			}
		}
		if n != 1 || item.Event.EventID != id {
			t.Fatal("claim overlap", n)
		}
		var payload []byte
		var transformed time.Time
		if e := runtime.QueryRow(ctx, "SELECT payload,transformed_at FROM transformer.transformed_blocks WHERE event_id=$1", item.Event.EventID).Scan(&payload, &transformed); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(item.Event.Payload, payload) || !item.Event.TransformedAt.Equal(transformed) || !reflect.DeepEqual(item.Event.ExtractedAt, testEvent(t).ExtractedAt) {
			t.Fatal("source bytes/provenance changed")
		}
		other, e := NewOutbox(runtime, "2", time.Minute, 3)
		if e != nil {
			t.Fatal(e)
		}
		if i, e := other.ClaimDelivery(ctx); e != nil || i != nil {
			t.Fatal("cross-chain claim", i, e)
		}
		if e := s.Published(ctx, item); e != nil {
			t.Fatal(e)
		}
		if e := s.Published(ctx, item); e != nil {
			t.Fatal("delivered replay", e)
		}
		seed()
		state(id, "delivered")
		if i, e := s.ClaimDelivery(ctx); e != nil || i != nil {
			t.Fatal("delivered event reclaimed")
		}
		if e := s.RetryDelivery(ctx, item, outbound.DeliveryFailure{Code: "unavailable"}); !errors.Is(e, outbound.ErrDeliveryLeaseLost) {
			t.Fatal("failure rewrote delivered event", e)
		}
	})
	t.Run("expired sender cannot complete or fail renewed claim", func(t *testing.T) {
		clear()
		id := seed()
		s := fresh()
		old := claim(s)
		expire()
		if e := s.Published(ctx, old); !errors.Is(e, outbound.ErrDeliveryLeaseLost) {
			t.Fatal("expired ACK", e)
		}
		restarted := fresh()
		current := claim(restarted)
		if current.Attempt != old.Attempt+1 || !reflect.DeepEqual(old.Event, current.Event) {
			t.Fatal("restart changed immutable event")
		}
		if e := s.Published(ctx, old); !errors.Is(e, outbound.ErrDeliveryLeaseLost) {
			t.Fatal(e)
		}
		if e := s.RetryDelivery(ctx, old, outbound.DeliveryFailure{Code: "unavailable"}); !errors.Is(e, outbound.ErrDeliveryLeaseLost) {
			t.Fatal(e)
		}
		if e := restarted.Published(ctx, current); e != nil {
			t.Fatal(e)
		}
		state(id, "delivered")
	})
	t.Run("backoff exhausted and permanent failures retained", func(t *testing.T) {
		clear()
		id := seed()
		s := fresh()
		item := claim(s)
		if e := s.RetryDelivery(ctx, item, outbound.DeliveryFailure{Code: "unavailable", Delay: time.Hour}); e != nil {
			t.Fatal(e)
		}
		state(id, "pending")
		if i, e := s.ClaimDelivery(ctx); e != nil || i != nil {
			t.Fatal("ignored backoff")
		}
		exec("UPDATE transformer.outbox_events SET next_attempt_at=clock_timestamp()")
		second := claim(s)
		if e := s.RetryDelivery(ctx, second, outbound.DeliveryFailure{Code: "unavailable"}); e != nil {
			t.Fatal(e)
		}
		third := claim(s)
		if third.Attempt != 3 {
			t.Fatal(third.Attempt)
		}
		expire()
		if i, e := s.ClaimDelivery(ctx); e != nil || i != nil {
			t.Fatal("exhausted crash reclaimed")
		}
		state(id, "failed")
		clear()
		id = seed()
		item = claim(s)
		if e := s.RetryDelivery(ctx, item, outbound.DeliveryFailure{Code: "conflict", Permanent: true}); e != nil {
			t.Fatal(e)
		}
		state(id, "failed")
		if i, e := s.ClaimDelivery(ctx); e != nil || i != nil {
			t.Fatal("permanent failure reclaimed")
		}
	})
	t.Run("ambiguous claim and ACK-state commits recover", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			clear()
			id := seed()
			s := fresh()
			faulty := *s
			faulty.db = deliveryFaultDB{s.db, after}
			if i, e := faulty.ClaimDelivery(ctx); i != nil || !errors.Is(e, outbound.ErrDeliveryCommitUnknown) {
				t.Fatal("claim ambiguity", i, e)
			}
			if after {
				state(id, "sending")
				expire()
			} else {
				state(id, "pending")
			}
			item := claim(s)
			if e := faulty.Published(ctx, item); !errors.Is(e, outbound.ErrDeliveryCommitUnknown) {
				t.Fatal(e)
			}
			if after {
				state(id, "delivered")
			} else {
				state(id, "sending")
			}
			if e := s.Published(ctx, item); e != nil {
				t.Fatal("resolve local commit", e)
			}
			state(id, "delivered")
		}
	})
	t.Run("leases cancellation pinned retry budget and restricted updates", func(t *testing.T) {
		clear()
		id := seed()
		s := fresh()
		item := claim(s)
		changed, e := NewOutbox(runtime, "1", time.Minute, 100)
		if e != nil {
			t.Fatal(e)
		}
		expire()
		item = claim(changed)
		if item.MaxAttempts != 3 {
			t.Fatal("changed persisted retry budget")
		}
		tx, e := owner.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, "SELECT event_id FROM transformer.outbox_events WHERE event_id=$1 FOR UPDATE", id); e != nil {
			t.Fatal(e)
		}
		c, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if e = s.Published(c, item); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
		if e = tx.Rollback(ctx); e != nil {
			t.Fatal(e)
		}
		if e = s.Published(ctx, item); e != nil {
			t.Fatal("lock leak", e)
		}
		for _, q := range []string{"UPDATE transformer.outbox_events SET event_id='changed'", "UPDATE transformer.outbox_events SET created_at=clock_timestamp()", "DELETE FROM transformer.outbox_events", "SELECT * FROM loader.inbox_events"} {
			if _, e := runtime.Exec(ctx, q); e == nil {
				t.Fatal("unauthorized", q)
			}
		}
	})
}
