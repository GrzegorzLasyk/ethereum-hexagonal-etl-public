//go:build postgresintegration

package postgres

import (
	"errors"
	"fmt"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
	"testing"
	"time"
)

func testAdmission(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	clear := func() {
		if _, e := owner.Exec(ctx, "TRUNCATE transformer.inbox_events CASCADE"); e != nil {
			t.Fatal(e)
		}
	}
	clear()
	defer clear()
	store, e := New(runtime, "1", time.Minute, 3, 1)
	if e != nil {
		t.Fatal(e)
	}
	other, e := New(runtime, "1", time.Minute, 3, 1)
	if e != nil {
		t.Fatal(e)
	}
	makeEvent := func(n byte) inbound.AcceptRawBlockRequest {
		r := testEvent(t)
		r.Block.Hash[0] = n
		r.EventID = fmt.Sprintf("raw-block/v1/1/%d/%x", r.Block.Number, r.Block.Hash)
		return r
	}
	accepted := make(chan inbound.AcceptRawBlockRequest, 24)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for n := byte(1); n <= 24; n++ {
		r := makeEvent(n)
		s := store
		if n%2 == 0 {
			s = other
		}
		wg.Go(func() {
			<-start
			duplicate, e := s.Accept(ctx, r)
			if e == nil {
				if duplicate {
					t.Error("new event duplicated")
				}
				accepted <- r
			} else if !errors.Is(e, inbound.ErrOverloaded) {
				t.Error(e)
			}
		})
	}
	close(start)
	wg.Wait()
	close(accepted)
	if len(accepted) != 1 {
		t.Fatal("concurrent admission exceeded capacity", len(accepted))
	}
	event := <-accepted
	// Same event remains acknowledgeable at capacity, including failed retained work.
	if _, e := owner.Exec(ctx, "UPDATE transformer.inbox_events SET status='failed' WHERE event_id=$1", event.EventID); e != nil {
		t.Fatal(e)
	}
	for range 12 {
		wg.Go(func() {
			dup, e := other.Accept(ctx, event)
			if e != nil || !dup {
				t.Error("full queue rejected accepted duplicate", e)
			}
		})
	}
	wg.Wait()
	changed := event
	changed.Provider = "different"
	if _, e := store.Accept(ctx, changed); !errors.Is(e, inbound.ErrConflict) {
		t.Fatal("full queue hid identity conflict", e)
	}
	next := makeEvent(99)
	if _, e := store.Accept(ctx, next); !errors.Is(e, inbound.ErrOverloaded) {
		t.Fatal("failed retained event did not consume capacity", e)
	}
	if _, e := owner.Exec(ctx, "UPDATE transformer.inbox_events SET status='pending' WHERE event_id=$1", event.EventID); e != nil {
		t.Fatal(e)
	}

	item, e := store.Claim(ctx)
	if e != nil || item == nil {
		t.Fatal("claim", e)
	}
	id := fmt.Sprintf("transformed-block/v1/1/%d/%x", event.Block.Number, event.Block.Hash)
	if e = store.Complete(ctx, *item, outbound.Result{EventID: id, PayloadVersion: 1, Payload: []byte(`{}`)}); e != nil {
		t.Fatal(e)
	}
	if _, e = store.Accept(ctx, next); !errors.Is(e, inbound.ErrOverloaded) {
		t.Fatal("Inbox completion prematurely released Outbox capacity", e)
	}
	q, e := NewOutbox(runtime, "1", time.Minute, 3)
	if e != nil {
		t.Fatal(e)
	}
	delivery, e := q.ClaimDelivery(ctx)
	if e != nil || delivery == nil {
		t.Fatal("claim delivery", e)
	}
	if e = q.Published(ctx, *delivery); e != nil {
		t.Fatal(e)
	}

	if duplicate, e := store.Accept(ctx, next); e != nil || duplicate {
		t.Fatal("drain did not recover admission", e)
	}
	var retained int
	if e := runtime.QueryRow(ctx, "SELECT count(*) FROM transformer.inbox_events").Scan(&retained); e != nil || retained != 2 {
		t.Fatal("drain removed retained history", retained, e)
	}
	clear()
	faulty := *store
	faulty.db = faultDB{database: store.db, after: true}
	if _, e := faulty.Accept(ctx, event); !errors.Is(e, outbound.ErrCommitUnknown) {
		t.Fatal("expected ambiguous commit", e)
	}
	if dup, e := other.Accept(ctx, event); e != nil || !dup {
		t.Fatal("lost ACK failed idempotent retry at capacity", e)
	}
}
