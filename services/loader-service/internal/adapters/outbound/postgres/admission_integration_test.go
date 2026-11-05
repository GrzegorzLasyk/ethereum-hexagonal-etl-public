//go:build postgresintegration

// admission_integration_test.go exercises concurrent durable admission, exact deduplication and
// capacity release in the outbound adapter layer.

package postgres

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testAdmission contends real owning-role admission transactions and checks capacity, identical
// duplicates and recovery.
func testAdmission(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	clear := func() {
		if _, e := owner.Exec(ctx, "TRUNCATE loader.inbox_events CASCADE"); e != nil {
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
	makeEvent := func(n byte) inbound.AcceptTransformedBlockRequest {
		r := testEvent(t)
		r.Block.Hash[0] = n
		r.EventID = fmt.Sprintf("transformed-block/v1/1/%d/%x", r.Block.Number, r.Block.Hash)
		r.SourceEventID = fmt.Sprintf("raw-block/v1/1/%d/%x", r.Block.Number, r.Block.Hash)
		return r
	}
	accepted := make(chan inbound.AcceptTransformedBlockRequest, 24)
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
	if _, e := owner.Exec(ctx, "UPDATE loader.inbox_events SET status='failed' WHERE event_id=$1", event.EventID); e != nil {
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
	if _, e := owner.Exec(ctx, "UPDATE loader.inbox_events SET status='pending' WHERE event_id=$1", event.EventID); e != nil {
		t.Fatal(e)
	}

	// Model a committed worker completion; normalized persistence has separate tests.
	if _, e := owner.Exec(ctx, "UPDATE loader.inbox_events SET status='completed',completed_at=clock_timestamp() WHERE event_id=$1", event.EventID); e != nil {
		t.Fatal(e)
	}

	if duplicate, e := store.Accept(ctx, next); e != nil || duplicate {
		t.Fatal("drain did not recover admission", e)
	}
	var retained int
	if e := runtime.QueryRow(ctx, "SELECT count(*) FROM loader.inbox_events").Scan(&retained); e != nil || retained != 2 {
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
