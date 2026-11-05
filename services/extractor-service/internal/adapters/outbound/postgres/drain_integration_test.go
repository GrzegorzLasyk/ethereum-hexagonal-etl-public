//go:build postgresintegration

// drain_integration_test.go exercises range delivery completion with retained failed/pending
// events in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/usecases"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testRangeDelivery checks complete, pending, failed and missing active-range Outbox state against
// real owning-role PostgreSQL.
func testRangeDelivery(t *testing.T, pool *pgxpool.Pool) {
	s, err := New(pool, "134001", 42)
	if err != nil {
		t.Fatal(err)
	}
	check := func(complete, failed bool) {
		t.Helper()
		r, err := s.RangeDelivery(t.Context(), 42, 42)
		if err != nil || r.Complete != complete || r.Failed != failed {
			t.Fatal("range state", r, err)
		}
	}
	check(false, false)
	e := snapshot("134001", 42, 2, 1)
	if _, err := s.Commit(t.Context(), outbound.CommitExtraction{Extraction: e}); err != nil {
		t.Fatal(err)
	}
	check(false, false) // An advanced checkpoint cannot certify delivery.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := usecases.Drain(ctx, s, 42, 42, time.Millisecond, time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pending did not time out", err)
	}
	update := func(q string) {
		t.Helper()
		if _, err := pool.Exec(t.Context(), q, e.Header.ID.EventID()); err != nil {
			t.Fatal(err)
		}
	}
	update("UPDATE extractor.outbox_events SET status='failed' WHERE event_id=$1")
	check(false, true)
	if err := usecases.Drain(t.Context(), s, 42, 42, time.Millisecond, time.Second); !errors.Is(err, outbound.ErrDeliveryFailed) {
		t.Fatal(err)
	}
	update("UPDATE extractor.outbox_events SET status='delivered',delivered_at=clock_timestamp() WHERE event_id=$1")
	check(true, false)
	if err := usecases.Drain(t.Context(), s, 42, 42, time.Millisecond, time.Second); err != nil {
		t.Fatal(err)
	}
	r, err := s.RangeDelivery(t.Context(), 42, 43)
	if err != nil || r.Complete {
		t.Fatal("partial range accepted", err)
	}
}
