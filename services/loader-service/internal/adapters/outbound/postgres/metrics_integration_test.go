//go:build postgresintegration

// metrics_integration_test.go exercises owning-schema metric snapshots and database-failure
// behavior in the outbound adapter layer.

package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

// testMetrics compares owning-schema metric snapshots with controlled queue state and database-
// failure/recovery cases.
func testMetrics(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	if _, err := owner.Exec(ctx, "TRUNCATE loader.inbox_events CASCADE"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := owner.Exec(ctx, "TRUNCATE loader.inbox_events CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	store, err := New(runtime, "1", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := store.ReadStatus(ctx)
	if err != nil || empty.Inbox.Pending != 0 || empty.Inbox.OldestAgeSeconds != 0 {
		t.Fatal(empty, err)
	}
	event := testEvent(t)
	if _, err := store.Accept(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, "UPDATE loader.inbox_events SET accepted_at=clock_timestamp()-interval '1 hour'"); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ReadStatus(ctx)
	if err != nil || pending.Inbox.Pending != 1 || pending.Inbox.OldestAgeSeconds < 3599 {
		t.Fatal(pending, err)
	}
	item, err := store.Claim(ctx)
	if err != nil || item == nil {
		t.Fatal(err)
	}
	active, err := store.ReadStatus(ctx)
	if err != nil || active.Inbox.InFlight != 1 || active.Inbox.Pending != 0 {
		t.Fatal(active, err)
	}
	if _, err := runtime.Exec(ctx, "UPDATE loader.inbox_events SET status='failed',lease_until=NULL"); err != nil {
		t.Fatal(err)
	}
	failed, err := store.ReadStatus(ctx)
	if err != nil || failed.Inbox.Failed != 1 || failed.Inbox.OldestAgeSeconds < 3599 {
		t.Fatal(failed, err)
	}
	// A service instance configured for a different chain sees no foreign-chain work.
	other, err := New(runtime, "2", time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	isolated, err := other.ReadStatus(ctx)
	if err != nil || isolated.Inbox.Failed != 0 {
		t.Fatal(isolated, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.ReadStatus(cancelled); err == nil {
		t.Fatal("canceled database read reported healthy zero")
	}
}
