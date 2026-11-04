//go:build postgresintegration

// metrics_integration_test.go exercises owning-schema metric snapshots and database-failure
// behavior in the outbound adapter layer.

package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

// testMetrics compares owning-schema metric snapshots with controlled queue state and database-
// failure/recovery cases.
func testMetrics(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	s, err := New(runtime, "7000001", 42)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := s.ReadStatus(ctx)
	if err != nil || empty.Checkpoint != nil || empty.Outbox.Pending != 0 || empty.Outbox.OldestAgeSeconds != 0 {
		t.Fatal(empty, err)
	}
	first := snapshot(s.chain, 42, 2, 1)
	if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: first}); err != nil {
		t.Fatal(err)
	}
	cp, err := s.Checkpoint(ctx, s.chain)
	if err != nil {
		t.Fatal(err)
	}
	second := snapshot(s.chain, 43, 3, 2)
	if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: second, ExpectedCheckpoint: cp}); err != nil {
		t.Fatal(err)
	}
	cp, err = s.Checkpoint(ctx, s.chain)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rewind(ctx, *cp, first.Header.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Exec(ctx, "UPDATE extractor.outbox_events SET created_at=clock_timestamp()-interval '1 hour' WHERE chain_id=$1", string(s.chain)); err != nil {
		t.Fatal(err)
	}
	status, err := s.ReadStatus(ctx)
	if err != nil || status.Checkpoint == nil || *status.Checkpoint != 42 || status.Revision != 1 || status.Outbox.Pending != 2 || status.Outbox.OldestAgeSeconds < 3599 {
		t.Fatal(status, err)
	}
	if _, err := runtime.Exec(ctx, "UPDATE extractor.outbox_events SET status='failed' WHERE chain_id=$1", string(s.chain)); err != nil {
		t.Fatal(err)
	}
	status, err = s.ReadStatus(ctx)
	if err != nil || status.Outbox.Failed != 2 || status.Outbox.Pending != 0 {
		t.Fatal(status, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ReadStatus(cancelled); err == nil {
		t.Fatal("canceled read returned healthy zero")
	}
}
