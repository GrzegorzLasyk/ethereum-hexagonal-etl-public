//go:build postgresintegration

package handoff_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/etlverify"
	"github.com/jackc/pgx/v5/pgxpool"
)

func completionQuery(t *testing.T, pools map[string]*pgxpool.Pool) etlverify.Query {
	return func(ctx context.Context, service, sql string) ([]byte, error) {
		var data []byte
		err := pools[service].QueryRow(ctx, sql).Scan(&data)
		return data, err
	}
}
func completionTarget(t *testing.T, ex *pgxpool.Pool, real *realInput) etlverify.Target {
	t.Helper()
	target := etlverify.Target{Transactions: 2, Receipts: 2, Logs: 1}
	if err := ex.QueryRow(t.Context(), "SELECT block_number::bigint,encode(block_hash,'hex') FROM extractor.raw_blocks").Scan(&target.Number, &target.Hash); err != nil {
		t.Fatal(err)
	}
	if real != nil {
		target.Transactions = len(real.transactions)
		target.Receipts = len(real.receipts)
		target.Logs = len(real.logs)
	}
	return target
}
func verifyPartialCompletion(t *testing.T, pools map[string]*pgxpool.Pool, target etlverify.Target) {
	t.Helper()
	q := completionQuery(t, pools)
	r, err := etlverify.Observe(t.Context(), q, target)
	if err != nil {
		t.Fatal("completion observation", err)
	}
	if r.Complete() || r["extractor"].States["outbox"] != "pending" || r["loader"].States["inbox"] != "missing" {
		t.Fatal("checkpoint incorrectly passed", r.Summary())
	}
	if _, err := etlverify.Wait(t.Context(), q, target, 30*time.Millisecond, 5*time.Millisecond); !errors.Is(err, etlverify.ErrIncomplete) {
		t.Fatal("partial ETL did not time out", err)
	}
	t.Log("PASS: advanced checkpoint and pending Outbox are incomplete; bounded verifier times out")
}
func verifyCompleteAndFaults(t *testing.T, pools map[string]*pgxpool.Pool, target etlverify.Target) {
	t.Helper()
	q := completionQuery(t, pools)
	if r, err := etlverify.Wait(t.Context(), q, target, time.Second, time.Millisecond); err != nil || !r.Complete() {
		t.Fatal("completion failed", err, r.Summary())
	}
	for _, tc := range []struct {
		service, table, assignment string
		want                       error
	}{
		{"extractor", "outbox_events", "status='failed',delivered_at=NULL", etlverify.ErrFailed},
		{"transformer", "inbox_events", "status='failed',completed_at=NULL", etlverify.ErrFailed},
		{"transformer", "outbox_events", "status='failed',delivered_at=NULL", etlverify.ErrFailed},
		{"loader", "inbox_events", "status='processing',completed_at=NULL,lease_until=clock_timestamp()-interval '1 second'", etlverify.ErrStuck},
	} {
		tx, err := pools[tc.service].Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		// Fault is visible only to this role's query on this connection. Rollback
		// preserves all committed history and avoids racing running workers.
		if _, err = tx.Exec(t.Context(), "UPDATE "+tc.service+"."+tc.table+" SET "+tc.assignment); err != nil {
			_ = tx.Rollback(t.Context())
			t.Fatal(err)
		}
		injected := func(ctx context.Context, service, sql string) ([]byte, error) {
			if service != tc.service {
				return q(ctx, service, sql)
			}
			var b []byte
			err := tx.QueryRow(ctx, sql).Scan(&b)
			return b, err
		}
		_, err = etlverify.Wait(t.Context(), injected, target, time.Second, time.Millisecond)
		rollback := tx.Rollback(t.Context())
		if rollback != nil || !errors.Is(err, tc.want) {
			t.Fatal("fault not detected", tc.service, err, rollback)
		}
	}
	t.Log("PASS: bounded completion verifier; failed Inbox/Outbox and expired lease detection; rolled-back fault probes")
}
