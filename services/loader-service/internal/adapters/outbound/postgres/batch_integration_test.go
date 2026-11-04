//go:build postgresintegration

// batch_integration_test.go exercises whole-row equivalence, replay and late-batch transaction
// rollback in the outbound adapter layer.

package postgres

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Invoked by the existing disposable Loader runner; never touches user databases.
func testBatches(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	exec := func(sql string) {
		t.Helper()
		if _, err := owner.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	clear := func() { exec("TRUNCATE loader.inbox_events CASCADE") }
	snapshot := func() []string {
		t.Helper()
		var result []string
		for _, table := range []string{"blocks", "transactions", "receipts", "logs"} {
			var data string
			// Compare every normalized field, including nulls and observations.
			// loaded_at is execution metadata and necessarily differs between runs.
			// Qualify the whole row: transactions also has a scalar column named r.
			if err := runtime.QueryRow(ctx, "SELECT COALESCE(jsonb_agg(to_jsonb(r.*)-'loaded_at' ORDER BY (to_jsonb(r.*)-'loaded_at')::text),'[]'::jsonb)::text FROM loader."+table+" r").Scan(&data); err != nil {
				t.Fatal(err)
			}
			result = append(result, data)
		}
		return result
	}
	for _, count := range []int{0, 259} {
		var reference []string
		for _, limit := range []int{1, 2, 64, 256} {
			clear()
			s, err := NewWithBatchSize(runtime, "1", time.Minute, 3, 128, limit)
			if err != nil {
				t.Fatal(err)
			}
			r := testEvent(t)
			r.PayloadJSON = largeBatchPayload(t, count)
			d, err := (jsondocument.Decoder{}).Decode(ctx, r.PayloadJSON)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Accept(ctx, r); err != nil {
				t.Fatal(err)
			}
			item, err := s.Claim(ctx)
			if err != nil || item == nil {
				t.Fatal("claim", err)
			}
			if err = s.Complete(ctx, *item, d); err != nil {
				t.Fatal(err)
			}
			got := snapshot()
			if limit == 1 {
				reference = got
			} else if !reflect.DeepEqual(reference, got) {
				t.Fatal("batched normalized values differ from reference", count, limit)
			}
			if err = s.Complete(ctx, *item, d); err != nil {
				t.Fatal("idempotent complete", err)
			}
			if duplicate, err := s.Accept(ctx, r); err != nil || !duplicate {
				t.Fatal("duplicate replay", err)
			}
			if !reflect.DeepEqual(got, snapshot()) {
				t.Fatal("replay changed normalized data")
			}
			var state, canonical string
			if err := runtime.QueryRow(ctx, "SELECT i.status,b.canonical_status FROM loader.inbox_events i JOIN loader.blocks b USING(event_id)").Scan(&state, &canonical); err != nil || state != "completed" || canonical != "unknown" {
				t.Fatal("completion/canonicality", err)
			}
		}
	}
	t.Run("late batch SQL failure rolls back earlier batches and recovers", func(t *testing.T) {
		clear()
		s, err := NewWithBatchSize(runtime, "1", time.Minute, 3, 128, 64)
		if err != nil {
			t.Fatal(err)
		}
		r := testEvent(t)
		r.PayloadJSON = largeBatchPayload(t, 259)
		d, err := (jsondocument.Decoder{}).Decode(ctx, r.PayloadJSON)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Accept(ctx, r); err != nil {
			t.Fatal(err)
		}
		item, err := s.Claim(ctx)
		if err != nil || item == nil {
			t.Fatal("claim", err)
		}
		exec(`CREATE FUNCTION loader.stage14_batch_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.log_index=647 THEN RAISE EXCEPTION 'injected unique conflict' USING ERRCODE='23505'; END IF; RETURN NEW; END $$`)
		defer exec("DROP FUNCTION IF EXISTS loader.stage14_batch_fail() CASCADE")
		exec("CREATE TRIGGER stage14_batch_fail BEFORE INSERT ON loader.logs FOR EACH ROW EXECUTE FUNCTION loader.stage14_batch_fail()")
		if err = s.Complete(ctx, *item, d); !errors.Is(err, inbound.ErrConflict) {
			t.Fatal("SQL classification lost", err)
		}
		for _, rows := range snapshot() {
			if rows != "[]" {
				t.Fatal("partial block committed")
			}
		}
		var state string
		if err = runtime.QueryRow(ctx, "SELECT status FROM loader.inbox_events WHERE event_id=$1", r.EventID).Scan(&state); err != nil || state != "processing" {
			t.Fatal("Inbox completed after failure", err)
		}
		exec("DROP FUNCTION loader.stage14_batch_fail() CASCADE")
		if err = s.Complete(ctx, *item, d); err != nil {
			t.Fatal("rollback did not release transaction/connection", err)
		}
		var logs int
		if err = runtime.QueryRow(ctx, "SELECT count(*) FROM loader.logs").Scan(&logs); err != nil || logs != 648 {
			t.Fatal("recovery incomplete", err)
		}
	})
}
