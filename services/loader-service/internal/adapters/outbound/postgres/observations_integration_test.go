//go:build postgresintegration

package postgres

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// forkEvent keeps shared transaction hashes but distinct block membership and log data.
func forkEvent(t *testing.T) inbound.AcceptTransformedBlockRequest {
	r := testEvent(t)
	old := fmt.Sprintf("%x", r.Block.Hash)
	for i := range r.Block.Hash {
		r.Block.Hash[i] = 0x22
	}
	next := fmt.Sprintf("%x", r.Block.Hash)
	r.EventID = fmt.Sprintf("transformed-block/v1/1/20000000/%s", next)
	r.SourceEventID = fmt.Sprintf("raw-block/v1/1/20000000/%s", next)
	r.PayloadJSON = bytes.ReplaceAll(r.PayloadJSON, []byte(old), []byte(next))
	// The fixture contains exactly one log. Preserve all membership identities.
	r.PayloadJSON = bytes.ReplaceAll(r.PayloadJSON, []byte(`"data":"0x000102"`), []byte(`"data":"0xbeef"`))
	return r
}

func testObservations(t *testing.T, runtime, owner *pgxpool.Pool) {
	ctx := t.Context()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, e := owner.Exec(ctx, q, args...); e != nil {
			t.Fatal(e)
		}
	}
	clear := func() { exec("TRUNCATE loader.inbox_events CASCADE") }
	s, e := New(runtime, "1", time.Minute, 3)
	if e != nil {
		t.Fatal(e)
	}
	prepare := func(r inbound.AcceptTransformedBlockRequest) (outbound.Item, domain.Document) {
		t.Helper()
		if _, e := s.Accept(ctx, r); e != nil {
			t.Fatal(e)
		}
		i, e := s.Claim(ctx)
		if e != nil || i == nil {
			t.Fatal("claim", e)
		}
		d, e := (jsondocument.Decoder{}).Decode(ctx, i.Event.PayloadJSON)
		if e != nil {
			t.Fatal(e)
		}
		return *i, d
	}
	complete := func(r inbound.AcceptTransformedBlockRequest) {
		t.Helper()
		i, d := prepare(r)
		if e := s.Complete(ctx, i, d); e != nil {
			t.Fatal(e)
		}
	}
	assertObservation := func(r inbound.AcceptTransformedBlockRequest, conflict bool, parent string) {
		t.Helper()
		var canonical, link, state string
		var got bool
		e := runtime.QueryRow(ctx, `SELECT b.canonical_status,b.height_conflict,b.parent_link,i.status
 FROM loader.blocks b JOIN loader.inbox_events i ON i.event_id=b.event_id WHERE b.event_id=$1`, r.EventID).Scan(&canonical, &got, &link, &state)
		if e != nil || canonical != "unknown" || got != conflict || link != parent || state != "completed" {
			t.Fatalf("%s observation=(%s,%v,%s,%s), err=%v", r.EventID, canonical, got, link, state, e)
		}
	}
	count := func(table string, want int) {
		t.Helper()
		var n int
		if e := runtime.QueryRow(ctx, "SELECT count(*) FROM loader."+table).Scan(&n); e != nil || n != want {
			t.Fatalf("%s count=%d want=%d err=%v", table, n, want, e)
		}
	}
	t.Run("concurrent competing hashes retain isolated children and unknown canonicality", func(t *testing.T) {
		clear()
		a, b := testEvent(t), forkEvent(t)
		ia, da := prepare(a)
		ib, db := prepare(b)
		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for _, work := range []struct {
			i outbound.Item
			d domain.Document
		}{{ia, da}, {ib, db}} {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; errs <- s.Complete(ctx, work.i, work.d) }()
		}
		close(start)
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		assertObservation(a, true, "unknown")
		assertObservation(b, true, "unknown")
		count("blocks", 2)
		count("transactions", 4)
		count("receipts", 4)
		count("logs", 2)
		for _, d := range []domain.Document{da, db} {
			for _, tx := range d.Snapshot.Transactions {
				var value string
				if e := runtime.QueryRow(ctx, `SELECT t.value::text FROM loader.transactions t
 JOIN loader.receipts r USING(chain_id,block_number,block_hash,transaction_hash,transaction_index)
 WHERE t.chain_id='1' AND t.block_number=20000000 AND t.block_hash=$1 AND t.transaction_hash=$2`, tx.BlockHash[:], tx.Hash[:]).Scan(&value); e != nil || value != string(tx.Value) {
					t.Fatalf("isolated transaction value=%s err=%v", value, e)
				}
			}
			for _, receipt := range d.Snapshot.Receipts {
				for _, log := range receipt.Logs {
					var data []byte
					if e := runtime.QueryRow(ctx, `SELECT data FROM loader.logs WHERE chain_id='1' AND block_number=20000000 AND block_hash=$1 AND log_index=$2`, log.BlockHash[:], fmt.Sprint(log.Index)).Scan(&data); e != nil || !bytes.Equal(data, log.Data) {
						t.Fatalf("isolated log data=%x err=%v", data, e)
					}
				}
			}
		}
		// Both exact replay and duplicate acceptance preserve observations and children.
		if e := s.Complete(ctx, ia, da); e != nil {
			t.Fatal(e)
		}
		if _, e := s.Accept(ctx, b); e != nil {
			t.Fatal(e)
		}
		assertObservation(a, true, "unknown")
		assertObservation(b, true, "unknown")
		count("logs", 2)
		// A child cannot borrow a different height or chain's parent membership.
		for _, q := range []string{
			"UPDATE loader.transactions SET block_number=20000001",
			"UPDATE loader.receipts SET chain_id='2'",
			"UPDATE loader.logs SET transaction_index=99",
		} {
			_, e := owner.Exec(ctx, q)
			var pe *pgconn.PgError
			if !errors.As(e, &pe) || pe.Code != "23503" {
				t.Fatalf("foreign key accepted mixed membership: %s (%v)", q, e)
			}
		}
		for _, q := range []string{"UPDATE loader.blocks SET canonical_status='unknown'", "UPDATE loader.blocks SET parent_hash=block_hash", "DELETE FROM loader.blocks"} {
			_, e := runtime.Exec(ctx, q)
			var pe *pgconn.PgError
			if !errors.As(e, &pe) || pe.Code != "42501" {
				t.Fatalf("runtime privilege: %s (%v)", q, e)
			}
		}
		if _, e := owner.Exec(ctx, "UPDATE loader.blocks SET canonical_status='canonical'"); e == nil {
			t.Fatal("canonical promotion was allowed")
		}
	})
	t.Run("late matching parent resolves observation without choosing canonical fork", func(t *testing.T) {
		clear()
		child, wrong, parent := emptyEvent(t, 12, 12, 11), emptyEvent(t, 11, 99, 10), emptyEvent(t, 11, 11, 10)
		complete(child)
		assertObservation(child, false, "unknown")
		complete(wrong)
		assertObservation(child, false, "mismatch")
		complete(parent)
		assertObservation(child, false, "linked")
		assertObservation(wrong, true, "unknown")
		assertObservation(parent, true, "unknown")
		complete(emptyEvent(t, 0, 1, 0))
		count("blocks", 4)
		// Exercise the real forward migration on a pre-0003 schema with data.
		migration, e := os.ReadFile("../../../../../../migrations/loader/0003_conflict_observations.sql")
		if e != nil {
			t.Fatal(e)
		}
		tx, e := owner.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, "ALTER TABLE loader.blocks DROP COLUMN canonical_status,DROP COLUMN height_conflict,DROP COLUMN parent_link"); e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec(ctx, string(migration)); e != nil {
			t.Fatal("forward backfill", e)
		}
		var n int
		if e = tx.QueryRow(ctx, `SELECT count(*) FROM loader.blocks WHERE canonical_status='unknown' AND
 ((block_number=11 AND height_conflict AND parent_link='unknown') OR
 (block_number=12 AND NOT height_conflict AND parent_link='linked') OR
 (block_number=0 AND NOT height_conflict AND parent_link='unknown'))`).Scan(&n); e != nil || n != 4 {
			t.Fatalf("backfill count=%d err=%v", n, e)
		}
	})
	t.Run("children cannot reference sibling fork membership", func(t *testing.T) {
		clear()
		a := testEvent(t)
		b := emptyEvent(t, a.Block.Number, 0x33, 0x11)
		complete(a)
		complete(b)
		for _, table := range []string{"receipts", "logs"} {
			_, err := owner.Exec(ctx, "UPDATE loader."+table+" SET block_hash=$1 WHERE block_hash=$2", b.Block.Hash[:], a.Block.Hash[:])
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "23503" {
				t.Fatalf("%s borrowed sibling membership: %v", table, err)
			}
		}
		assertObservation(a, true, "unknown")
		assertObservation(b, true, "unknown")
		count("transactions", 2)
		count("receipts", 2)
		count("logs", 1)
	})
	t.Run("completion failure rolls back older candidate flags and new records", func(t *testing.T) {
		clear()
		a, b := testEvent(t), forkEvent(t)
		complete(a)
		assertObservation(a, false, "unknown")
		i, d := prepare(b)
		exec(`CREATE FUNCTION loader.reject_observation_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test completion failure'; END $$`)
		exec(`CREATE TRIGGER reject_observation_completion BEFORE UPDATE ON loader.inbox_events FOR EACH ROW WHEN (NEW.status='completed') EXECUTE FUNCTION loader.reject_observation_completion()`)
		defer exec("DROP FUNCTION IF EXISTS loader.reject_observation_completion() CASCADE")
		if e := s.Complete(ctx, i, d); e == nil {
			t.Fatal("injected completion succeeded")
		}
		assertObservation(a, false, "unknown")
		count("blocks", 1)
		count("transactions", 2)
		count("receipts", 2)
		count("logs", 1)
		var state string
		if e := runtime.QueryRow(ctx, "SELECT status FROM loader.inbox_events WHERE event_id=$1", b.EventID).Scan(&state); e != nil || state != "processing" {
			t.Fatalf("inbox=%s err=%v", state, e)
		}
		exec("DROP FUNCTION loader.reject_observation_completion() CASCADE")
		if e := s.Complete(ctx, i, d); e != nil {
			t.Fatal(e)
		}
		assertObservation(a, true, "unknown")
		assertObservation(b, true, "unknown")
	})
}
