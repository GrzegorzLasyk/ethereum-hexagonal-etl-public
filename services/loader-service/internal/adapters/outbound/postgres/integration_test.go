//go:build postgresintegration

// integration_test.go exercises real owning-role PostgreSQL transactions, claims and failure
// recovery in the outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	receiver "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/inbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// integrationPool opens the explicitly configured integration connection and verifies the expected
// role before test SQL.
func integrationPool(t *testing.T, variable, role string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(variable)
	if dsn == "" {
		t.Skip("run go run ./test/integration/cmd/loader-postgres for an isolated database")
	}
	if os.Getenv("LOADER_TEST_ISOLATED") != "1" {
		t.Fatal("tests require the disposable runner")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid integration database configuration")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	p, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(dbError(err))
	}
	t.Cleanup(p.Close)
	var user, db string
	if err := p.QueryRow(t.Context(), "SELECT current_user,current_database()").Scan(&user, &db); err != nil {
		t.Fatal(dbError(err))
	}
	if user != role || db != "ethereum_etl" {
		t.Fatal("incorrect isolated role/database")
	}
	return p
}

// testEvent builds a valid deterministic application/transport request so one invariant can be
// varied per case.
func testEvent(t *testing.T) inbound.AcceptTransformedBlockRequest {
	t.Helper()
	b, e := os.ReadFile("../jsondocument/testdata/transformed_v1.json")
	if e != nil {
		t.Fatal(e)
	}
	var hash, parent [32]byte
	for i := range hash {
		hash[i] = 0x10
		parent[i] = 0x11
	}
	return inbound.AcceptTransformedBlockRequest{EventID: fmt.Sprintf("transformed-block/v1/1/20000000/%x", hash), SourceEventID: fmt.Sprintf("raw-block/v1/1/20000000/%x", hash), EventVersion: 1, PayloadVersion: 1, Block: inbound.BlockIdentity{ChainID: "1", Number: 20000000, Hash: hash, ParentHash: parent}, Provider: "fixture", ExtractedAt: time.Unix(100, 123).UTC(), TransformedAt: time.Unix(200, 456).UTC(), PayloadJSON: b}
}

// wire loads the controlled normalized/raw payload used by persistence integration assertions.
func wire(r inbound.AcceptTransformedBlockRequest) *etlv1.SubmitTransformedBlockRequest {
	return &etlv1.SubmitTransformedBlockRequest{Metadata: &etlv1.EventMetadata{EventId: r.EventID, EventVersion: 1, PayloadVersion: 1, Block: &etlv1.BlockIdentity{ChainId: r.Block.ChainID, Number: proto.Uint64(r.Block.Number), BlockHash: r.Block.Hash[:], ParentHash: r.Block.ParentHash[:]}, Provider: r.Provider, ExtractedAt: timestamppb.New(r.ExtractedAt)}, PayloadJson: r.PayloadJSON, SourceEventId: r.SourceEventID, TransformedAt: timestamppb.New(r.TransformedAt)}
}

// emptyEvent supplies coherent identities for arbitrary delivery ordering tests.
func emptyEvent(t *testing.T, number uint64, hash, parent byte) inbound.AcceptTransformedBlockRequest {
	r := testEvent(t)
	r.Block.Number = number
	for i := range r.Block.Hash {
		r.Block.Hash[i] = hash
		r.Block.ParentHash[i] = parent
	}
	r.EventID = fmt.Sprintf("transformed-block/v1/1/%d/%x", number, r.Block.Hash)
	r.SourceEventID = fmt.Sprintf("raw-block/v1/1/%d/%x", number, r.Block.Hash)
	var d map[string]json.RawMessage
	_ = json.Unmarshal(r.PayloadJSON, &d)
	var b map[string]json.RawMessage
	_ = json.Unmarshal(d["block"], &b)
	encode := func(v any) json.RawMessage {
		out, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		return out
	}
	b["number"] = encode(strconv.FormatUint(number, 10))
	b["hash"] = encode(fmt.Sprintf("0x%x", r.Block.Hash))
	b["parent_hash"] = encode(fmt.Sprintf("0x%x", r.Block.ParentHash))
	b["gas_used"] = encode("0")
	d["block"] = encode(b)
	d["source_event_id"] = encode(r.SourceEventID)
	for _, key := range []string{"transactions", "receipts", "logs"} {
		d[key] = json.RawMessage(`[]`)
	}
	r.PayloadJSON = encode(d)
	return r
}

// faultDB wraps real database transactions to inject an uncertain acceptance/completion commit
// outcome.
type faultDB struct {
	database
	after bool
}

// BeginTx wraps or returns the controlled test transaction so commit/failure boundaries can be
// injected without changing production code.
func (d faultDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.database.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return faultTx{tx, d.after}, nil
}

// faultTx can fail before or after a real commit so durable state and reported error can be
// checked separately.
type faultTx struct {
	pgx.Tx
	after bool
}

// Commit injects a controlled lost/failed commit acknowledgement around the real transaction to
// test explicit outcome uncertainty.
func (tx faultTx) Commit(ctx context.Context) error {
	if tx.after {
		if err := tx.Tx.Commit(ctx); err != nil {
			return err
		}
	}
	return errors.New("simulated commit acknowledgement loss")
}

// TestPostgresIntegration runs owning-role PostgreSQL persistence and failure scenarios only with
// explicit integration database configuration.
func TestPostgresIntegration(t *testing.T) {
	runtime := integrationPool(t, "LOADER_TEST_DATABASE_URL", "loader_runtime")
	owner := integrationPool(t, "LOADER_TEST_MIGRATOR_URL", "loader_migrator")
	t.Run("bounded SQL batches", func(t *testing.T) { testBatches(t, runtime, owner) })
	t.Run("bounded admission", func(t *testing.T) { testAdmission(t, runtime, owner) })
	t.Run("operational metrics", func(t *testing.T) { testMetrics(t, runtime, owner) })
	t.Run("schema readiness", func(t *testing.T) { testReadiness(t, runtime, owner) })
	t.Run("conflict observations", func(t *testing.T) { testObservations(t, runtime, owner) })
	t.Run("audited recovery", func(t *testing.T) { testRepair(t, runtime, owner) })
	ctx := t.Context()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, e := owner.Exec(ctx, query, args...); e != nil {
			t.Fatal("owner operation", e)
		}
	}
	clear := func() { exec("TRUNCATE loader.inbox_events CASCADE") }
	fresh := func() *Store {
		s, e := New(runtime, "1", time.Minute, 3)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	accept := func(s *Store, r inbound.AcceptTransformedBlockRequest) {
		t.Helper()
		if _, e := s.Accept(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	claim := func(s *Store) *outbound.Item {
		t.Helper()
		i, e := s.Claim(ctx)
		if e != nil || i == nil {
			t.Fatal("claim", e)
		}
		return i
	}
	document := func(r inbound.AcceptTransformedBlockRequest) domain.Document {
		t.Helper()
		d, e := (jsondocument.Decoder{}).Decode(ctx, r.PayloadJSON)
		if e != nil {
			t.Fatal(e)
		}
		return d
	}
	process := func(s *Store) *usecases.Process {
		p, e := usecases.NewProcess(s, jsondocument.Decoder{}, time.Millisecond, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	state := func(id string) string {
		var s string
		if e := runtime.QueryRow(ctx, "SELECT status FROM loader.inbox_events WHERE event_id=$1", id).Scan(&s); e != nil {
			t.Fatal(e)
		}
		return s
	}
	counts := func(want ...int) {
		t.Helper()
		for i, table := range []string{"inbox_events", "blocks", "transactions", "receipts", "logs"} {
			var n int
			if e := runtime.QueryRow(ctx, "SELECT count(*) FROM loader."+table).Scan(&n); e != nil || n != want[i] {
				t.Fatalf("%s count=%d want=%d err=%v", table, n, want[i], e)
			}
		}
	}
	t.Run("durable ACK exact duplicates and conflicting content", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		u, _ := usecases.NewAccept("1", 64<<20, s)
		h, _ := receiver.NewReceiver("1", grpccontract.DefaultLimits(), u)
		response, e := h.SubmitTransformedBlock(ctx, wire(r))
		if e != nil || response.Acceptance.Status != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED {
			t.Fatal(response, e)
		}
		counts(1, 0, 0, 0, 0)
		response, e = h.SubmitTransformedBlock(ctx, wire(r))
		if e != nil || response.Acceptance.Status != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
			t.Fatal(response, e)
		}
		var payload []byte
		var extracted, transformed string
		if e = runtime.QueryRow(ctx, "SELECT payload,extracted_at,transformed_at FROM loader.inbox_events WHERE event_id=$1", r.EventID).Scan(&payload, &extracted, &transformed); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(payload, r.PayloadJSON) || extracted != r.ExtractedAt.Format(time.RFC3339Nano) || transformed != r.TransformedAt.Format(time.RFC3339Nano) {
			t.Fatal("exact content lost")
		}
		for _, change := range []func(*inbound.AcceptTransformedBlockRequest){
			func(r *inbound.AcceptTransformedBlockRequest) {
				r.PayloadJSON = append(bytes.Clone(r.PayloadJSON), ' ')
			}, func(r *inbound.AcceptTransformedBlockRequest) { r.Provider = "other" },
			func(r *inbound.AcceptTransformedBlockRequest) { r.ExtractedAt = r.ExtractedAt.Add(time.Nanosecond) }, func(r *inbound.AcceptTransformedBlockRequest) { r.TransformedAt = r.TransformedAt.Add(time.Nanosecond) },
			func(r *inbound.AcceptTransformedBlockRequest) { r.Block.ParentHash[0]++ },
		} {
			other := r
			change(&other)
			if response, e := h.SubmitTransformedBlock(ctx, wire(other)); response != nil || status.Code(e) != codes.AlreadyExists {
				t.Fatal("conflict ACKed", response, e)
			}
		}
		counts(1, 0, 0, 0, 0)
	})
	t.Run("concurrent duplicate acceptance and exclusive claim", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() { _, e := s.Accept(ctx, r); errs <- e })
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		items := make(chan *outbound.Item, 8)
		errs = make(chan error, 8)
		for range 8 {
			wg.Go(func() { i, e := s.Claim(ctx); items <- i; errs <- e })
		}
		wg.Wait()
		close(items)
		close(errs)
		found := 0
		for i := range items {
			if i != nil {
				found++
			}
		}
		for e := range errs {
			if e != nil {
				t.Fatal(e)
			}
		}
		if found != 1 {
			t.Fatal("nonexclusive claim", found)
		}
		counts(1, 0, 0, 0, 0)
	})
	t.Run("normalized records exact numbers and duplicate completion", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		item := claim(s)
		d := document(r)
		if e := s.Complete(ctx, *item, d); e != nil {
			t.Fatal(e)
		}
		if e := s.Complete(ctx, *item, d); e != nil {
			t.Fatal("completion replay", e)
		}
		counts(1, 1, 2, 2, 1)
		accept(s, r)
		if got, e := process(s).ProcessOne(ctx); e != nil || got.Found {
			t.Fatal("duplicate reprocessed", got, e)
		}
		var value, at string
		var creation, absent, totalNull, totalPresent bool
		if e := runtime.QueryRow(ctx, `SELECT value::text FROM loader.transactions WHERE transaction_index=0`).Scan(&value); e != nil {
			t.Fatal(e)
		}
		if value != "115792089237316195423570985008687907853269984665640564039457584007913129639935" {
			t.Fatal("quantity truncated", value)
		}
		if e := runtime.QueryRow(ctx, `SELECT to_address IS NULL, NOT ('access_list'=ANY(present_fields)) FROM loader.transactions WHERE transaction_index=1`).Scan(&creation, &absent); e != nil {
			t.Fatal(e)
		}
		if e := runtime.QueryRow(ctx, `SELECT total_difficulty IS NULL,'total_difficulty'=ANY(present_fields) FROM loader.blocks`).Scan(&totalNull, &totalPresent); e != nil {
			t.Fatal(e)
		}
		if !creation || !absent || !totalNull || !totalPresent {
			t.Fatal("optional presence lost")
		}
		if e := runtime.QueryRow(ctx, `SELECT i.extracted_at FROM loader.blocks b JOIN loader.inbox_events i USING(event_id)`).Scan(&at); e != nil || at != r.ExtractedAt.Format(time.RFC3339Nano) {
			t.Fatal("provenance lost", e)
		}
		var failed bool
		var topics [][]byte
		if e := runtime.QueryRow(ctx, `SELECT gas_used::text,status=0 FROM loader.receipts WHERE transaction_index=1`).Scan(new(string), &failed); e != nil || !failed {
			t.Fatal("failed receipt lost", e)
		}
		if e := runtime.QueryRow(ctx, `SELECT topics FROM loader.logs`).Scan(&topics); e != nil || len(topics) != 1 {
			t.Fatal("topics lost", e)
		}
	})
	t.Run("restart lease fencing and final crash", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		old := claim(s)
		exec("UPDATE loader.inbox_events SET lease_until=clock_timestamp()-interval '1 second'")
		p := integrationPool(t, "LOADER_TEST_DATABASE_URL", "loader_runtime")
		restarted, _ := New(p, "1", time.Minute, 3)
		current := claim(restarted)
		if current.Attempt != old.Attempt+1 || !bytes.Equal(current.Event.PayloadJSON, r.PayloadJSON) {
			t.Fatal("restart lost state")
		}
		if e := s.Complete(ctx, *old, document(r)); !errors.Is(e, outbound.ErrLeaseLost) {
			t.Fatal(e)
		}
		if e := s.Fail(ctx, *old, outbound.Failure{Code: "transient"}); !errors.Is(e, outbound.ErrLeaseLost) {
			t.Fatal(e)
		}
		if e := restarted.Complete(ctx, *current, document(r)); e != nil {
			t.Fatal(e)
		}
		counts(1, 1, 2, 2, 1)
		clear()
		accept(s, r)
		_ = claim(s)
		exec("UPDATE loader.inbox_events SET lease_until=clock_timestamp()-interval '1 second',attempts=max_attempts")
		if i, e := s.Claim(ctx); e != nil || i != nil || state(r.EventID) != "failed" {
			t.Fatal("final crash not terminal", e)
		}
	})
	t.Run("poison missing receipts retains accepted input", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		var d map[string]json.RawMessage
		_ = json.Unmarshal(r.PayloadJSON, &d)
		d["receipts"] = json.RawMessage(`[]`)
		r.PayloadJSON, _ = json.Marshal(d)
		accept(s, r)
		got, e := process(s).ProcessOne(ctx)
		if e != nil || got.Outcome != "failed" || state(r.EventID) != "failed" {
			t.Fatal(got, e)
		}
		counts(1, 0, 0, 0, 0)
		accept(s, r)
		if i, e := s.Claim(ctx); e != nil || i != nil {
			t.Fatal("poison reset", e)
		}
		var code string
		if e := runtime.QueryRow(ctx, "SELECT last_error_code FROM loader.inbox_events").Scan(&code); e != nil || code != "incomplete" {
			t.Fatal(code, e)
		}
	})
	t.Run("transient backoff and exhaustion", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		i := claim(s)
		if e := s.Fail(ctx, *i, outbound.Failure{Code: "transient", Delay: time.Hour}); e != nil {
			t.Fatal(e)
		}
		if i, e := s.Claim(ctx); e != nil || i != nil {
			t.Fatal("early retry", e)
		}
		for attempt := 2; attempt <= 3; attempt++ {
			exec("UPDATE loader.inbox_events SET next_attempt_at=clock_timestamp()-interval '1 second'")
			i = claim(s)
			if i.Attempt != attempt {
				t.Fatal(i.Attempt)
			}
			if e := s.Fail(ctx, *i, outbound.Failure{Code: "transient"}); e != nil {
				t.Fatal(e)
			}
		}
		if state(r.EventID) != "failed" {
			t.Fatal("retry budget ignored")
		}
		counts(1, 0, 0, 0, 0)
	})
	t.Run("rollback at every persistence boundary", func(t *testing.T) {
		for _, table := range []string{"inbox_events", "blocks", "transactions", "receipts", "logs", "completion"} {
			t.Run(table, func(t *testing.T) {
				clear()
				s := fresh()
				r := testEvent(t)
				var item *outbound.Item
				if table != "inbox_events" {
					accept(s, r)
					item = claim(s)
				}
				exec(`CREATE FUNCTION loader.stage8_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$`)
				triggerTable, action := table, "INSERT"
				if table == "completion" {
					triggerTable = "inbox_events"
					action = "UPDATE"
				}
				exec("CREATE TRIGGER stage8_fail BEFORE " + action + " ON loader." + triggerTable + " FOR EACH ROW EXECUTE FUNCTION loader.stage8_fail()")
				defer exec("DROP FUNCTION loader.stage8_fail() CASCADE")
				if table == "inbox_events" {
					u, _ := usecases.NewAccept("1", 64<<20, s)
					h, _ := receiver.NewReceiver("1", grpccontract.DefaultLimits(), u)
					if response, e := h.SubmitTransformedBlock(ctx, wire(r)); response != nil || status.Code(e) != codes.Unavailable {
						t.Fatal("ACK before commit", response, e)
					}
					counts(0, 0, 0, 0, 0)
				} else {
					if e := s.Complete(ctx, *item, document(r)); e == nil {
						t.Fatal("injected failure ignored")
					}
					counts(1, 0, 0, 0, 0)
					if state(r.EventID) != "processing" {
						t.Fatal("partial completion")
					}
				}
			})
		}
	})
	t.Run("ambiguous acceptance and completion commits", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			clear()
			s := fresh()
			r := testEvent(t)
			faulty := *s
			faulty.db = faultDB{s.db, after}
			if _, e := faulty.Accept(ctx, r); !errors.Is(e, outbound.ErrCommitUnknown) {
				t.Fatal(e)
			}
			duplicate, e := s.Accept(ctx, r)
			if e != nil || duplicate != after {
				t.Fatal("acceptance recovery", duplicate, e)
			}
			i := claim(s)
			if e := faulty.Complete(ctx, *i, document(r)); !errors.Is(e, outbound.ErrCommitUnknown) {
				t.Fatal(e)
			}
			if after {
				counts(1, 1, 2, 2, 1)
			} else {
				counts(1, 0, 0, 0, 0)
			}
			if e := s.Complete(ctx, *i, document(r)); e != nil {
				t.Fatal("commit recovery", e)
			}
			counts(1, 1, 2, 2, 1)
		}
	})
	t.Run("late out of order and maximum height", func(t *testing.T) {
		clear()
		s := fresh()
		for _, r := range []inbound.AcceptTransformedBlockRequest{emptyEvent(t, 12, 12, 11), emptyEvent(t, 10, 10, 9), emptyEvent(t, 11, 11, 10), emptyEvent(t, ^uint64(0), 99, 98)} {
			accept(s, r)
			got, e := process(s).ProcessOne(ctx)
			if e != nil || got.Outcome != "completed" {
				t.Fatal(got, e)
			}
		}
		counts(4, 4, 0, 0, 0)
	})
	t.Run("lease expiry during persistence rolls everything back", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		item := claim(s)
		exec(`CREATE FUNCTION loader.stage8_expire() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE loader.inbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=NEW.event_id; RETURN NEW; END $$`)
		exec("CREATE TRIGGER stage8_expire AFTER INSERT ON loader.blocks FOR EACH ROW EXECUTE FUNCTION loader.stage8_expire()")
		defer exec("DROP FUNCTION loader.stage8_expire() CASCADE")
		if e := s.Complete(ctx, *item, document(r)); !errors.Is(e, outbound.ErrLeaseLost) {
			t.Fatal(e)
		}
		counts(1, 0, 0, 0, 0)
		if state(r.EventID) != "processing" {
			t.Fatal("partial completion after lease expiry")
		}
	})
	t.Run("cancel while waiting for inbox row lock", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		item := claim(s)
		tx, e := owner.Begin(ctx)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, "SELECT event_id FROM loader.inbox_events WHERE event_id=$1 FOR UPDATE", r.EventID); e != nil {
			t.Fatal(e)
		}
		limited, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if e = s.Complete(limited, *item, document(r)); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
		if e = tx.Rollback(ctx); e != nil {
			t.Fatal(e)
		}
		counts(1, 0, 0, 0, 0)
		if e = s.Complete(ctx, *item, document(r)); e != nil {
			t.Fatal("lock leak or unrecoverable cancellation", e)
		}
		counts(1, 1, 2, 2, 1)
	})
	t.Run("least privilege and canceled IO", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		for _, q := range []string{"SELECT * FROM extractor.raw_blocks", "SELECT * FROM transformer.inbox_events", "UPDATE loader.inbox_events SET payload='bad'::bytea", "DELETE FROM loader.inbox_events", "UPDATE loader.blocks SET gas_used=0", "DELETE FROM loader.transactions", "CREATE TABLE loader.forbidden(id integer)", "UPDATE loader.schema_migrations SET checksum='bad'"} {
			if _, e := runtime.Exec(ctx, q); e == nil {
				t.Fatal("unauthorized operation", q)
			}
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, e := s.Claim(canceled); !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
		counts(1, 0, 0, 0, 0)
	})
}
