//go:build postgresintegration

// integration_test.go exercises real owning-role PostgreSQL transactions, claims and failure
// recovery in the outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/gen/etl/v1"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/grpccontract"
	receiver "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/inbound/grpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/usecases"
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
		t.Skip("run go run ./test/integration/cmd/transformer-postgres for an isolated database")
	}
	if os.Getenv("TRANSFORMER_TEST_ISOLATED") != "1" {
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
func testEvent(t *testing.T) inbound.AcceptRawBlockRequest {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile("../ethereumjson/testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var h, p [32]byte
	for i := range h {
		h[i] = 0x10
		p[i] = 0x11
	}
	return inbound.AcceptRawBlockRequest{EventID: fmt.Sprintf("raw-block/v1/1/20000000/%x", h), EventVersion: 1, PayloadVersion: 1, Block: inbound.BlockIdentity{ChainID: "1", Number: 20000000, Hash: h, ParentHash: p}, Provider: "fixture", ExtractedAt: time.Unix(100, 123).UTC(), RawBlock: read("block.json"), RawReceipts: [][]byte{read("receipt_0.json"), read("receipt_1.json")}}
}

// wire loads the controlled normalized/raw payload used by persistence integration assertions.
func wire(r inbound.AcceptRawBlockRequest) *etlv1.SubmitRawBlockRequest {
	return &etlv1.SubmitRawBlockRequest{Metadata: &etlv1.EventMetadata{EventId: r.EventID, EventVersion: 1, PayloadVersion: 1,
		Block: &etlv1.BlockIdentity{ChainId: r.Block.ChainID, Number: proto.Uint64(r.Block.Number), BlockHash: r.Block.Hash[:], ParentHash: r.Block.ParentHash[:]}, Provider: r.Provider, ExtractedAt: timestamppb.New(r.ExtractedAt)},
		Payload: &etlv1.RawBlockPayload{BlockJson: r.RawBlock, Receipts: &etlv1.RawReceipts{Json: r.RawReceipts}}}
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
	runtime := integrationPool(t, "TRANSFORMER_TEST_DATABASE_URL", "transformer_runtime")
	owner := integrationPool(t, "TRANSFORMER_TEST_MIGRATOR_URL", "transformer_migrator")
	t.Run("bounded admission", func(t *testing.T) { testAdmission(t, runtime, owner) })
	t.Run("operational metrics", func(t *testing.T) { testMetrics(t, runtime, owner) })
	t.Run("schema readiness", func(t *testing.T) { testReadiness(t, runtime, owner) })
	t.Run("outbox delivery", func(t *testing.T) { testOutbox(t, runtime, owner) })
	t.Run("audited recovery", func(t *testing.T) { testRepair(t, runtime, owner) })
	ctx := t.Context()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := owner.Exec(ctx, sql, args...); err != nil {
			t.Fatal("owner SQL operation failed", err)
		}
	}
	fresh := func() *Store {
		s, err := New(runtime, "1", time.Minute, 3)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	clear := func() { exec("TRUNCATE transformer.inbox_events CASCADE") }
	counts := func(inbox, results, outbox int) {
		t.Helper()
		var a, b, c int
		err := runtime.QueryRow(ctx, `SELECT (SELECT count(*) FROM transformer.inbox_events),(SELECT count(*) FROM transformer.transformed_blocks),(SELECT count(*) FROM transformer.outbox_events)`).Scan(&a, &b, &c)
		if err != nil || a != inbox || b != results || c != outbox {
			t.Fatalf("counts=%d/%d/%d want=%d/%d/%d err=%v", a, b, c, inbox, results, outbox, err)
		}
	}
	accept := func(s *Store, r inbound.AcceptRawBlockRequest) {
		t.Helper()
		if _, err := s.Accept(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	claim := func(s *Store) *outbound.Item {
		t.Helper()
		item, err := s.Claim(ctx)
		if err != nil || item == nil {
			t.Fatal("claim", err)
		}
		return item
	}
	result := func(item *outbound.Item) outbound.Result {
		return outbound.Result{EventID: fmt.Sprintf("transformed-block/v1/%s/%d/%x", item.Event.Block.ChainID, item.Event.Block.Number, item.Event.Block.Hash), PayloadVersion: 1, Payload: []byte(`{"schema_version":1}`)}
	}
	makeProcess := func(s *Store) *usecases.Process {
		e, _ := jsondocument.New(64 << 20)
		p, err := usecases.NewProcess(s, ethereumjson.Decoder{}, e, time.Millisecond, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	statusOf := func(id string) string {
		var state string
		if err := runtime.QueryRow(ctx, "SELECT status FROM transformer.inbox_events WHERE event_id=$1", id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}

	t.Run("durable ACK duplicate conflict and exact bytes", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		a, _ := usecases.NewAccept("1", 64<<20, s)
		handler, _ := receiver.NewReceiver("1", grpccontract.DefaultLimits(), a)
		response, err := handler.SubmitRawBlock(ctx, wire(r))
		if err != nil || response.Acceptance.Status != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED {
			t.Fatal(response, err)
		}
		counts(1, 0, 0)
		response, err = handler.SubmitRawBlock(ctx, wire(r))
		if err != nil || response.Acceptance.Status != etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED {
			t.Fatal(response, err)
		}
		var block []byte
		var receipts [][]byte
		var extracted string
		if err := runtime.QueryRow(ctx, "SELECT raw_block,raw_receipts,extracted_at FROM transformer.inbox_events WHERE event_id=$1", r.EventID).Scan(&block, &receipts, &extracted); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(block, r.RawBlock) || len(receipts) != 2 || !bytes.Equal(receipts[1], r.RawReceipts[1]) || extracted != r.ExtractedAt.Format(time.RFC3339Nano) {
			t.Fatal("source bytes/provenance changed")
		}
		for _, change := range []func(*inbound.AcceptRawBlockRequest){
			func(r *inbound.AcceptRawBlockRequest) { r.RawBlock = append(bytes.Clone(r.RawBlock), ' ') },
			func(r *inbound.AcceptRawBlockRequest) {
				r.RawReceipts = [][]byte{append(bytes.Clone(r.RawReceipts[0]), ' '), r.RawReceipts[1]}
			},
			func(r *inbound.AcceptRawBlockRequest) { r.Provider = "other" }, func(r *inbound.AcceptRawBlockRequest) { r.ExtractedAt = r.ExtractedAt.Add(time.Nanosecond) },
		} {
			other := r
			change(&other)
			response, err := handler.SubmitRawBlock(ctx, wire(other))
			if response != nil || status.Code(err) != codes.AlreadyExists {
				t.Fatal("conflicting event ACKed", response, err)
			}
		}
		counts(1, 0, 0)
	})
	t.Run("concurrent deduplication and exclusive claims", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for range 8 {
			wg.Go(func() { _, err := s.Accept(ctx, r); errs <- err })
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		counts(1, 0, 0)
		items := make(chan *outbound.Item, 8)
		errs = make(chan error, 8)
		for range 8 {
			wg.Go(func() { item, err := s.Claim(ctx); items <- item; errs <- err })
		}
		wg.Wait()
		close(items)
		close(errs)
		found := 0
		for item := range items {
			if item != nil {
				found++
			}
		}
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if found != 1 {
			t.Fatal("claim not exclusive", found)
		}
	})
	t.Run("restart expired lease and stale fencing", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		old := claim(s)
		exec("UPDATE transformer.inbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", r.EventID)
		freshPool := integrationPool(t, "TRANSFORMER_TEST_DATABASE_URL", "transformer_runtime")
		restarted, _ := New(freshPool, "1", time.Minute, 3)
		current := claim(restarted)
		if current.Attempt != old.Attempt+1 || !bytes.Equal(current.Event.RawBlock, r.RawBlock) {
			t.Fatal("restart lost state")
		}
		if err := s.Complete(ctx, *old, result(old)); !errors.Is(err, outbound.ErrLeaseLost) {
			t.Fatal(err)
		}
		if err := s.Fail(ctx, *old, outbound.Failure{Code: "transient"}); !errors.Is(err, outbound.ErrLeaseLost) {
			t.Fatal(err)
		}
		if err := restarted.Complete(ctx, *current, result(current)); err != nil {
			t.Fatal(err)
		}
		counts(1, 1, 1)
		if item, err := s.Claim(ctx); item != nil || err != nil {
			t.Fatal("completed event reclaimed")
		}
	})
	t.Run("transformation without loader and replay", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		progress, err := makeProcess(s).ProcessOne(ctx)
		if err != nil || progress.Outcome != "completed" {
			t.Fatal(progress, err)
		}
		counts(1, 1, 1)
		var payload []byte
		if err := runtime.QueryRow(ctx, "SELECT payload FROM transformer.transformed_blocks").Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(payload, []byte(`"schema_version":1`)) || !bytes.Contains(payload, []byte(`"status":"0"`)) {
			t.Fatal("incorrect normalized result")
		}
		accept(s, r)
		if progress, err := makeProcess(fresh()).ProcessOne(ctx); err != nil || progress.Found {
			t.Fatal("duplicate reprocessed", progress, err)
		}
		counts(1, 1, 1)
	})
	t.Run("poison missing receipt retained", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		r.RawReceipts = r.RawReceipts[:1]
		accept(s, r)
		progress, err := makeProcess(s).ProcessOne(ctx)
		if err != nil || progress.Outcome != "failed" || statusOf(r.EventID) != "failed" {
			t.Fatal(progress, err)
		}
		counts(1, 0, 0)
		accept(s, r)
		if item, err := s.Claim(ctx); err != nil || item != nil {
			t.Fatal("poison reset")
		}
	})
	t.Run("retry delay and final crashed attempt", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		item := claim(s)
		if err := s.Fail(ctx, *item, outbound.Failure{Code: "transient", Delay: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if item, err := s.Claim(ctx); err != nil || item != nil {
			t.Fatal("retried before backoff")
		}
		exec("UPDATE transformer.inbox_events SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE event_id=$1", r.EventID)
		item = claim(s)
		if item.Attempt != 2 {
			t.Fatal(item.Attempt)
		}
		exec("UPDATE transformer.inbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", r.EventID)
		item = claim(s)
		if item.Attempt != 3 {
			t.Fatal(item.Attempt)
		}
		exec("UPDATE transformer.inbox_events SET lease_until=clock_timestamp()-interval '1 second' WHERE event_id=$1", r.EventID)
		if item, err := s.Claim(ctx); item != nil || err != nil || statusOf(r.EventID) != "failed" {
			t.Fatal("exhausted crash not terminal", err)
		}
		counts(1, 0, 0)
	})
	t.Run("atomic rollback at every write boundary", func(t *testing.T) {
		for _, table := range []string{"inbox_events", "transformed_blocks", "outbox_events"} {
			t.Run(table, func(t *testing.T) {
				clear()
				s := fresh()
				r := testEvent(t)
				if table != "inbox_events" {
					accept(s, r)
				}
				exec(`CREATE FUNCTION transformer.stage7_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$`)
				exec("CREATE TRIGGER stage7_fail BEFORE INSERT ON transformer." + table + " FOR EACH ROW EXECUTE FUNCTION transformer.stage7_fail()")
				defer exec("DROP FUNCTION transformer.stage7_fail() CASCADE")
				if table == "inbox_events" {
					a, _ := usecases.NewAccept("1", 64<<20, s)
					h, _ := receiver.NewReceiver("1", grpccontract.DefaultLimits(), a)
					if response, err := h.SubmitRawBlock(ctx, wire(r)); response != nil || status.Code(err) != codes.Unavailable {
						t.Fatal("ACK before commit", response, err)
					}
					counts(0, 0, 0)
				} else {
					item := claim(s)
					if err := s.Complete(ctx, *item, result(item)); err == nil {
						t.Fatal("injected failure ignored")
					}
					counts(1, 0, 0)
					if statusOf(r.EventID) != "processing" {
						t.Fatal("partial completion")
					}
				}
			})
		}
	})
	t.Run("ambiguous acceptance and completion commit", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			clear()
			s := fresh()
			r := testEvent(t)
			faulty := *s
			faulty.db = faultDB{s.db, after}
			if _, err := faulty.Accept(ctx, r); !errors.Is(err, outbound.ErrCommitUnknown) {
				t.Fatal(err)
			}
			duplicate, err := s.Accept(ctx, r)
			if err != nil || duplicate != after {
				t.Fatal("acceptance recovery", duplicate, err)
			}
			counts(1, 0, 0)
			item := claim(s)
			if err := faulty.Complete(ctx, *item, result(item)); !errors.Is(err, outbound.ErrCommitUnknown) {
				t.Fatal(err)
			}
			if after {
				counts(1, 1, 1)
				if statusOf(r.EventID) != "completed" {
					t.Fatal("commit not retained")
				}
			} else {
				counts(1, 0, 0)
				if err := s.Complete(ctx, *item, result(item)); err != nil {
					t.Fatal(err)
				}
				counts(1, 1, 1)
			}
		}
	})
	t.Run("least privilege and cancellation", func(t *testing.T) {
		clear()
		s := fresh()
		r := testEvent(t)
		accept(s, r)
		for _, sql := range []string{
			"SELECT * FROM extractor.raw_blocks", "SELECT * FROM loader.schema_migrations",
			"UPDATE transformer.inbox_events SET raw_block='bad'::bytea",
			"DELETE FROM transformer.inbox_events", "UPDATE transformer.transformed_blocks SET payload='bad'::bytea",
			"CREATE TABLE transformer.forbidden (id integer)",
		} {
			if _, err := runtime.Exec(ctx, sql); err == nil {
				t.Fatal("unauthorized SQL succeeded", sql)
			}
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Claim(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		counts(1, 0, 0)
	})
}
