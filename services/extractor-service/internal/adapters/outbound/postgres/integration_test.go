//go:build postgresintegration

package postgres

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationPool(t *testing.T, variable, role string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(variable)
	if dsn == "" {
		t.Skip("run go run ./test/integration/cmd/extractor-postgres from the repository root to provide an isolated database")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid integration database configuration")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	p, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(databaseError(err))
	}
	t.Cleanup(p.Close)
	var user, db string
	if err := p.QueryRow(t.Context(), `SELECT current_user,current_database()`).Scan(&user, &db); err != nil {
		t.Fatal(databaseError(err))
	}
	if user != role || db != "ethereum_etl" {
		t.Fatal("integration test requires the dedicated service role and database")
	}
	return p
}

func TestPostgresIntegration(t *testing.T) {
	runtime := integrationPool(t, "EXTRACTOR_TEST_DATABASE_URL", "extractor_runtime")
	owner := integrationPool(t, "EXTRACTOR_TEST_MIGRATOR_URL", "extractor_migrator")
	ctx := t.Context()
	var next uint64 = 100000
	newStore := func(t *testing.T, start uint64) *Store {
		t.Helper()
		next++
		s, err := New(runtime, domain.ChainID(strconv.FormatUint(next, 10)), start)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	commit := func(t *testing.T, s *Store, e domain.Extraction, cp *domain.Checkpoint) outbound.CommitResult {
		t.Helper()
		r, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: e, ExpectedCheckpoint: cp})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	checkpoint := func(t *testing.T, s *Store) *domain.Checkpoint {
		t.Helper()
		cp, err := s.Checkpoint(ctx, s.chain)
		if err != nil {
			t.Fatal(err)
		}
		return cp
	}
	counts := func(t *testing.T, s *Store, raw, outbox, cp int) {
		t.Helper()
		var a, b, c int
		err := runtime.QueryRow(ctx, `SELECT
(SELECT count(*) FROM extractor.raw_blocks WHERE chain_id=$1),
(SELECT count(*) FROM extractor.outbox_events WHERE chain_id=$1),
(SELECT count(*) FROM extractor.extraction_checkpoints WHERE chain_id=$1)`, string(s.chain)).Scan(&a, &b, &c)
		if err != nil {
			t.Fatal(databaseError(err))
		}
		if a != raw || b != outbox || c != cp {
			t.Fatalf("counts=(%d,%d,%d), want=(%d,%d,%d)", a, b, c, raw, outbox, cp)
		}
	}
	t.Run("duplicate provenance and restart recovery", func(t *testing.T) {
		s := newStore(t, 42)
		e := snapshot(s.chain, 42, 2, 1)
		if cp := checkpoint(t, s); cp != nil {
			t.Fatal("unexpected checkpoint")
		}
		if commit(t, s, e, nil).AlreadyStored {
			t.Fatal("first insert is a duplicate")
		}
		cp := checkpoint(t, s)
		if cp == nil || cp.Block != e.Header.ID {
			t.Fatal("wrong checkpoint")
		}
		altered := e.Clone()
		altered.Provider = "another-provider"
		altered.ExtractedAt = e.ExtractedAt.Add(time.Hour)
		altered.RawBlock = []byte(`{"different":"retry"}`)
		if !commit(t, s, altered, nil).AlreadyStored {
			t.Fatal("lost-response retry not deduplicated")
		}
		got, err := s.Load(ctx, e.Header.ID)
		if err != nil || !reflect.DeepEqual(got, e) {
			t.Fatalf("snapshot/provenance changed: %v", err)
		}
		var event string
		if err := runtime.QueryRow(ctx, `SELECT event_id FROM extractor.outbox_events WHERE chain_id=$1`, string(s.chain)).Scan(&event); err != nil || event != e.Header.ID.EventID() {
			t.Fatal("event identity mismatch")
		}
		// Independent pool simulates a fresh process with no remembered progress.
		fresh := integrationPool(t, "EXTRACTOR_TEST_DATABASE_URL", "extractor_runtime")
		restarted, err := New(fresh, s.chain, 42)
		if err != nil {
			t.Fatal(err)
		}
		recovered := checkpoint(t, restarted)
		if !sameCheckpoint(cp, recovered) {
			t.Fatal("checkpoint not recovered")
		}
		child := snapshot(s.chain, 43, 3, 2)
		commit(t, restarted, child, recovered)
		if !commit(t, s, e, nil).AlreadyStored {
			t.Fatal("old exact retry must be harmless")
		}
		if checkpoint(t, s).Block != child.Header.ID {
			t.Fatal("checkpoint regressed")
		}
		counts(t, s, 2, 2, 1)
	})
	t.Run("duplicate identity is not snapshot equivalence", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			change func(*domain.Extraction)
		}{
			{"raw block", func(e *domain.Extraction) { e.RawBlock = []byte(`{"providerExtension":true}`) }},
			{"raw receipt", func(e *domain.Extraction) { e.RawReceipts[0] = []byte(`{"providerExtension":true}`) }},
			{"typed metadata", func(e *domain.Extraction) { e.Header.Timestamp++; e.Receipts[0].Logs[0].Data = []byte{42} }},
			{"provenance", func(e *domain.Extraction) {
				e.Provider = "other-provider"
				e.ExtractedAt = e.ExtractedAt.Add(time.Hour)
			}},
			// Valid according to the domain, but intentionally outside RFC3339.
			// A retry must not encode provenance that will never replace the original.
			{"unencodable replacement provenance", func(e *domain.Extraction) { e.ExtractedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		} {
			t.Run(test.name, func(t *testing.T) {
				s := newStore(t, 42)
				original := snapshot(s.chain, 42, 2, 1)
				commit(t, s, original, nil)
				retry := original.Clone()
				test.change(&retry)
				if err := retry.Validate(); err != nil {
					t.Fatal(err)
				}
				if !commit(t, s, retry, nil).AlreadyStored {
					t.Fatal("identity retry not deduplicated")
				}
				stored, err := s.Load(ctx, original.Header.ID)
				if err != nil || !reflect.DeepEqual(stored, original) {
					t.Fatalf("first snapshot changed: %v", err)
				}
				counts(t, s, 1, 1, 1)
			})
		}
		// Deferring storage encoding must not accept an unencodable NEW snapshot.
		s := newStore(t, 42)
		e := snapshot(s.chain, 42, 2, 1)
		e.ExtractedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: e}); !errors.Is(err, domain.ErrInvalidData) {
			t.Fatal(err)
		}
		counts(t, s, 0, 0, 0)
		e.ExtractedAt = time.Now().UTC()
		commit(t, s, e, nil)
		// Identity-only deduplication does not bypass domain completeness checks.
		e.RawReceipts = nil
		if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: e}); !errors.Is(err, domain.ErrIncomplete) {
			t.Fatal(err)
		}
		counts(t, s, 1, 1, 1)
	})
	t.Run("conflicts and sequence boundaries", func(t *testing.T) {
		for _, test := range []struct {
			name         string
			number       uint64
			hash, parent byte
			expected     string
			want         error
		}{
			{"gap", 44, 4, 2, "current", domain.ErrOutOfSequence},
			{"older absent block", 41, 4, 1, "current", domain.ErrOutOfSequence},
			{"parent mismatch", 43, 3, 9, "current", domain.ErrBlockConflict},
			{"same height different hash", 42, 9, 1, "current", domain.ErrBlockConflict},
			{"same hash different parent", 42, 2, 9, "current", domain.ErrBlockConflict},
			{"stale nil checkpoint", 43, 3, 2, "nil", domain.ErrCheckpointConflict},
			{"expected hash conflict", 43, 3, 2, "wrong hash", domain.ErrCheckpointConflict},
		} {
			t.Run(test.name, func(t *testing.T) {
				s := newStore(t, 42)
				commit(t, s, snapshot(s.chain, 42, 2, 1), nil)
				cp := checkpoint(t, s)
				if test.expected == "nil" {
					cp = nil
				} else if test.expected == "wrong hash" {
					cp.Block.Hash = domain.Hash{9}
				}
				_, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, test.number, test.hash, test.parent), ExpectedCheckpoint: cp})
				if !errors.Is(err, test.want) {
					t.Fatalf("want %v, got %v", test.want, err)
				}
				counts(t, s, 1, 1, 1)
				if checkpoint(t, s).Block.Number != 42 {
					t.Fatal("failure advanced checkpoint")
				}
			})
		}
		s := newStore(t, 42)
		if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 43, 3, 2)}); !errors.Is(err, domain.ErrOutOfSequence) {
			t.Fatal(err)
		}
		counts(t, s, 0, 0, 0)
		s = newStore(t, math.MaxUint64)
		e := snapshot(s.chain, math.MaxUint64, 2, 1)
		commit(t, s, e, nil)
		if checkpoint(t, s).Block.Number != math.MaxUint64 {
			t.Fatal("uint64 was truncated")
		}
		if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 0, 3, 2), ExpectedCheckpoint: checkpoint(t, s)}); !errors.Is(err, domain.ErrOutOfSequence) {
			t.Fatal(err)
		}
	})
	t.Run("rollback after each write and retry", func(t *testing.T) {
		for _, table := range []string{"raw_blocks", "outbox_events", "extraction_checkpoints"} {
			for _, continuation := range []bool{false, true} {
				t.Run(table+strconv.FormatBool(continuation), func(t *testing.T) {
					s := newStore(t, 42)
					e := snapshot(s.chain, 42, 2, 1)
					var cp *domain.Checkpoint
					before := 0
					if continuation {
						commit(t, s, e, nil)
						cp = checkpoint(t, s)
						before = 1
						e = snapshot(s.chain, 43, 3, 2)
					}
					_, err := owner.Exec(ctx, `CREATE FUNCTION extractor.stage5_fail() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'injected partial failure'; END $$`)
					if err != nil {
						t.Fatal(databaseError(err))
					}
					cleanup := func() {
						_, err := owner.Exec(ctx, `DROP TRIGGER IF EXISTS stage5_fail ON extractor.`+table+`; DROP FUNCTION IF EXISTS extractor.stage5_fail()`)
						if err != nil {
							t.Error(databaseError(err))
						}
					}
					t.Cleanup(cleanup)
					_, err = owner.Exec(ctx, `CREATE TRIGGER stage5_fail BEFORE INSERT OR UPDATE ON extractor.`+table+` FOR EACH ROW EXECUTE FUNCTION extractor.stage5_fail()`)
					if err != nil {
						t.Fatal(databaseError(err))
					}
					_, err = s.Commit(ctx, outbound.CommitExtraction{Extraction: e, ExpectedCheckpoint: cp})
					if !errors.Is(err, ErrDatabase) || errors.Is(err, ErrCommitUnknown) {
						t.Fatalf("expected definite rollback, got %v", err)
					}
					counts(t, s, before, before, before)
					if !sameCheckpoint(cp, checkpoint(t, s)) {
						t.Fatal("partial checkpoint")
					}
					cleanup()
					commit(t, s, e, cp)
					counts(t, s, before+1, before+1, 1)
				})
			}
		}
	})
	t.Run("concurrent bootstrap and continuation", func(t *testing.T) {
		for _, conflicting := range []bool{false, true} {
			for _, continuation := range []bool{false, true} {
				t.Run(strconv.FormatBool(conflicting)+"/"+strconv.FormatBool(continuation), func(t *testing.T) {
					s := newStore(t, 42)
					e := snapshot(s.chain, 42, 2, 1)
					var cp *domain.Checkpoint
					before := 0
					if continuation {
						commit(t, s, e, nil)
						cp = checkpoint(t, s)
						e = snapshot(s.chain, 43, 3, 2)
						before = 1
					}
					other := e.Clone()
					if conflicting {
						other = snapshot(s.chain, e.Header.ID.Number, 9, e.Header.ParentHash[0])
					}
					start := make(chan struct{})
					var wg sync.WaitGroup
					results := make([]outbound.CommitResult, 2)
					errs := make([]error, 2)
					for i, input := range []domain.Extraction{e, other} {
						wg.Go(func() {
							<-start
							results[i], errs[i] = s.Commit(ctx, outbound.CommitExtraction{Extraction: input, ExpectedCheckpoint: cp})
						})
					}
					close(start)
					wg.Wait()
					created, duplicates, conflicts := 0, 0, 0
					for i, err := range errs {
						if errors.Is(err, domain.ErrBlockConflict) {
							conflicts++
						} else if err != nil {
							t.Fatal(err)
						} else if results[i].AlreadyStored {
							duplicates++
						} else {
							created++
						}
					}
					if created != 1 || conflicting && conflicts != 1 || !conflicting && duplicates != 1 {
						t.Fatalf("created=%d duplicates=%d conflicts=%d", created, duplicates, conflicts)
					}
					counts(t, s, before+1, before+1, 1)
				})
			}
		}
	})
	t.Run("ambiguous commit before and after server commit", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			t.Run(strconv.FormatBool(after), func(t *testing.T) {
				s := newStore(t, 42)
				e := snapshot(s.chain, 42, 2, 1)
				s.db = commitFaultDB{database: runtime, after: after}
				_, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: e})
				if !errors.Is(err, ErrCommitUnknown) {
					t.Fatalf("must not claim outcome: %v", err)
				}
				if after {
					counts(t, s, 1, 1, 1)
				} else {
					counts(t, s, 0, 0, 0)
				}
				s.db = runtime
				r := commit(t, s, e, nil)
				if r.AlreadyStored != after {
					t.Fatal("retry did not resolve commit outcome")
				}
				counts(t, s, 1, 1, 1)
			})
		}
	})
	t.Run("cancellation while waiting for lock and after success", func(t *testing.T) {
		s := newStore(t, 42)
		e := snapshot(s.chain, 42, 2, 1)
		lock, err := runtime.Begin(ctx)
		if err != nil {
			t.Fatal(databaseError(err))
		}
		defer lock.Rollback(ctx)
		if _, err := lock.Exec(ctx, `SELECT pg_advisory_xact_lock(174728,hashtext($1))`, string(s.chain)); err != nil {
			t.Fatal(databaseError(err))
		}
		short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		_, err = s.Commit(short, outbound.CommitExtraction{Extraction: e})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		counts(t, s, 0, 0, 0)
		if err := lock.Rollback(ctx); err != nil {
			t.Fatal(databaseError(err))
		}
		late, cancel := context.WithCancel(ctx)
		defer cancel()
		s.db = commitFaultDB{database: runtime, after: true, cancel: cancel}
		if _, err := s.Commit(late, outbound.CommitExtraction{Extraction: e}); err != nil {
			t.Fatal("late cancellation hid successful commit", err)
		}
		counts(t, s, 1, 1, 1)
	})
	t.Run("cancelled ambiguous commit and retry after further progress", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			t.Run(strconv.FormatBool(after), func(t *testing.T) {
				s := newStore(t, 42)
				e := snapshot(s.chain, 42, 2, 1)
				commitCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				s.db = commitFaultDB{database: runtime, after: after, cancel: cancel, failure: context.Canceled}
				_, err := s.Commit(commitCtx, outbound.CommitExtraction{Extraction: e})
				if !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, context.Canceled) {
					t.Fatalf("lost ambiguous/cancellation identity: %v", err)
				}
				s.db = runtime
				if after {
					child := snapshot(s.chain, 43, 3, 2)
					commit(t, s, child, checkpoint(t, s))
				}
				// Retry uses a fresh context and the original nil expectation.
				if r := commit(t, s, e, nil); r.AlreadyStored != after {
					t.Fatal("retry did not resolve commit outcome")
				}
				want := 1
				if after {
					want = 2
				}
				counts(t, s, want, want, 1)
				if checkpoint(t, s).Block.Number != 41+uint64(want) {
					t.Fatal("retry regressed progress")
				}
				stored, err := s.Load(ctx, e.Header.ID)
				if err != nil || !reflect.DeepEqual(stored, e) {
					t.Fatalf("retry changed original snapshot: %v", err)
				}
			})
		}
	})
	t.Run("transaction scoped locks released on every exit", func(t *testing.T) {
		s := newStore(t, 42)
		e := snapshot(s.chain, 42, 2, 1)
		probe := func(want bool) {
			t.Helper()
			// Owner pool guarantees a different backend from the runtime writer.
			tx, err := owner.Begin(ctx)
			if err != nil {
				t.Fatal(databaseError(err))
			}
			defer tx.Rollback(ctx)
			var locked bool
			if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(174728,hashtext($1))`, string(s.chain)).Scan(&locked); err != nil {
				t.Fatal(databaseError(err))
			}
			if locked != want {
				t.Fatalf("lock availability=%v, want=%v", locked, want)
			}
		}
		lock, err := runtime.Begin(ctx)
		if err != nil {
			t.Fatal(databaseError(err))
		}
		defer lock.Rollback(ctx)
		if _, err := lock.Exec(ctx, `SELECT pg_advisory_xact_lock(174728,hashtext($1))`, string(s.chain)); err != nil {
			t.Fatal(databaseError(err))
		}
		probe(false)
		if err := lock.Rollback(ctx); err != nil {
			t.Fatal(databaseError(err))
		}
		probe(true)
		commit(t, s, e, nil)
		probe(true)
		commit(t, s, e, nil) // Duplicate path exits by rollback, not COMMIT.
		probe(true)
		_, err = s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 43, 3, 9), ExpectedCheckpoint: checkpoint(t, s)})
		if !errors.Is(err, domain.ErrBlockConflict) {
			t.Fatal(err)
		}
		probe(true)
	})
	t.Run("checkpoint foreign keys require full identity and outbox", func(t *testing.T) {
		s := newStore(t, 42)
		e := snapshot(s.chain, 42, 2, 1)
		id := e.Header.ID
		insertCheckpoint := func(chain string, hash []byte) error {
			_, err := runtime.Exec(ctx, `INSERT INTO extractor.extraction_checkpoints(chain_id,block_number,block_hash) VALUES ($1,42,$2)`, chain, hash)
			return err
		}
		foreignKeyFailure := func(err error) {
			t.Helper()
			var server *pgconn.PgError
			if !errors.As(err, &server) || server.Code != "23503" {
				t.Fatalf("expected FK violation, got %v", databaseError(err))
			}
		}
		foreignKeyFailure(insertCheckpoint(string(s.chain), id.Hash[:]))
		metadata, when, err := encodeMetadata(e)
		if err != nil {
			t.Fatal(err)
		}
		_, err = runtime.Exec(ctx, `INSERT INTO extractor.raw_blocks
(chain_id,block_number,block_hash,parent_hash,raw_block,raw_receipts,metadata,provider,extracted_at)
VALUES ($1,42,$2,$3,$4,$5,$6,$7,$8)`, string(s.chain), id.Hash[:], e.Header.ParentHash[:], e.RawBlock, e.RawReceipts, metadata, e.Provider, when)
		if err != nil {
			t.Fatal(databaseError(err))
		}
		foreignKeyFailure(insertCheckpoint(string(s.chain), id.Hash[:])) // Raw exists, outbox does not.
		if _, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: e}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("partial stored state was accepted as a duplicate: %v", err)
		}
		counts(t, s, 1, 0, 0)
		_, err = runtime.Exec(ctx, `INSERT INTO extractor.outbox_events(event_id,chain_id,block_number,block_hash) VALUES ($1,$2,42,$3)`, id.EventID(), string(s.chain), id.Hash[:])
		if err != nil {
			t.Fatal(databaseError(err))
		}
		wrong := domain.Hash{9}
		foreignKeyFailure(insertCheckpoint(string(s.chain), wrong[:]))
		foreignKeyFailure(insertCheckpoint("9"+string(s.chain), id.Hash[:]))
		if err := insertCheckpoint(string(s.chain), id.Hash[:]); err != nil {
			t.Fatal(databaseError(err))
		}
		counts(t, s, 1, 1, 1)
	})
	t.Run("runtime least privilege", func(t *testing.T) {
		for _, sql := range []string{
			`DELETE FROM extractor.raw_blocks`, `UPDATE extractor.raw_blocks SET provider='forbidden'`,
			`DELETE FROM extractor.outbox_events`, `UPDATE extractor.outbox_events SET event_id='forbidden'`,
			`DELETE FROM extractor.extraction_checkpoints`, `TRUNCATE extractor.raw_blocks CASCADE`,
			`UPDATE extractor.extraction_checkpoints SET chain_id='forbidden'`,
			`SELECT * FROM extractor.schema_migrations`, `CREATE TABLE extractor.forbidden(id integer)`,
			`SELECT * FROM transformer.schema_migrations`, `SELECT * FROM loader.schema_migrations`,
			`SET ROLE extractor_migrator`,
		} {
			_, err := runtime.Exec(ctx, sql)
			var server *pgconn.PgError
			if !errors.As(err, &server) || server.Code != "42501" {
				t.Fatalf("expected permission denial for %s: %v", sql, databaseError(err))
			}
		}
	})
}

// Fault injection surrounds a REAL PostgreSQL COMMIT; the production adapter
// has no test hooks. This models lost acknowledgement without pretending that
// an I/O error proves either commit or rollback.
type commitFaultDB struct {
	database
	after   bool
	cancel  context.CancelFunc
	failure error
}

func (d commitFaultDB) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.database.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return commitFaultTx{Tx: tx, after: d.after, cancel: d.cancel, failure: d.failure}, nil
}

type commitFaultTx struct {
	pgx.Tx
	after   bool
	cancel  context.CancelFunc
	failure error
}

func (t commitFaultTx) Commit(ctx context.Context) error {
	if t.after {
		if err := t.Tx.Commit(ctx); err != nil {
			return err
		}
	}
	if t.cancel != nil {
		t.cancel()
		return t.failure
	}
	return io.ErrUnexpectedEOF
}
