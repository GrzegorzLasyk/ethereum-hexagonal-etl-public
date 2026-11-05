//go:build postgresintegration

// indexing_integration_test.go exercises ordered checkpoint and retained-reorg behavior in
// PostgreSQL in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testIndexing exercises sequential progress, rewind and retained-history invariants with real
// local transactions.
func testIndexing(t *testing.T, pool, owner *pgxpool.Pool) {
	ctx := t.Context()
	serial := 900000
	limits := domain.CapacityLimits{Events: 10000, Bytes: 1 << 30, DatabaseBytes: 1 << 50}
	fresh := func(t *testing.T) *Store {
		t.Helper()
		serial++
		s, err := New(pool, domain.ChainID(strconv.Itoa(serial)), 42)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	cp := func(t *testing.T, s *Store) *domain.Checkpoint {
		t.Helper()
		c, e := s.Checkpoint(ctx, s.chain)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	put := func(t *testing.T, s *Store, n uint64, h, p byte) {
		t.Helper()
		_, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, n, h, p), ExpectedCheckpoint: cp(t, s)})
		if err != nil {
			t.Fatal(err)
		}
	}
	seed := func(t *testing.T, s *Store) {
		t.Helper()
		put(t, s, 42, 2, 1)
		put(t, s, 43, 3, 2)
		put(t, s, 44, 4, 3)
	}
	counts := func(t *testing.T, s *Store, raw, active, rewinds int) {
		t.Helper()
		var a, b, c, d int
		err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM extractor.raw_blocks WHERE chain_id=$1),(SELECT count(*) FROM extractor.outbox_events WHERE chain_id=$1),(SELECT count(*) FROM extractor.active_blocks WHERE chain_id=$1),(SELECT count(*) FROM extractor.rewinds WHERE chain_id=$1)`, string(s.chain)).Scan(&a, &b, &c, &d)
		if err != nil || a != raw || b != raw || c != active || d != rewinds {
			t.Fatalf("counts %d %d %d %d: %v", a, b, c, d, err)
		}
	}
	t.Run("forward migration preserves populated first lineage", func(t *testing.T) {
		tx, err := owner.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, err = tx.Exec(ctx, `DROP TABLE extractor.active_blocks,extractor.rewinds,extractor.reindex_jobs,extractor.replays; ALTER TABLE extractor.extraction_checkpoints DROP COLUMN revision,DROP COLUMN start_number; ALTER TABLE extractor.raw_blocks ADD UNIQUE(chain_id,block_number); ALTER TABLE extractor.outbox_events DROP CONSTRAINT outbox_events_max_attempts_check; ALTER TABLE extractor.outbox_events ADD CHECK(max_attempts BETWEEN 1 AND 1000)`)
		if err != nil {
			t.Fatal(err)
		}
		e := snapshot("899999", 42, 2, 1)
		metadata, at, err := encodeMetadata(e)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO extractor.raw_blocks(chain_id,block_number,block_hash,parent_hash,raw_block,raw_receipts,metadata,provider,extracted_at) VALUES($1,42,$2,$3,$4,$5,$6,$7,$8)`, string(e.Header.ID.ChainID), e.Header.ID.Hash[:], e.Header.ParentHash[:], e.RawBlock, e.RawReceipts, metadata, e.Provider, at)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO extractor.outbox_events(event_id,chain_id,block_number,block_hash,status,delivered_at) VALUES($1,'899999',42,$2,'delivered',clock_timestamp())`, e.Header.ID.EventID(), e.Header.ID.Hash[:])
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO extractor.extraction_checkpoints(chain_id,block_number,block_hash) VALUES('899999',42,$1)`, e.Header.ID.Hash[:])
		if err != nil {
			t.Fatal(err)
		}
		sql, err := os.ReadFile("../../../../../../migrations/extractor/0004_active_branch.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
		c, err := readCheckpoint(tx.QueryRow(ctx, checkpointSQL, "899999"), "899999")
		if err != nil || c.Start != 42 || c.Revision != 0 || c.Block != e.Header.ID {
			t.Fatalf("migrated checkpoint %+v %v", c, err)
		}
		var raw []byte
		var state, event string
		err = tx.QueryRow(ctx, `SELECT b.raw_block,o.status,o.event_id FROM extractor.active_blocks a JOIN extractor.raw_blocks b USING(chain_id,block_number,block_hash) JOIN extractor.outbox_events o USING(chain_id,block_number,block_hash) WHERE a.chain_id='899999'`).Scan(&raw, &state, &event)
		if err != nil || !reflect.DeepEqual(raw, e.RawBlock) || state != "delivered" || event != e.Header.ID.EventID() {
			t.Fatal("migration changed history", err)
		}
	})
	t.Run("rewind preserves candidates and fences ABA writers", func(t *testing.T) {
		s := fresh(t)
		seed(t, s)
		old := *cp(t, s)
		ancestor, _ := s.ActiveHeader(ctx, 42)
		if err := s.Rewind(ctx, old, ancestor.ID); err != nil {
			t.Fatal(err)
		}
		counts(t, s, 3, 1, 1)
		now := cp(t, s)
		if now.Block != ancestor.ID || now.Revision != 1 {
			t.Fatal(now)
		}
		restarted, err := New(integrationPool(t, "EXTRACTOR_TEST_DATABASE_URL", "extractor_runtime"), s.chain, 42)
		if err != nil {
			t.Fatal(err)
		}
		put(t, restarted, 43, 30, 2)
		put(t, restarted, 44, 40, 30)
		counts(t, s, 5, 3, 1)
		original, err := s.Load(ctx, old.Block)
		if err != nil || original.Header.ID != old.Block {
			t.Fatal("history lost", err)
		}
		// Retry the original successful rewind after forward progress: no second rewind.
		if err := s.Rewind(ctx, old, ancestor.ID); err != nil {
			t.Fatal(err)
		}
		if cp(t, s).Block.Hash != (domain.Hash{40}) {
			t.Fatal("repeated rewind regressed cursor")
		}
		current := *cp(t, s)
		if err := s.Rewind(ctx, current, ancestor.ID); err != nil {
			t.Fatal(err)
		}
		put(t, s, 43, 3, 2)
		put(t, s, 44, 4, 3) // original tip reappears; revision still fences old work
		_, err = s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 45, 5, 4), ExpectedCheckpoint: &old})
		if !errors.Is(err, domain.ErrCheckpointConflict) {
			t.Fatal("ABA writer accepted", err)
		}
		counts(t, s, 5, 3, 2)
		changedStart, _ := New(pool, s.chain, 43)
		if _, err := changedStart.Checkpoint(ctx, s.chain); !errors.Is(err, domain.ErrCheckpointConflict) {
			t.Fatal("changed start accepted", err)
		}
	})
	t.Run("rewind rolls back every local mutation", func(t *testing.T) {
		for _, table := range []string{"rewinds", "active_blocks", "extraction_checkpoints"} {
			t.Run(table, func(t *testing.T) {
				s := fresh(t)
				seed(t, s)
				before := *cp(t, s)
				ancestor, _ := s.ActiveHeader(ctx, 42)
				_, err := owner.Exec(ctx, `CREATE FUNCTION extractor.stage10_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER stage10_fail BEFORE INSERT OR UPDATE OR DELETE ON extractor.`+table+` FOR EACH ROW EXECUTE FUNCTION extractor.stage10_fail()`)
				if err != nil {
					t.Fatal(err)
				}
				cleanup := func() {
					_, err := owner.Exec(ctx, `DROP TRIGGER IF EXISTS stage10_fail ON extractor.`+table+`; DROP FUNCTION IF EXISTS extractor.stage10_fail()`)
					if err != nil {
						t.Error(err)
					}
				}
				t.Cleanup(cleanup)
				if err := s.Rewind(ctx, before, ancestor.ID); !errors.Is(err, ErrDatabase) {
					t.Fatal(err)
				}
				if *cp(t, s) != before {
					t.Fatal("partial rewind")
				}
				counts(t, s, 3, 3, 0)
				cleanup()
				if err := s.Rewind(ctx, before, ancestor.ID); err != nil {
					t.Fatal(err)
				}
				counts(t, s, 3, 1, 1)
			})
		}
	})
	t.Run("ambiguous rewind and concurrent rewind retries", func(t *testing.T) {
		for _, after := range []bool{false, true} {
			t.Run(strconv.FormatBool(after), func(t *testing.T) {
				s := fresh(t)
				seed(t, s)
				before := *cp(t, s)
				ancestor, _ := s.ActiveHeader(ctx, 42)
				s.db = commitFaultDB{database: pool, after: after}
				if err := s.Rewind(ctx, before, ancestor.ID); !errors.Is(err, ErrCommitUnknown) {
					t.Fatal(err)
				}
				s.db = pool
				if after {
					counts(t, s, 3, 1, 1)
				} else {
					counts(t, s, 3, 3, 0)
				}
				var wg sync.WaitGroup
				errs := make(chan error, 4)
				for range 4 {
					wg.Go(func() { errs <- s.Rewind(ctx, before, ancestor.ID) })
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatal(err)
					}
				}
				counts(t, s, 3, 1, 1)
				if cp(t, s).Revision != 1 {
					t.Fatal("multiple rewind effects")
				}
			})
		}
	})
	t.Run("concurrent forward and rewind never resurrect stale work", func(t *testing.T) {
		s := fresh(t)
		seed(t, s)
		before := *cp(t, s)
		ancestor, _ := s.ActiveHeader(ctx, 42)
		gate := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Go(func() { <-gate; errs <- s.Rewind(ctx, before, ancestor.ID) })
		wg.Go(func() {
			<-gate
			_, err := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 45, 5, 4), ExpectedCheckpoint: &before})
			errs <- err
		})
		close(gate)
		wg.Wait()
		close(errs)
		success := 0
		for err := range errs {
			if err == nil {
				success++
			} else if !errors.Is(err, domain.ErrCheckpointConflict) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatal("expected exactly one state transition", success)
		}
		c := cp(t, s)
		if c.Block.Number == 42 {
			counts(t, s, 3, 1, 1)
		} else if c.Block.Number == 45 {
			counts(t, s, 4, 4, 0)
		} else {
			t.Fatal(c)
		}
	})
	t.Run("missing and nonactive ancestor stop safely", func(t *testing.T) {
		s := fresh(t)
		seed(t, s)
		before := *cp(t, s)
		bad := domain.BlockID{ChainID: s.chain, Number: 42, Hash: domain.Hash{99}}
		if err := s.Rewind(ctx, before, bad); !errors.Is(err, domain.ErrCheckpointConflict) {
			t.Fatal(err)
		}
		bad.Number = 41
		if err := s.Rewind(ctx, before, bad); !errors.Is(err, domain.ErrRecoveryRequired) {
			t.Fatal(err)
		}
		if *cp(t, s) != before {
			t.Fatal("invalid recovery moved cursor")
		}
		counts(t, s, 3, 3, 0)
	})
	t.Run("capacity enforced atomically with concurrent writers and failed events", func(t *testing.T) {
		s := fresh(t)
		l := limits
		l.Events = 1
		req := outbound.ReindexRequest{ID: "capacity", Start: 100, End: 101}
		job, err := s.OpenReindex(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, e := s.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s.chain, 42, 2, 1), Limits: &l})
			errs <- e
		})
		wg.Go(func() { errs <- s.CommitReindex(ctx, job, snapshot(s.chain, 100, 9, 8), l) })
		wg.Wait()
		close(errs)
		success := 0
		for err := range errs {
			if err == nil {
				success++
			} else if !errors.Is(err, domain.ErrBackpressure) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatal(success)
		}
		_, err = owner.Exec(ctx, `UPDATE extractor.outbox_events SET status='failed' WHERE chain_id=$1`, string(s.chain))
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.Capacity(ctx)
		if err != nil || c.Events != 1 || c.Bytes == 0 || c.DatabaseBytes == 0 {
			t.Fatal(c, err)
		}
		if c.Allows(l, 0) {
			t.Fatal("failed event excluded")
		}
		s2 := fresh(t)
		small := limits
		small.Bytes = 1
		if _, err := s2.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s2.chain, 42, 2, 1), Limits: &small}); !errors.Is(err, domain.ErrBackpressure) {
			t.Fatal(err)
		}
		counts(t, s2, 0, 0, 0)
		small = limits
		small.DatabaseBytes = 1
		if _, err := s2.Commit(ctx, outbound.CommitExtraction{Extraction: snapshot(s2.chain, 42, 2, 1), Limits: &small}); !errors.Is(err, domain.ErrBackpressure) {
			t.Fatal(err)
		}
		counts(t, s2, 0, 0, 0)
	})
	t.Run("reindex durable cursor candidates and immutable duplicate", func(t *testing.T) {
		s := fresh(t)
		seed(t, s)
		before := *cp(t, s)
		r := outbound.ReindexRequest{ID: "history-1", Start: 42, End: 44}
		state, err := s.OpenReindex(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		retry := snapshot(s.chain, 42, 2, 1)
		retry.Provider = "new-provider"
		if err := s.CommitReindex(ctx, state, retry, limits); err != nil {
			t.Fatal(err)
		}
		got, err := s.Load(ctx, retry.Header.ID)
		if err != nil || got.Provider == retry.Provider {
			t.Fatal("reindex replaced provenance", err)
		}
		state, err = s.OpenReindex(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		e := snapshot(s.chain, 43, 30, 2)
		for _, after := range []bool{false, true} {
			s.db = commitFaultDB{database: pool, after: after}
			err = s.CommitReindex(ctx, state, e, limits)
			s.db = pool
			if !errors.Is(err, ErrCommitUnknown) {
				t.Fatal(err)
			}
		}
		if err := s.CommitReindex(ctx, state, e, limits); err != nil {
			t.Fatal(err)
		}
		conflictingRetry := e.Clone()
		conflictingRetry.Header.ParentHash = domain.Hash{99}
		if err := s.CommitReindex(ctx, state, conflictingRetry, limits); !errors.Is(err, domain.ErrBlockConflict) {
			t.Fatal("conflicting reindex retry accepted", err)
		}
		state, err = s.OpenReindex(ctx, r)
		if err != nil || state.Last.Number != 43 {
			t.Fatal(state, err)
		}
		if err := s.CommitReindex(ctx, state, snapshot(s.chain, 44, 40, 30), limits); err != nil {
			t.Fatal(err)
		}
		if *cp(t, s) != before {
			t.Fatal("historical job changed live cursor")
		}
		counts(t, s, 5, 3, 0)
		r.End = 45
		if _, err := s.OpenReindex(ctx, r); !errors.Is(err, domain.ErrCheckpointConflict) {
			t.Fatal("operation arguments changed", err)
		}
	})
	t.Run("replay retains bytes fences old claims and resolves unknown commit", func(t *testing.T) {
		s := fresh(t)
		put(t, s, 42, 2, 1)
		id := cp(t, s).Block
		q, err := NewOutbox(pool, string(s.chain), time.Minute, 2)
		if err != nil {
			t.Fatal(err)
		}
		stale, err := q.ClaimDelivery(ctx)
		if err != nil || stale == nil {
			t.Fatal(err)
		}
		r := outbound.ReplayRequest{ID: "replay-1", EventID: id.EventID(), Allowance: 3}
		s.db = commitFaultDB{database: pool, after: true}
		err = s.Replay(ctx, r)
		s.db = pool
		if !errors.Is(err, ErrCommitUnknown) {
			t.Fatal(err)
		}
		if err := s.Replay(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := q.Published(ctx, *stale); !errors.Is(err, outbound.ErrDeliveryLeaseLost) {
			t.Fatal("stale publisher accepted", err)
		}
		if err := q.RetryDelivery(ctx, *stale, outbound.DeliveryFailure{Code: "unavailable"}); !errors.Is(err, outbound.ErrDeliveryLeaseLost) {
			t.Fatal("stale failure accepted", err)
		}
		current, err := q.ClaimDelivery(ctx)
		if err != nil || current == nil || current.Attempt <= stale.Attempt || current.MaxAttempts-current.Attempt != 2 {
			t.Fatal(current, err)
		}
		if !reflect.DeepEqual(current.Event, stale.Event) {
			t.Fatal("replay changed event content")
		}
		if err := q.Published(ctx, *current); err != nil {
			t.Fatal(err)
		}
		if err := s.Replay(ctx, r); err != nil {
			t.Fatal(err)
		} // retry must not requeue again
		if next, err := q.ClaimDelivery(ctx); err != nil || next != nil {
			t.Fatal("duplicate replay changed delivery", err)
		}
		r.ID = "replay-2"
		if err := s.Replay(ctx, r); err != nil {
			t.Fatal(err)
		}
		next, err := q.ClaimDelivery(ctx)
		if err != nil || next == nil || next.Attempt <= current.Attempt {
			t.Fatal(next, err)
		}
		r.Allowance = 4
		if err := s.Replay(ctx, r); !errors.Is(err, domain.ErrCheckpointConflict) {
			t.Fatal("operation ID reused with changed allowance", err)
		}
		counts(t, s, 1, 1, 0)
	})
	t.Run("scheduler shallow recovery and restart against PostgreSQL", func(t *testing.T) {
		s := fresh(t)
		seed(t, s)
		provider := &indexProvider{blocks: map[uint64]domain.Extraction{42: snapshot(s.chain, 42, 2, 1), 43: snapshot(s.chain, 43, 30, 2), 44: snapshot(s.chain, 44, 40, 30), 45: snapshot(s.chain, 45, 50, 40)}, head: 45}
		end := uint64(45)
		o := usecases.IndexOptions{Chain: s.chain, Start: 42, End: &end, RecoveryDepth: 2, Batch: 2, Limits: limits}
		u, err := usecases.NewIndex(o, provider, provider, s)
		if err != nil {
			t.Fatal(err)
		}
		r, err := u.Cycle(ctx)
		if err != nil || !r.Rewound {
			t.Fatal(r, err)
		}
		counts(t, s, 3, 1, 1)
		restart, _ := New(pool, s.chain, 42)
		u, err = usecases.NewIndex(o, provider, provider, restart)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			r, err = u.Cycle(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if r.Done {
				break
			}
		}
		if !r.Done || cp(t, s).Block.Hash != (domain.Hash{50}) {
			t.Fatal(r)
		}
		counts(t, s, 6, 4, 1)
		provider.blocks[42] = snapshot(s.chain, 42, 99, 98)
		provider.blocks[43] = snapshot(s.chain, 43, 100, 99)
		provider.blocks[44] = snapshot(s.chain, 44, 101, 100)
		provider.blocks[45] = snapshot(s.chain, 45, 102, 101)
		if _, err := u.Cycle(ctx); !errors.Is(err, domain.ErrRecoveryRequired) {
			t.Fatal("deep reorg not stopped", err)
		}
		counts(t, s, 6, 4, 1)
	})
	t.Run("new audit and source privileges stay restricted", func(t *testing.T) {
		for _, sql := range []string{`DELETE FROM extractor.rewinds`, `UPDATE extractor.replays SET allowance=1`, `DELETE FROM extractor.reindex_jobs`, `UPDATE extractor.extraction_checkpoints SET start_number=0`, `UPDATE extractor.active_blocks SET block_hash=decode(repeat('00',32),'hex')`} {
			if _, err := pool.Exec(ctx, sql); err == nil {
				t.Fatal("unexpected permission", sql)
			}
		}
	})
}

// indexProvider serves an in-memory height map and movable tip for database recovery tests.
type indexProvider struct {
	blocks map[uint64]domain.Extraction
	head   uint64
}

// ChainID returns the fake provider chain for explicit source-identity checks.
func (p *indexProvider) ChainID(context.Context) (domain.ChainID, error) {
	return p.blocks[p.head].Header.ID.ChainID, nil
}

// Block returns or fails a controlled complete source snapshot so acquisition/persistence
// boundaries can be exercised.
func (p *indexProvider) Block(_ context.Context, n uint64) (domain.Extraction, error) {
	e, ok := p.blocks[n]
	if !ok {
		return e, fmt.Errorf("%w: %d", domain.ErrUnavailable, n)
	}
	return e, nil
}

// Header returns the controlled provider ancestry at a height for deterministic reorg tests.
func (p *indexProvider) Header(ctx context.Context, n uint64) (domain.Header, error) {
	e, err := p.Block(ctx, n)
	return e.Header, err
}

// Head returns the current fake provider tip; tests can move it without external RPC.
func (p *indexProvider) Head(ctx context.Context) (domain.Header, error) {
	return p.Header(ctx, p.head)
}
