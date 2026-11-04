// index_test.go exercises ordered acquisition and bounded provider-relative branch reconciliation
// in the application orchestration layer.

package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// branchFixture models provider ancestry, active membership and retained history separately for
// indexing/reindex tests.
type branchFixture struct {
	blocks              map[uint64]domain.Extraction
	active              map[uint64]domain.Header
	retained            map[string]domain.Extraction
	cp                  *domain.Checkpoint
	head                uint64
	fetched             []uint64
	capacity            domain.Capacity
	rewinds             int
	headerHook          func(uint64)
	blockErr, commitErr error
	job                 outbound.ReindexState
	replayCalls         int
}

// emptyExtraction builds a complete deterministic extraction for boundary tests; its
// hashes/provenance are synthetic.
func emptyExtraction(n uint64, hash, parent byte) domain.Extraction {
	return domain.Extraction{Header: domain.Header{ID: domain.BlockID{ChainID: "1", Number: n, Hash: domain.Hash{hash}}, ParentHash: domain.Hash{parent}}, Transactions: []domain.Transaction{}, Receipts: []domain.Receipt{}, RawBlock: []byte(`{"transactions":[]}`), RawReceipts: [][]byte{}, Provider: "fixture", ExtractedAt: time.Unix(1, 0)}
}

// newBranch creates independent in-memory provider and retained-branch state for controlled
// fork/cursor tests.
func newBranch() *branchFixture {
	b := &branchFixture{blocks: map[uint64]domain.Extraction{}, active: map[uint64]domain.Header{}, retained: map[string]domain.Extraction{}, head: 15}
	for n := uint64(10); n <= 15; n++ {
		b.blocks[n] = emptyExtraction(n, byte(n+1), byte(n))
	}
	return b
}

// ChainID returns the fake provider chain for explicit source-identity checks.
func (b *branchFixture) ChainID(context.Context) (domain.ChainID, error) { return "1", nil }

// Head returns the current fake provider tip; tests can move it without external RPC.
func (b *branchFixture) Head(ctx context.Context) (domain.Header, error) {
	return b.Header(ctx, b.head)
}

// Header returns the controlled provider ancestry at a height for deterministic reorg tests.
func (b *branchFixture) Header(ctx context.Context, n uint64) (domain.Header, error) {
	if err := ctx.Err(); err != nil {
		return domain.Header{}, err
	}
	if b.headerHook != nil {
		b.headerHook(n)
	}
	e, ok := b.blocks[n]
	if !ok {
		return domain.Header{}, domain.ErrUnavailable
	}
	return e.Header, nil
}

// Block returns or fails a controlled complete source snapshot so acquisition/persistence
// boundaries can be exercised.
func (b *branchFixture) Block(ctx context.Context, n uint64) (domain.Extraction, error) {
	if err := ctx.Err(); err != nil {
		return domain.Extraction{}, err
	}
	b.fetched = append(b.fetched, n)
	if b.blockErr != nil {
		return domain.Extraction{}, b.blockErr
	}
	e, ok := b.blocks[n]
	if !ok {
		return e, domain.ErrUnavailable
	}
	return e.Clone(), nil
}

// Checkpoint reads simulated durable progress for extraction/cursor assertions.
func (b *branchFixture) Checkpoint(context.Context, domain.ChainID) (*domain.Checkpoint, error) {
	if b.cp == nil {
		return nil, nil
	}
	cp := *b.cp
	return &cp, nil
}

// Capacity returns controlled admission watermarks to force acquisition pauses without filling a
// real disk.
func (b *branchFixture) Capacity(context.Context) (domain.Capacity, error) { return b.capacity, nil }

// ActiveHeader reads retained fake branch membership separately from the currently reported
// provider branch.
func (b *branchFixture) ActiveHeader(_ context.Context, n uint64) (domain.Header, error) {
	h, ok := b.active[n]
	if !ok {
		return h, domain.ErrUnavailable
	}
	return h, nil
}

// Commit simulates the application persistence boundary and records committed identity/progress
// for assertions; it is not a production database.
func (b *branchFixture) Commit(ctx context.Context, c outbound.CommitExtraction) (outbound.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return outbound.CommitResult{}, err
	}
	if b.commitErr != nil {
		return outbound.CommitResult{}, b.commitErr
	}
	if !reflect.DeepEqual(b.cp, c.ExpectedCheckpoint) {
		return outbound.CommitResult{}, domain.ErrCheckpointConflict
	}
	if err := domain.CheckContinuity(c.Extraction.Header, b.cp); err != nil {
		return outbound.CommitResult{}, err
	}
	rev := uint64(0)
	if b.cp != nil {
		rev = b.cp.Revision
	}
	b.cp = &domain.Checkpoint{Block: c.Extraction.Header.ID, Revision: rev, Start: 10}
	b.active[b.cp.Block.Number] = c.Extraction.Header
	b.retained[b.cp.Block.EventID()] = c.Extraction.Clone()
	b.capacity.Events++
	return outbound.CommitResult{}, nil
}

// Rewind models a revision-fenced active-branch change while retaining fake source history for
// assertions.
func (b *branchFixture) Rewind(_ context.Context, cp domain.Checkpoint, to domain.BlockID) error {
	if b.cp == nil || *b.cp != cp {
		return domain.ErrCheckpointConflict
	}
	b.rewinds++
	for n := range b.active {
		if n > to.Number {
			delete(b.active, n)
		}
	}
	b.cp = &domain.Checkpoint{Block: to, Revision: cp.Revision + 1, Start: cp.Start}
	return nil
}

// opts provides a bounded default index range/capacity policy for deterministic use-case
// scenarios.
func opts() usecases.IndexOptions {
	return usecases.IndexOptions{Chain: "1", Start: 10, RecoveryDepth: 4, Batch: 3, Limits: domain.CapacityLimits{Events: 100, Bytes: 100000, DatabaseBytes: 1000000}}
}

// index wires the real application use case to controlled test ports and fails immediately on
// invalid setup.
func index(t *testing.T, b *branchFixture, o usecases.IndexOptions) *usecases.Index {
	t.Helper()
	u, err := usecases.NewIndex(o, b, b, b)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// seed advances the controlled initial branch to the requested test height before injecting
// changes.
func seed(t *testing.T, b *branchFixture, end uint64) {
	t.Helper()
	o := opts()
	o.End = &end
	u := index(t, b, o)
	for {
		r, e := u.Cycle(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		if r.Done {
			return
		}
	}
}

// fork replaces only the fake provider's branch suffix, leaving retained local history available
// for recovery assertions.
func fork(b *branchFixture, from uint64) {
	for n := from; n <= b.head; n++ {
		parent := byte(n + 99)
		if n == from {
			parent = b.blocks[n].Header.ParentHash[0]
		}
		b.blocks[n] = emptyExtraction(n, byte(n+100), parent)
	}
}

// TestIndexRangeRestartAndConfirmations checks inclusive finite ranges, checkpoint resumption and
// confirmation eligibility.
func TestIndexRangeRestartAndConfirmations(t *testing.T) {
	b := newBranch()
	o := opts()
	end := uint64(13)
	o.End = &end
	o.Batch = 2
	o.Confirmations = 3
	r, e := index(t, b, o).Cycle(t.Context())
	if e != nil || r.Processed != 2 || b.cp.Block.Number != 11 {
		t.Fatalf("first cycle %+v %v", r, e)
	}
	// A fresh use case reads durable progress instead of starting at START_BLOCK.
	r, e = index(t, b, o).Cycle(t.Context())
	if e != nil || r.Processed != 1 || !r.Waiting || b.cp.Block.Number != 12 {
		t.Fatalf("restart %+v %v", r, e)
	}
	b.head = 16
	b.blocks[16] = emptyExtraction(16, 17, 16)
	r, e = index(t, b, o).Cycle(t.Context())
	if e != nil || !r.Done || b.cp.Block.Number != 13 {
		t.Fatalf("range %+v %v", r, e)
	}
	if !reflect.DeepEqual(b.fetched, []uint64{10, 11, 12, 13}) {
		t.Fatal(b.fetched)
	}
}

// TestIndexFollowHeadAndCapacity checks waiting for eligible head data and capacity without
// skipping sequential heights.
func TestIndexFollowHeadAndCapacity(t *testing.T) {
	b := newBranch()
	b.head = 10
	o := opts()
	u := index(t, b, o)
	r, e := u.Cycle(t.Context())
	if e != nil || !r.Waiting || r.Done || r.Processed != 1 {
		t.Fatalf("head %+v %v", r, e)
	}
	b.head = 11
	b.capacity.Events = o.Limits.Events
	r, e = u.Cycle(t.Context())
	if e != nil || !r.Backpressure || len(b.fetched) != 1 {
		t.Fatalf("capacity %+v %v", r, e)
	}
	b.capacity = domain.Capacity{Bytes: o.Limits.Bytes}
	r, e = u.Cycle(t.Context())
	if e != nil || !r.Backpressure {
		t.Fatalf("bytes %+v %v", r, e)
	}
	b.capacity = domain.Capacity{DatabaseBytes: o.Limits.DatabaseBytes}
	r, e = u.Cycle(t.Context())
	if e != nil || !r.Backpressure {
		t.Fatalf("db %+v %v", r, e)
	}
	b.capacity = domain.Capacity{}
	r, e = u.Cycle(t.Context())
	if e != nil || r.Processed != 1 || b.cp.Block.Number != 11 {
		t.Fatalf("resume %+v %v", r, e)
	}
}

// TestIndexShallowReorgAndRetainedHistory walks a provable retained ancestor, rewinds active
// membership and preserves old source/event history.
func TestIndexShallowReorgAndRetainedHistory(t *testing.T) {
	b := newBranch()
	seed(t, b, 13)
	old := b.cp.Block
	fork(b, 12)
	r, e := index(t, b, opts()).Cycle(t.Context())
	if e != nil || !r.Rewound || b.cp.Block.Number != 11 || b.cp.Revision != 1 {
		t.Fatalf("rewind %+v %v", r, e)
	}
	if r.Block == nil || *r.Block != old || r.Ancestor == nil || *r.Ancestor != b.cp.Block || !r.Recovering {
		t.Fatal("recovery identity lost", r)
	}
	if _, ok := b.retained[old.EventID()]; !ok {
		t.Fatal("lost historical observation")
	}
	end := uint64(13)
	o := opts()
	o.End = &end
	r, e = index(t, b, o).Cycle(t.Context())
	if e != nil || !r.Done || b.cp.Block == old {
		t.Fatalf("replacement %+v %v", r, e)
	}
	if len(b.retained) != 6 {
		t.Fatalf("retained %d", len(b.retained))
	}
}

// TestIndexStopsWithoutProvenAncestor requires a safe halt when depth, retained ancestry or
// provider evidence cannot justify recovery.
func TestIndexStopsWithoutProvenAncestor(t *testing.T) {
	for _, name := range []string{"deep", "start anchor", "missing local", "missing provider", "broken parents", "regressed head", "moving tip"} {
		t.Run(name, func(t *testing.T) {
			b := newBranch()
			seed(t, b, 13)
			before := *b.cp
			o := opts()
			fork(b, 12)
			want := domain.ErrRecoveryRequired
			switch name {
			case "deep":
				o.RecoveryDepth = 1
			case "start anchor":
				fork(b, 10)
			case "missing local":
				delete(b.active, 12)
			case "missing provider":
				delete(b.blocks, 12)
				want = domain.ErrUnavailable
			case "broken parents":
				e := b.blocks[13]
				e.Header.ParentHash = domain.Hash{200}
				b.blocks[13] = e
				want = domain.ErrUnstableChain
			case "regressed head":
				b.head = 12
			case "moving tip":
				calls := 0
				b.headerHook = func(n uint64) {
					if n == 13 {
						calls++
						if calls == 2 {
							e := b.blocks[n]
							e.Header.ID.Hash = domain.Hash{222}
							b.blocks[n] = e
						}
					}
				}
				want = domain.ErrUnstableChain
			}
			_, err := index(t, b, o).Cycle(t.Context())
			if !errors.Is(err, want) || *b.cp != before || b.rewinds != 0 {
				t.Fatalf("error=%v checkpoint=%+v", err, b.cp)
			}
		})
	}
}

// TestIndexFailuresDoNotAdvance injects source/persistence failures and checks they do not move
// the durable checkpoint.
func TestIndexFailuresDoNotAdvance(t *testing.T) {
	for _, name := range []string{"RPC", "missing", "partial", "commit", "moving block", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			b := newBranch()
			o := opts()
			ctx := t.Context()
			switch name {
			case "RPC":
				b.blockErr = testFailure
			case "missing":
				delete(b.blocks, 10)
			case "partial":
				e := b.blocks[10]
				e.RawBlock = nil
				b.blocks[10] = e
			case "commit":
				b.commitErr = testFailure
			case "moving block":
				b.headerHook = func(n uint64) {
					if n == 10 {
						e := b.blocks[n]
						e.Header.ID.Hash = domain.Hash{200}
						b.blocks[n] = e
					}
				}
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if _, err := index(t, b, o).Cycle(ctx); err == nil || b.cp != nil {
				t.Fatalf("err=%v cp=%+v", err, b.cp)
			}
		})
	}
}

// TestIndexUint64BoundaryAndDepthUnderflow checks height/depth arithmetic cannot wrap into a
// fabricated eligible range.
func TestIndexUint64BoundaryAndDepthUnderflow(t *testing.T) {
	b := newBranch()
	b.head = math.MaxUint64
	b.blocks[b.head] = emptyExtraction(b.head, 99, 98)
	o := opts()
	o.Start = b.head
	o.Batch = 1
	r, e := index(t, b, o).Cycle(t.Context())
	if e != nil || r.Processed != 1 {
		t.Fatalf("max %+v %v", r, e)
	}
	r, e = index(t, b, o).Cycle(t.Context())
	if e != nil || !r.Done {
		t.Fatalf("overflow %+v %v", r, e)
	}
	b = newBranch()
	o = opts()
	o.Confirmations = 16
	r, e = index(t, b, o).Cycle(t.Context())
	if e != nil || !r.Waiting || len(b.fetched) != 0 {
		t.Fatalf("underflow %+v %v", r, e)
	}
}

// OpenReindex creates/resumes the fake historical operation independently of the active
// checkpoint.
func (b *branchFixture) OpenReindex(_ context.Context, r outbound.ReindexRequest) (outbound.ReindexState, error) {
	if b.job.Request.ID == "" {
		b.job.Request = r
	}
	if b.job.Request != r {
		return b.job, domain.ErrCheckpointConflict
	}
	return b.job, nil
}

// CommitReindex updates only the fake historical cursor and retained observation for isolation
// assertions.
func (b *branchFixture) CommitReindex(_ context.Context, s outbound.ReindexState, e domain.Extraction, _ domain.CapacityLimits) error {
	if b.commitErr != nil {
		return b.commitErr
	}
	if !reflect.DeepEqual(s, b.job) {
		return domain.ErrCheckpointConflict
	}
	id := e.Header.ID
	b.job.Last = &id
	if _, ok := b.retained[id.EventID()]; !ok {
		b.retained[id.EventID()] = e.Clone()
	}
	return nil
}

// Replay records the requested original-event retry operation in the fake store.
func (b *branchFixture) Replay(_ context.Context, r outbound.ReplayRequest) error {
	b.replayCalls++
	return b.commitErr
}

// TestReindexRestartsWithoutChangingLiveCursor resumes historical work through its own cursor and
// checks the active checkpoint is untouched.
func TestReindexRestartsWithoutChangingLiveCursor(t *testing.T) {
	b := newBranch()
	seed(t, b, 13)
	before := *b.cp
	fork(b, 11)
	o := opts()
	end := uint64(12)
	o.End = &end
	o.Batch = 2
	u, err := usecases.NewReindex(o, "repair-1", b, b, b)
	if err != nil {
		t.Fatal(err)
	}
	r, err := u.Cycle(t.Context())
	if err != nil || r.Processed != 2 || !r.Reindex || len(r.Completed) != 2 {
		t.Fatalf("reindex %+v %v", r, err)
	}
	u, err = usecases.NewReindex(o, "repair-1", b, b, b)
	if err != nil {
		t.Fatal(err)
	}
	r, err = u.Cycle(t.Context())
	if err != nil || !r.Done || *b.cp != before || b.job.Last.Number != 12 {
		t.Fatalf("restart %+v %v", r, err)
	}
	if len(b.retained) != 6 {
		t.Fatal(len(b.retained))
	}
	e := b.blocks[12]
	e.Header.ID.Hash = domain.Hash{222}
	b.blocks[12] = e
	if _, err := u.Cycle(t.Context()); !errors.Is(err, domain.ErrRecoveryRequired) {
		t.Fatal(err)
	}
	if err := usecases.Replay(t.Context(), b, outbound.ReplayRequest{ID: "retry-1", EventID: before.Block.EventID(), Allowance: 2}); err != nil || b.replayCalls != 1 {
		t.Fatal(err)
	}
	if err := usecases.Replay(t.Context(), b, outbound.ReplayRequest{}); err == nil || b.replayCalls != 1 {
		t.Fatal("invalid replay reached store")
	}
}

// TestIndexRejectsSameIdentityWithChangedParent treats inconsistent parent data under the same
// block identity as unstable source evidence.
func TestIndexRejectsSameIdentityWithChangedParent(t *testing.T) {
	b := newBranch()
	seed(t, b, 13)
	before := *b.cp
	e := b.blocks[13]
	e.Header.ParentHash = domain.Hash{200}
	b.blocks[13] = e
	if _, err := index(t, b, opts()).Cycle(t.Context()); !errors.Is(err, domain.ErrUnstableChain) || *b.cp != before || b.rewinds != 0 {
		t.Fatal(err)
	}
}

// TestIndexProgressPreservesBatchAndFailedIdentity requires both completed batch members and the
// failing identity to remain inspectable.
func TestIndexProgressPreservesBatchAndFailedIdentity(t *testing.T) {
	b := newBranch()
	o := opts()
	o.Batch = 2
	r, err := index(t, b, o).Cycle(t.Context())
	if err != nil || len(r.Completed) != 2 || r.Completed[0].Block != b.blocks[10].Header.ID || r.Completed[1].Block != b.blocks[11].Header.ID {
		t.Fatal("lost batch identity", r, err)
	}
	b.commitErr = outbound.ErrCommitUnknown
	r, err = index(t, b, o).Cycle(t.Context())
	if !errors.Is(err, outbound.ErrCommitUnknown) || r.Block == nil || *r.Block != b.blocks[12].Header.ID || len(r.Completed) != 0 {
		t.Fatal("lost ambiguous commit identity", r, err)
	}
	b.blockErr = testFailure
	r, err = index(t, b, o).Cycle(t.Context())
	if err == nil || r.Block != nil || r.Next == nil || *r.Next != 12 || r.ChainID != "1" {
		t.Fatal("fetch failure invented hash or lost height", r, err)
	}
}

// TestFailedBatchRetainsEarlierCommittedIdentity requires progress reporting to retain earlier
// successful block identities after a later cycle failure.
func TestFailedBatchRetainsEarlierCommittedIdentity(t *testing.T) {
	b := newBranch()
	e := b.blocks[11]
	e.RawBlock = nil
	b.blocks[11] = e
	r, err := index(t, b, opts()).Cycle(t.Context())
	if err == nil || len(r.Completed) != 1 || r.Completed[0].Block != b.blocks[10].Header.ID || r.Block == nil || *r.Block != b.blocks[11].Header.ID {
		t.Fatal(r, err)
	}
}

// TestReindexAmbiguousCommitKeepsIdentity requires the independent operation identity to remain
// available when the historical commit outcome is uncertain.
func TestReindexAmbiguousCommitKeepsIdentity(t *testing.T) {
	b := newBranch()
	b.commitErr = outbound.ErrCommitUnknown
	o := opts()
	end := uint64(11)
	o.End = &end
	u, err := usecases.NewReindex(o, "repair-log", b, b, b)
	if err != nil {
		t.Fatal(err)
	}
	r, err := u.Cycle(t.Context())
	if !errors.Is(err, outbound.ErrCommitUnknown) || !r.Reindex || r.Block == nil || *r.Block != b.blocks[10].Header.ID || len(r.Completed) != 0 {
		t.Fatal(r, err)
	}
}

// activeReadFailure injects a retained-ancestry read error at a selected call rather than
// pretending the ancestor is absent.
type activeReadFailure struct {
	*branchFixture
	calls, failAt int
	err           error
}

// ActiveHeader injects storage read failure rather than absence, checking recovery does not invent
// missing ancestry.
func (s *activeReadFailure) ActiveHeader(ctx context.Context, n uint64) (domain.Header, error) {
	s.calls++
	if s.calls == s.failAt {
		return domain.Header{}, s.err
	}
	return s.branchFixture.ActiveHeader(ctx, n)
}

// TestActiveReadFailureDoesNotInventMissingAncestry distinguishes a storage outage from proven
// absence of retained ancestry before recovery decisions.
func TestActiveReadFailureDoesNotInventMissingAncestry(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		for _, cause := range []error{errors.New("database offline"), context.Canceled, context.DeadlineExceeded, domain.ErrUnavailable} {
			t.Run(fmt.Sprintf("recovery=%v/%v", recovery, cause), func(t *testing.T) {
				b := newBranch()
				seed(t, b, 14)
				before := *b.cp
				store := &activeReadFailure{branchFixture: b, failAt: 1, err: cause}
				if recovery {
					fork(b, 13)
					store.failAt = 3
				}
				u, err := usecases.NewIndex(opts(), b, b, store)
				if err != nil {
					t.Fatal(err)
				}
				_, err = u.Cycle(t.Context())
				if !errors.Is(err, cause) || errors.Is(err, domain.ErrRecoveryRequired) != errors.Is(cause, domain.ErrUnavailable) {
					t.Fatalf("incorrect error classification: %v", err)
				}
				if *b.cp != before || b.rewinds != 0 {
					t.Fatal("failed read changed progress")
				}
				store.failAt = 0
				if _, err = u.Cycle(t.Context()); err != nil {
					t.Fatalf("read recovery failed: %v", err)
				}
			})
		}
	}
}

// Exhaustion during acquisition or ancestor verification must not authorize any
// partial observation, lost Outbox intent, or checkpoint rewind.
type exhaustedObserver struct {
	*branchFixture
	calls int
	err   error
}

// Header injects request-budget exhaustion while preserving the already committed test branch.
func (b *exhaustedObserver) Header(ctx context.Context, n uint64) (domain.Header, error) {
	b.calls++
	if b.calls == 2 {
		return domain.Header{}, b.err
	}
	return b.branchFixture.Header(ctx, n)
}

// TestExecutionBudgetPreservesDurableProgress exhausts source execution limits after progress and
// checks the last committed cursor/history remain intact.
func TestExecutionBudgetPreservesDurableProgress(t *testing.T) {
	for _, failure := range []error{outbound.ErrRPCBudgetExceeded, context.DeadlineExceeded} {
		for _, recovery := range []bool{false, true} {
			b := newBranch()
			seed(t, b, 13)
			cp, retained, pending := *b.cp, len(b.retained), b.capacity.Events
			var observer outbound.ChainObserver = b
			if recovery {
				fork(b, 12)
				observer = &exhaustedObserver{branchFixture: b, err: failure}
			} else {
				b.blockErr = failure
			}
			u, err := usecases.NewIndex(opts(), b, observer, b)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = u.Cycle(t.Context()); !errors.Is(err, failure) {
				t.Fatal(err)
			}
			if *b.cp != cp || len(b.retained) != retained || b.capacity.Events != pending || b.rewinds != 0 {
				t.Fatal("budget changed durable progress/history/outbox")
			}
			if !recovery {
				b.blockErr = nil
				if r, err := index(t, b, opts()).Cycle(t.Context()); err != nil || r.Processed != 2 || b.cp.Block.Number != 15 {
					t.Fatal("restart failed", r, err)
				}
			}
		}
	}
}
