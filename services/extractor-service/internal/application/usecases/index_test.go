package usecases_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

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

func emptyExtraction(n uint64, hash, parent byte) domain.Extraction {
	return domain.Extraction{Header: domain.Header{ID: domain.BlockID{ChainID: "1", Number: n, Hash: domain.Hash{hash}}, ParentHash: domain.Hash{parent}}, Transactions: []domain.Transaction{}, Receipts: []domain.Receipt{}, RawBlock: []byte(`{"transactions":[]}`), RawReceipts: [][]byte{}, Provider: "fixture", ExtractedAt: time.Unix(1, 0)}
}
func newBranch() *branchFixture {
	b := &branchFixture{blocks: map[uint64]domain.Extraction{}, active: map[uint64]domain.Header{}, retained: map[string]domain.Extraction{}, head: 15}
	for n := uint64(10); n <= 15; n++ {
		b.blocks[n] = emptyExtraction(n, byte(n+1), byte(n))
	}
	return b
}
func (b *branchFixture) ChainID(context.Context) (domain.ChainID, error) { return "1", nil }
func (b *branchFixture) Head(ctx context.Context) (domain.Header, error) {
	return b.Header(ctx, b.head)
}
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
func (b *branchFixture) Checkpoint(context.Context, domain.ChainID) (*domain.Checkpoint, error) {
	if b.cp == nil {
		return nil, nil
	}
	cp := *b.cp
	return &cp, nil
}
func (b *branchFixture) Capacity(context.Context) (domain.Capacity, error) { return b.capacity, nil }
func (b *branchFixture) ActiveHeader(_ context.Context, n uint64) (domain.Header, error) {
	h, ok := b.active[n]
	if !ok {
		return h, domain.ErrUnavailable
	}
	return h, nil
}
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
func opts() usecases.IndexOptions {
	return usecases.IndexOptions{Chain: "1", Start: 10, RecoveryDepth: 4, Batch: 3, Limits: domain.CapacityLimits{Events: 100, Bytes: 100000, DatabaseBytes: 1000000}}
}
func index(t *testing.T, b *branchFixture, o usecases.IndexOptions) *usecases.Index {
	t.Helper()
	u, err := usecases.NewIndex(o, b, b, b)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
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
func fork(b *branchFixture, from uint64) {
	for n := from; n <= b.head; n++ {
		parent := byte(n + 99)
		if n == from {
			parent = b.blocks[n].Header.ParentHash[0]
		}
		b.blocks[n] = emptyExtraction(n, byte(n+100), parent)
	}
}

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
func TestIndexShallowReorgAndRetainedHistory(t *testing.T) {
	b := newBranch()
	seed(t, b, 13)
	old := b.cp.Block
	fork(b, 12)
	r, e := index(t, b, opts()).Cycle(t.Context())
	if e != nil || !r.Rewound || b.cp.Block.Number != 11 || b.cp.Revision != 1 {
		t.Fatalf("rewind %+v %v", r, e)
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
func (b *branchFixture) OpenReindex(_ context.Context, r outbound.ReindexRequest) (outbound.ReindexState, error) {
	if b.job.Request.ID == "" {
		b.job.Request = r
	}
	if b.job.Request != r {
		return b.job, domain.ErrCheckpointConflict
	}
	return b.job, nil
}
func (b *branchFixture) CommitReindex(_ context.Context, s outbound.ReindexState, e domain.Extraction, _ domain.CapacityLimits) error {
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
func (b *branchFixture) Replay(_ context.Context, r outbound.ReplayRequest) error {
	b.replayCalls++
	return b.commitErr
}
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
	if err != nil || r.Processed != 2 {
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
