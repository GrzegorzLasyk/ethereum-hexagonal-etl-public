// extract_block_test.go exercises single-block validation and the source/Outbox/checkpoint commit
// boundary in the application orchestration layer.

package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// testFailure signals injected failure. Callers can match this category with errors.Is without
// parsing error text.
var testFailure = errors.New("injected failure")

// fakeSource provides controlled complete source observations and failures at the block-source
// port.
type fakeSource struct {
	chain                  domain.ChainID
	data                   domain.Extraction
	chainErr, blockErr     error
	afterChain, afterBlock func()
	chainCalls, blockCalls int
	contexts               []context.Context
}

// ChainID returns the fake provider chain for explicit source-identity checks.
func (s *fakeSource) ChainID(ctx context.Context) (domain.ChainID, error) {
	s.chainCalls++
	s.contexts = append(s.contexts, ctx)
	if s.afterChain != nil {
		s.afterChain()
	}
	return s.chain, s.chainErr
}

// Block returns or fails a controlled complete source snapshot so acquisition/persistence
// boundaries can be exercised.
func (s *fakeSource) Block(ctx context.Context, _ uint64) (domain.Extraction, error) {
	s.blockCalls++
	s.contexts = append(s.contexts, ctx)
	if s.afterBlock != nil {
		s.afterBlock()
	}
	return s.data, s.blockErr
}

// fakeStore models an atomic contract for use-case tests, not real durability.
type fakeStore struct {
	checkpoint                               *domain.Checkpoint
	records                                  map[string]domain.Extraction
	checkpointErr, commitErr, afterCommitErr error
	afterRead, beforeCommit, afterCommit     func()
	reads, commits                           int
	contexts                                 []context.Context
}

// Checkpoint reads simulated durable progress for extraction/cursor assertions.
func (s *fakeStore) Checkpoint(ctx context.Context, _ domain.ChainID) (*domain.Checkpoint, error) {
	s.reads++
	s.contexts = append(s.contexts, ctx)
	var result *domain.Checkpoint
	if s.checkpoint != nil {
		copy := *s.checkpoint
		result = &copy
	}
	if s.afterRead != nil {
		s.afterRead()
	}
	return result, s.checkpointErr
}

// Commit simulates the application persistence boundary and records committed identity/progress
// for assertions; it is not a production database.
func (s *fakeStore) Commit(ctx context.Context, command outbound.CommitExtraction) (outbound.CommitResult, error) {
	s.commits++
	s.contexts = append(s.contexts, ctx)
	if s.beforeCommit != nil {
		s.beforeCommit()
	}
	if err := ctx.Err(); err != nil {
		return outbound.CommitResult{}, err
	}
	if s.commitErr != nil {
		return outbound.CommitResult{}, s.commitErr
	}
	id := command.Extraction.Header.ID
	if _, exists := s.records[id.EventID()]; exists {
		return outbound.CommitResult{AlreadyStored: true}, nil
	}
	if !reflect.DeepEqual(s.checkpoint, command.ExpectedCheckpoint) {
		return outbound.CommitResult{}, domain.ErrCheckpointConflict
	}
	if err := domain.CheckContinuity(command.Extraction.Header, s.checkpoint); err != nil {
		return outbound.CommitResult{}, err
	}
	if s.records == nil {
		s.records = make(map[string]domain.Extraction)
	}
	s.records[id.EventID()] = command.Extraction.Clone()
	s.checkpoint = &domain.Checkpoint{Block: id}
	if s.afterCommit != nil {
		s.afterCommit()
	}
	return outbound.CommitResult{}, s.afterCommitErr
}

// extractionAt builds a complete deterministic extraction for boundary tests; its
// hashes/provenance are synthetic.
func extractionAt(number uint64) domain.Extraction {
	id := domain.BlockID{ChainID: "1", Number: number, Hash: domain.Hash{byte(number + 1)}}
	tx := domain.Transaction{Block: id, Hash: domain.Hash{42}, Index: 0}
	return domain.Extraction{
		Header:       domain.Header{ID: id, ParentHash: domain.Hash{byte(number)}, Timestamp: 100},
		Transactions: []domain.Transaction{tx},
		Receipts: []domain.Receipt{{
			Block: id, TransactionHash: tx.Hash, TransactionIndex: 0,
			Logs: []domain.Log{{Block: id, TransactionHash: tx.Hash, Index: 0}},
		}},
		// Opaque bytes deliberately exercise the contract without a JSON parser.
		RawBlock:    []byte("complete block result"),
		RawReceipts: [][]byte{[]byte("complete receipt result")},
		Provider:    "fixture-source", ExtractedAt: time.Unix(1000, 0).UTC(),
	}
}

// newUseCase wires the real application use case to controlled test ports and fails immediately on
// invalid setup.
func newUseCase(t *testing.T, source *fakeSource, store *fakeStore) *usecases.ExtractBlock {
	t.Helper()
	u, err := usecases.NewExtractBlock("1", 10, source, store)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// request builds a valid deterministic application/transport request so one invariant can be
// varied per case.
func request(number uint64) inbound.ExtractBlockRequest {
	return inbound.ExtractBlockRequest{ChainID: "1", Number: number}
}

// TestExtractBlockErrors injects invalid request/source/storage outcomes and checks the atomic
// persistence boundary is not crossed prematurely.
func TestExtractBlockErrors(t *testing.T) {
	tests := []struct {
		name             string
		setup            func(*fakeSource, *fakeStore, *inbound.ExtractBlockRequest)
		want             error
		fetches, commits int
	}{
		{"invalid request", func(_ *fakeSource, _ *fakeStore, r *inbound.ExtractBlockRequest) { r.ChainID = "" }, domain.ErrInvalidData, 0, 0},
		{"wrong requested chain", func(_ *fakeSource, _ *fakeStore, r *inbound.ExtractBlockRequest) { r.ChainID = "2" }, domain.ErrChainMismatch, 0, 0},
		{"source identity failure", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.chainErr = testFailure }, testFailure, 0, 0},
		{"invalid source identity", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.chain = "" }, domain.ErrInvalidData, 0, 0},
		{"wrong source chain", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.chain = "2" }, domain.ErrChainMismatch, 0, 0},
		{"checkpoint failure", func(_ *fakeSource, s *fakeStore, _ *inbound.ExtractBlockRequest) { s.checkpointErr = testFailure }, testFailure, 0, 0},
		{"bad initial height", func(_ *fakeSource, _ *fakeStore, r *inbound.ExtractBlockRequest) { r.Number++ }, domain.ErrOutOfSequence, 0, 0},
		{"invalid checkpoint", func(_ *fakeSource, s *fakeStore, _ *inbound.ExtractBlockRequest) { s.checkpoint = &domain.Checkpoint{} }, domain.ErrCheckpointConflict, 0, 0},
		{"unavailable block", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.blockErr = domain.ErrUnavailable }, domain.ErrUnavailable, 1, 0},
		{"source timeout", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) {
			s.blockErr = fmt.Errorf("timeout: %w", context.DeadlineExceeded)
		}, context.DeadlineExceeded, 1, 0},
		{"source cancellation", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.blockErr = context.Canceled }, context.Canceled, 1, 0},
		{"source error", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.blockErr = testFailure }, testFailure, 1, 0},
		{"missing receipt", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.data.Receipts = nil }, domain.ErrIncomplete, 1, 0},
		{"invalid snapshot", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.data.Transactions[0].Index = 2 }, domain.ErrInvalidData, 1, 0},
		{"different returned height", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) { s.data = extractionAt(11) }, domain.ErrBlockConflict, 1, 0},
		{"changed chain during fetch", func(s *fakeSource, _ *fakeStore, _ *inbound.ExtractBlockRequest) {
			s.data.Header.ID.ChainID = "2"
			s.data.Transactions[0].Block.ChainID = "2"
			s.data.Receipts[0].Block.ChainID = "2"
			s.data.Receipts[0].Logs[0].Block.ChainID = "2"
		}, domain.ErrChainMismatch, 1, 0},
		{"parent conflict", func(s *fakeSource, p *fakeStore, r *inbound.ExtractBlockRequest) {
			p.checkpoint = &domain.Checkpoint{Block: s.data.Header.ID}
			s.data = extractionAt(11)
			s.data.Header.ParentHash = domain.Hash{99}
			r.Number = 11
		}, domain.ErrBlockConflict, 1, 0},
		{"checkpoint height replacement", func(s *fakeSource, p *fakeStore, _ *inbound.ExtractBlockRequest) {
			p.checkpoint = &domain.Checkpoint{Block: s.data.Header.ID}
			p.checkpoint.Block.Hash = domain.Hash{99}
		}, domain.ErrBlockConflict, 1, 0},
		{"atomic write failed", func(_ *fakeSource, s *fakeStore, _ *inbound.ExtractBlockRequest) { s.commitErr = testFailure }, testFailure, 1, 1},
		{"concurrent progress", func(_ *fakeSource, s *fakeStore, _ *inbound.ExtractBlockRequest) {
			s.beforeCommit = func() { s.checkpoint = &domain.Checkpoint{Block: extractionAt(11).Header.ID} }
		}, domain.ErrCheckpointConflict, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &fakeSource{chain: "1", data: extractionAt(10)}
			store := &fakeStore{}
			r := request(10)
			tt.setup(source, store, &r)
			u := newUseCase(t, source, store)
			result, err := u.ExtractBlock(context.Background(), r)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if result != (inbound.ExtractBlockResult{}) {
				t.Fatalf("failure returned success data: %+v", result)
			}
			if source.blockCalls != tt.fetches || store.commits != tt.commits {
				t.Fatalf("fetches/commits = %d/%d, want %d/%d", source.blockCalls, store.commits, tt.fetches, tt.commits)
			}
			if len(store.records) != 0 {
				t.Fatal("failed extraction stored data/event")
			}
		})
	}
}

// TestCancellationBoundaries injects cancellation at source/storage boundaries to distinguish
// uncommitted work from an acknowledged durable commit.
func TestCancellationBoundaries(t *testing.T) {
	for _, stage := range []string{"before request", "source identity", "checkpoint", "fetch", "commit", "after commit"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &fakeSource{chain: "1", data: extractionAt(10)}
			store := &fakeStore{}
			switch stage {
			case "before request":
				cancel()
			case "source identity":
				source.afterChain = cancel
			case "checkpoint":
				store.afterRead = cancel
			case "fetch":
				source.afterBlock = cancel
			case "commit":
				store.beforeCommit = cancel
			case "after commit":
				store.afterCommit = cancel
			}
			_, err := newUseCase(t, source, store).ExtractBlock(ctx, request(10))
			if stage == "after commit" {
				if err != nil || len(store.records) != 1 {
					t.Fatalf("durable success lost: %v", err)
				}
			} else {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v", err)
				}
				if len(store.records) != 0 || store.checkpoint != nil {
					t.Fatal("cancellation advanced progress")
				}
				if stage != "commit" && store.commits != 0 {
					t.Fatal("called persistence after cancellation")
				}
			}
			for _, passed := range append(source.contexts, store.contexts...) {
				if passed != ctx {
					t.Fatal("context replaced at port boundary")
				}
			}
			if stage == "before request" && source.chainCalls != 0 {
				t.Fatal("I/O after cancellation")
			}
		})
	}
}

// TestExtractionRecoveryAndIdempotency retries complete identities after simulated failure and
// verifies stable stored progress and duplicate handling.
func TestExtractionRecoveryAndIdempotency(t *testing.T) {
	for _, failure := range []string{"none", "before commit", "ambiguous commit"} {
		t.Run(failure, func(t *testing.T) {
			source := &fakeSource{chain: "1", data: extractionAt(10)}
			store := &fakeStore{}
			original := source.data.Clone()
			switch failure {
			case "before commit":
				store.commitErr = testFailure
			case "ambiguous commit":
				store.afterCommitErr = testFailure
			}
			u := newUseCase(t, source, store)
			result, err := u.ExtractBlock(context.Background(), request(10))
			if failure == "none" {
				if err != nil || result.AlreadyStored {
					t.Fatalf("initial result = %+v, %v", result, err)
				}
			} else if !errors.Is(err, testFailure) {
				t.Fatalf("failure not surfaced: %v", err)
			}
			if failure == "before commit" && (store.checkpoint != nil || len(store.records) != 0) {
				t.Fatal("failed transaction changed progress")
			}
			store.commitErr, store.afterCommitErr = nil, nil
			// A new use-case instance models process restart with the same store.
			u = newUseCase(t, source, store)
			result, err = u.ExtractBlock(context.Background(), request(10))
			if err != nil {
				t.Fatal(err)
			}
			if result.AlreadyStored != (failure != "before commit") {
				t.Fatalf("unexpected duplicate status: %+v", result)
			}
			if result.EventID != original.Header.ID.EventID() || len(store.records) != 1 {
				t.Fatal("retry created another logical event")
			}
			source.data.Provider = "another-provider"
			source.data.ExtractedAt = source.data.ExtractedAt.Add(time.Hour)
			result, err = u.ExtractBlock(context.Background(), request(10))
			if err != nil || !result.AlreadyStored {
				t.Fatalf("duplicate = %+v, %v", result, err)
			}
			if !reflect.DeepEqual(store.records[result.EventID], original) {
				t.Fatal("duplicate overwrote original snapshot")
			}
			source.data = extractionAt(11)
			result, err = u.ExtractBlock(context.Background(), request(11))
			if err != nil || result.AlreadyStored {
				t.Fatalf("resume = %+v, %v", result, err)
			}
			if store.checkpoint.Block.Number != 11 || len(store.records) != 2 {
				t.Fatal("resume did not atomically advance")
			}
			_, err = u.ExtractBlock(context.Background(), request(10))
			if !errors.Is(err, domain.ErrOutOfSequence) || store.checkpoint.Block.Number != 11 {
				t.Fatal("historical request regressed progress")
			}
		})
	}
}

// TestNewExtractBlock checks chain/start and dependency validation at construction without
// performing acquisition.
func TestNewExtractBlock(t *testing.T) {
	for _, tt := range []struct {
		name   string
		chain  domain.ChainID
		source outbound.BlockSource
		store  outbound.ExtractionStore
	}{
		{"invalid chain", "", &fakeSource{}, &fakeStore{}},
		{"nil source", "1", nil, &fakeStore{}},
		{"nil store", "1", &fakeSource{}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := usecases.NewExtractBlock(tt.chain, 0, tt.source, tt.store); !errors.Is(err, domain.ErrInvalidData) {
				t.Fatalf("constructor error = %v", err)
			}
		})
	}
}
