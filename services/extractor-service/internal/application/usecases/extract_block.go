// extract_block.go implements single-block validation and the source/Outbox/checkpoint commit
// boundary in the application orchestration layer.

package usecases

import (
	"context"
	"fmt"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ExtractBlock coordinates validation and one atomic source/Outbox/checkpoint persistence
// operation through ports.
type ExtractBlock struct {
	chain  domain.ChainID
	start  uint64
	source outbound.BlockSource
	store  outbound.ExtractionStore
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ inbound.BlockExtractor = (*ExtractBlock)(nil)

// NewExtractBlock binds the explicit chain/start and source/store ports after validation;
// construction performs no acquisition.
func NewExtractBlock(chain domain.ChainID, start uint64, source outbound.BlockSource, store outbound.ExtractionStore) (*ExtractBlock, error) {
	if err := chain.Validate(); err != nil {
		return nil, fmt.Errorf("configure extractor: %w", err)
	}
	if source == nil || store == nil {
		return nil, fmt.Errorf("%w: source and store are required", domain.ErrInvalidData)
	}
	return &ExtractBlock{chain: chain, start: start, source: source, store: store}, nil
}

// ExtractBlock validates one requested identity and complete source snapshot before the atomic
// checkpoint/Outbox commit boundary.
func (u *ExtractBlock) ExtractBlock(ctx context.Context, request inbound.ExtractBlockRequest) (inbound.ExtractBlockResult, error) {
	var result inbound.ExtractBlockResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := request.ChainID.Validate(); err != nil {
		return result, fmt.Errorf("validate request: %w", err)
	}
	if request.ChainID != u.chain {
		return result, fmt.Errorf("request: %w", domain.ErrChainMismatch)
	}
	chain, err := u.source.ChainID(ctx)
	if err != nil {
		return result, fmt.Errorf("read source chain: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := chain.Validate(); err != nil {
		return result, fmt.Errorf("validate source chain: %w", err)
	}
	if chain != u.chain {
		return result, fmt.Errorf("source: %w", domain.ErrChainMismatch)
	}
	previous, err := u.store.Checkpoint(ctx, u.chain)
	if err != nil {
		return result, fmt.Errorf("read checkpoint: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Own the expected value even if an adapter returns a reusable pointer.
	if previous != nil {
		copy := *previous
		previous = &copy
	}
	if err := domain.CheckPosition(u.chain, u.start, request.Number, previous); err != nil {
		return result, fmt.Errorf("check extraction position: %w", err)
	}
	extraction, err := u.source.Block(ctx, request.Number)
	if err != nil {
		return result, fmt.Errorf("fetch block %d: %w", request.Number, err)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := extraction.Validate(); err != nil {
		return result, fmt.Errorf("validate block %d: %w", request.Number, err)
	}
	if extraction.Header.ID.ChainID != u.chain {
		return result, fmt.Errorf("block: %w", domain.ErrChainMismatch)
	}
	if extraction.Header.ID.Number != request.Number {
		return result, fmt.Errorf("%w: source returned a different height", domain.ErrBlockConflict)
	}
	if err := domain.CheckContinuity(extraction.Header, previous); err != nil {
		return result, fmt.Errorf("check block continuity: %w", err)
	}
	command := outbound.CommitExtraction{
		Extraction:         extraction.Clone(),
		ExpectedCheckpoint: previous,
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	committed, err := u.store.Commit(ctx, command)
	if err != nil {
		return result, fmt.Errorf("commit block %d: %w", request.Number, err)
	}
	// A cancellation arriving after a successful commit cannot undo durability.
	return inbound.ExtractBlockResult{
		Block:         extraction.Header.ID,
		EventID:       extraction.Header.ID.EventID(),
		AlreadyStored: committed.AlreadyStored,
	}, nil
}
