package outbound

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// BlockSource validates wire data outside the application and returns a complete
// mined-block snapshot per ADR 0002. Missing/null resources are errors.
// Returned buffers belong to the caller; the source must not mutate them later.
type BlockSource interface {
	ChainID(context.Context) (domain.ChainID, error)
	Block(context.Context, uint64) (domain.Extraction, error)
}

type CommitExtraction struct {
	Extraction         domain.Extraction
	ExpectedCheckpoint *domain.Checkpoint
}

type CommitResult struct {
	AlreadyStored bool
}

// ExtractionStore owns the atomic source-data + outbox-intent + checkpoint
// boundary. Checkpoint returns nil, nil only for an uninitialized chain.
//
// Commit must compare ExpectedCheckpoint inside its local transaction, recheck
// continuity, and either commit all effects or none. A previously stored exact
// block identity is an idempotent success (AlreadyStored), even if a concurrent
// identical extraction has advanced progress. Retain its original data/event.
// Other checkpoint changes return domain.ErrCheckpointConflict; incompatible
// hashes return domain.ErrBlockConflict. Never regress progress.
//
// The event deduplication key is Extraction.Header.ID.EventID(). No independent
// checkpoint setter or network publisher is exposed. An ambiguous commit error
// must remain an error; retry with the same block identity resolves it.
type ExtractionStore interface {
	Checkpoint(context.Context, domain.ChainID) (*domain.Checkpoint, error)
	Commit(context.Context, CommitExtraction) (CommitResult, error)
}
