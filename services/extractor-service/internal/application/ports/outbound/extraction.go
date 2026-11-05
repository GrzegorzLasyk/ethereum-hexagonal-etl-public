// extraction.go defines contracts for complete snapshot acquisition and atomic persistence
// contracts in the application outbound boundary.

package outbound

import (
	"context"
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// BlockSource validates wire data outside the application and returns a complete
// mined-block snapshot per ADR 0002. Missing/null resources are errors.
// Returned buffers belong to the caller; the source must not mutate them later.
type BlockSource interface {
	// ChainID returns the verified explicit source chain; mismatch/unavailability is an error.
	ChainID(context.Context) (domain.ChainID, error)
	// Block returns a complete ordered raw snapshot for the height or an error; partial receipt
	// batches are not valid results.
	Block(context.Context, uint64) (domain.Extraction, error)
}

// CommitExtraction carries a complete snapshot with the expected checkpoint/revision and optional
// admission watermarks.
type CommitExtraction struct {
	Extraction         domain.Extraction
	ExpectedCheckpoint *domain.Checkpoint
	Limits             *domain.CapacityLimits
}

// CommitResult distinguishes a newly retained extraction from an existing full block identity.
type CommitResult struct {
	// AlreadyStored confirms durable block identity, not equality of the
	// submitted raw JSON, typed snapshot metadata or extraction provenance.
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
// hashes return domain.ErrBlockConflict. Only the separately approved BranchStore.Rewind may regress active progress.
// Compare checkpoint revisions before activating any new branch member.
//
// The event deduplication key is Extraction.Header.ID.EventID(). No independent
// checkpoint setter or network publisher is exposed. An ambiguous commit error
// must remain an error; retry with the same block identity resolves it.
type ExtractionStore interface {
	// Checkpoint returns the last locally committed cursor or nil when uninitialized; it says nothing
	// about downstream processing.
	Checkpoint(context.Context, domain.ChainID) (*domain.Checkpoint, error)
	// Commit atomically retains source, Outbox and checkpoint under the expected-cursor fence,
	// preserving duplicate first observations.
	Commit(context.Context, CommitExtraction) (CommitResult, error)
}

// ErrCommitUnknown requires rereading durable state or retrying the same identity.
var ErrCommitUnknown = errors.New("extraction transaction outcome unknown; retry the same operation")
