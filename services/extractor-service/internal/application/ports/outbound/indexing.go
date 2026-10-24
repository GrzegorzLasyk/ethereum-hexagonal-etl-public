package outbound

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// BranchStore owns only Extractor data. Rewind is conditional on the full
// checkpoint including revision and atomically audits the operation. Retrying
// an identical rewind resolves a lost acknowledgement without rewinding again.
type BranchStore interface {
	ExtractionStore
	ActiveHeader(context.Context, uint64) (domain.Header, error)
	Capacity(context.Context) (domain.Capacity, error)
	Rewind(context.Context, domain.Checkpoint, domain.BlockID) error
}
type ReindexRequest struct {
	ID         string
	Start, End uint64
}
type ReindexState struct {
	Request ReindexRequest
	Last    *domain.BlockID
}
type ReindexStore interface {
	OpenReindex(context.Context, ReindexRequest) (ReindexState, error)
	CommitReindex(context.Context, ReindexState, domain.Extraction, domain.CapacityLimits) error
	Capacity(context.Context) (domain.Capacity, error)
}
type ReplayRequest struct {
	ID, EventID string
	Allowance   int
}
type ReplayStore interface {
	Replay(context.Context, ReplayRequest) error
}
