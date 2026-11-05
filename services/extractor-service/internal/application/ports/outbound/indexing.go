// indexing.go defines contracts for range eligibility, admission and operation-identity rules in
// the application outbound boundary.

package outbound

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// BranchStore owns only Extractor data. Rewind is conditional on the full
// checkpoint including revision and atomically audits the operation. Retrying
// an identical rewind resolves a lost acknowledgement without rewinding again.
type BranchStore interface {
	ExtractionStore
	// ActiveHeader reads local active membership at a height; missing retained ancestry must be
	// distinguishable from storage failure.
	ActiveHeader(context.Context, uint64) (domain.Header, error)
	// Capacity observes admission watermarks before acquisition without reserving disk or queue space.
	Capacity(context.Context) (domain.Capacity, error)
	// Rewind conditionally audits and rewinds active membership/checkpoint using the expected
	// revision, retaining immutable history.
	Rewind(context.Context, domain.Checkpoint, domain.BlockID) error
}

// ReindexRequest identifies a bounded historical operation with stable audit ID and inclusive
// start/end heights.
type ReindexRequest struct {
	ID         string
	Start, End uint64
}

// ReindexState retains the independent historical operation and optional last committed
// observation.
type ReindexState struct {
	Request ReindexRequest
	Last    *domain.BlockID
}

// ReindexStore persists historical operation progress and raw/Outbox observations without moving
// the main checkpoint.
type ReindexStore interface {
	// OpenReindex creates or resumes an audited finite range with its independent cursor.
	OpenReindex(context.Context, ReindexRequest) (ReindexState, error)
	// CommitReindex atomically retains an observation/intent and advances only that historical
	// operation cursor.
	CommitReindex(context.Context, ReindexState, domain.Extraction, domain.CapacityLimits) error
	// Capacity observes admission watermarks before acquisition without reserving disk or queue space.
	Capacity(context.Context) (domain.Capacity, error)
}

// ReplayRequest names an immutable retained event and an audited finite additional attempt
// allowance.
type ReplayRequest struct {
	ID, EventID string
	Allowance   int
}

// ReplayStore grants audited original-event delivery retries without reacquiring Ethereum data.
type ReplayStore interface {
	// Replay grants a finite audited delivery allowance to a retained original event and fences old
	// claims.
	Replay(context.Context, ReplayRequest) error
}
