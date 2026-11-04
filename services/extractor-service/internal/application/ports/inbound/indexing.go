// indexing.go defines contracts for range eligibility, admission and operation-identity rules in
// the application inbound boundary.

package inbound

import (
	"context"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// IndexedBlock records only a confirmed durable commit, without source payloads.
type IndexedBlock struct {
	Block    domain.BlockID
	Duration time.Duration
}

// IndexResult reports committed identities, wait/backpressure/recovery state and finite-range
// completion from one cycle.
type IndexResult struct {
	Head                                 *uint64
	HeadObservedAt                       time.Time
	ChainID                              string
	Block                                *domain.BlockID
	Next                                 *uint64
	Completed                            []IndexedBlock
	Recovering, Reindex                  bool
	Ancestor                             *domain.BlockID
	Duration                             time.Duration
	Processed                            int
	Done, Waiting, Rewound, Backpressure bool
}

// Indexer is the polling adapter entry point for a bounded acquisition or reindex cycle.
type Indexer interface {
	// Cycle performs bounded sequential acquisition work and reports completion, waiting or a
	// classified error.
	Cycle(context.Context) (IndexResult, error)
}
