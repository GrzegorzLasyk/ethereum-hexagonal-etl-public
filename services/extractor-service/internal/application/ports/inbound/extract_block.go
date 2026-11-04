// extract_block.go defines contracts for single-block validation and the source/Outbox/checkpoint
// commit boundary in the application inbound boundary.

package inbound

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ExtractBlockRequest names the explicit chain and numbered block requested from the extraction
// use case.
type ExtractBlockRequest struct {
	ChainID domain.ChainID
	Number  uint64
}

// ExtractBlockResult returns the durable block identity and duplicate outcome after the local
// commit boundary.
type ExtractBlockResult struct {
	Block         domain.BlockID
	EventID       string
	AlreadyStored bool
}

// BlockExtractor is the inbound single-block extraction contract; persistence success is distinct
// from downstream delivery.
type BlockExtractor interface {
	// ExtractBlock validates and atomically persists one requested complete block observation.
	ExtractBlock(context.Context, ExtractBlockRequest) (ExtractBlockResult, error)
}
