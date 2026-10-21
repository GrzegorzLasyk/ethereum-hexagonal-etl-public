package inbound

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type ExtractBlockRequest struct {
	ChainID domain.ChainID
	Number  uint64
}

type ExtractBlockResult struct {
	Block         domain.BlockID
	EventID       string
	AlreadyStored bool
}

type BlockExtractor interface {
	ExtractBlock(context.Context, ExtractBlockRequest) (ExtractBlockResult, error)
}
