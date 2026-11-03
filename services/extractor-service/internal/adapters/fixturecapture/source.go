package fixturecapture

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ReplaySource is local regression tooling. It can only return the one verified
// snapshot, through the production RPC decoder's in-memory transport. It has no
// live endpoint, head discovery or fallback. Original provenance is retained.
type ReplaySource struct{ artifact Artifact }

var _ outbound.BlockSource = (*ReplaySource)(nil)

func NewReplaySource(ctx context.Context, path string, expected Candidate) (*ReplaySource, error) {
	a, err := readVerified(ctx, path, expected, nil)
	if err != nil {
		return nil, err
	}
	return &ReplaySource{artifact: a}, nil
}

func (s *ReplaySource) ChainID(ctx context.Context) (domain.ChainID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return domain.ChainID(s.artifact.Candidate.ChainID), nil
}

func (s *ReplaySource) Block(ctx context.Context, number uint64) (domain.Extraction, error) {
	if number != *s.artifact.Candidate.BlockNumber {
		return domain.Extraction{}, domain.ErrUnavailable
	}
	e, err := replay(ctx, s.artifact)
	if err != nil {
		return domain.Extraction{}, err
	}
	e.Provider = s.artifact.Provider
	e.ExtractedAt = s.artifact.AcquiredAt
	return e, nil
}
