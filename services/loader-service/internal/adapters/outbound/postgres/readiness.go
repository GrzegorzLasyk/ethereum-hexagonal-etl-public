package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/dbreadiness"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
)

var _ outbound.ReadinessChecker = (*Store)(nil)

func (s *Store) CheckReadiness(ctx context.Context) error {
	return dbreadiness.Check(ctx, s.db, readinessPolicy)
}
