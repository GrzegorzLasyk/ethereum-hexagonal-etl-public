// readiness.go implements owning-schema database readiness at the adapter boundary in the outbound
// adapter layer.

package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/dbreadiness"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.ReadinessChecker = (*Store)(nil)

// CheckReadiness checks owning-schema connectivity, required objects/migrations and runtime
// privileges within the caller deadline.
func (s *Store) CheckReadiness(ctx context.Context) error {
	return dbreadiness.Check(ctx, s.db, readinessPolicy)
}
