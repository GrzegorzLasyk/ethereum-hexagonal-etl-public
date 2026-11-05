// repair.go implements audited finite retry allowance and recovery guards in the application
// orchestration layer.

package usecases

import (
	"context"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/outbound"
)

// Repair validates the audited recovery request before invoking its owning-store port; it does not
// reset attempts directly.
func Repair(ctx context.Context, chain string, store outbound.RepairStore, r outbound.RepairRequest) error {
	if store == nil {
		return outbound.ErrRepairRefused
	}
	if err := r.Validate(chain); err != nil {
		return err
	}
	return store.Repair(ctx, r)
}
