package usecases

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
)

func Repair(ctx context.Context, chain string, store outbound.RepairStore, r outbound.RepairRequest) error {
	if store == nil {
		return outbound.ErrRepairRefused
	}
	if err := r.Validate(chain); err != nil {
		return err
	}
	return store.Repair(ctx, r)
}
