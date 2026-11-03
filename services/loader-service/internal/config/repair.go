package config

import (
	"os"
	"strconv"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
)

func loadRepair(chain string) (*outbound.RepairRequest, error) {
	const prefix = "LOADER_REPAIR_"
	mode := os.Getenv("LOADER_MODE")
	if mode != "repair" {
		if mode != "" || os.Getenv(prefix+"EVENT_ID") != "" || os.Getenv(prefix+"OPERATION_ID") != "" || os.Getenv(prefix+"ALLOWANCE") != "" || os.Getenv(prefix+"QUEUE") != "" {
			return nil, outbound.ErrRepairRefused
		}
		return nil, nil
	}
	n, err := strconv.Atoi(os.Getenv(prefix + "ALLOWANCE"))
	if err != nil {
		return nil, outbound.ErrRepairRefused
	}
	r := outbound.RepairRequest{Queue: os.Getenv(prefix + "QUEUE"), EventID: os.Getenv(prefix + "EVENT_ID"), OperationID: os.Getenv(prefix + "OPERATION_ID"), Allowance: n}
	if err := r.Validate(chain); err != nil {
		return nil, err
	}
	return &r, nil
}
