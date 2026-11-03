package outbound

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

var ErrRepairRefused = errors.New("recovery request refused")

type RepairRequest struct {
	Queue, EventID, OperationID string
	Allowance                   int
}
type RepairStore interface {
	Repair(context.Context, RepairRequest) error
}

// One failed event only. No payload editing, completed-state reset or bulk retry.
func (r RepairRequest) Validate(chain string) error {
	if r.Queue != "inbox" || r.Allowance < 1 || r.Allowance > 1000 || !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(r.OperationID) {
		return ErrRepairRefused
	}
	prefix := "transformed-block"
	parts := strings.Split(r.EventID, "/")
	if len(parts) != 5 || parts[0] != prefix || parts[1] != "v1" || parts[2] != chain || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(parts[4]) || parts[4] == strings.Repeat("0", 64) {
		return ErrRepairRefused
	}
	if !domain.Decimal(parts[3], 64) {
		return ErrRepairRefused
	}
	return nil
}
