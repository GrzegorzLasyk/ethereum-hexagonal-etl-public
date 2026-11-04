// repair.go defines contracts for audited finite retry allowance and recovery guards in the
// application outbound boundary.

package outbound

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

// ErrRepairRefused signals recovery request refused. Callers can match this category with
// errors.Is without parsing error text.
var ErrRepairRefused = errors.New("recovery request refused")

// RepairRequest identifies an audited owning-queue operation and bounded new attempt allowance.
type RepairRequest struct {
	Queue, EventID, OperationID string
	Allowance                   int
}

// RepairStore performs an idempotent audited repair without deleting input or resetting downstream
// state.
type RepairStore interface {
	// Repair grants an audited finite attempt allowance without deleting input or bypassing event
	// identity.
	Repair(context.Context, RepairRequest) error
}

// Validate rejects repair requests outside the audited single-event policy.
//
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
