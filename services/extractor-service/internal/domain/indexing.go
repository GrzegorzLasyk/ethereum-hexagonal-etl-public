// indexing.go implements range eligibility, admission and operation-identity rules in the pure
// domain layer.

package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrRecoveryRequired signals automatic reorg recovery cannot be proven; operator investigation
	// required. Callers can match this category with errors.Is without parsing error text.
	ErrRecoveryRequired = errors.New("automatic reorg recovery cannot be proven; operator investigation required")
	// ErrUnstableChain signals provider branch changed during observation. Callers can match this
	// category with errors.Is without parsing error text.
	ErrUnstableChain = errors.New("provider branch changed during observation")
	// ErrBackpressure signals extraction capacity limit reached. Callers can match this category with
	// errors.Is without parsing error text.
	ErrBackpressure = errors.New("extraction capacity limit reached")
)

// EligibleHeight requires depth descendants; confirmations are not finality.
func EligibleHeight(head, depth uint64) (uint64, bool) {
	if head < depth {
		return 0, false
	}
	return head - depth, true
}

// CapacityLimits defines positive outstanding-event, raw-byte and database-size admission
// watermarks.
type CapacityLimits struct{ Events, Bytes, DatabaseBytes int64 }

// Validate requires positive event, payload-byte and database-size watermarks.
func (l CapacityLimits) Validate() error {
	if l.Events <= 0 || l.Bytes <= 0 || l.DatabaseBytes <= 0 {
		return fmt.Errorf("%w: positive capacity limits required", ErrInvalidData)
	}
	return nil
}

// Capacity records current owning-Outbox work and database size for admission decisions, not
// reserved capacity.
type Capacity struct{ Events, Bytes, DatabaseBytes int64 }

// Allows tests event/byte/database admission watermarks including the proposed payload; it is not
// a reservation of disk space.
func (c Capacity) Allows(l CapacityLimits, additional int64) bool {
	return additional >= 0 && c.Events < l.Events && c.Bytes < l.Bytes && additional <= l.Bytes-c.Bytes && c.DatabaseBytes < l.DatabaseBytes
}

// ValidateOperationID checks a stable audited operator identifier.
//
// Operation IDs bind repeatable operator commands to their original arguments.
func ValidateOperationID(id string) error {
	if len(id) == 0 || len(id) > 128 {
		return fmt.Errorf("%w: operation ID length", ErrInvalidData)
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return fmt.Errorf("%w: operation ID characters", ErrInvalidData)
		}
	}
	return nil
}
