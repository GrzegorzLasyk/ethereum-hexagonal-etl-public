package domain

import (
	"errors"
	"fmt"
)

var (
	ErrRecoveryRequired = errors.New("automatic reorg recovery cannot be proven; operator investigation required")
	ErrUnstableChain    = errors.New("provider branch changed during observation")
	ErrBackpressure     = errors.New("extraction capacity limit reached")
)

// EligibleHeight requires depth descendants; confirmations are not finality.
func EligibleHeight(head, depth uint64) (uint64, bool) {
	if head < depth {
		return 0, false
	}
	return head - depth, true
}

type CapacityLimits struct{ Events, Bytes, DatabaseBytes int64 }

func (l CapacityLimits) Validate() error {
	if l.Events <= 0 || l.Bytes <= 0 || l.DatabaseBytes <= 0 {
		return fmt.Errorf("%w: positive capacity limits required", ErrInvalidData)
	}
	return nil
}

type Capacity struct{ Events, Bytes, DatabaseBytes int64 }

func (c Capacity) Allows(l CapacityLimits, additional int64) bool {
	return additional >= 0 && c.Events < l.Events && c.Bytes < l.Bytes && additional <= l.Bytes-c.Bytes && c.DatabaseBytes < l.DatabaseBytes
}

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
