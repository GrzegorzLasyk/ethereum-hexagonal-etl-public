// errors.go implements safe error-category mapping without payload or credential disclosure in the
// service technical adapter layer.

// Package logging provides safe error-category mapping without payload or credential disclosure in
// the service technical adapter layer.
package logging

import (
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
)

// ErrorCode exposes only reviewed categories, never underlying error text.
func ErrorCode(err error) string {
	for _, entry := range []struct {
		err  error
		code string
	}{
		{outbound.ErrCommitUnknown, "commit_unknown"},
		{outbound.ErrRepairRefused, "repair_refused"},
		{outbound.ErrLeaseLost, "lease_lost"},
		{inbound.ErrOverloaded, "overloaded"},
		{inbound.ErrUnavailable, "unavailable"},
		{inbound.ErrInvalidEvent, "invalid"},
		{inbound.ErrUnsupportedVersion, "unsupported"},
		{inbound.ErrConflict, "conflict"},
		{domain.ErrInvalid, "invalid"},
		{domain.ErrIdentity, "identity"},
		{domain.ErrUnsupported, "unsupported"},
		{domain.ErrIncomplete, "incomplete"},
	} {
		if errors.Is(err, entry.err) {
			return entry.code
		}
	}
	return logevent.ErrorCode(err)
}
