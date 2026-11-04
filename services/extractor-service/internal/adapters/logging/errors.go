// errors.go implements safe error-category mapping without payload or credential disclosure in the
// service technical adapter layer.

// Package logging provides safe error-category mapping without payload or credential disclosure in
// the service technical adapter layer.
package logging

import (
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// ErrorCode exposes only reviewed categories, never underlying error text.
func ErrorCode(err error) string {
	for _, entry := range []struct {
		err  error
		code string
	}{
		{outbound.ErrDeliveryCommitUnknown, "commit_unknown"},
		{outbound.ErrDeliveryLeaseLost, "lease_lost"},
		{outbound.ErrPublishACK, "invalid_ack"},
		{outbound.ErrPublishOverloaded, "overloaded"},
		{outbound.ErrPublishUnavailable, "unavailable"},
		{outbound.ErrPublishInvalid, "invalid"},
		{outbound.ErrPublishConflict, "conflict"},
		{outbound.ErrPublishOversize, "oversize"},
		{outbound.ErrCommitUnknown, "commit_unknown"},
		{outbound.ErrRetryDeferred, "retry_deferred"},
		{outbound.ErrSourcePermanent, "source_permanent"},
		{outbound.ErrRPCBudgetExceeded, "rpc_budget_exhausted"},
		{outbound.ErrRunBudgetExceeded, "run_budget_exhausted"},
		{outbound.ErrDeliveryIncomplete, "delivery_incomplete"},
		{outbound.ErrDeliveryFailed, "delivery_failed"},
		{domain.ErrRecoveryRequired, "recovery_required"},
		{domain.ErrUnstableChain, "unstable_chain"},
		{domain.ErrBackpressure, "backpressure"},
		{domain.ErrCheckpointConflict, "checkpoint_conflict"},
		{domain.ErrBlockConflict, "block_conflict"},
		{domain.ErrOutOfSequence, "out_of_sequence"},
		{domain.ErrChainMismatch, "chain_mismatch"},
		{domain.ErrUnavailable, "unavailable"},
		{domain.ErrInvalidData, "invalid"},
		{domain.ErrIncomplete, "incomplete"},
	} {
		if errors.Is(err, entry.err) {
			return entry.code
		}
	}
	return logevent.ErrorCode(err)
}
