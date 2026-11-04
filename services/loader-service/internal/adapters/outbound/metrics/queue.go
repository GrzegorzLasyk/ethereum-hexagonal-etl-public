// queue.go implements persistence-port timing and outcome observations without changing queue
// behavior in the outbound adapter layer.

// Package metrics provides persistence-port timing and outcome observations without changing queue
// behavior in the outbound adapter layer.
package metrics

import (
	"context"
	shared "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/logevent"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/logging"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"time"
)

// Queue measures the actual persistence port call, excluding decode and claim.
// It preserves every return value and never performs an additional state change.
type Queue struct {
	outbound.Queue
	Registry *shared.Registry
}

// Complete times the wrapped persistence port and records its outcome without changing
// commit/error semantics.
func (q Queue) Complete(ctx context.Context, item outbound.Item, doc domain.Document) error {
	started := time.Now()
	err := q.Queue.Complete(ctx, item, doc)
	outcome := "completed"
	if err != nil {
		outcome = logevent.FailureOutcome(logging.ErrorCode(err))
	}
	q.Registry.Observe("persist_block", outcome, time.Since(started))
	return err
}
