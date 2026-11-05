// handler.go implements owning-service status translation into shared metric exposition in the
// inbound adapter layer.

// Package metrics maps service-owned operational views to technical exposition.
package metrics

import (
	"context"
	"net/http"

	shared "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
)

// Handler adapts the service's owning-schema status reader into shared metric exposition without
// cross-schema queries.
func Handler(reader outbound.StatusReader, registry *shared.Registry) http.Handler {
	return registry.Handler(func(ctx context.Context) (shared.Snapshot, error) {
		status, err := reader.ReadStatus(ctx)
		if err != nil {
			return shared.Snapshot{}, err
		}
		snapshot := shared.Snapshot{}
		snapshot.Queues = append(snapshot.Queues, shared.Queue{Kind: "outbox", Pending: status.Outbox.Pending, InFlight: status.Outbox.InFlight, Failed: status.Outbox.Failed, Done: status.Outbox.Done, OldestAge: status.Outbox.OldestAgeSeconds})
		snapshot.Extractor = true
		snapshot.Checkpoint = status.Checkpoint
		snapshot.Revision = status.Revision
		return snapshot, nil
	})
}
