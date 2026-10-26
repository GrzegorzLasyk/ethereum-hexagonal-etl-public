// Package metrics maps service-owned operational views to technical exposition.
package metrics

import (
	"context"
	shared "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"net/http"
)

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
