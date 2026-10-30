// Package metrics maps service-owned operational views to technical exposition.
package metrics

import (
	"context"
	shared "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/metrics"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"net/http"
)

func Handler(reader outbound.StatusReader, registry *shared.Registry) http.Handler {
	return registry.Handler(func(ctx context.Context) (shared.Snapshot, error) {
		status, err := reader.ReadStatus(ctx)
		if err != nil {
			return shared.Snapshot{}, err
		}
		snapshot := shared.Snapshot{AdmissionUsed: status.AdmissionUsed, AdmissionLimit: status.AdmissionLimit}
		snapshot.Queues = append(snapshot.Queues, shared.Queue{Kind: "inbox", Pending: status.Inbox.Pending, InFlight: status.Inbox.InFlight, Failed: status.Inbox.Failed, Done: status.Inbox.Done, OldestAge: status.Inbox.OldestAgeSeconds})
		return snapshot, nil
	})
}
