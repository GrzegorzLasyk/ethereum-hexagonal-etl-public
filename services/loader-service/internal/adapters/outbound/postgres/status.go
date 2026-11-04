// status.go implements owning-schema queue/progress snapshots for operational visibility in the
// outbound adapter layer.

package postgres

import (
	"context"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5"
)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.StatusReader = (*Store)(nil)

// ReadStatus uses one read-only repeatable snapshot and only service-owned tables.
func (s *Store) ReadStatus(ctx context.Context) (outbound.Status, error) {
	var result outbound.Status
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return outbound.Status{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE e.status='pending'),count(*) FILTER (WHERE e.status='processing'),count(*) FILTER (WHERE e.status='failed'),count(*) FILTER (WHERE e.status='completed'), COALESCE(GREATEST(0,extract(epoch FROM transaction_timestamp()-min(e.accepted_at) FILTER (WHERE e.status<>'completed'))),0)::float8 FROM loader.inbox_events e WHERE e.chain_id=$1`, string(s.chain)).Scan(&result.Inbox.Pending, &result.Inbox.InFlight, &result.Inbox.Failed, &result.Inbox.Done, &result.Inbox.OldestAgeSeconds)
	if err != nil {
		return outbound.Status{}, err
	}
	result.AdmissionLimit = int64(s.capacity)
	err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM loader.inbox_events WHERE status<>'completed')`).Scan(&result.AdmissionUsed)
	if err != nil {
		return outbound.Status{}, err
	}
	return result, nil
}
