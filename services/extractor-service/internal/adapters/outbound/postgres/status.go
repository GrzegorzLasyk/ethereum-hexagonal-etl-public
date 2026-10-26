package postgres

import (
	"context"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5"
	"strconv"
	"time"
)

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
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE e.status='pending'),count(*) FILTER (WHERE e.status='sending'),count(*) FILTER (WHERE e.status='failed'),count(*) FILTER (WHERE e.status='delivered'), COALESCE(GREATEST(0,extract(epoch FROM transaction_timestamp()-min(e.created_at) FILTER (WHERE e.status<>'delivered'))),0)::float8 FROM extractor.outbox_events e WHERE e.chain_id=$1`, string(s.chain)).Scan(&result.Outbox.Pending, &result.Outbox.InFlight, &result.Outbox.Failed, &result.Outbox.Done, &result.Outbox.OldestAgeSeconds)
	if err != nil {
		return outbound.Status{}, err
	}
	var height string
	err = tx.QueryRow(ctx, `SELECT COALESCE((SELECT block_number::text FROM extractor.extraction_checkpoints WHERE chain_id=$1),''),COALESCE((SELECT revision FROM extractor.extraction_checkpoints WHERE chain_id=$1),0)`, string(s.chain)).Scan(&height, &result.Revision)
	if err != nil {
		return outbound.Status{}, err
	}
	if height != "" {
		n, err := strconv.ParseUint(height, 10, 64)
		if err != nil {
			return outbound.Status{}, err
		}
		result.Checkpoint = &n
	}
	return result, nil
}
