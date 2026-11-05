// complete.go implements atomic normalized block writes and Inbox completion under the chain/lease
// fence in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// Complete fences the claimant and commits all query records and completion in
// one local transaction. No accepted source fields are overwritten.
func (s *Store) Complete(ctx context.Context, item outbound.Item, doc domain.Document) error {
	if err := inbound.ValidateEnvelope(item.Event, s.chain, 64<<20); err != nil {
		return err
	}
	if err := inbound.ValidateDocument(item.Event, doc); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	// Same-chain completions see the previous committed observations before
	// reading neighbors. Acquire this lock before any Inbox/block row locks.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('loader-observations/' || $1::text,0))", string(s.chain)); err != nil {
		return dbError(err)
	}
	var state string
	var attempt int
	var active bool
	err = tx.QueryRow(ctx, `SELECT status,attempts,COALESCE(lease_until>clock_timestamp(),false)
 FROM loader.inbox_events WHERE event_id=$1 AND chain_id=$2 FOR UPDATE`, item.Event.EventID, string(s.chain)).Scan(&state, &attempt, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return outbound.ErrLeaseLost
	}
	if err != nil {
		return dbError(err)
	}
	if attempt != item.Attempt {
		return outbound.ErrLeaseLost
	}
	if state == "completed" {
		return nil
	} // same successful attempt; COMMIT response may have been lost
	if state != "processing" || !active {
		return outbound.ErrLeaseLost
	}
	snap := doc.Snapshot
	writes := &recordBatch{tx: tx, limit: s.batchSize}
	if err = insertBlock(ctx, writes, snap.ChainID, snap.Block, item.Event.EventID); err != nil {
		return err
	}
	for i, transaction := range snap.Transactions {
		if err = insertTransaction(ctx, writes, snap.ChainID, transaction); err != nil {
			return err
		}
		receipt := snap.Receipts[i]
		if err = insertReceipt(ctx, writes, snap.ChainID, receipt); err != nil {
			return err
		}
		for _, log := range receipt.Logs {
			if err = insertLog(ctx, writes, snap.ChainID, log); err != nil {
				return err
			}
		}
	}
	if err = writes.flush(ctx); err != nil {
		return dbError(err)
	}
	if err = updateObservations(ctx, tx, snap.ChainID, snap.Block); err != nil {
		return err
	}
	changed, err := tx.Exec(ctx, `UPDATE loader.inbox_events SET status='completed',lease_until=NULL,
 completed_at=clock_timestamp(),last_error_code=NULL WHERE event_id=$1 AND chain_id=$2
 AND attempts=$3 AND status='processing' AND lease_until>clock_timestamp()`, item.Event.EventID, string(s.chain), item.Attempt)
	if err != nil {
		return dbError(err)
	}
	if changed.RowsAffected() != 1 {
		return outbound.ErrLeaseLost
	}
	return commit(ctx, tx)
}
