package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Outbox reads only its owning schema. No transaction spans an RPC call.
type Outbox struct {
	db          database
	chain       string
	lease       time.Duration
	maxAttempts int
}

var _ outbound.DeliveryQueue = (*Outbox)(nil)

func NewOutbox(pool *pgxpool.Pool, chain string, lease time.Duration, maxAttempts int) (*Outbox, error) {
	if pool == nil || !domain.ChainID(chain).Valid() || lease < time.Millisecond || lease > 24*time.Hour || maxAttempts < 1 || maxAttempts > 1000 {
		return nil, fmt.Errorf("invalid outbox configuration")
	}
	return &Outbox{pool, chain, lease, maxAttempts}, nil
}
func deliveryError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("outbox database operation failed")
}
func deliveryRollback(ctx context.Context, tx pgx.Tx) {
	c, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(c)
}
func deliveryCommit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return deliveryError(err)
		}
		return errors.Join(outbound.ErrDeliveryCommitUnknown, deliveryError(err))
	}
	return nil
}
func (s *Outbox) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, deliveryError(err)
	}
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		deliveryRollback(ctx, tx)
		return nil, deliveryError(err)
	}
	return tx, nil
}
func (s *Outbox) ClaimDelivery(ctx context.Context) (*outbound.Delivery, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer deliveryRollback(ctx, tx)
	// Recover crashes on the final allowed attempt without dropping the event.
	_, err = tx.Exec(ctx, `WITH exhausted AS (SELECT o.event_id FROM transformer.outbox_events o JOIN transformer.transformed_blocks b ON b.event_id=o.event_id JOIN transformer.inbox_events i ON i.event_id=b.source_event_id
 WHERE i.chain_id=$1 AND o.status='sending' AND o.lease_until<=clock_timestamp() AND o.attempts>=o.max_attempts
 ORDER BY o.lease_until LIMIT 100 FOR UPDATE OF o SKIP LOCKED)
 UPDATE transformer.outbox_events o SET status='failed',lease_until=NULL,last_error_code='attempts_exhausted'
 FROM exhausted e WHERE o.event_id=e.event_id`, s.chain)
	if err != nil {
		return nil, deliveryError(err)
	}
	var item outbound.Delivery
	err = tx.QueryRow(ctx, `WITH candidate AS (SELECT o.event_id FROM transformer.outbox_events o JOIN transformer.transformed_blocks b ON b.event_id=o.event_id JOIN transformer.inbox_events i ON i.event_id=b.source_event_id
 WHERE i.chain_id=$1 AND o.attempts<COALESCE(o.max_attempts,$3)
 AND ((o.status='pending' AND o.next_attempt_at<=clock_timestamp()) OR (o.status='sending' AND o.lease_until<=clock_timestamp()))
 ORDER BY o.created_at,o.event_id LIMIT 1 FOR UPDATE OF o SKIP LOCKED)
 UPDATE transformer.outbox_events o SET status='sending',attempts=o.attempts+1,max_attempts=COALESCE(o.max_attempts,$3),
 lease_until=clock_timestamp()+$2::bigint*interval '1 microsecond'
 WHERE o.event_id IN (SELECT event_id FROM candidate) RETURNING o.event_id,o.attempts,o.max_attempts`, s.chain, s.lease.Microseconds(), s.maxAttempts).Scan(&item.Event.EventID, &item.Attempt, &item.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, deliveryCommit(ctx, tx)
	}
	if err != nil {
		return nil, deliveryError(err)
	}
	event := &item.Event
	var number, extracted string
	var hash, parent []byte
	err = tx.QueryRow(ctx, `SELECT i.chain_id,i.block_number::text,i.block_hash,i.parent_hash,i.provider,i.extracted_at,b.payload,b.source_event_id,b.transformed_at FROM transformer.outbox_events o JOIN transformer.transformed_blocks b ON b.event_id=o.event_id JOIN transformer.inbox_events i ON i.event_id=b.source_event_id WHERE i.chain_id=$1 AND o.event_id=$2`, s.chain, event.EventID).Scan(&event.ChainID, &number, &hash, &parent, &event.Provider, &extracted, &event.Payload, &event.SourceEventID, &event.TransformedAt)
	if err != nil {
		return nil, deliveryError(err)
	}
	var parseErr error
	event.Number, parseErr = strconv.ParseUint(number, 10, 64)
	if parseErr != nil || len(hash) != 32 || len(parent) != 32 {
		return nil, errors.New("invalid stored outbox identity")
	}
	event.Hash = [32]byte(hash)
	event.ParentHash = [32]byte(parent)
	event.ExtractedAt, parseErr = time.Parse(time.RFC3339Nano, extracted)
	if parseErr != nil {
		return nil, errors.New("invalid stored outbox provenance")
	}
	if err = deliveryCommit(ctx, tx); err != nil {
		return nil, err
	}
	return &item, nil
}
func (s *Outbox) Published(ctx context.Context, item outbound.Delivery) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer deliveryRollback(ctx, tx)
	var state string
	var attempt int
	var active bool
	err = tx.QueryRow(ctx, `SELECT o.status,o.attempts,COALESCE(o.lease_until>clock_timestamp(),false) FROM transformer.outbox_events o JOIN transformer.transformed_blocks b ON b.event_id=o.event_id JOIN transformer.inbox_events i ON i.event_id=b.source_event_id
 WHERE i.chain_id=$1 AND o.event_id=$2 FOR UPDATE OF o`, s.chain, item.Event.EventID).Scan(&state, &attempt, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return outbound.ErrDeliveryLeaseLost
	}
	if err != nil {
		return deliveryError(err)
	}
	if attempt != item.Attempt {
		return outbound.ErrDeliveryLeaseLost
	}
	if state == "delivered" {
		return nil
	} // resolve a lost local COMMIT response
	if state != "sending" || !active {
		return outbound.ErrDeliveryLeaseLost
	}
	changed, err := tx.Exec(ctx, `UPDATE transformer.outbox_events SET status='delivered',lease_until=NULL,delivered_at=clock_timestamp(),last_error_code=NULL
 WHERE event_id=$1 AND attempts=$2 AND status='sending' AND lease_until>clock_timestamp()`, item.Event.EventID, item.Attempt)
	if err != nil {
		return deliveryError(err)
	}
	if changed.RowsAffected() != 1 {
		return outbound.ErrDeliveryLeaseLost
	}
	return deliveryCommit(ctx, tx)
}
func (s *Outbox) RetryDelivery(ctx context.Context, item outbound.Delivery, f outbound.DeliveryFailure) error {
	if f.Delay < 0 || f.Delay > 24*time.Hour {
		return fmt.Errorf("invalid delivery retry delay")
	}
	switch f.Code {
	case "invalid", "conflict", "oversize", "invalid_ack", "unavailable":
	default:
		return fmt.Errorf("invalid delivery failure code")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer deliveryRollback(ctx, tx)
	changed, err := tx.Exec(ctx, `UPDATE transformer.outbox_events SET
 status=CASE WHEN $4 OR attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
 lease_until=NULL,last_error_code=$5,next_attempt_at=clock_timestamp()+$6::bigint*interval '1 microsecond'
 WHERE event_id IN (SELECT o.event_id FROM transformer.outbox_events o JOIN transformer.transformed_blocks b ON b.event_id=o.event_id JOIN transformer.inbox_events i ON i.event_id=b.source_event_id WHERE i.chain_id=$1 AND o.event_id=$2)
 AND attempts=$3 AND status='sending' AND lease_until>clock_timestamp()`, s.chain, item.Event.EventID, item.Attempt, f.Permanent, f.Code, f.Delay.Microseconds())
	if err != nil {
		return deliveryError(err)
	}
	if changed.RowsAffected() != 1 {
		return outbound.ErrDeliveryLeaseLost
	}
	return deliveryCommit(ctx, tx)
}
