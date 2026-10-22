// Package postgres persists only Transformer-owned state using local transactions.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type database interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}
type Store struct {
	db          database
	chain       domain.ChainID
	lease       time.Duration
	maxAttempts int
}

var _ outbound.Inbox = (*Store)(nil)
var _ outbound.Queue = (*Store)(nil)

func New(pool *pgxpool.Pool, chain domain.ChainID, lease time.Duration, maxAttempts int) (*Store, error) {
	if pool == nil || !chain.Valid() || lease < time.Millisecond || lease > 24*time.Hour || maxAttempts < 1 || maxAttempts > 1000 {
		return nil, fmt.Errorf("invalid inbox store configuration")
	}
	return &Store{pool, chain, lease, maxAttempts}, nil
}
func dbError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return inbound.ErrUnavailable // never expose server details or source bytes
}
func rollback(ctx context.Context, tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(cleanup)
}
func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return dbError(err)
		}
		return errors.Join(outbound.ErrCommitUnknown, dbError(err))
	}
	return nil
}
func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, dbError(err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		rollback(ctx, tx)
		return nil, dbError(err)
	}
	return tx, nil
}
func (s *Store) Accept(ctx context.Context, r inbound.AcceptRawBlockRequest) (bool, error) {
	if err := inbound.ValidateEnvelope(r, s.chain, 64<<20); err != nil {
		return false, err
	}
	receipts := r.RawReceipts
	if receipts == nil {
		receipts = [][]byte{}
	}
	args := []any{r.EventID, r.Block.ChainID, strconv.FormatUint(r.Block.Number, 10), r.Block.Hash[:], r.Block.ParentHash[:], int(r.EventVersion), int(r.PayloadVersion), r.Provider, r.ExtractedAt.UTC().Format(time.RFC3339Nano), r.RawBlock, receipts, s.maxAttempts}
	tx, err := s.begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(ctx, tx)
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO transformer.inbox_events
 (event_id,chain_id,block_number,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,raw_block,raw_receipts,max_attempts)
 VALUES ($1,$2,$3::numeric,$4,$5,$6,$7,$8,$9,$10,$11,$12)
 ON CONFLICT (event_id) DO NOTHING RETURNING event_id`, args...).Scan(&id)
	if err == nil {
		if err := commit(ctx, tx); err != nil {
			return false, err
		}
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, dbError(err)
	}
	// INSERT waits for concurrent identity writers. This following Read Committed
	// statement sees their committed row and compares ALL immutable event content.
	var identical bool
	err = tx.QueryRow(ctx, `SELECT
 (chain_id,block_number,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,raw_block,raw_receipts)
 IS NOT DISTINCT FROM ($2::text,$3::numeric,$4::bytea,$5::bytea,$6::integer,$7::integer,$8::text,$9::text,$10::bytea,$11::bytea[])
 FROM transformer.inbox_events WHERE event_id=$1`, args[:11]...).Scan(&identical)
	if err != nil {
		return false, dbError(err)
	}
	if !identical {
		return false, inbound.ErrConflict
	}
	// No writes on this branch: the record observed above is already durable.
	return true, nil
}

const eventColumns = `event_id,chain_id,block_number::text,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,raw_block,raw_receipts,attempts,max_attempts`

func scanItem(row pgx.Row) (*outbound.Item, error) {
	var item outbound.Item
	r := &item.Event
	var height, extracted string
	var hash, parent []byte
	var eventVersion, payloadVersion int
	err := row.Scan(&r.EventID, &r.Block.ChainID, &height, &hash, &parent, &eventVersion, &payloadVersion, &r.Provider, &extracted, &r.RawBlock, &r.RawReceipts, &item.Attempt, &item.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	n, e1 := strconv.ParseUint(height, 10, 64)
	at, e2 := time.Parse(time.RFC3339Nano, extracted)
	if e1 != nil || e2 != nil || len(hash) != 32 || len(parent) != 32 || eventVersion != 1 || payloadVersion != 1 {
		return nil, inbound.ErrUnavailable
	}
	r.Block.Number = n
	r.Block.Hash = [32]byte(hash)
	r.Block.ParentHash = [32]byte(parent)
	r.EventVersion = uint32(eventVersion)
	r.PayloadVersion = uint32(payloadVersion)
	r.ExtractedAt = at
	return &item, nil
}
func (s *Store) Claim(ctx context.Context) (*outbound.Item, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	// Bounded recovery of crashes on the final permitted attempt.
	_, err = tx.Exec(ctx, `WITH expired AS (
 SELECT event_id FROM transformer.inbox_events WHERE chain_id=$1 AND status='processing'
 AND lease_until<=clock_timestamp() AND attempts>=max_attempts
 ORDER BY lease_until LIMIT 100 FOR UPDATE SKIP LOCKED)
 UPDATE transformer.inbox_events i SET status='failed',lease_until=NULL,last_error_code='attempts_exhausted'
 FROM expired e WHERE i.event_id=e.event_id`, string(s.chain))
	if err != nil {
		return nil, dbError(err)
	}
	item, err := scanItem(tx.QueryRow(ctx, `WITH candidate AS (
 SELECT event_id FROM transformer.inbox_events WHERE chain_id=$1 AND attempts<max_attempts
 AND ((status='pending' AND next_attempt_at<=clock_timestamp()) OR (status='processing' AND lease_until<=clock_timestamp()))
 ORDER BY accepted_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED)
 UPDATE transformer.inbox_events SET status='processing',attempts=attempts+1,
 lease_until=clock_timestamp()+$2::bigint*interval '1 microsecond'
 WHERE event_id IN (SELECT event_id FROM candidate) RETURNING `+eventColumns, string(s.chain), s.lease.Microseconds()))
	if err != nil {
		return nil, err
	}
	if err := commit(ctx, tx); err != nil {
		return nil, err
	}
	return item, nil
}
func (s *Store) Complete(ctx context.Context, item outbound.Item, result outbound.Result) error {
	r := item.Event
	expected := fmt.Sprintf("transformed-block/v1/%s/%d/%x", r.Block.ChainID, r.Block.Number, r.Block.Hash)
	if r.Block.ChainID != string(s.chain) || result.EventID != expected || result.PayloadVersion != 1 || len(result.Payload) == 0 || len(result.Payload) > 64<<20 {
		return domain.Invalid("transformation result")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	changed, err := tx.Exec(ctx, `UPDATE transformer.inbox_events SET status='completed',lease_until=NULL,completed_at=clock_timestamp(),last_error_code=NULL
 WHERE event_id=$1 AND chain_id=$2 AND attempts=$3 AND status='processing' AND lease_until>clock_timestamp()`, r.EventID, string(s.chain), item.Attempt)
	if err != nil {
		return dbError(err)
	}
	if changed.RowsAffected() != 1 {
		return outbound.ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `INSERT INTO transformer.transformed_blocks (event_id,source_event_id,payload_version,payload)
 VALUES ($1,$2,$3,$4)`, result.EventID, r.EventID, int(result.PayloadVersion), result.Payload)
	if err != nil {
		return dbError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO transformer.outbox_events (event_id) VALUES ($1)`, result.EventID); err != nil {
		return dbError(err)
	}
	return commit(ctx, tx)
}
func (s *Store) Fail(ctx context.Context, item outbound.Item, f outbound.Failure) error {
	if f.Delay < 0 || f.Delay > 24*time.Hour {
		return domain.Invalid("retry delay")
	}
	switch f.Code {
	case "incomplete", "identity", "unsupported", "invalid", "output_too_large", "transient":
	default:
		return domain.Invalid("failure code")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	changed, err := tx.Exec(ctx, `UPDATE transformer.inbox_events
 SET status=CASE WHEN $4 OR attempts>=max_attempts THEN 'failed' ELSE 'pending' END,
 lease_until=NULL,last_error_code=$5,next_attempt_at=clock_timestamp()+$6::bigint*interval '1 microsecond'
 WHERE event_id=$1 AND chain_id=$2 AND attempts=$3 AND status='processing' AND lease_until>clock_timestamp()`,
		item.Event.EventID, string(s.chain), item.Attempt, f.Permanent, f.Code, f.Delay.Microseconds())
	if err != nil {
		return dbError(err)
	}
	if changed.RowsAffected() != 1 {
		return outbound.ErrLeaseLost
	}
	return commit(ctx, tx)
}
