// store.go implements durable acceptance, transactional state changes and safe database error
// mapping in the outbound adapter layer.

// Package postgres persists only Loader-owned state using local transactions.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// database is the pgx operation subset used by the adapter and its fault-injection tests; it is
// not an application port.
type database interface {
	// BeginTx starts an adapter-owned database transaction with the supplied options; the caller owns
	// completion/rollback.
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// Store binds owning-schema persistence to explicit chain/lease/capacity settings; caller-owned
// pools outlive its operations.
type Store struct {
	db          database
	chain       domain.ChainID
	lease       time.Duration
	maxAttempts int
	capacity    int
	batchSize   int
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.Inbox = (*Store)(nil)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.Queue = (*Store)(nil)

// New validates owning-chain/storage policy and binds the caller-owned pool; construction does not
// migrate or claim work.
func New(pool *pgxpool.Pool, chain domain.ChainID, lease time.Duration, maxAttempts int, capacities ...int) (*Store, error) {
	if pool == nil || !chain.Valid() || lease < time.Millisecond || lease > 24*time.Hour || maxAttempts < 1 || maxAttempts > 1000 {
		return nil, fmt.Errorf("invalid inbox store configuration")
	}
	capacity := 128
	if len(capacities) > 0 {
		capacity = capacities[0]
	}
	if capacity < 1 || capacity > 10000 {
		return nil, fmt.Errorf("invalid admission capacity")
	}
	return &Store{db: pool, chain: chain, lease: lease, maxAttempts: maxAttempts, capacity: capacity, batchSize: DefaultBatchSize}, nil
}

// NewWithBatchSize configures SQL coalescing without changing transaction boundaries.
func NewWithBatchSize(pool *pgxpool.Pool, chain domain.ChainID, lease time.Duration, maxAttempts, capacity, batchSize int) (*Store, error) {
	if batchSize < 1 || batchSize > MaxBatchSize {
		return nil, fmt.Errorf("invalid SQL batch size")
	}
	s, err := New(pool, chain, lease, maxAttempts, capacity)
	if err != nil {
		return nil, err
	}
	s.batchSize = batchSize
	return s, nil
}

// dbError preserves cancellation/deadline identity and hides database server details behind the
// service's safe error vocabulary.
func dbError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "23505" {
		return inbound.ErrConflict
	}
	return inbound.ErrUnavailable // never expose server details or source bytes
}

// rollback attempts rollback with a separate bounded cleanup context so caller cancellation does
// not skip cleanup.
func rollback(ctx context.Context, tx pgx.Tx) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(cleanup)
}

// commit returns explicit commit uncertainty when the acknowledgement cannot establish success or
// rollback.
func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return dbError(err)
		}
		return errors.Join(outbound.ErrCommitUnknown, dbError(err))
	}
	return nil
}

// begin opens a local read-committed transaction with synchronous commit; initialization failure
// rolls it back.
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

// Accept serializes durable admission, checks exact duplicates before capacity and commits new
// Inbox input before returning success.
func (s *Store) Accept(ctx context.Context, r inbound.AcceptTransformedBlockRequest) (bool, error) {
	if err := inbound.ValidateEnvelope(r, s.chain, 64<<20); err != nil {
		return false, err
	}
	args := []any{r.EventID, r.Block.ChainID, strconv.FormatUint(r.Block.Number, 10), r.Block.Hash[:], r.Block.ParentHash[:], int(r.EventVersion), int(r.PayloadVersion), r.Provider, r.ExtractedAt.UTC().Format(time.RFC3339Nano), r.PayloadJSON, r.SourceEventID, r.TransformedAt.UTC().Format(time.RFC3339Nano), s.maxAttempts}
	tx, err := s.begin(ctx)
	if err != nil {
		return false, err
	}
	defer rollback(ctx, tx)
	// Serialize new admissions across all instances of this owning service.
	// Completion transfers one Transformer slot from Inbox to Outbox atomically;
	// delivery only decreases outstanding work. Failed rows retain their slot.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(14202)"); err != nil {
		return false, dbError(err)
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM loader.inbox_events WHERE event_id=$1)", r.EventID).Scan(&exists); err != nil {
		return false, dbError(err)
	}
	if exists {
		var identical bool
		err = tx.QueryRow(ctx, `SELECT
 (chain_id,block_number,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,payload,source_event_id,transformed_at)
 IS NOT DISTINCT FROM ($2::text,$3::numeric,$4::bytea,$5::bytea,$6::integer,$7::integer,$8::text,$9::text,$10::bytea,$11::text,$12::text)
 FROM loader.inbox_events WHERE event_id=$1`, args[:12]...).Scan(&identical)
		if err != nil {
			return false, dbError(err)
		}
		if !identical {
			return false, inbound.ErrConflict
		}
		return true, nil
	}
	full, err := s.admissionFull(ctx, tx)
	if err != nil {
		return false, err
	}
	if full {
		return false, inbound.ErrOverloaded
	}
	var id string
	err = tx.QueryRow(ctx, `INSERT INTO loader.inbox_events
 (event_id,chain_id,block_number,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,payload,source_event_id,transformed_at,max_attempts)
 VALUES ($1,$2,$3::numeric,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
 ON CONFLICT (event_id) DO NOTHING RETURNING event_id`, args...).Scan(&id)
	if err != nil {
		return false, dbError(err)
	}
	if err = commit(ctx, tx); err != nil {
		return false, err
	}
	return false, nil
}

// eventColumns fixes the SQL projection order consumed by the claimed-event scanner.
const eventColumns = `event_id,chain_id,block_number::text,block_hash,parent_hash,event_version,payload_version,provider,extracted_at,payload,source_event_id,transformed_at,attempts,max_attempts`

// scanItem decodes a claimed row into an application item, keeping no-row distinct from invalid or
// unavailable storage.
func scanItem(row pgx.Row) (*outbound.Item, error) {
	var item outbound.Item
	r := &item.Event
	var height, extracted, transformed string
	var hash, parent []byte
	var eventVersion, payloadVersion int
	err := row.Scan(&r.EventID, &r.Block.ChainID, &height, &hash, &parent, &eventVersion, &payloadVersion, &r.Provider, &extracted, &r.PayloadJSON, &r.SourceEventID, &transformed, &item.Attempt, &item.MaxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	n, e1 := strconv.ParseUint(height, 10, 64)
	at, e2 := time.Parse(time.RFC3339Nano, extracted)
	transformedAt, e3 := time.Parse(time.RFC3339Nano, transformed)
	if e1 != nil || e2 != nil || e3 != nil || len(hash) != 32 || len(parent) != 32 || eventVersion != 1 || payloadVersion != 1 {
		return nil, inbound.ErrUnavailable
	}
	r.Block.Number = n
	r.Block.Hash = [32]byte(hash)
	r.Block.ParentHash = [32]byte(parent)
	r.EventVersion = uint32(eventVersion)
	r.PayloadVersion = uint32(payloadVersion)
	r.ExtractedAt = at
	r.TransformedAt = transformedAt
	return &item, nil
}

// Claim reclaims expired/due Inbox work with SKIP LOCKED and a new attempt fence; it returns only
// after the claim transaction commits.
func (s *Store) Claim(ctx context.Context) (*outbound.Item, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(ctx, tx)
	// Bounded recovery of crashes on the final permitted attempt.
	_, err = tx.Exec(ctx, `WITH expired AS (
 SELECT event_id FROM loader.inbox_events WHERE chain_id=$1 AND status='processing'
 AND lease_until<=clock_timestamp() AND attempts>=max_attempts
 ORDER BY lease_until LIMIT 100 FOR UPDATE SKIP LOCKED)
 UPDATE loader.inbox_events i SET status='failed',lease_until=NULL,last_error_code='attempts_exhausted'
 FROM expired e WHERE i.event_id=e.event_id`, string(s.chain))
	if err != nil {
		return nil, dbError(err)
	}
	item, err := scanItem(tx.QueryRow(ctx, `WITH candidate AS (
 SELECT event_id FROM loader.inbox_events WHERE chain_id=$1 AND attempts<max_attempts
 AND ((status='pending' AND next_attempt_at<=clock_timestamp()) OR (status='processing' AND lease_until<=clock_timestamp()))
 ORDER BY accepted_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED)
 UPDATE loader.inbox_events SET status='processing',attempts=attempts+1,
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

// Fail persists a safe retry delay or terminal state for the matching live Inbox attempt; source
// content remains retained.
func (s *Store) Fail(ctx context.Context, item outbound.Item, f outbound.Failure) error {
	if f.Delay < 0 || f.Delay > 24*time.Hour {
		return domain.Invalid("retry delay")
	}
	switch f.Code {
	case "incomplete", "identity", "unsupported", "invalid", "conflict", "transient":
	default:
		return domain.Invalid("failure code")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	changed, err := tx.Exec(ctx, `UPDATE loader.inbox_events
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
