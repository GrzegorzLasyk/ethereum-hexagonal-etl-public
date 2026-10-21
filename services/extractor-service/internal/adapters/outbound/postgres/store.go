// Package postgres implements Extractor-owned atomic persistence.
package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

var (
	ErrDatabase      = errors.New("extractor database operation failed")
	ErrCommitUnknown = errors.New("extraction commit outcome unknown; retry the same block identity")
	ErrCorrupt       = errors.New("inconsistent stored extraction")
	ErrNotFound      = errors.New("stored extraction not found")
)

// DatabaseError omits server messages/details that may contain source data or
// secrets. SQLState is available for operational diagnostics, never retry policy
// that silently advances a checkpoint.
type DatabaseError struct{ SQLState string }

func (e *DatabaseError) Error() string { return "extractor database SQLSTATE " + e.SQLState }
func (e *DatabaseError) Unwrap() error { return ErrDatabase }

func databaseError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var server *pgconn.PgError
	if errors.As(err, &server) {
		return &DatabaseError{SQLState: server.Code}
	}
	return ErrDatabase
}

type database interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Store struct {
	db    database
	chain domain.ChainID
	start uint64
}

var _ outbound.ExtractionStore = (*Store)(nil)

// New borrows a pool authenticated as extractor_runtime in ethereum_etl.
// The caller owns pool creation/closure and supplies the same chain/start as
// the use case. No connection string, credentials or implicit migrations here.
func New(pool *pgxpool.Pool, chain domain.ChainID, start uint64) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: database pool required", domain.ErrInvalidData)
	}
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	return &Store{db: pool, chain: chain, start: start}, nil
}

const checkpointSQL = `SELECT block_number::text, block_hash
FROM extractor.extraction_checkpoints WHERE chain_id = $1`

func readCheckpoint(row pgx.Row, chain domain.ChainID) (*domain.Checkpoint, error) {
	var number string
	var hash []byte
	if err := row.Scan(&number, &hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, databaseError(err)
	}
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || len(hash) != 32 {
		return nil, ErrCorrupt
	}
	cp := &domain.Checkpoint{Block: domain.BlockID{ChainID: chain, Number: n, Hash: domain.Hash(hash)}}
	if cp.Block.Validate() != nil {
		return nil, ErrCorrupt
	}
	return cp, nil
}

func (s *Store) Checkpoint(ctx context.Context, chain domain.ChainID) (*domain.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := chain.Validate(); err != nil {
		return nil, err
	}
	if chain != s.chain {
		return nil, domain.ErrChainMismatch
	}
	return readCheckpoint(s.db.QueryRow(ctx, checkpointSQL, string(chain)), chain)
}

func sameCheckpoint(a, b *domain.Checkpoint) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Block == b.Block
}

func (s *Store) Commit(ctx context.Context, command outbound.CommitExtraction) (outbound.CommitResult, error) {
	var result outbound.CommitResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	e := command.Extraction
	if err := e.Validate(); err != nil {
		return result, err
	}
	id := e.Header.ID
	if id.ChainID != s.chain {
		return result, domain.ErrChainMismatch
	}
	expected := command.ExpectedCheckpoint
	if expected != nil {
		copy := *expected
		expected = &copy
		if err := expected.Block.Validate(); err != nil {
			return result, err
		}
		if expected.Block.ChainID != s.chain {
			return result, domain.ErrChainMismatch
		}
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, databaseError(err)
	}
	defer func() {
		// Rollback must still release the transaction after caller cancellation.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	// This transaction-scoped lock also serializes bootstrap when no checkpoint
	// row exists. Hash collisions only serialize unrelated chains. Read Committed
	// ensures the reads AFTER acquiring the lock observe the preceding commit.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(174728, hashtext($1))`, string(s.chain)); err != nil {
		return result, databaseError(err)
	}
	actual, err := readCheckpoint(tx.QueryRow(ctx, checkpointSQL, string(s.chain)), s.chain)
	if err != nil {
		return result, err
	}
	var hash, parent []byte
	err = tx.QueryRow(ctx, `SELECT block_hash, parent_hash FROM extractor.raw_blocks
WHERE chain_id = $1 AND block_number = $2::numeric`, string(s.chain), strconv.FormatUint(id.Number, 10)).Scan(&hash, &parent)
	if err == nil {
		if !bytes.Equal(hash, id.Hash[:]) || !bytes.Equal(parent, e.Header.ParentHash[:]) {
			return result, domain.ErrBlockConflict
		}
		var present bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM extractor.outbox_events WHERE event_id = $1)`, id.EventID()).Scan(&present); err != nil {
			return result, databaseError(err)
		}
		if !present || actual == nil || actual.Block.Number < id.Number {
			return result, ErrCorrupt
		}
		// Identity deduplication does not assert full snapshot equivalence.
		// Retain original bytes, provenance and event, even after later progress.
		return outbound.CommitResult{AlreadyStored: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, databaseError(err)
	}
	if !sameCheckpoint(actual, expected) {
		return result, domain.ErrCheckpointConflict
	}
	if err := domain.CheckPosition(s.chain, s.start, id.Number, actual); err != nil {
		return result, err
	}
	if err := domain.CheckContinuity(e.Header, actual); err != nil {
		return result, err
	}
	// Encode only a new snapshot. A valid identity retry must not depend on
	// whether its replacement provenance can be represented by storage: that
	// replacement is never written when the block is already durable.
	metadata, extractedAt, err := encodeMetadata(e)
	if err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO extractor.raw_blocks
(chain_id,block_number,block_hash,parent_hash,raw_block,raw_receipts,metadata,provider,extracted_at)
VALUES ($1,$2::numeric,$3,$4,$5,$6,$7::jsonb,$8,$9)`, string(s.chain), strconv.FormatUint(id.Number, 10), id.Hash[:], e.Header.ParentHash[:], e.RawBlock, e.RawReceipts, metadata, e.Provider, extractedAt); err != nil {
		return result, databaseError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO extractor.outbox_events (event_id,chain_id,block_number,block_hash)
VALUES ($1,$2,$3::numeric,$4)`, id.EventID(), string(s.chain), strconv.FormatUint(id.Number, 10), id.Hash[:]); err != nil {
		return result, databaseError(err)
	}
	var changed pgconn.CommandTag
	if actual == nil {
		changed, err = tx.Exec(ctx, `INSERT INTO extractor.extraction_checkpoints (chain_id,block_number,block_hash)
VALUES ($1,$2::numeric,$3) ON CONFLICT (chain_id) DO NOTHING`, string(s.chain), strconv.FormatUint(id.Number, 10), id.Hash[:])
	} else {
		changed, err = tx.Exec(ctx, `UPDATE extractor.extraction_checkpoints
SET block_number=$2::numeric, block_hash=$3, updated_at=clock_timestamp()
WHERE chain_id=$1 AND block_number=$4::numeric AND block_hash=$5`, string(s.chain), strconv.FormatUint(id.Number, 10), id.Hash[:], strconv.FormatUint(actual.Block.Number, 10), actual.Block.Hash[:])
	}
	if err != nil {
		return result, databaseError(err)
	}
	if changed.RowsAffected() != 1 {
		return result, domain.ErrCheckpointConflict
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return result, databaseError(err)
		}
		return result, errors.Join(ErrCommitUnknown, databaseError(err))
	}
	// Never reinterpret a successful COMMIT as failure on late cancellation.
	return result, nil
}
