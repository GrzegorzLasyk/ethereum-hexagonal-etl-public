// Package postgres implements Extractor-owned atomic persistence.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

var (
	ErrDatabase      = errors.New("extractor database operation failed")
	ErrCommitUnknown = outbound.ErrCommitUnknown
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

const checkpointSQL = `SELECT block_number::text, block_hash, revision, start_number::text
FROM extractor.extraction_checkpoints WHERE chain_id = $1`

func readCheckpoint(row pgx.Row, chain domain.ChainID) (*domain.Checkpoint, error) {
	var number, start string
	var revision uint64
	var hash []byte
	if err := row.Scan(&number, &hash, &revision, &start); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, databaseError(err)
	}
	n, err := strconv.ParseUint(number, 10, 64)
	if err != nil || len(hash) != 32 {
		return nil, ErrCorrupt
	}
	origin, err := strconv.ParseUint(start, 10, 64)
	if err != nil || origin > n {
		return nil, ErrCorrupt
	}
	cp := &domain.Checkpoint{Revision: revision, Start: origin, Block: domain.BlockID{ChainID: chain, Number: n, Hash: domain.Hash(hash)}}
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
	cp, err := readCheckpoint(s.db.QueryRow(ctx, checkpointSQL, string(chain)), chain)
	if err == nil && cp != nil && cp.Start != s.start {
		return nil, domain.ErrCheckpointConflict
	}
	return cp, err
}

func sameCheckpoint(a, b *domain.Checkpoint) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Block == b.Block && a.Revision == b.Revision
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
	if e.Header.ID.ChainID != s.chain {
		return result, domain.ErrChainMismatch
	}
	if command.Limits != nil {
		if err := command.Limits.Validate(); err != nil {
			return result, err
		}
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
	tx, err := s.beginBranch(ctx)
	if err != nil {
		return result, err
	}
	defer deliveryRollback(ctx, tx)
	actual, err := readCheckpoint(tx.QueryRow(ctx, checkpointSQL, string(s.chain)), s.chain)
	if err != nil {
		return result, err
	}
	if actual != nil && actual.Start != s.start {
		return result, domain.ErrCheckpointConflict
	}
	id := e.Header.ID
	var activeHash []byte
	err = tx.QueryRow(ctx, `SELECT block_hash FROM extractor.active_blocks WHERE chain_id=$1 AND block_number=$2::numeric`, string(s.chain), number(id.Number)).Scan(&activeHash)
	if err == nil {
		if len(activeHash) != 32 {
			return result, ErrCorrupt
		}
		if domain.Hash(activeHash) != id.Hash {
			return result, domain.ErrBlockConflict
		}
		if actual == nil || actual.Block.Number < id.Number {
			return result, ErrCorrupt
		}
		stored, err := s.observationExists(ctx, tx, e)
		if err != nil {
			return result, err
		}
		if !stored {
			return result, ErrCorrupt
		}
		// An old duplicate can succeed without mutating either branch or progress.
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
	stored, err := s.observationExists(ctx, tx, e)
	if err != nil {
		return result, err
	}
	if !stored {
		if command.Limits != nil {
			if err := s.checkCapacity(ctx, tx, *command.Limits, payloadBytes(e)); err != nil {
				return result, err
			}
		}
		if err := s.insertObservation(ctx, tx, e); err != nil {
			return result, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO extractor.active_blocks(chain_id,block_number,block_hash) VALUES($1,$2::numeric,$3)`, string(s.chain), number(id.Number), id.Hash[:]); err != nil {
		return result, databaseError(err)
	}
	var changed pgconn.CommandTag
	if actual == nil {
		changed, err = tx.Exec(ctx, `INSERT INTO extractor.extraction_checkpoints(chain_id,block_number,block_hash,start_number) VALUES($1,$2::numeric,$3,$2::numeric) ON CONFLICT(chain_id) DO NOTHING`, string(s.chain), number(id.Number), id.Hash[:])
	} else {
		changed, err = tx.Exec(ctx, `UPDATE extractor.extraction_checkpoints SET block_number=$2::numeric,block_hash=$3,updated_at=clock_timestamp() WHERE chain_id=$1 AND block_number=$4::numeric AND block_hash=$5 AND revision=$6`, string(s.chain), number(id.Number), id.Hash[:], number(actual.Block.Number), actual.Block.Hash[:], actual.Revision)
	}
	if err != nil {
		return result, databaseError(err)
	}
	if changed.RowsAffected() != 1 {
		return result, domain.ErrCheckpointConflict
	}
	if err := commitBranch(ctx, tx); err != nil {
		return result, err
	}
	return outbound.CommitResult{AlreadyStored: stored}, nil
}
