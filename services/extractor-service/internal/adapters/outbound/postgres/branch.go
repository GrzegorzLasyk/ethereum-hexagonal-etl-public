// branch.go implements per-chain transaction coordination, immutable observations and
// revision-fenced rewinds in the outbound adapter layer.

package postgres

import (
	"context"
	"errors"
	"math"
	"strconv"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.BranchStore = (*Store)(nil)

// number formats a uint64 height as decimal text for exact PostgreSQL numeric parameters.
func number(n uint64) string { return strconv.FormatUint(n, 10) }

// beginBranch starts a synchronous local transaction and takes the per-chain advisory lock shared
// by commits and rewinds.
func (s *Store) beginBranch(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, databaseError(err)
	}
	if _, err = tx.Exec(ctx, `SET LOCAL synchronous_commit=on`); err == nil {
		_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(174728,hashtext($1))`, string(s.chain))
	}
	if err != nil {
		deliveryRollback(ctx, tx)
		return nil, databaseError(err)
	}
	return tx, nil
}

// commitBranch commits branch changes and marks uncertain acknowledgements explicitly; callers
// must not assume rollback.
func commitBranch(ctx context.Context, tx pgx.Tx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return databaseError(err)
		}
		return errors.Join(ErrCommitUnknown, databaseError(err))
	}
	return nil
}

// payloadBytes sums retained raw block/receipt lengths for admission accounting, not total decoded
// memory.
func payloadBytes(e domain.Extraction) int64 {
	n := int64(len(e.RawBlock))
	for _, r := range e.RawReceipts {
		n += int64(len(r))
	}
	return n
}

// observationExists checks retained full block identity, parent and Outbox intent without
// replacing the original snapshot.
func (s *Store) observationExists(ctx context.Context, tx pgx.Tx, e domain.Extraction) (bool, error) {
	id := e.Header.ID
	var parent []byte
	var intent bool
	err := tx.QueryRow(ctx, `SELECT parent_hash,EXISTS(SELECT 1 FROM extractor.outbox_events WHERE event_id=$4) FROM extractor.raw_blocks WHERE chain_id=$1 AND block_number=$2::numeric AND block_hash=$3`, string(s.chain), number(id.Number), id.Hash[:], id.EventID()).Scan(&parent, &intent)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, databaseError(err)
	}
	if len(parent) != 32 {
		return false, ErrCorrupt
	}
	if domain.Hash(parent) != e.Header.ParentHash {
		return false, domain.ErrBlockConflict
	}
	if !intent {
		return false, ErrCorrupt
	}
	return true, nil
}

// insertObservation inserts immutable raw data and its matching Outbox intent inside the caller's
// transaction.
func (s *Store) insertObservation(ctx context.Context, tx pgx.Tx, e domain.Extraction) error {
	metadata, at, err := encodeMetadata(e)
	if err != nil {
		return err
	}
	id := e.Header.ID
	_, err = tx.Exec(ctx, `INSERT INTO extractor.raw_blocks(chain_id,block_number,block_hash,parent_hash,raw_block,raw_receipts,metadata,provider,extracted_at) VALUES($1,$2::numeric,$3,$4,$5,$6,$7::jsonb,$8,$9)`, string(s.chain), number(id.Number), id.Hash[:], e.Header.ParentHash[:], e.RawBlock, e.RawReceipts, metadata, e.Provider, at)
	if err != nil {
		return databaseError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO extractor.outbox_events(event_id,chain_id,block_number,block_hash) VALUES($1,$2,$3::numeric,$4)`, id.EventID(), string(s.chain), number(id.Number), id.Hash[:])
	if err != nil {
		return databaseError(err)
	}
	return nil
}

// capacitySQL observes owning-chain outstanding payloads and database-size watermark for
// acquisition admission.
const capacitySQL = `SELECT count(*)::bigint,COALESCE(sum(octet_length(b.raw_block)+COALESCE((SELECT sum(octet_length(r)) FROM unnest(b.raw_receipts) r),0)),0)::bigint,pg_database_size(current_database()) FROM extractor.outbox_events o JOIN extractor.raw_blocks b USING(chain_id,block_number,block_hash) WHERE o.chain_id=$1 AND o.status<>'delivered'`

// readCapacity maps one owning-Outbox/database-size query into admission counters, propagating
// query errors.
func readCapacity(row pgx.Row) (domain.Capacity, error) {
	var c domain.Capacity
	if err := row.Scan(&c.Events, &c.Bytes, &c.DatabaseBytes); err != nil {
		return c, databaseError(err)
	}
	return c, nil
}

// Capacity reads outstanding local work and the database-size watermark; these are observations,
// not disk reservations.
func (s *Store) Capacity(ctx context.Context) (domain.Capacity, error) {
	return readCapacity(s.db.QueryRow(ctx, capacitySQL, string(s.chain)))
}

// checkCapacity rechecks acquisition watermarks inside the serialized write transaction before
// adding a new observation.
func (s *Store) checkCapacity(ctx context.Context, tx pgx.Tx, l domain.CapacityLimits, bytes int64) error {
	c, err := readCapacity(tx.QueryRow(ctx, capacitySQL, string(s.chain)))
	if err != nil {
		return err
	}
	if !c.Allows(l, bytes) {
		return domain.ErrBackpressure
	}
	return nil
}

// ActiveHeader loads the retained active branch identity and parent at a height; missing history
// is an explicit error.
func (s *Store) ActiveHeader(ctx context.Context, n uint64) (domain.Header, error) {
	var h domain.Header
	var hash, parent []byte
	err := s.db.QueryRow(ctx, `SELECT a.block_hash,b.parent_hash FROM extractor.active_blocks a JOIN extractor.raw_blocks b USING(chain_id,block_number,block_hash) WHERE a.chain_id=$1 AND a.block_number=$2::numeric`, string(s.chain), number(n)).Scan(&hash, &parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return h, domain.ErrUnavailable
	}
	if err != nil {
		return h, databaseError(err)
	}
	if len(hash) != 32 || len(parent) != 32 {
		return h, ErrCorrupt
	}
	h.ID = domain.BlockID{ChainID: s.chain, Number: n, Hash: domain.Hash(hash)}
	h.ParentHash = domain.Hash(parent)
	return h, nil
}

// Rewind conditionally audits and moves the active checkpoint to a retained ancestor under its
// revision fence. Historical snapshots and Outbox events remain intact.
func (s *Store) Rewind(ctx context.Context, expected domain.Checkpoint, ancestor domain.BlockID) error {
	if expected.Block.Validate() != nil || ancestor.Validate() != nil || expected.Revision >= math.MaxInt64 {
		return domain.ErrInvalidData
	}
	if expected.Block.ChainID != s.chain || ancestor.ChainID != s.chain {
		return domain.ErrChainMismatch
	}
	if ancestor.Number < s.start || ancestor.Number >= expected.Block.Number || expected.Block.Number-ancestor.Number > 10000 {
		return domain.ErrRecoveryRequired
	}
	tx, err := s.beginBranch(ctx)
	if err != nil {
		return err
	}
	defer deliveryRollback(ctx, tx)
	// Resolve ambiguous COMMIT by the durable original operation, even after later progress.
	var same bool
	err = tx.QueryRow(ctx, `SELECT from_number=$3::numeric AND from_hash=$4 AND to_number=$5::numeric AND to_hash=$6 FROM extractor.rewinds WHERE chain_id=$1 AND previous_revision=$2`, string(s.chain), expected.Revision, number(expected.Block.Number), expected.Block.Hash[:], number(ancestor.Number), ancestor.Hash[:]).Scan(&same)
	if err == nil {
		if same {
			return nil
		}
		return domain.ErrCheckpointConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return databaseError(err)
	}
	actual, err := readCheckpoint(tx.QueryRow(ctx, checkpointSQL, string(s.chain)), s.chain)
	if err != nil {
		return err
	}
	if !sameCheckpoint(actual, &expected) || actual.Start != s.start {
		return domain.ErrCheckpointConflict
	}
	// Require a retained active ancestor and an unbroken local path to the old tip.
	var count uint64
	var linked bool
	err = tx.QueryRow(ctx, `SELECT count(*)::bigint,COALESCE(bool_and(a.block_number=$2::numeric OR COALESCE(b.parent_hash=p.block_hash,false)),false) FROM extractor.active_blocks a JOIN extractor.raw_blocks b USING(chain_id,block_number,block_hash) LEFT JOIN extractor.active_blocks p ON p.chain_id=a.chain_id AND p.block_number=a.block_number-1 WHERE a.chain_id=$1 AND a.block_number BETWEEN $2::numeric AND $3::numeric`, string(s.chain), number(ancestor.Number), number(expected.Block.Number)).Scan(&count, &linked)
	if err != nil {
		return databaseError(err)
	}
	if !linked || count != expected.Block.Number-ancestor.Number+1 {
		return ErrCorrupt
	}
	var hash []byte
	err = tx.QueryRow(ctx, `SELECT block_hash FROM extractor.active_blocks WHERE chain_id=$1 AND block_number=$2::numeric`, string(s.chain), number(ancestor.Number)).Scan(&hash)
	if err != nil {
		return databaseError(err)
	}
	if len(hash) != 32 || domain.Hash(hash) != ancestor.Hash {
		return domain.ErrCheckpointConflict
	}
	_, err = tx.Exec(ctx, `INSERT INTO extractor.rewinds(chain_id,previous_revision,from_number,from_hash,to_number,to_hash) VALUES($1,$2,$3::numeric,$4,$5::numeric,$6)`, string(s.chain), expected.Revision, number(expected.Block.Number), expected.Block.Hash[:], number(ancestor.Number), ancestor.Hash[:])
	if err != nil {
		return databaseError(err)
	}
	_, err = tx.Exec(ctx, `DELETE FROM extractor.active_blocks WHERE chain_id=$1 AND block_number>$2::numeric`, string(s.chain), number(ancestor.Number))
	if err != nil {
		return databaseError(err)
	}
	_, err = tx.Exec(ctx, `UPDATE extractor.extraction_checkpoints SET block_number=$2::numeric,block_hash=$3,revision=revision+1,updated_at=clock_timestamp() WHERE chain_id=$1`, string(s.chain), number(ancestor.Number), ancestor.Hash[:])
	if err != nil {
		return databaseError(err)
	}
	return commitBranch(ctx, tx)
}
