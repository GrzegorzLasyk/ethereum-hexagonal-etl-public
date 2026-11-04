// reindex.go implements independent historical range cursors and original-event replay in the
// outbound adapter layer.

package postgres

import (
	"context"
	"errors"
	"strconv"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.ReindexStore = (*Store)(nil)

// reindexSQL selects the independent historical range cursor without reading another service
// schema.
const reindexSQL = `SELECT start_number::text,end_number::text,last_number::text,last_hash FROM extractor.reindex_jobs WHERE chain_id=$1 AND operation_id=$2`

// readReindex decodes the persisted independent range cursor, rejecting corrupt or incompatible
// stored identity.
func readReindex(row pgx.Row, chain domain.ChainID, id string) (outbound.ReindexState, error) {
	var state outbound.ReindexState
	var start, end string
	var last *string
	var hash []byte
	if err := row.Scan(&start, &end, &last, &hash); err != nil {
		return state, databaseError(err)
	}
	state.Request.ID = id
	var err error
	state.Request.Start, err = strconv.ParseUint(start, 10, 64)
	if err != nil {
		return state, ErrCorrupt
	}
	state.Request.End, err = strconv.ParseUint(end, 10, 64)
	if err != nil {
		return state, ErrCorrupt
	}
	if last != nil {
		n, err := strconv.ParseUint(*last, 10, 64)
		if err != nil || len(hash) != 32 {
			return state, ErrCorrupt
		}
		state.Last = &domain.BlockID{ChainID: chain, Number: n, Hash: domain.Hash(hash)}
	}
	return state, nil
}

// OpenReindex creates or resumes a stable operation ID and range; conflicting arguments cannot
// reuse the audit identity.
func (s *Store) OpenReindex(ctx context.Context, r outbound.ReindexRequest) (outbound.ReindexState, error) {
	var empty outbound.ReindexState
	if domain.ValidateOperationID(r.ID) != nil || r.End < r.Start {
		return empty, domain.ErrInvalidData
	}
	tx, err := s.beginBranch(ctx)
	if err != nil {
		return empty, err
	}
	defer deliveryRollback(ctx, tx)
	_, err = tx.Exec(ctx, `INSERT INTO extractor.reindex_jobs(chain_id,operation_id,start_number,end_number) VALUES($1,$2,$3::numeric,$4::numeric) ON CONFLICT DO NOTHING`, string(s.chain), r.ID, number(r.Start), number(r.End))
	if err != nil {
		return empty, databaseError(err)
	}
	state, err := readReindex(tx.QueryRow(ctx, reindexSQL, string(s.chain), r.ID), s.chain, r.ID)
	if err != nil {
		return empty, err
	}
	if state.Request != r {
		return empty, domain.ErrCheckpointConflict
	}
	if err = commitBranch(ctx, tx); err != nil {
		return empty, err
	}
	return state, nil
}

// sameID compares optional full block identities without collapsing an absent cursor into genesis.
func sameID(a, b *domain.BlockID) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// CommitReindex atomically retains an observation/Outbox intent and advances only the reindex
// cursor, not the active checkpoint.
func (s *Store) CommitReindex(ctx context.Context, expected outbound.ReindexState, e domain.Extraction, limits domain.CapacityLimits) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.Header.ID.ChainID != s.chain {
		return domain.ErrChainMismatch
	}
	if err := limits.Validate(); err != nil {
		return err
	}
	tx, err := s.beginBranch(ctx)
	if err != nil {
		return err
	}
	defer deliveryRollback(ctx, tx)
	actual, err := readReindex(tx.QueryRow(ctx, reindexSQL, string(s.chain), expected.Request.ID), s.chain, expected.Request.ID)
	if err != nil {
		return err
	}
	if actual.Request != expected.Request {
		return domain.ErrCheckpointConflict
	}
	if !sameID(actual.Last, expected.Last) {
		if actual.Last != nil && *actual.Last == e.Header.ID {
			exists, err := s.observationExists(ctx, tx, e)
			if err != nil {
				return err
			}
			if !exists {
				return ErrCorrupt
			}
			return nil
		}
		return domain.ErrCheckpointConflict
	}
	next := actual.Request.Start
	if actual.Last != nil {
		if actual.Last.Number >= actual.Request.End {
			return domain.ErrOutOfSequence
		}
		next = actual.Last.Number + 1
		if e.Header.ParentHash != actual.Last.Hash {
			return domain.ErrBlockConflict
		}
	}
	if e.Header.ID.Number != next || next > actual.Request.End {
		return domain.ErrOutOfSequence
	}
	exists, err := s.observationExists(ctx, tx, e)
	if err != nil {
		return err
	}
	if !exists {
		if err = s.checkCapacity(ctx, tx, limits, payloadBytes(e)); err != nil {
			return err
		}
		if err = s.insertObservation(ctx, tx, e); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `UPDATE extractor.reindex_jobs SET last_number=$3::numeric,last_hash=$4,updated_at=clock_timestamp() WHERE chain_id=$1 AND operation_id=$2`, string(s.chain), actual.Request.ID, number(next), e.Header.ID.Hash[:])
	if err != nil {
		return databaseError(err)
	}
	return commitBranch(ctx, tx)
}

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.ReplayStore = (*Store)(nil)

// Replay validates a replay request and grants an audited finite Outbox allowance while fencing
// previous claims.
func (s *Store) Replay(ctx context.Context, r outbound.ReplayRequest) error {
	if domain.ValidateOperationID(r.ID) != nil || r.EventID == "" || r.Allowance < 1 || r.Allowance > 1000 {
		return domain.ErrInvalidData
	}
	tx, err := s.beginBranch(ctx)
	if err != nil {
		return err
	}
	defer deliveryRollback(ctx, tx)
	var matches bool
	err = tx.QueryRow(ctx, `SELECT event_id=$3 AND allowance=$4 FROM extractor.replays WHERE chain_id=$1 AND operation_id=$2`, string(s.chain), r.ID, r.EventID, r.Allowance).Scan(&matches)
	if err == nil {
		if matches {
			return nil
		}
		return domain.ErrCheckpointConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return databaseError(err)
	}
	var attempt int
	err = tx.QueryRow(ctx, `UPDATE extractor.outbox_events SET status='pending',attempts=attempts+1,max_attempts=attempts+1+$3,next_attempt_at=clock_timestamp(),lease_until=NULL,delivered_at=NULL,last_error_code=NULL WHERE chain_id=$1 AND event_id=$2 AND attempts<=2147483646-$3 RETURNING attempts`, string(s.chain), r.EventID, r.Allowance).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrUnavailable
	}
	if err != nil {
		return databaseError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO extractor.replays(chain_id,operation_id,event_id,allowance,fenced_attempt) VALUES($1,$2,$3,$4,$5)`, string(s.chain), r.ID, r.EventID, r.Allowance, attempt)
	if err != nil {
		return databaseError(err)
	}
	return commitBranch(ctx, tx)
}
