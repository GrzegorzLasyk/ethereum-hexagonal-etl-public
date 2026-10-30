package postgres

import (
	"context"
	"errors"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/jackc/pgx/v5"
)

var _ outbound.RepairStore = (*Store)(nil)

func (s *Store) Repair(ctx context.Context, r outbound.RepairRequest) error {
	if err := r.Validate(string(s.chain)); err != nil {
		return err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	// Same operation ID is serialized, including retries after ambiguous COMMIT.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,135))", "loader/repair/"+string(s.chain)+"/"+r.OperationID); err != nil {
		return dbError(err)
	}
	var matches bool
	err = tx.QueryRow(ctx, `SELECT queue=$3 AND event_id=$4 AND allowance=$5 FROM loader.recovery_operations WHERE chain_id=$1 AND operation_id=$2`, string(s.chain), r.OperationID, r.Queue, r.EventID, r.Allowance).Scan(&matches)
	if err == nil {
		if matches {
			return nil
		}
		return outbound.ErrRepairRefused
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return dbError(err)
	}
	table, terminal, ownership := "loader.inbox_events", "completed_at", "chain_id=$1"

	var attempt int
	err = tx.QueryRow(ctx, "UPDATE "+table+" SET status='pending',attempts=attempts+1,max_attempts=attempts+1+$3,next_attempt_at=clock_timestamp(),lease_until=NULL,"+terminal+"=NULL,last_error_code=NULL WHERE "+ownership+" AND event_id=$2 AND status='failed' AND attempts<=2147483646-$3 RETURNING attempts", string(s.chain), r.EventID, r.Allowance).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return outbound.ErrRepairRefused
	}
	if err != nil {
		return dbError(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO loader.recovery_operations(chain_id,operation_id,queue,event_id,allowance,fenced_attempt) VALUES($1,$2,$3,$4,$5,$6)`, string(s.chain), r.OperationID, r.Queue, r.EventID, r.Allowance, attempt)
	if err != nil {
		return dbError(err)
	}
	return commit(ctx, tx)
}
