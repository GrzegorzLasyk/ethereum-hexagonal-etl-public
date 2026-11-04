// admission.go implements bounded durable or in-flight acceptance and resource accounting in the
// outbound adapter layer.

package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
)

// admissionFull counts at most the capacity limit of unfinished Loader Inbox rows.
// The caller holds the admission lock; completed history does not consume capacity.
func (s *Store) admissionFull(ctx context.Context, tx pgx.Tx) (bool, error) {
	var full bool
	err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM (SELECT event_id FROM loader.inbox_events WHERE status<>'completed' ORDER BY event_id LIMIT $1) i) >= $1`, s.capacity).Scan(&full)
	if err != nil {
		return false, dbError(err)
	}
	return full, nil
}
