// admission.go implements bounded durable or in-flight acceptance and resource accounting in the
// outbound adapter layer.

package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
)

// A single MVCC snapshot observes Inbox -> Outbox transfer without a gap.
func (s *Store) admissionFull(ctx context.Context, tx pgx.Tx) (bool, error) {
	var full bool
	err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM (SELECT event_id FROM transformer.inbox_events WHERE status<>'completed' ORDER BY event_id LIMIT $1) i) + (SELECT count(*) FROM (SELECT event_id FROM transformer.outbox_events WHERE status<>'delivered' ORDER BY event_id LIMIT $1) o) >= $1`, s.capacity).Scan(&full)
	if err != nil {
		return false, dbError(err)
	}
	return full, nil
}
