// drain.go implements bounded verification of durable outgoing range delivery in the outbound
// adapter layer.

package postgres

import (
	"context"
	"strconv"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// Compile-time interface assertion checks adapter/interface compatibility at compile time without
// allocating runtime state.
var _ outbound.RangeDeliveryReader = (*Store)(nil)

// RangeDelivery checks active range coverage and durable Outbox delivery together; missing or
// failed work cannot report completion.
func (s *Store) RangeDelivery(ctx context.Context, start, end uint64) (outbound.RangeDelivery, error) {
	if end < start {
		return outbound.RangeDelivery{}, domain.ErrInvalidData
	}
	var r outbound.RangeDelivery
	err := s.db.QueryRow(ctx, `SELECT
 count(DISTINCT r.block_number)=$3::numeric-$2::numeric+1
 AND count(*) FILTER (WHERE o.status IS DISTINCT FROM 'delivered')=0,
 count(*) FILTER (WHERE o.status='failed')>0
 FROM extractor.raw_blocks r LEFT JOIN extractor.outbox_events o ON (o.chain_id,o.block_number,o.block_hash)=(r.chain_id,r.block_number,r.block_hash)
 WHERE r.chain_id=$1 AND r.block_number BETWEEN $2::numeric AND $3::numeric`, string(s.chain), strconv.FormatUint(start, 10), strconv.FormatUint(end, 10)).Scan(&r.Complete, &r.Failed)
	if err != nil {
		return outbound.RangeDelivery{}, databaseError(err)
	}
	return r, nil
}
