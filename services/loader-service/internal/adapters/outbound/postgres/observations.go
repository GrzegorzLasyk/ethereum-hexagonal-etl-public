// observations.go implements neighbor observation updates without changing canonicality in the
// outbound adapter layer.

package postgres

import (
	"context"
	"strconv"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// updateObservations supplies facts to the domain and persists its decisions.
// Complete holds the chain transaction lock throughout this operation.
func updateObservations(ctx context.Context, tx pgx.Tx, chain domain.ChainID, b domain.Block) error {
	low, high := domain.ObservationRange(b.Number)
	rows, err := tx.Query(ctx, `SELECT block_number::text,block_hash,parent_hash FROM loader.blocks
 WHERE chain_id=$1 AND block_number BETWEEN $2::numeric AND $3::numeric
 ORDER BY block_number,block_hash`, string(chain), strconv.FormatUint(low, 10), strconv.FormatUint(high, 10))
	if err != nil {
		return dbError(err)
	}
	defer rows.Close()
	neighbors := make([]domain.BlockReference, 0)
	for rows.Next() {
		var height string
		var hash, parent []byte
		if err = rows.Scan(&height, &hash, &parent); err != nil {
			return dbError(err)
		}
		number, e := strconv.ParseUint(height, 10, 64)
		if e != nil || len(hash) != 32 || len(parent) != 32 {
			return inbound.ErrUnavailable
		}
		neighbors = append(neighbors, domain.BlockReference{ChainID: chain, Number: number, Hash: domain.Hash(hash), ParentHash: domain.Hash(parent)})
	}
	if err = rows.Err(); err != nil {
		return dbError(err)
	}
	rows.Close()
	arrived := domain.BlockReference{ChainID: chain, Number: b.Number, Hash: b.Hash, ParentHash: b.ParentHash}
	for _, o := range domain.ObserveArrival(arrived, neighbors) {
		// Canonical status deliberately has no runtime UPDATE privilege or transition.
		_, err = tx.Exec(ctx, `UPDATE loader.blocks SET height_conflict=$4,parent_link=$5
 WHERE chain_id=$1 AND block_number=$2::numeric AND block_hash=$3
 AND (height_conflict,parent_link) IS DISTINCT FROM ($4::boolean,$5::text)`,
			string(chain), strconv.FormatUint(o.Block.Number, 10), o.Block.Hash[:], o.HeightConflict, string(o.ParentLink))
		if err != nil {
			return dbError(err)
		}
	}
	return nil
}
