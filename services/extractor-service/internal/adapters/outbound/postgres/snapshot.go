// snapshot.go implements versioned storage metadata and exact raw snapshot reconstruction in the
// outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// metadataV1 is a private storage format, not an inter-service event contract.
// Raw source bytes are separate bytea columns, never normalized through jsonb.
type metadataV1 struct {
	Version      int                  `json:"version"`
	Header       domain.Header        `json:"header"`
	Transactions []domain.Transaction `json:"transactions"`
	Receipts     []domain.Receipt     `json:"receipts"`
}

// encodeMetadata serializes versioned extraction metadata and nanosecond provenance independently
// of unchanged raw JSON bytes.
func encodeMetadata(e domain.Extraction) ([]byte, string, error) {
	// time.Time.MarshalText validates the RFC3339 year range and retains nanos.
	timestamp, err := e.ExtractedAt.MarshalText()
	if err != nil {
		return nil, "", domain.ErrInvalidData
	}
	b, err := json.Marshal(metadataV1{1, e.Header, e.Transactions, e.Receipts})
	if err != nil {
		return nil, "", domain.ErrInvalidData
	}
	return b, string(timestamp), nil
}

// decodeSnapshot reconstructs retained metadata and raw bytes, rejecting corrupt storage rather
// than returning a partial snapshot.
func decodeSnapshot(metadata, raw []byte, receipts [][]byte, provider, extractedAt string) (domain.Extraction, error) {
	var m metadataV1
	if json.Unmarshal(metadata, &m) != nil || m.Version != 1 {
		return domain.Extraction{}, ErrCorrupt
	}
	timestamp, err := time.Parse(time.RFC3339Nano, extractedAt)
	if err != nil {
		return domain.Extraction{}, ErrCorrupt
	}
	e := domain.Extraction{Header: m.Header, Transactions: m.Transactions, Receipts: m.Receipts,
		RawBlock: raw, RawReceipts: receipts, Provider: provider, ExtractedAt: timestamp}
	if e.Validate() != nil {
		return domain.Extraction{}, ErrCorrupt
	}
	return e, nil
}

// Load retrieves an immutable local snapshot by full identity. It is an adapter
// read facility, not a new application port or permission for cross-schema reads.
func (s *Store) Load(ctx context.Context, id domain.BlockID) (domain.Extraction, error) {
	var empty domain.Extraction
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := id.Validate(); err != nil {
		return empty, err
	}
	if id.ChainID != s.chain {
		return empty, domain.ErrChainMismatch
	}
	var metadata, raw, parent []byte
	var receipts [][]byte
	var provider, extractedAt string
	err := s.db.QueryRow(ctx, `SELECT metadata,raw_block,raw_receipts,provider,extracted_at,parent_hash
FROM extractor.raw_blocks WHERE chain_id=$1 AND block_number=$2::numeric AND block_hash=$3`,
		string(id.ChainID), strconv.FormatUint(id.Number, 10), id.Hash[:]).Scan(&metadata, &raw, &receipts, &provider, &extractedAt, &parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return empty, ErrNotFound
	}
	if err != nil {
		return empty, databaseError(err)
	}
	e, err := decodeSnapshot(metadata, raw, receipts, provider, extractedAt)
	if err != nil {
		return empty, err
	}
	if e.Header.ID != id || !bytes.Equal(e.Header.ParentHash[:], parent) {
		return empty, ErrCorrupt
	}
	return e, nil
}
