// encode.go implements deterministic versioned normalized JSON encoding with provenance in the
// outbound adapter layer.

// Package jsondocument owns the normalized JSON v1 output DTOs.
package jsondocument

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/transformer-service/internal/domain"
	"strconv"
	"time"
)

// Encoder produces bounded versioned normalized JSON with explicit source provenance.
type Encoder struct{ MaxBytes int }

// New validates the maximum encoded-document budget before constructing the encoder.
func New(maxBytes int) (*Encoder, error) {
	if maxBytes <= 0 || maxBytes > 64<<20 {
		return nil, fmt.Errorf("invalid output byte limit")
	}
	return &Encoder{maxBytes}, nil
}

// Encode serializes a validated snapshot/provenance into deterministic versioned JSON and rejects
// output above the configured bound.
func (e *Encoder) Encode(ctx context.Context, s domain.Snapshot, p domain.Provenance) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if p.SourceEventID == "" || p.Provider == "" || p.ExtractedAt.IsZero() {
		return nil, domain.Invalid("provenance")
	}
	transactions := make([]any, 0, len(s.Transactions))
	receipts := make([]any, 0, len(s.Receipts))
	logs := make([]any, 0)
	for i, t := range s.Transactions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		transactions = append(transactions, transactionObject(t))
		r := s.Receipts[i]
		receipts = append(receipts, receiptObject(r))
		for _, l := range r.Logs {
			logs = append(logs, logObject(l))
		}
	}
	// Maps here are output DTOs; encoding/json sorts their keys. Ordered arrays
	// retain source ordering. No wall clock, random ID or processing attempt enters
	// the document, so retries produce identical bytes.
	document := map[string]any{
		"schema_version": 1, "chain_id": string(s.ChainID), "source_event_id": p.SourceEventID,
		"provider": p.Provider, "extracted_at": p.ExtractedAt.UTC().Format(time.RFC3339Nano),
		"block": blockObject(s.Block), "transactions": transactions, "receipts": receipts, "logs": logs,
	}
	data, err := json.Marshal(document)
	if err != nil {
		return nil, domain.Invalid("document encoding")
	}
	if len(data) > e.MaxBytes {
		return nil, outbound.ErrOutputTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

// hexData encodes bytes as prefixed hex for JSON fields that preserve Ethereum data
// representation.
func hexData(b []byte) string { return "0x" + hex.EncodeToString(b) }

// hashes encodes the ordered hash values without changing their source order.
func hashes(v []domain.Hash) any {
	r := make([]string, 0, len(v))
	for _, h := range v {
		r = append(r, hexData(h[:]))
	}
	return r
}

// accesses encodes the ordered access-list values without changing their source order.
func accesses(v []domain.Access) any {
	r := make([]any, 0, len(v))
	for _, a := range v {
		r = append(r, accessObject(a))
	}
	return r
}

// authorizations encodes the ordered authorization values without changing their source order.
func authorizations(v []domain.Authorization) any {
	r := make([]any, 0, len(v))
	for _, a := range v {
		r = append(r, authorizationObject(a))
	}
	return r
}

// put adds only supplied optional fields to the encoded document, retaining explicit null when
// required.
func put[T any](o map[string]any, key string, v domain.Optional[T], convert func(T) any) {
	if !v.Present {
		return
	}
	if v.Null {
		o[key] = nil
		return
	}
	o[key] = convert(v.Value)
}

// blockObject builds the normalized block JSON projection with exact quantities and modeled
// optional fields.
func blockObject(v domain.Block) map[string]any {
	o := map[string]any{}
	o["number"] = strconv.FormatUint(v.Number, 10)
	o["hash"] = hexData(v.Hash[:])
	o["parent_hash"] = hexData(v.ParentHash[:])
	o["timestamp"] = strconv.FormatUint(v.Timestamp, 10)
	o["nonce"] = hexData(v.Nonce[:])
	o["sha3_uncles"] = hexData(v.Sha3Uncles[:])
	o["logs_bloom"] = hexData(v.LogsBloom[:])
	o["transactions_root"] = hexData(v.TransactionsRoot[:])
	o["state_root"] = hexData(v.StateRoot[:])
	o["receipts_root"] = hexData(v.ReceiptsRoot[:])
	o["miner"] = hexData(v.Miner[:])
	o["difficulty"] = string(v.Difficulty)
	o["extra_data"] = hexData(v.ExtraData)
	o["gas_limit"] = strconv.FormatUint(v.GasLimit, 10)
	o["gas_used"] = strconv.FormatUint(v.GasUsed, 10)
	o["mix_hash"] = hexData(v.MixHash[:])
	o["size"] = string(v.Size)
	o["uncles"] = hashes(v.Uncles)
	put(o, "total_difficulty", v.TotalDifficulty, func(x domain.Quantity) any { return string(x) })
	put(o, "base_fee_per_gas", v.BaseFeePerGas, func(x domain.Quantity) any { return string(x) })
	put(o, "withdrawals_root", v.WithdrawalsRoot, func(x domain.Hash) any { return hexData(x[:]) })
	put(o, "blob_gas_used", v.BlobGasUsed, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "excess_blob_gas", v.ExcessBlobGas, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "parent_beacon_block_root", v.ParentBeaconBlockRoot, func(x domain.Hash) any { return hexData(x[:]) })
	put(o, "requests_hash", v.RequestsHash, func(x domain.Hash) any { return hexData(x[:]) })
	return o
}

// transactionObject builds the normalized transaction projection, preserving type-specific fields
// and recipient nullability.
func transactionObject(v domain.Transaction) map[string]any {
	o := map[string]any{}
	o["hash"] = hexData(v.Hash[:])
	o["block_hash"] = hexData(v.BlockHash[:])
	o["block_number"] = strconv.FormatUint(v.BlockNumber, 10)
	o["transaction_index"] = strconv.FormatUint(v.Index, 10)
	o["from"] = hexData(v.From[:])
	put(o, "to", v.To, func(x domain.Address) any { return hexData(x[:]) })
	o["nonce"] = strconv.FormatUint(v.Nonce, 10)
	o["value"] = string(v.Value)
	o["gas"] = strconv.FormatUint(v.Gas, 10)
	o["input"] = hexData(v.Input)
	o["r"] = string(v.R)
	o["s"] = string(v.S)
	put(o, "v", v.V, func(x domain.Quantity) any { return string(x) })
	put(o, "y_parity", v.YParity, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "type", v.Type, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "chain_id", v.ChainID, func(x domain.ChainID) any { return string(x) })
	put(o, "gas_price", v.GasPrice, func(x domain.Quantity) any { return string(x) })
	put(o, "max_fee_per_gas", v.MaxFeePerGas, func(x domain.Quantity) any { return string(x) })
	put(o, "max_priority_fee_per_gas", v.MaxPriorityFeePerGas, func(x domain.Quantity) any { return string(x) })
	put(o, "access_list", v.AccessList, func(x []domain.Access) any { return accesses(x) })
	put(o, "max_fee_per_blob_gas", v.MaxFeePerBlobGas, func(x domain.Quantity) any { return string(x) })
	put(o, "blob_versioned_hashes", v.BlobVersionedHashes, func(x []domain.Hash) any { return hashes(x) })
	put(o, "authorization_list", v.AuthorizationList, func(x []domain.Authorization) any { return authorizations(x) })
	return o
}

// receiptObject builds the normalized receipt projection, including historical roots, outcome and
// ordered logs.
func receiptObject(v domain.Receipt) map[string]any {
	o := map[string]any{}
	o["transaction_hash"] = hexData(v.TransactionHash[:])
	o["transaction_index"] = strconv.FormatUint(v.Index, 10)
	o["block_hash"] = hexData(v.BlockHash[:])
	o["block_number"] = strconv.FormatUint(v.BlockNumber, 10)
	o["from"] = hexData(v.From[:])
	put(o, "to", v.To, func(x domain.Address) any { return hexData(x[:]) })
	put(o, "contract_address", v.ContractAddress, func(x domain.Address) any { return hexData(x[:]) })
	o["cumulative_gas_used"] = strconv.FormatUint(v.CumulativeGasUsed, 10)
	o["gas_used"] = strconv.FormatUint(v.GasUsed, 10)
	o["logs_bloom"] = hexData(v.LogsBloom[:])
	put(o, "type", v.Type, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "effective_gas_price", v.EffectiveGasPrice, func(x domain.Quantity) any { return string(x) })
	put(o, "blob_gas_used", v.BlobGasUsed, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "blob_gas_price", v.BlobGasPrice, func(x domain.Quantity) any { return string(x) })
	put(o, "status", v.Status, func(x uint64) any { return strconv.FormatUint(x, 10) })
	put(o, "root", v.Root, func(x domain.Hash) any { return hexData(x[:]) })
	return o
}

// logObject builds a normalized log object with original identity, ordered topics and exact data.
func logObject(v domain.Log) map[string]any {
	o := map[string]any{}
	o["block_hash"] = hexData(v.BlockHash[:])
	o["block_number"] = strconv.FormatUint(v.BlockNumber, 10)
	o["transaction_hash"] = hexData(v.TransactionHash[:])
	o["transaction_index"] = strconv.FormatUint(v.TransactionIndex, 10)
	o["log_index"] = strconv.FormatUint(v.Index, 10)
	o["address"] = hexData(v.Address[:])
	o["topics"] = hashes(v.Topics)
	o["data"] = hexData(v.Data)
	put(o, "removed", v.Removed, func(x bool) any { return x })
	put(o, "block_timestamp", v.BlockTimestamp, func(x uint64) any { return strconv.FormatUint(x, 10) })
	return o
}

// accessObject builds the normalized access-list entry without sorting or losing storage keys.
func accessObject(v domain.Access) map[string]any {
	o := map[string]any{}
	o["address"] = hexData(v.Address[:])
	o["storage_keys"] = hashes(v.StorageKeys)
	return o
}

// authorizationObject builds a normalized authorization entry with exact chain and signature
// values.
func authorizationObject(v domain.Authorization) map[string]any {
	o := map[string]any{}
	o["chain_id"] = string(v.ChainID)
	o["address"] = hexData(v.Address[:])
	o["nonce"] = strconv.FormatUint(v.Nonce, 10)
	o["y_parity"] = strconv.FormatUint(v.YParity, 10)
	o["r"] = string(v.R)
	o["s"] = string(v.S)
	return o
}
