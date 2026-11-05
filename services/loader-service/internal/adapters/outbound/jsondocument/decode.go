// decode.go implements strict source/document decoding into service-owned domain values in the
// outbound adapter layer.

// Package jsondocument decodes the existing transformed JSON v1 contract.
package jsondocument

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/domain"
)

// Decoder implements the application decoding port using adapter-owned JSON representation rules.
type Decoder struct{}

// Decode checks the normalized JSON version and field shapes, then builds Loader-owned values.
// The application separately validates snapshot invariants and envelope/provenance binding.
func (Decoder) Decode(ctx context.Context, payload []byte) (domain.Document, error) {
	var doc domain.Document
	if len(payload) == 0 || len(payload) > 64<<20 {
		return doc, domain.Invalid("payload size")
	}
	root, err := objectOf(ctx, payload)
	if err != nil {
		return doc, err
	}
	if !bytes.Equal(bytes.TrimSpace(root["schema_version"]), []byte("1")) {
		return doc, domain.ErrUnsupported
	}
	r := reader{o: root}
	doc.Snapshot.ChainID = read(&r, "chain_id", chainID)
	doc.Provenance.SourceEventID = read(&r, "source_event_id", textValue)
	doc.Provenance.Provider = read(&r, "provider", textValue)
	doc.Provenance.ExtractedAt = read(&r, "extracted_at", timestamp)
	if r.err != nil {
		return doc, r.err
	}
	block, err := plainObject(root["block"])
	if err != nil {
		return doc, err
	}
	doc.Snapshot.Block, err = blockFrom(block)
	if err != nil {
		return doc, err
	}
	txs, err := array(root["transactions"])
	if err != nil {
		return doc, domain.ErrIncomplete
	}
	receipts, err := array(root["receipts"])
	if err != nil || len(txs) != len(receipts) {
		return doc, domain.ErrIncomplete
	}
	logs, err := array(root["logs"])
	if err != nil {
		return doc, domain.ErrIncomplete
	}
	doc.Snapshot.Transactions = make([]domain.Transaction, 0, len(txs))
	doc.Snapshot.Receipts = make([]domain.Receipt, 0, len(receipts))
	for i, raw := range txs {
		if err := ctx.Err(); err != nil {
			return doc, err
		}
		o, err := plainObject(raw)
		if err != nil {
			return doc, err
		}
		tx, err := transactionFrom(o)
		if err != nil {
			return doc, err
		}
		doc.Snapshot.Transactions = append(doc.Snapshot.Transactions, tx)
		o, err = plainObject(receipts[i])
		if err != nil {
			return doc, err
		}
		receipt, err := receiptFrom(o)
		if err != nil {
			return doc, err
		}
		receipt.Logs = []domain.Log{}
		doc.Snapshot.Receipts = append(doc.Snapshot.Receipts, receipt)
	}
	var previous uint64
	for i, raw := range logs {
		if err := ctx.Err(); err != nil {
			return doc, err
		}
		o, err := plainObject(raw)
		if err != nil {
			return doc, err
		}
		log, err := logFrom(o)
		if err != nil {
			return doc, err
		}
		if log.TransactionIndex >= uint64(len(receipts)) || log.Index != uint64(i) || log.TransactionIndex < previous {
			return doc, domain.ErrIdentity
		}
		previous = log.TransactionIndex
		receipt := &doc.Snapshot.Receipts[log.TransactionIndex]
		receipt.Logs = append(receipt.Logs, log)
	}
	return doc, nil
}

// textValue reads a required normalized string field; malformed JSON values cannot become empty
// defaults.
func textValue(raw json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(raw, &s) != nil || s == "" {
		return "", domain.Invalid("string")
	}
	return s, nil
}

// timestamp parses the normalized source timestamp and rejects invalid provenance.
func timestamp(raw json.RawMessage) (time.Time, error) {
	s, err := textValue(raw)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.IsZero() {
		return time.Time{}, domain.Invalid("timestamp")
	}
	return t, nil
}

// blockFrom maps a decoded block object into service-owned fields while preserving optional fork-
// era values.
func blockFrom(o object) (domain.Block, error) {
	r := reader{o: o}
	v := domain.Block{
		Number:                read(&r, "number", uintValue),
		Hash:                  read(&r, "hash", hash),
		ParentHash:            read(&r, "parent_hash", hash),
		Timestamp:             read(&r, "timestamp", uintValue),
		Nonce:                 read(&r, "nonce", nonce),
		Sha3Uncles:            read(&r, "sha3_uncles", hash),
		LogsBloom:             read(&r, "logs_bloom", bloom),
		TransactionsRoot:      read(&r, "transactions_root", hash),
		StateRoot:             read(&r, "state_root", hash),
		ReceiptsRoot:          read(&r, "receipts_root", hash),
		Miner:                 read(&r, "miner", address),
		Difficulty:            read(&r, "difficulty", quantity),
		ExtraData:             read(&r, "extra_data", data),
		GasLimit:              read(&r, "gas_limit", uintValue),
		GasUsed:               read(&r, "gas_used", uintValue),
		MixHash:               read(&r, "mix_hash", hash),
		Size:                  read(&r, "size", quantity),
		Uncles:                read(&r, "uncles", hashes),
		TotalDifficulty:       optional(&r, "total_difficulty", quantity, false),
		BaseFeePerGas:         optional(&r, "base_fee_per_gas", quantity, false),
		WithdrawalsRoot:       optional(&r, "withdrawals_root", hash, false),
		BlobGasUsed:           optional(&r, "blob_gas_used", uintValue, false),
		ExcessBlobGas:         optional(&r, "excess_blob_gas", uintValue, false),
		ParentBeaconBlockRoot: optional(&r, "parent_beacon_block_root", hash, false),
		RequestsHash:          optional(&r, "requests_hash", hash, false),
	}
	return v, r.err
}

// transactionFrom maps a transaction object into exact quantities, fixed identities and type-
// dependent optional fields.
func transactionFrom(o object) (domain.Transaction, error) {
	r := reader{o: o}
	v := domain.Transaction{
		Hash:                 read(&r, "hash", hash),
		BlockHash:            read(&r, "block_hash", hash),
		BlockNumber:          read(&r, "block_number", uintValue),
		Index:                read(&r, "transaction_index", uintValue),
		From:                 read(&r, "from", address),
		To:                   optional(&r, "to", address, true),
		Nonce:                read(&r, "nonce", uintValue),
		Value:                read(&r, "value", quantity),
		Gas:                  read(&r, "gas", uintValue),
		Input:                read(&r, "input", data),
		R:                    read(&r, "r", quantity),
		S:                    read(&r, "s", quantity),
		V:                    optional(&r, "v", quantity, false),
		YParity:              optional(&r, "y_parity", uintValue, false),
		Type:                 optional(&r, "type", uintValue, false),
		ChainID:              optional(&r, "chain_id", chainID, false),
		GasPrice:             optional(&r, "gas_price", quantity, false),
		MaxFeePerGas:         optional(&r, "max_fee_per_gas", quantity, false),
		MaxPriorityFeePerGas: optional(&r, "max_priority_fee_per_gas", quantity, false),
		AccessList:           optional(&r, "access_list", accesses, false),
		MaxFeePerBlobGas:     optional(&r, "max_fee_per_blob_gas", quantity, false),
		BlobVersionedHashes:  optional(&r, "blob_versioned_hashes", hashes, false),
		AuthorizationList:    optional(&r, "authorization_list", authorizations, false),
	}
	return v, r.err
}

// receiptFrom maps execution outcome, gas and ordered logs while preserving historical root/status
// distinctions.
func receiptFrom(o object) (domain.Receipt, error) {
	r := reader{o: o}
	v := domain.Receipt{
		TransactionHash:   read(&r, "transaction_hash", hash),
		Index:             read(&r, "transaction_index", uintValue),
		BlockHash:         read(&r, "block_hash", hash),
		BlockNumber:       read(&r, "block_number", uintValue),
		From:              read(&r, "from", address),
		To:                optional(&r, "to", address, true),
		ContractAddress:   optional(&r, "contract_address", address, true),
		CumulativeGasUsed: read(&r, "cumulative_gas_used", uintValue),
		GasUsed:           read(&r, "gas_used", uintValue),
		LogsBloom:         read(&r, "logs_bloom", bloom),
		Type:              optional(&r, "type", uintValue, false),
		EffectiveGasPrice: optional(&r, "effective_gas_price", quantity, false),
		BlobGasUsed:       optional(&r, "blob_gas_used", uintValue, false),
		BlobGasPrice:      optional(&r, "blob_gas_price", quantity, false),
		Status:            optional(&r, "status", uintValue, false),
		Root:              optional(&r, "root", hash, false),
	}
	return v, r.err
}

// logFrom maps block/transaction positions, topics and data for subsequent domain membership
// validation.
func logFrom(o object) (domain.Log, error) {
	r := reader{o: o}
	v := domain.Log{
		BlockHash:        read(&r, "block_hash", hash),
		BlockNumber:      read(&r, "block_number", uintValue),
		TransactionHash:  read(&r, "transaction_hash", hash),
		TransactionIndex: read(&r, "transaction_index", uintValue),
		Index:            read(&r, "log_index", uintValue),
		Address:          read(&r, "address", address),
		Topics:           read(&r, "topics", hashes),
		Data:             read(&r, "data", data),
		Removed:          optional(&r, "removed", boolean, false),
		BlockTimestamp:   optional(&r, "block_timestamp", uintValue, false),
	}
	return v, r.err
}

// accessFrom decodes one access-list address and storage-key set without changing their order.
func accessFrom(o object) (domain.Access, error) {
	r := reader{o: o}
	v := domain.Access{
		Address:     read(&r, "address", address),
		StorageKeys: read(&r, "storage_keys", hashes),
	}
	return v, r.err
}

// authorizationFrom decodes an authorization tuple with exact integer/signature values and
// required widths.
func authorizationFrom(o object) (domain.Authorization, error) {
	r := reader{o: o}
	v := domain.Authorization{
		ChainID: read(&r, "chain_id", quantity),
		Address: read(&r, "address", address),
		Nonce:   read(&r, "nonce", uintValue),
		YParity: read(&r, "y_parity", uintValue),
		R:       read(&r, "r", quantity),
		S:       read(&r, "s", quantity),
	}
	return v, r.err
}

// accesses decodes the ordered access-list collection and rejects malformed elements.
func accesses(raw json.RawMessage) ([]domain.Access, error) {
	items, err := array(raw)
	if err != nil {
		return nil, err
	}
	result := make([]domain.Access, 0, len(items))
	for _, item := range items {
		o, err := plainObject(item)
		if err != nil {
			return nil, err
		}
		v, err := accessFrom(o)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

// authorizations decodes the ordered authorization collection and rejects malformed elements.
func authorizations(raw json.RawMessage) ([]domain.Authorization, error) {
	items, err := array(raw)
	if err != nil {
		return nil, err
	}
	result := make([]domain.Authorization, 0, len(items))
	for _, item := range items {
		o, err := plainObject(item)
		if err != nil {
			return nil, err
		}
		v, err := authorizationFrom(o)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}
