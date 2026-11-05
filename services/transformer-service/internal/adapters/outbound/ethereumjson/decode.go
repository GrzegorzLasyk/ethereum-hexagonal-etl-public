// decode.go implements strict source/document decoding into service-owned domain values in the
// outbound adapter layer.

package ethereumjson

import (
	"context"
	"encoding/json"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/domain"
)

// Decoder implements the application decoding port using adapter-owned JSON representation rules.
type Decoder struct{}

// Decode converts only wire representations; the use case calls domain.Validate.
func (Decoder) Decode(ctx context.Context, block []byte, receipts [][]byte, chain domain.ChainID) (domain.Snapshot, error) {
	s := domain.Snapshot{ChainID: chain}
	o, err := objectOf(ctx, block)
	if err != nil {
		return s, err
	}
	s.Block, err = blockFrom(o)
	if err != nil {
		return s, err
	}
	rawTxs, err := array(o["transactions"])
	if err != nil {
		return s, domain.Invalid("transactions")
	}
	if len(rawTxs) != len(receipts) {
		return s, domain.ErrIncomplete
	}
	s.Transactions = make([]domain.Transaction, 0, len(rawTxs))
	s.Receipts = make([]domain.Receipt, 0, len(receipts))
	for i, raw := range rawTxs {
		if err := ctx.Err(); err != nil {
			return s, err
		}
		txObject, err := plainObject(raw)
		if err != nil {
			return s, err
		}
		tx, err := transactionFrom(txObject)
		if err != nil {
			return s, err
		}
		s.Transactions = append(s.Transactions, tx)
		ro, err := objectOf(ctx, receipts[i])
		if err != nil {
			return s, err
		}
		receipt, err := receiptFrom(ro)
		if err != nil {
			return s, err
		}
		logs, err := array(ro["logs"])
		if err != nil {
			return s, domain.ErrIncomplete
		}
		receipt.Logs = make([]domain.Log, 0, len(logs))
		for _, rawLog := range logs {
			if err := ctx.Err(); err != nil {
				return s, err
			}
			lo, err := plainObject(rawLog)
			if err != nil {
				return s, err
			}
			log, err := logFrom(lo)
			if err != nil {
				return s, err
			}
			receipt.Logs = append(receipt.Logs, log)
		}
		s.Receipts = append(s.Receipts, receipt)
	}
	return s, nil
}

// blockFrom maps a decoded block object into service-owned fields while preserving optional fork-
// era values.
func blockFrom(o object) (domain.Block, error) {
	r := reader{o: o}
	v := domain.Block{
		Number:                read(&r, "number", uintValue),
		Hash:                  read(&r, "hash", hash),
		ParentHash:            read(&r, "parentHash", hash),
		Timestamp:             read(&r, "timestamp", uintValue),
		Nonce:                 read(&r, "nonce", nonce),
		Sha3Uncles:            read(&r, "sha3Uncles", hash),
		LogsBloom:             read(&r, "logsBloom", bloom),
		TransactionsRoot:      read(&r, "transactionsRoot", hash),
		StateRoot:             read(&r, "stateRoot", hash),
		ReceiptsRoot:          read(&r, "receiptsRoot", hash),
		Miner:                 read(&r, "miner", address),
		Difficulty:            read(&r, "difficulty", quantity),
		ExtraData:             read(&r, "extraData", data),
		GasLimit:              read(&r, "gasLimit", uintValue),
		GasUsed:               read(&r, "gasUsed", uintValue),
		MixHash:               read(&r, "mixHash", hash),
		Size:                  read(&r, "size", quantity),
		Uncles:                read(&r, "uncles", hashes),
		TotalDifficulty:       optional(&r, "totalDifficulty", quantity, false),
		BaseFeePerGas:         optional(&r, "baseFeePerGas", quantity, false),
		WithdrawalsRoot:       optional(&r, "withdrawalsRoot", hash, false),
		BlobGasUsed:           optional(&r, "blobGasUsed", uintValue, false),
		ExcessBlobGas:         optional(&r, "excessBlobGas", uintValue, false),
		ParentBeaconBlockRoot: optional(&r, "parentBeaconBlockRoot", hash, false),
		RequestsHash:          optional(&r, "requestsHash", hash, false),
	}
	return v, r.err
}

// transactionFrom maps a transaction object into exact quantities, fixed identities and type-
// dependent optional fields.
func transactionFrom(o object) (domain.Transaction, error) {
	r := reader{o: o}
	v := domain.Transaction{
		Hash:                 read(&r, "hash", hash),
		BlockHash:            read(&r, "blockHash", hash),
		BlockNumber:          read(&r, "blockNumber", uintValue),
		Index:                read(&r, "transactionIndex", uintValue),
		From:                 read(&r, "from", address),
		To:                   optional(&r, "to", address, true),
		Nonce:                read(&r, "nonce", uintValue),
		Value:                read(&r, "value", quantity),
		Gas:                  read(&r, "gas", uintValue),
		Input:                read(&r, "input", data),
		R:                    read(&r, "r", quantity),
		S:                    read(&r, "s", quantity),
		V:                    optional(&r, "v", quantity, false),
		YParity:              optional(&r, "yParity", uintValue, false),
		Type:                 optional(&r, "type", uintValue, false),
		ChainID:              optional(&r, "chainId", chainID, false),
		GasPrice:             optional(&r, "gasPrice", quantity, false),
		MaxFeePerGas:         optional(&r, "maxFeePerGas", quantity, false),
		MaxPriorityFeePerGas: optional(&r, "maxPriorityFeePerGas", quantity, false),
		AccessList:           optional(&r, "accessList", accesses, false),
		MaxFeePerBlobGas:     optional(&r, "maxFeePerBlobGas", quantity, false),
		BlobVersionedHashes:  optional(&r, "blobVersionedHashes", hashes, false),
		AuthorizationList:    optional(&r, "authorizationList", authorizations, false),
	}
	return v, r.err
}

// receiptFrom maps execution outcome, gas and ordered logs while preserving historical root/status
// distinctions.
func receiptFrom(o object) (domain.Receipt, error) {
	r := reader{o: o}
	v := domain.Receipt{
		TransactionHash:   read(&r, "transactionHash", hash),
		Index:             read(&r, "transactionIndex", uintValue),
		BlockHash:         read(&r, "blockHash", hash),
		BlockNumber:       read(&r, "blockNumber", uintValue),
		From:              read(&r, "from", address),
		To:                optional(&r, "to", address, true),
		ContractAddress:   optional(&r, "contractAddress", address, true),
		CumulativeGasUsed: read(&r, "cumulativeGasUsed", uintValue),
		GasUsed:           read(&r, "gasUsed", uintValue),
		LogsBloom:         read(&r, "logsBloom", bloom),
		Type:              optional(&r, "type", uintValue, false),
		EffectiveGasPrice: optional(&r, "effectiveGasPrice", quantity, false),
		BlobGasUsed:       optional(&r, "blobGasUsed", uintValue, false),
		BlobGasPrice:      optional(&r, "blobGasPrice", quantity, false),
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
		BlockHash:        read(&r, "blockHash", hash),
		BlockNumber:      read(&r, "blockNumber", uintValue),
		TransactionHash:  read(&r, "transactionHash", hash),
		TransactionIndex: read(&r, "transactionIndex", uintValue),
		Index:            read(&r, "logIndex", uintValue),
		Address:          read(&r, "address", address),
		Topics:           read(&r, "topics", hashes),
		Data:             read(&r, "data", data),
		Removed:          optional(&r, "removed", boolean, false),
		BlockTimestamp:   optional(&r, "blockTimestamp", uintValue, false),
	}
	return v, r.err
}

// accessFrom decodes one access-list address and storage-key set without changing their order.
func accessFrom(o object) (domain.Access, error) {
	r := reader{o: o}
	v := domain.Access{
		Address:     read(&r, "address", address),
		StorageKeys: read(&r, "storageKeys", hashes),
	}
	return v, r.err
}

// authorizationFrom decodes an authorization tuple with exact integer/signature values and
// required widths.
func authorizationFrom(o object) (domain.Authorization, error) {
	r := reader{o: o}
	v := domain.Authorization{
		ChainID: read(&r, "chainId", quantity),
		Address: read(&r, "address", address),
		Nonce:   read(&r, "nonce", uintValue),
		YParity: read(&r, "yParity", uintValue),
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
