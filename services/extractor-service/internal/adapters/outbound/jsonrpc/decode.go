// decode.go implements strict source/document decoding into service-owned domain values in the
// outbound adapter layer.

package jsonrpc

import (
	"bytes"
	"encoding/json"
	"math/big"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// transactionInfo retains decoded transaction identity and fields required to validate its
// receipt.
type transactionInfo struct {
	meta   domain.Transaction
	from   domain.Address
	to     *domain.Address
	kind   uint64
	gas    uint64
	london bool
}

// decodeBlock validates a mined block and full transaction membership while retaining its original
// JSON result bytes.
func decodeBlock(raw json.RawMessage, chain domain.ChainID, requested uint64) (domain.Extraction, []transactionInfo, uint64, error) {
	var empty domain.Extraction
	o, err := objectOf(raw)
	if err != nil {
		return empty, nil, 0, err
	}
	id, err := blockReference(o, chain, "number", "hash")
	if err != nil {
		return empty, nil, 0, err
	}
	if id.Number != requested {
		return empty, nil, 0, domain.ErrBlockConflict
	}
	if err := o.data(32, "parentHash", "sha3Uncles", "transactionsRoot", "stateRoot", "receiptsRoot", "mixHash"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.data(8, "nonce"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.data(256, "logsBloom"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.data(20, "miner"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.data(-1, "extraData"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.quantities(256, "difficulty", "size"); err != nil {
		return empty, nil, 0, err
	}
	parent, _ := hashField(o, "parentHash")
	timestamp, err := uintField(o, "timestamp")
	if err != nil {
		return empty, nil, 0, err
	}
	gasLimit, err := uintField(o, "gasLimit")
	if err != nil {
		return empty, nil, 0, err
	}
	gasUsed, err := uintField(o, "gasUsed")
	if err != nil || gasUsed > gasLimit {
		return empty, nil, 0, invalid("gasUsed")
	}
	if err := o.optionalQuantities(256, "baseFeePerGas", "totalDifficulty"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.optionalQuantities(64, "blobGasUsed", "excessBlobGas", "slotNumber"); err != nil {
		return empty, nil, 0, err
	}
	if err := o.optionalData(32, "withdrawalsRoot", "parentBeaconBlockRoot", "requestsHash", "blockAccessListHash"); err != nil {
		return empty, nil, 0, err
	}
	if _, err := hashArray(o, "uncles"); err != nil {
		return empty, nil, 0, err
	}
	if err := checkWithdrawals(o); err != nil {
		return empty, nil, 0, err
	}
	_, blobUsed := o["blobGasUsed"]
	_, blobExcess := o["excessBlobGas"]
	if blobUsed != blobExcess {
		return empty, nil, 0, invalid("blob gas fields")
	}
	rawTxs, err := arrayField(o, "transactions")
	if err != nil {
		return empty, nil, 0, err
	}
	extraction := domain.Extraction{
		Header:       domain.Header{ID: id, ParentHash: parent, Timestamp: timestamp},
		Transactions: make([]domain.Transaction, 0, len(rawTxs)),
		Receipts:     make([]domain.Receipt, 0, len(rawTxs)),
		RawBlock:     raw, RawReceipts: make([][]byte, 0, len(rawTxs)),
	}
	_, london := o["baseFeePerGas"]
	txs := make([]transactionInfo, 0, len(rawTxs))
	seen := make(map[domain.Hash]bool, len(rawTxs))
	for i, rawTx := range rawTxs {
		tx, err := decodeTransaction(rawTx, id, uint64(i), london)
		if err != nil {
			return empty, nil, 0, err
		}
		if seen[tx.meta.Hash] {
			return empty, nil, 0, invalid("duplicate transaction")
		}
		if tx.kind == 3 && !blobUsed {
			return empty, nil, 0, invalid("blob gas fields")
		}
		seen[tx.meta.Hash] = true
		extraction.Transactions = append(extraction.Transactions, tx.meta)
		txs = append(txs, tx)
	}
	return extraction, txs, gasUsed, nil
}

// hashArray validates an array of fixed-width hashes without silently accepting null/missing
// arrays.
func hashArray(o object, key string) ([]domain.Hash, error) {
	raw, err := arrayField(o, key)
	if err != nil {
		return nil, err
	}
	result := make([]domain.Hash, 0, len(raw))
	for _, value := range raw {
		hash, err := hashField(object{"hash": value}, "hash")
		if err != nil {
			return nil, invalid(key)
		}
		result = append(result, hash)
	}
	return result, nil
}

// checkWithdrawals validates supplied withdrawal fields without inventing missing fork-era data.
func checkWithdrawals(o object) error {
	_, root := o["withdrawalsRoot"]
	_, list := o["withdrawals"]
	if root != list {
		return invalid("withdrawals fields")
	}
	if !list {
		return nil
	}
	items, err := arrayField(o, "withdrawals")
	if err != nil {
		return err
	}
	for _, item := range items {
		w, err := objectOf(item)
		if err != nil {
			return err
		}
		if err := w.quantities(64, "index", "validatorIndex", "amount"); err != nil {
			return err
		}
		if err := w.data(20, "address"); err != nil {
			return err
		}
	}
	return nil
}

// decodeTransaction parses transaction identity and type-specific fields needed to verify receipt
// membership.
func decodeTransaction(raw json.RawMessage, block domain.BlockID, index uint64, london bool) (transactionInfo, error) {
	var empty transactionInfo
	o, err := objectOf(raw)
	if err != nil {
		return empty, err
	} // Hash-only arrays are not full transactions.
	ref, err := blockReference(o, block.ChainID, "blockNumber", "blockHash")
	if err != nil {
		return empty, err
	}
	if ref != block {
		return empty, domain.ErrBlockConflict
	}
	txIndex, err := uintField(o, "transactionIndex")
	if err != nil || txIndex != index {
		return empty, invalid("transactionIndex")
	}
	hash, err := hashField(o, "hash")
	if err != nil || hash == (domain.Hash{}) {
		return empty, invalid("transaction hash")
	}
	from, err := addressField(o, "from", false)
	if err != nil {
		return empty, err
	}
	to, err := addressField(o, "to", true)
	if err != nil {
		return empty, err
	}
	gas, err := uintField(o, "gas")
	if err != nil {
		return empty, err
	}
	if err := o.quantities(64, "nonce"); err != nil {
		return empty, err
	}
	if err := o.quantities(256, "value", "r", "s"); err != nil {
		return empty, err
	}
	if err := o.data(-1, "input"); err != nil {
		return empty, err
	}
	kind := uint64(0)
	if _, exists := o["type"]; exists {
		kind, err = uintField(o, "type")
		if err != nil || kind > 4 {
			return empty, invalid("unsupported transaction type")
		}
	}
	if err := checkTransactionFields(o, kind, block.ChainID); err != nil {
		return empty, err
	}
	if kind >= 2 && !london {
		return empty, invalid("baseFeePerGas")
	}
	if kind >= 3 && to == nil {
		return empty, invalid("typed transaction recipient")
	}
	return transactionInfo{
		meta: domain.Transaction{Block: ref, Hash: hash, Index: txIndex},
		from: *from, to: to, kind: kind, gas: gas, london: london,
	}, nil
}

// checkTransactionFields requires the fee/signature/access/blob/authorization fields applicable to
// the transaction type.
func checkTransactionFields(o object, kind uint64, chain domain.ChainID) error {
	if kind <= 1 {
		if err := o.quantities(256, "gasPrice"); err != nil {
			return err
		}
	} else {
		fee, err := parseQuantity(o["maxFeePerGas"], 256)
		if err != nil {
			return invalid("maxFeePerGas")
		}
		tip, err := parseQuantity(o["maxPriorityFeePerGas"], 256)
		if err != nil || tip.Cmp(fee) > 0 {
			return invalid("maxPriorityFeePerGas")
		}
	}
	if err := o.optionalQuantities(256, "gasPrice"); err != nil {
		return err
	}
	if kind == 0 {
		v, err := parseQuantity(o["v"], 256)
		if err != nil {
			return invalid("v")
		}
		unprotected := v.Cmp(big.NewInt(27)) == 0 || v.Cmp(big.NewInt(28)) == 0
		if !unprotected {
			if v.Cmp(big.NewInt(35)) < 0 {
				return invalid("v")
			}
			derived := new(big.Int).Sub(v, big.NewInt(35))
			derived.Div(derived, big.NewInt(2))
			if domain.ChainID(derived.String()) != chain {
				return domain.ErrChainMismatch
			}
		}
		if raw, exists := o["chainId"]; exists {
			n, err := parseQuantity(raw, 0)
			if err != nil {
				return invalid("chainId")
			}
			if domain.ChainID(n.String()) != chain && !(unprotected && n.Sign() == 0) {
				return domain.ErrChainMismatch
			}
		}
		return nil
	}
	n, err := parseQuantity(o["chainId"], 0)
	if err != nil {
		return invalid("chainId")
	}
	if domain.ChainID(n.String()) != chain {
		return domain.ErrChainMismatch
	}
	v, hasV := o["v"]
	parity, hasParity := o["yParity"]
	if !hasV && !hasParity {
		return invalid("signature parity")
	}
	for _, value := range []json.RawMessage{v, parity} {
		if value == nil {
			continue
		}
		p, err := parseQuantity(value, 1)
		if err != nil || p.Uint64() > 1 {
			return invalid("signature parity")
		}
	}
	if hasV && hasParity {
		a, _ := parseQuantity(v, 1)
		b, _ := parseQuantity(parity, 1)
		if a.Cmp(b) != 0 {
			return invalid("signature parity")
		}
	}
	items, err := arrayField(o, "accessList")
	if err != nil {
		return err
	}
	for _, item := range items {
		entry, err := objectOf(item)
		if err != nil {
			return err
		}
		if err := entry.data(20, "address"); err != nil {
			return err
		}
		if _, err := hashArray(entry, "storageKeys"); err != nil {
			return err
		}
	}
	if kind == 3 {
		if err := o.quantities(256, "maxFeePerBlobGas"); err != nil {
			return err
		}
		hashes, err := hashArray(o, "blobVersionedHashes")
		if err != nil || len(hashes) == 0 {
			return invalid("blobVersionedHashes")
		}
	}
	if kind == 4 {
		items, err := arrayField(o, "authorizationList")
		if err != nil || len(items) == 0 {
			return invalid("authorizationList")
		}
		for _, item := range items {
			a, err := objectOf(item)
			if err != nil {
				return err
			}
			if err := a.quantities(256, "chainId", "r", "s"); err != nil {
				return err
			}
			if err := a.quantities(64, "nonce"); err != nil {
				return err
			}
			if err := a.quantities(1, "yParity"); err != nil {
				return err
			}
			if err := a.data(20, "address"); err != nil {
				return err
			}
		}
	}
	return nil
}

// decodeReceipt validates receipt identity, index, outcome and logs against its enclosing block
// and transaction.
func decodeReceipt(raw json.RawMessage, header domain.Header, tx transactionInfo) (domain.Receipt, uint64, error) {
	var empty domain.Receipt
	o, err := objectOf(raw)
	if err != nil {
		return empty, 0, err
	}
	block, err := blockReference(o, header.ID.ChainID, "blockNumber", "blockHash")
	if err != nil {
		return empty, 0, err
	}
	if block != header.ID {
		return empty, 0, domain.ErrBlockConflict
	}
	hash, err := hashField(o, "transactionHash")
	if err != nil || hash != tx.meta.Hash {
		return empty, 0, invalid("receipt transactionHash")
	}
	index, err := uintField(o, "transactionIndex")
	if err != nil || index != tx.meta.Index {
		return empty, 0, invalid("receipt transactionIndex")
	}
	from, err := addressField(o, "from", false)
	if err != nil || *from != tx.from {
		return empty, 0, invalid("receipt from")
	}
	to, err := addressField(o, "to", true)
	if err != nil || !sameAddress(to, tx.to) {
		return empty, 0, invalid("receipt to")
	}
	contract, err := addressField(o, "contractAddress", true)
	if err != nil || to != nil && contract != nil {
		return empty, 0, invalid("contractAddress")
	}
	kind := uint64(0)
	if _, exists := o["type"]; exists {
		kind, err = uintField(o, "type")
		if err != nil {
			return empty, 0, err
		}
	}
	if kind != tx.kind {
		return empty, 0, invalid("receipt type")
	}
	gas, err := uintField(o, "gasUsed")
	if err != nil || gas > tx.gas {
		return empty, 0, invalid("receipt gasUsed")
	}
	if err := o.quantities(64, "cumulativeGasUsed"); err != nil {
		return empty, 0, err
	}
	if err := o.data(256, "logsBloom"); err != nil {
		return empty, 0, err
	}
	_, status := o["status"]
	_, root := o["root"]
	if status == root || tx.kind != 0 && !status {
		return empty, 0, invalid("receipt status/root")
	}
	if status {
		if err := o.quantities(1, "status"); err != nil {
			return empty, 0, err
		}
	} else if err := o.data(32, "root"); err != nil {
		return empty, 0, err
	}
	if tx.london {
		if err := o.quantities(256, "effectiveGasPrice"); err != nil {
			return empty, 0, err
		}
	} else if err := o.optionalQuantities(256, "effectiveGasPrice"); err != nil {
		return empty, 0, err
	}
	if tx.kind == 3 {
		if err := o.quantities(64, "blobGasUsed"); err != nil {
			return empty, 0, err
		}
		if err := o.quantities(256, "blobGasPrice"); err != nil {
			return empty, 0, err
		}
	} else {
		if err := o.optionalQuantities(64, "blobGasUsed"); err != nil {
			return empty, 0, err
		}
		if err := o.optionalQuantities(256, "blobGasPrice"); err != nil {
			return empty, 0, err
		}
	}
	rawLogs, err := arrayField(o, "logs")
	if err != nil {
		return empty, 0, err
	}
	logs := make([]domain.Log, 0, len(rawLogs))
	for _, rawLog := range rawLogs {
		log, err := decodeLog(rawLog, header, tx)
		if err != nil {
			return empty, 0, err
		}
		logs = append(logs, log)
	}
	return domain.Receipt{Block: block, TransactionHash: hash, TransactionIndex: index, Logs: logs}, gas, nil
}

// sameAddress compares nullable recipient/address representations without treating null as the
// zero address.
func sameAddress(a, b *domain.Address) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// decodeLog validates log identity, ordering fields and topic/data shapes within the receipt.
func decodeLog(raw json.RawMessage, header domain.Header, tx transactionInfo) (domain.Log, error) {
	var empty domain.Log
	o, err := objectOf(raw)
	if err != nil {
		return empty, err
	}
	block, err := blockReference(o, header.ID.ChainID, "blockNumber", "blockHash")
	if err != nil {
		return empty, err
	}
	if block != header.ID {
		return empty, domain.ErrBlockConflict
	}
	hash, err := hashField(o, "transactionHash")
	if err != nil || hash != tx.meta.Hash {
		return empty, invalid("log transactionHash")
	}
	txIndex, err := uintField(o, "transactionIndex")
	if err != nil || txIndex != tx.meta.Index {
		return empty, invalid("log transactionIndex")
	}
	index, err := uintField(o, "logIndex")
	if err != nil {
		return empty, err
	}
	address, err := addressField(o, "address", false)
	if err != nil {
		return empty, err
	}
	topics, err := hashArray(o, "topics")
	if err != nil {
		return empty, err
	}
	data, err := dataField(o, "data", -1)
	if err != nil {
		return empty, err
	}
	var removed bool
	if value, exists := o["removed"]; exists {
		if bytes.Equal(value, []byte("null")) || json.Unmarshal(value, &removed) != nil {
			return empty, invalid("removed")
		}
	}
	if _, exists := o["blockTimestamp"]; exists {
		timestamp, err := uintField(o, "blockTimestamp")
		if err != nil || timestamp != header.Timestamp {
			return empty, invalid("log blockTimestamp")
		}
	}
	return domain.Log{
		Block: block, TransactionHash: hash, TransactionIndex: txIndex, Index: index,
		Address: *address, Topics: topics, Data: data, Removed: removed,
	}, nil
}
