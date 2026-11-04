// chain.go constructs explicitly synthetic deterministic Ethereum RPC blocks and receipts for
// local tests; it contains no authentic Mainnet captures.

// Package fixture supplies a synthetic, bounded chain for local integration tests.
// Identities are deterministic labels, not cryptographically valid Ethereum hashes.
package fixture

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
)

// Start is the first synthetic fixture height, not a Mainnet acquisition default.
const Start uint64 = 100

// Last bounds the deterministic synthetic fixture chain used by offline scenarios.
const Last uint64 = 110

// Hash derives a deterministic synthetic block hash from its height and branch; it is not a
// cryptographic Ethereum header hash.
func Hash(n uint64, fork bool) string {
	branch := "a"
	if fork && n >= 102 {
		branch = "b"
	}
	return digest(fmt.Sprintf("stage11-block-%s-%d", branch, n))
}

// TxHash derives a deterministic synthetic transaction identity from block position and branch.
func TxHash(n uint64, fork bool, index int) string {
	return digest(fmt.Sprintf("%s-tx-%d", Hash(n, fork), index))
}

// digest hashes the fixture seed into a stable 32-byte identity for reproducible tests.
func digest(s string) string { return fmt.Sprintf("0x%x", sha256.Sum256([]byte(s))) }

// quantity formats a deterministic synthetic integer as an Ethereum RPC hex quantity.
func quantity(n uint64) string { return fmt.Sprintf("0x%x", n) }

// Block generates two transactions, two receipts and one log per observation.
// The first value is deliberately above the exact integer range of float64.
func Block(n uint64, fork bool, full bool) (json.RawMessage, []json.RawMessage) {
	hash, number := Hash(n, fork), quantity(n)
	zeroBloom := "0x" + strings.Repeat("00", 256)
	from, to := "0x"+strings.Repeat("aa", 20), "0x"+strings.Repeat("bb", 20)
	transactions := make([]any, 0, 2)
	receipts := make([]json.RawMessage, 0, 2)
	for i := 0; i < 2; i++ {
		tx := map[string]any{"blockHash": hash, "blockNumber": number, "hash": TxHash(n, fork, i), "transactionIndex": quantity(uint64(i)), "from": from, "to": to, "nonce": quantity(n*2 + uint64(i)), "value": "0x20000000000001", "input": "0x", "r": "0x1", "s": "0x2", "gas": "0x186a0", "type": "0x0", "gasPrice": "0x77359400", "v": "0x25"}
		logs := []any{}
		gas, cumulative, status := "0x61a8", "0x61a8", "0x1"
		if i == 0 {
			logs = append(logs, map[string]any{"blockHash": hash, "blockNumber": number, "transactionHash": TxHash(n, fork, i), "transactionIndex": "0x0", "logIndex": "0x0", "address": "0x" + strings.Repeat("dd", 20), "topics": []string{digest("stage11-topic")}, "data": "0x000102", "removed": false})
		} else {
			tx["to"] = nil
			tx["value"] = "0x0"
			gas = "0x5208"
			cumulative = "0xb3b0"
			status = "0x0"
		}
		if full {
			transactions = append(transactions, tx)
		} else {
			transactions = append(transactions, tx["hash"])
		}
		r := map[string]any{"blockHash": hash, "blockNumber": number, "transactionHash": tx["hash"], "transactionIndex": tx["transactionIndex"], "from": from, "to": tx["to"], "contractAddress": nil, "logsBloom": zeroBloom, "effectiveGasPrice": "0x77359400", "type": "0x0", "gasUsed": gas, "cumulativeGasUsed": cumulative, "status": status, "logs": logs, "providerExtension": "retained fixture receipt"}
		encoded, _ := json.Marshal(r)
		receipts = append(receipts, encoded)
	}
	b := map[string]any{"number": number, "hash": hash, "parentHash": Hash(n-1, fork), "timestamp": quantity(1700000000 + n), "nonce": "0x0000000000000000", "sha3Uncles": digest("uncles"), "logsBloom": zeroBloom, "transactionsRoot": digest("transactions"), "stateRoot": digest("state"), "receiptsRoot": digest("receipts"), "miner": "0x" + strings.Repeat("cc", 20), "difficulty": "0x0", "extraData": "0x1234", "gasLimit": "0x1c9c380", "gasUsed": "0xb3b0", "mixHash": digest("mix"), "size": "0x1000", "transactions": transactions, "uncles": []string{}, "baseFeePerGas": "0x3b9aca00", "futureExtension": map[string]bool{"retained": true}}
	encoded, _ := json.Marshal(b)
	return encoded, receipts
}
