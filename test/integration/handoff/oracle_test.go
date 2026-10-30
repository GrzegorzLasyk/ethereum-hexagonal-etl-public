package handoff_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Independent test oracle: compare the approved SQL representation directly to
// original RPC fields, without calling the Transformer to calculate expectations.
type rpcField struct {
	rpc, column, kind string
	optional          bool
}

func fields(spec string, optional bool) []rpcField {
	var out []rpcField
	for _, item := range strings.Fields(spec) {
		p := strings.Split(item, ":")
		out = append(out, rpcField{p[0], p[1], p[2], optional})
	}
	return out
}
func recordFields(table string) []rpcField {
	required, optional := "", ""
	switch table {
	case "blocks":
		required = "number:block_number:q hash:block_hash:b parentHash:parent_hash:b timestamp:timestamp:q nonce:nonce:b sha3Uncles:sha3_uncles:b logsBloom:logs_bloom:b transactionsRoot:transactions_root:b stateRoot:state_root:b receiptsRoot:receipts_root:b miner:miner:b difficulty:difficulty:q extraData:extra_data:b gasLimit:gas_limit:q gasUsed:gas_used:q mixHash:mix_hash:b size:size:q uncles:uncles:a"
		optional = "totalDifficulty:total_difficulty:q baseFeePerGas:base_fee_per_gas:q withdrawalsRoot:withdrawals_root:b blobGasUsed:blob_gas_used:q excessBlobGas:excess_blob_gas:q parentBeaconBlockRoot:parent_beacon_block_root:b requestsHash:requests_hash:b"
	case "transactions":
		required = "hash:transaction_hash:b blockHash:block_hash:b blockNumber:block_number:q transactionIndex:transaction_index:q from:from_address:b nonce:nonce:q value:value:q gas:gas:q input:input_data:b r:r:q s:s:q"
		optional = "to:to_address:b v:v:q yParity:y_parity:q type:type:q chainId:signature_chain_id:s gasPrice:gas_price:q maxFeePerGas:max_fee_per_gas:q maxPriorityFeePerGas:max_priority_fee_per_gas:q accessList:access_list:l maxFeePerBlobGas:max_fee_per_blob_gas:q blobVersionedHashes:blob_versioned_hashes:a authorizationList:authorization_list:u"
	case "receipts":
		required = "transactionHash:transaction_hash:b transactionIndex:transaction_index:q blockHash:block_hash:b blockNumber:block_number:q from:from_address:b cumulativeGasUsed:cumulative_gas_used:q gasUsed:gas_used:q logsBloom:logs_bloom:b"
		optional = "to:to_address:b contractAddress:contract_address:b type:type:q effectiveGasPrice:effective_gas_price:q blobGasUsed:blob_gas_used:q blobGasPrice:blob_gas_price:q status:status:q root:root:b"
	case "logs":
		required = "blockHash:block_hash:b blockNumber:block_number:q transactionHash:transaction_hash:b transactionIndex:transaction_index:q logIndex:log_index:q address:address:b topics:topics:a data:data:b"
		optional = "removed:removed:z blockTimestamp:block_timestamp:q"
	}
	return append(fields(required, false), fields(optional, true)...)
}
func decodeObject(data []byte) (map[string]any, error) {
	var obj map[string]any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	err := d.Decode(&obj)
	return obj, err
}
func sqlValue(value any, kind string) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch kind {
	case "q", "s":
		s, ok := value.(string)
		if !ok || !strings.HasPrefix(s, "0x") {
			return nil, fmt.Errorf("invalid quantity")
		}
		n, ok := new(big.Int).SetString(s[2:], 16)
		if !ok {
			return nil, fmt.Errorf("invalid quantity")
		}
		if kind == "s" {
			return n.String(), nil
		}
		return json.Number(n.String()), nil
	case "b":
		s, ok := value.(string)
		if !ok || !strings.HasPrefix(s, "0x") {
			return nil, fmt.Errorf("invalid bytes")
		}
		return `\x` + strings.ToLower(s[2:]), nil
	case "a":
		a, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("invalid array")
		}
		out := make([]any, 0, len(a))
		for _, v := range a {
			converted, err := sqlValue(v, "b")
			if err != nil {
				return nil, err
			}
			out = append(out, converted)
		}
		return out, nil
	case "l", "u":
		a, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("invalid list")
		}
		out := make([]any, 0, len(a))
		for _, v := range a {
			m, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid list item")
			}
			if kind == "l" {
				out = append(out, map[string]any{"address": m["address"], "storage_keys": m["storageKeys"]})
				continue
			}
			item := map[string]any{"address": m["address"]}
			for src, dst := range map[string]string{"chainId": "chain_id", "nonce": "nonce", "yParity": "y_parity", "r": "r", "s": "s"} {
				n, err := sqlValue(m[src], "s")
				if err != nil {
					return nil, err
				}
				item[dst] = n
			}
			out = append(out, item)
		}
		return out, nil
	case "z":
		if _, ok := value.(bool); !ok {
			return nil, fmt.Errorf("invalid boolean")
		}
		return value, nil
	}
	return nil, fmt.Errorf("unknown conversion")
}
func compareRecord(table string, source, stored map[string]any) error {
	if stored["chain_id"] != "1" {
		return fmt.Errorf("%s chain identity", table)
	}
	present := []string{}
	for _, f := range recordFields(table) {
		value, exists := source[f.rpc]
		if !exists && !f.optional {
			return fmt.Errorf("%s missing required source field %s", table, f.rpc)
		}
		expected, err := sqlValue(value, f.kind)
		if err != nil {
			return fmt.Errorf("%s invalid source field %s", table, f.rpc)
		}
		actual, existsDB := stored[f.column]
		if !existsDB || !reflect.DeepEqual(expected, actual) {
			return fmt.Errorf("%s field mismatch %s", table, f.column)
		}
		if f.optional && exists {
			name := f.column
			if name == "to_address" {
				name = "to"
			}
			if name == "signature_chain_id" {
				name = "chain_id"
			}
			present = append(present, name)
		}
	}
	sort.Strings(present)
	got, ok := stored["present_fields"].([]any)
	if !ok {
		return fmt.Errorf("%s missing presence metadata", table)
	}
	names := make([]string, 0, len(got))
	for _, v := range got {
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("invalid presence metadata")
		}
		names = append(names, s)
	}
	sort.Strings(names)
	if !reflect.DeepEqual(present, names) {
		return fmt.Errorf("%s presence metadata mismatch", table)
	}
	return nil
}
func TestOfflineOracle(t *testing.T) {
	v, err := sqlValue("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "q")
	if err != nil || v != json.Number("115792089237316195423570985008687907853269984665640564039457584007913129639935") {
		t.Fatal("quantity precision lost")
	}
	source := map[string]any{}
	stored := map[string]any{"chain_id": "1", "present_fields": []any{"removed"}}
	for _, f := range recordFields("logs") {
		if f.optional {
			continue
		}
		switch f.kind {
		case "q":
			source[f.rpc] = "0x1"
		case "b":
			source[f.rpc] = "0xab"
		case "a":
			source[f.rpc] = []any{"0xab"}
		}
		stored[f.column], _ = sqlValue(source[f.rpc], f.kind)
	}
	source["removed"] = false
	stored["removed"] = false
	stored["block_timestamp"] = nil
	if err := compareRecord("logs", source, stored); err != nil {
		t.Fatal(err)
	}
	stored["data"] = `\xcd`
	if compareRecord("logs", source, stored) == nil {
		t.Fatal("changed bytes accepted")
	}
	stored["data"] = `\xab`
	stored["present_fields"] = []any{}
	if compareRecord("logs", source, stored) == nil {
		t.Fatal("missing presence accepted")
	}
	if _, err := sqlValue("bad", "q"); err == nil {
		t.Fatal("invalid quantity accepted")
	}
}
