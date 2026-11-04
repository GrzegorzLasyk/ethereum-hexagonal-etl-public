// decode_test.go exercises strict source/document decoding into service-owned domain values in the
// outbound adapter layer.

package jsondocument_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

// fixture loads committed deterministic source JSON used for offline decoder and transport tests.
func fixture(t *testing.T) []byte {
	t.Helper()
	b, e := os.ReadFile("testdata/transformed_v1.json")
	if e != nil {
		t.Fatal(e)
	}
	return b
}

// TestTransformerV1Compatibility checks Loader can decode the normalized v1 representation emitted
// by Transformer.
func TestTransformerV1Compatibility(t *testing.T) {
	b := fixture(t)
	original := bytes.Clone(b)
	d, e := (jsondocument.Decoder{}).Decode(t.Context(), b)
	if e != nil {
		t.Fatal(e)
	}
	if e = d.Snapshot.Validate(); e != nil {
		t.Fatal(e)
	}
	s := d.Snapshot
	if s.ChainID != "1" || s.Block.Number != 20000000 || len(s.Transactions) != 2 || len(s.Receipts) != 2 || len(s.Receipts[0].Logs) != 1 {
		t.Fatal("lost normalized entities")
	}
	if s.Transactions[0].Value != "115792089237316195423570985008687907853269984665640564039457584007913129639935" {
		t.Fatal("lost uint256 precision")
	}
	if !s.Block.TotalDifficulty.Null || s.Transactions[1].AccessList.Present || !s.Transactions[1].To.Null || s.Receipts[1].Status.Value != 0 || d.Provenance.ExtractedAt.Nanosecond() != 123 {
		t.Fatal("lost provenance, failed transaction, or optionality")
	}
	if !bytes.Equal(b, original) {
		t.Fatal("borrowed payload mutated")
	}
}

// TestRejectedDocuments checks normalized version/shape/identity failures do not produce a usable
// Loader document.
func TestRejectedDocuments(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"numeric quantity", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"number":"20000000"`), []byte(`"number":20000000`), 1)
		}},
		{"hex quantity", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"number":"20000000"`), []byte(`"number":"0x1312d00"`), 1)
		}},
		{"leading zero", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"number":"20000000"`), []byte(`"number":"020000000"`), 1)
		}},
		{"overflow", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"number":"20000000"`), []byte(`"number":"18446744073709551616"`), 1)
		}},
		{"duplicate key", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"number":`), []byte(`"number":"0","number":`), 1)
		}},
		{"schema", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"schema_version":1`), []byte(`"schema_version":2`), 1)
		}},
		{"missing receipts", func(b []byte) []byte { return bytes.Replace(b, []byte(`"receipts":`), []byte(`"other":`), 1) }},
		{"missing logs", func(b []byte) []byte { return bytes.Replace(b, []byte(`"logs":`), []byte(`"other":`), 1) }},
		{"wrong log index", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"log_index":"0"`), []byte(`"log_index":"1"`), 1)
		}},
		{"removed log", func(b []byte) []byte { return bytes.Replace(b, []byte(`"removed":false`), []byte(`"removed":true`), 1) }},
		{"wrong identity", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"block_number":"20000000"`), []byte(`"block_number":"1"`), 1)
		}},
		{"null document", func([]byte) []byte { return []byte("null") }},
		{"trailing data", func(b []byte) []byte { return append(b, []byte("{}")...) }},
		{"invalid UTF8", func(b []byte) []byte { return append(b, 255) }},
		{"deep nesting", func([]byte) []byte {
			return []byte(`{"x":` + strings.Repeat("[", 70) + "0" + strings.Repeat("]", 70) + "}")
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := fixture(t)
			edited := tt.edit(b)
			if bytes.Equal(b, edited) {
				t.Fatal("test did not modify fixture")
			}
			d, e := (jsondocument.Decoder{}).Decode(t.Context(), edited)
			if e == nil {
				e = d.Snapshot.Validate()
			}
			if e == nil {
				t.Fatal("invalid document accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e := (jsondocument.Decoder{}).Decode(ctx, fixture(t)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

// TestEmptyAndHistoricalDocuments checks explicit empty collections and historical receipt
// representation without inventing modern fields.
func TestEmptyAndHistoricalDocuments(t *testing.T) {
	var doc map[string]json.RawMessage
	if e := json.Unmarshal(fixture(t), &doc); e != nil {
		t.Fatal(e)
	}
	var block map[string]json.RawMessage
	_ = json.Unmarshal(doc["block"], &block)
	block["gas_used"] = json.RawMessage(`"0"`)
	doc["block"], _ = json.Marshal(block)
	doc["transactions"] = json.RawMessage(`[]`)
	doc["receipts"] = json.RawMessage(`[]`)
	doc["logs"] = json.RawMessage(`[]`)
	b, _ := json.Marshal(doc)
	d, e := (jsondocument.Decoder{}).Decode(t.Context(), b)
	if e != nil {
		t.Fatal(e)
	}
	if e = d.Snapshot.Validate(); e != nil {
		t.Fatal(e)
	}
	if d.Snapshot.Transactions == nil || d.Snapshot.Receipts == nil {
		t.Fatal("empty collections became nil")
	}
	// A historical receipt keeps root and absence of status.
	_ = json.Unmarshal(fixture(t), &doc)
	var txs, receipts []map[string]json.RawMessage
	_ = json.Unmarshal(doc["transactions"], &txs)
	_ = json.Unmarshal(doc["receipts"], &receipts)
	for i := range txs {
		txs[i]["type"] = json.RawMessage(`"0"`)
		txs[i]["v"] = json.RawMessage(`"27"`)
		delete(txs[i], "chain_id")
		txs[i]["gas_price"] = json.RawMessage(`"1"`)
		receipts[i]["type"] = json.RawMessage(`"0"`)
		delete(receipts[i], "status")
		receipts[i]["root"] = json.RawMessage(`"0x` + strings.Repeat("ab", 32) + `"`)
	}
	doc["transactions"], _ = json.Marshal(txs)
	doc["receipts"], _ = json.Marshal(receipts)
	b, _ = json.Marshal(doc)
	d, e = (jsondocument.Decoder{}).Decode(t.Context(), b)
	if e != nil {
		t.Fatal(e)
	}
	if e = d.Snapshot.Validate(); e != nil {
		t.Fatal(e)
	}
	if d.Snapshot.Receipts[0].Status.Present || !d.Snapshot.Receipts[0].Root.HasValue() {
		t.Fatal("historical outcome lost")
	}
}

// TestBlobAndAuthorizationFields checks modeled blob/authorization optional fields survive
// document decoding without losing exact values.
func TestBlobAndAuthorizationFields(t *testing.T) {
	for _, kind := range []string{"3", "4"} {
		t.Run(kind, func(t *testing.T) {
			var doc map[string]json.RawMessage
			_ = json.Unmarshal(fixture(t), &doc)
			var txs, receipts []map[string]json.RawMessage
			_ = json.Unmarshal(doc["transactions"], &txs)
			_ = json.Unmarshal(doc["receipts"], &receipts)
			txs[0]["type"] = json.RawMessage(`"` + kind + `"`)
			receipts[0]["type"] = txs[0]["type"]
			txs[0]["access_list"] = json.RawMessage(`[{"address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","storage_keys":["0x` + strings.Repeat("ab", 32) + `"]}]`)
			txs[0]["max_fee_per_blob_gas"] = json.RawMessage(`"18446744073709551616"`)
			txs[0]["blob_versioned_hashes"] = json.RawMessage(`["0x01` + strings.Repeat("ab", 31) + `"]`)
			receipts[0]["blob_gas_used"] = json.RawMessage(`"0"`)
			receipts[0]["blob_gas_price"] = json.RawMessage(`"1"`)
			txs[0]["authorization_list"] = json.RawMessage(`[{"chain_id":"0","address":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","nonce":"2","y_parity":"1","r":"3","s":"4"}]`)
			doc["transactions"], _ = json.Marshal(txs)
			doc["receipts"], _ = json.Marshal(receipts)
			b, _ := json.Marshal(doc)
			d, e := (jsondocument.Decoder{}).Decode(t.Context(), b)
			if e != nil {
				t.Fatal(e)
			}
			if e = d.Snapshot.Validate(); e != nil {
				t.Fatal(e)
			}
			tx := d.Snapshot.Transactions[0]
			if tx.MaxFeePerBlobGas.Value != domain.Quantity("18446744073709551616") || len(tx.AccessList.Value) != 1 || len(tx.BlobVersionedHashes.Value) != 1 || len(tx.AuthorizationList.Value) != 1 || tx.AuthorizationList.Value[0].YParity != 1 {
				t.Fatal("typed fields lost")
			}
		})
	}
}
