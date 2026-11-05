// decode_test.go exercises strict source/document decoding into service-owned domain values in the
// outbound adapter layer.

package ethereumjson_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/outbound/ethereumjson"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/adapters/outbound/jsondocument"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/domain"
)

// fixtures loads committed deterministic source JSON used for offline decoder and transport tests.
func fixtures(t *testing.T) ([]byte, [][]byte) {
	t.Helper()
	read := func(name string) []byte {
		b, e := os.ReadFile("testdata/" + name)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	return read("block.json"), [][]byte{read("receipt_0.json"), read("receipt_1.json")}
}

// TestDeterministicExactOutput compares repeat normalized output and exact value/optional-field
// representation from controlled source JSON.
func TestDeterministicExactOutput(t *testing.T) {
	b, r := fixtures(t)
	b = bytes.Replace(b, []byte(`"value": "0x0"`), []byte(`"value": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"`), 1)
	b = bytes.Replace(b, []byte(`"totalDifficulty"`), []byte(`"unused"`), 1)
	original := bytes.Clone(b)
	s, err := (ethereumjson.Decoder{}).Decode(t.Context(), b, r, "1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	e, err := jsondocument.New(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	provenance := domain.Provenance{SourceEventID: "raw-block/v1/1/20000000/fixture", Provider: "fixture", ExtractedAt: time.Unix(100, 123)}
	first, err := e.Encode(t.Context(), s, provenance)
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Encode(t.Context(), s, provenance)
	if err != nil || !bytes.Equal(first, second) || !bytes.Equal(b, original) {
		t.Fatal("nondeterministic output or mutated source", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(first, &doc); err != nil {
		t.Fatal(err)
	}
	if string(doc["schema_version"]) != "1" {
		t.Fatal("schema version")
	}
	var txs []map[string]json.RawMessage
	_ = json.Unmarshal(doc["transactions"], &txs)
	if string(txs[0]["value"]) != `"115792089237316195423570985008687907853269984665640564039457584007913129639935"` {
		t.Fatal("lost numeric precision")
	}
	if string(txs[1]["to"]) != "null" {
		t.Fatal("contract creation changed")
	}
	if _, ok := txs[1]["access_list"]; ok {
		t.Fatal("absent field invented")
	}
	var receipts []map[string]json.RawMessage
	_ = json.Unmarshal(doc["receipts"], &receipts)
	if string(receipts[1]["status"]) != `"0"` {
		t.Fatal("failed transaction discarded")
	}
	if string(doc["logs"]) == "[]" {
		t.Fatal("lost logs")
	}
	small, _ := jsondocument.New(len(first) - 1)
	if _, err := small.Encode(t.Context(), s, provenance); !errors.Is(err, outbound.ErrOutputTooLarge) {
		t.Fatal(err)
	}
	exact, _ := jsondocument.New(len(first))
	if _, err := exact.Encode(t.Context(), s, provenance); err != nil {
		t.Fatal(err)
	}
}

// TestMalformedAndIncompleteInput mutates JSON shapes/required fields and rejects incomplete
// source rather than normalizing invented defaults.
func TestMalformedAndIncompleteInput(t *testing.T) {
	tests := []struct {
		name   string
		change func([]byte, [][]byte) ([]byte, [][]byte)
	}{
		{"missing receipt", func(b []byte, r [][]byte) ([]byte, [][]byte) { return b, r[:1] }},
		{"missing transactions", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			return bytes.Replace(b, []byte(`"transactions":`), []byte(`"other":`), 1), r
		}},
		{"duplicate field", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			return bytes.Replace(b, []byte(`"number":`), []byte(`"number":"0x1","number":`), 1), r
		}},
		{"hex leading zero", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			return bytes.Replace(b, []byte(`"0x1312d00"`), []byte(`"0x01312d00"`), 1), r
		}},
		{"height overflow", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			return bytes.Replace(b, []byte(`"0x1312d00"`), []byte(`"0x10000000000000000"`), 1), r
		}},
		{"wrong receipt hash", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			r[0] = bytes.ReplaceAll(r[0], []byte("202020"), []byte("909090"))
			return b, r
		}},
		{"missing logs", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			r[0] = bytes.Replace(r[0], []byte(`"logs":`), []byte(`"other":`), 1)
			return b, r
		}},
		{"removed log", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			r[0] = bytes.Replace(r[0], []byte("false"), []byte("true"), 1)
			return b, r
		}},
		{"null receipt", func(b []byte, r [][]byte) ([]byte, [][]byte) { r[0] = []byte("null"); return b, r }},
		{"trailing JSON", func(b []byte, r [][]byte) ([]byte, [][]byte) { return append(b, []byte("{}")...), r }},
		{"wrong JSON primitive", func(b []byte, r [][]byte) ([]byte, [][]byte) { return []byte("42"), r }},
		{"invalid UTF8", func(b []byte, r [][]byte) ([]byte, [][]byte) { return append(b, 0xff), r }},
		{"deep JSON", func(b []byte, r [][]byte) ([]byte, [][]byte) {
			return []byte(`{"x":` + strings.Repeat("[", 70) + "0" + strings.Repeat("]", 70) + "}"), r
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, r := fixtures(t)
			b, r = tt.change(b, r)
			s, err := (ethereumjson.Decoder{}).Decode(t.Context(), b, r, "1")
			if err == nil {
				err = s.Validate()
			}
			if err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b, r := fixtures(t)
	if _, err := (ethereumjson.Decoder{}).Decode(ctx, b, r, "1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestHistoricalAndOptionalValues checks historical receipt roots and absent/null/value
// distinctions in normalized output.
func TestHistoricalAndOptionalValues(t *testing.T) {
	b, r := fixtures(t)
	var block map[string]json.RawMessage
	_ = json.Unmarshal(b, &block)
	var txs []map[string]json.RawMessage
	_ = json.Unmarshal(block["transactions"], &txs)
	for _, tx := range txs {
		tx["type"] = json.RawMessage(`"0x0"`)
		tx["v"] = json.RawMessage(`"0x1b"`)
		tx["gasPrice"] = json.RawMessage(`"0x1"`)
		delete(tx, "chainId")
	}
	block["transactions"], _ = json.Marshal(txs)
	delete(block, "baseFeePerGas")
	block["totalDifficulty"] = json.RawMessage("null")
	b, _ = json.Marshal(block)
	for i := range r {
		var o map[string]json.RawMessage
		_ = json.Unmarshal(r[i], &o)
		o["type"] = json.RawMessage(`"0x0"`)
		delete(o, "status")
		o["root"] = json.RawMessage(`"0x` + strings.Repeat("1", 64) + `"`)
		r[i], _ = json.Marshal(o)
	}
	s, err := (ethereumjson.Decoder{}).Decode(t.Context(), b, r, "1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if !s.Block.TotalDifficulty.Null || !s.Receipts[0].Root.HasValue() || s.Receipts[0].Status.Present {
		t.Fatal("historical/null distinction lost")
	}
	e, _ := jsondocument.New(64 << 20)
	encoded, err := e.Encode(t.Context(), s, domain.Provenance{SourceEventID: "fixture", Provider: "fixture", ExtractedAt: time.Unix(100, 123)})
	if err != nil || !bytes.Contains(encoded, []byte(`"total_difficulty":null`)) || !bytes.Contains(encoded, []byte(`"root":"0x`)) || bytes.Contains(encoded, []byte(`"status":`)) {
		t.Fatal("historical fields changed in output", err)
	}
}

// TestTypedTransactionNormalization checks the type-dependent fee, access, blob and authorization
// representations against deterministic source cases.
func TestTypedTransactionNormalization(t *testing.T) {
	for _, kind := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			b, receipts := fixtures(t)
			var block map[string]json.RawMessage
			var txs []map[string]json.RawMessage
			var receipt map[string]json.RawMessage
			if err := json.Unmarshal(b, &block); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(block["transactions"], &txs); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(receipts[0], &receipt); err != nil {
				t.Fatal(err)
			}
			tx := txs[0]
			tx["type"] = json.RawMessage(fmt.Sprintf(`"0x%x"`, kind))
			receipt["type"] = tx["type"]
			tx["accessList"] = json.RawMessage(`[{"address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","storageKeys":["0x` + strings.Repeat("ab", 32) + `"]}]`)
			if kind == 1 {
				tx["gasPrice"] = json.RawMessage(`"0x77359400"`)
				delete(tx, "maxFeePerGas")
				delete(tx, "maxPriorityFeePerGas")
			}
			if kind == 3 {
				tx["maxFeePerBlobGas"] = json.RawMessage(`"0x10000000000000000"`)
				tx["blobVersionedHashes"] = json.RawMessage(`["0x01` + strings.Repeat("ab", 31) + `"]`)
				block["blobGasUsed"] = json.RawMessage(`"0x20000"`)
				receipt["blobGasUsed"] = json.RawMessage(`"0x20000"`)
				receipt["blobGasPrice"] = json.RawMessage(`"0x1"`)
			}
			if kind == 4 {
				tx["authorizationList"] = json.RawMessage(`[{"chainId":"0x0","address":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","nonce":"0x2","yParity":"0x1","r":"0x3","s":"0x4"}]`)
			}
			block["transactions"], _ = json.Marshal(txs)
			b, _ = json.Marshal(block)
			receipts[0], _ = json.Marshal(receipt)
			s, err := (ethereumjson.Decoder{}).Decode(t.Context(), b, receipts, "1")
			if err != nil {
				t.Fatal(err)
			}
			e, _ := jsondocument.New(64 << 20)
			encoded, err := e.Encode(t.Context(), s, domain.Provenance{SourceEventID: "fixture", Provider: "fixture", ExtractedAt: time.Unix(100, 123)})
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Transactions []map[string]json.RawMessage `json:"transactions"`
				Receipts     []map[string]json.RawMessage `json:"receipts"`
				Logs         []map[string]json.RawMessage `json:"logs"`
			}
			if err := json.Unmarshal(encoded, &doc); err != nil {
				t.Fatal(err)
			}
			got := doc.Transactions[0]
			if string(got["type"]) != fmt.Sprintf(`"%d"`, kind) || string(got["access_list"]) != `[{"address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","storage_keys":["0x`+strings.Repeat("ab", 32)+`"]}]` {
				t.Fatal("typed transaction/access list changed")
			}
			if len(doc.Logs) == 0 || string(doc.Logs[0]["transaction_hash"]) != string(got["hash"]) || string(doc.Logs[0]["log_index"]) != `"0"` {
				t.Fatal("log membership/index lost")
			}
			if kind == 3 && (string(got["max_fee_per_blob_gas"]) != `"18446744073709551616"` || string(got["blob_versioned_hashes"]) != string(tx["blobVersionedHashes"]) || string(doc.Receipts[0]["blob_gas_used"]) != `"131072"`) {
				t.Fatal("blob fields changed")
			}
			if kind == 4 && string(got["authorization_list"]) != `[{"address":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","chain_id":"0","nonce":"2","r":"3","s":"4","y_parity":"1"}]` {
				t.Fatal("authorization fields changed")
			}
			// Missing type-specific required fields must fail in the domain,
			// independently of whether the parser or an alternate adapter supplied them.
			switch kind {
			case 1:
				s.Transactions[0].GasPrice = domain.Optional[domain.Quantity]{}
			case 2:
				s.Transactions[0].MaxFeePerGas = domain.Optional[domain.Quantity]{}
			case 3:
				s.Receipts[0].BlobGasUsed = domain.Optional[uint64]{}
			case 4:
				s.Transactions[0].AuthorizationList.Value = []domain.Authorization{}
			}
			if !errors.Is(s.Validate(), domain.ErrInvalid) {
				t.Fatal("missing required field accepted")
			}
		})
	}
}

// TestEmptyBlockCollections checks explicit empty collections and historical receipt
// representation without inventing modern fields.
func TestEmptyBlockCollections(t *testing.T) {
	b, _ := fixtures(t)
	var block map[string]json.RawMessage
	if err := json.Unmarshal(b, &block); err != nil {
		t.Fatal(err)
	}
	block["transactions"] = json.RawMessage(`[]`)
	block["gasUsed"] = json.RawMessage(`"0x0"`)
	b, _ = json.Marshal(block)
	s, err := (ethereumjson.Decoder{}).Decode(t.Context(), b, [][]byte{}, "1")
	if err != nil {
		t.Fatal(err)
	}
	e, _ := jsondocument.New(64 << 20)
	encoded, err := e.Encode(t.Context(), s, domain.Provenance{SourceEventID: "fixture", Provider: "fixture", ExtractedAt: time.Unix(100, 123)})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"transactions", "receipts", "logs", "uncles"} {
		if !bytes.Contains(encoded, []byte(`"`+field+`":[]`)) {
			t.Fatal("empty collection became null", field)
		}
	}
}
