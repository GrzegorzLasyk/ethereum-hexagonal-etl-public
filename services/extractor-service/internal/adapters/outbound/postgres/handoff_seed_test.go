//go:build postgresintegration

// handoff_seed_test.go exercises deterministic source setup for downstream real-process handoff in
// the outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// fixedProvenanceSource wraps the block source with deterministic provenance for duplicate handoff
// assertions.
type fixedProvenanceSource struct{ outbound.BlockSource }

// Block returns or fails a controlled complete source snapshot so acquisition/persistence
// boundaries can be exercised.
func (s fixedProvenanceSource) Block(c context.Context, n uint64) (domain.Extraction, error) {
	e, err := s.BlockSource.Block(c, n)
	e.ExtractedAt = time.Unix(100, 123).UTC()
	return e, err
}

// TestSeedHandoff seeds the isolated handoff with production acquisition logic.
//
// Only the isolated Stage 9 runner invokes this seed through the real extraction
// use case and PostgreSQL adapter. No other service imports Extractor internals.
func TestSeedHandoff(t *testing.T) {
	if os.Getenv("HANDOFF_TEST_ISOLATED") != "1" {
		t.Skip("requires isolated handoff runner")
	}
	pool := integrationPool(t, "EXTRACTOR_TEST_DATABASE_URL", "extractor_runtime")
	read := func(name string) []byte {
		b, e := os.ReadFile("../jsonrpc/testdata/" + name + ".json")
		if e != nil {
			t.Fatal(e)
		}
		return bytes.TrimSpace(b)
	}
	block, first, second := read("block"), read("receipt_0"), read("receipt_1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     uint64   `json:"id"`
			Method string   `json:"method"`
			Params []string `json:"params"`
		}
		// Block parameters also contain a boolean; decode method/ID separately.
		var raw struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&raw) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		req.ID, req.Method = raw.ID, raw.Method
		var result []byte
		switch req.Method {
		case "eth_chainId":
			result = []byte(`"0x1"`)
		case "eth_getBlockByNumber":
			result = block
		case "eth_getTransactionReceipt":
			var hash string
			if len(raw.Params) != 1 || json.Unmarshal(raw.Params[0], &hash) != nil {
				http.Error(w, "bad receipt", 400)
				return
			}
			if hash == "0x2020202020202020202020202020202020202020202020202020202020202020" {
				result = first
			} else if hash == "0x2121212121212121212121212121212121212121212121212121212121212121" {
				result = second
			} else {
				http.Error(w, "unknown receipt", 400)
				return
			}
		default:
			http.Error(w, "unknown method", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, result)
	}))
	defer server.Close()
	source, e := jsonrpc.New(jsonrpc.Config{URL: server.URL, ChainID: "1", Provider: "fixture", RequestTimeout: time.Second, MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxResponseBytes: 1 << 20, MaxSnapshotBytes: 4 << 20}, server.Client())
	if e != nil {
		t.Fatal(e)
	}
	store, e := New(pool, "1", 20000000)
	if e != nil {
		t.Fatal(e)
	}
	usecase, e := usecases.NewExtractBlock("1", 20000000, fixedProvenanceSource{source}, store)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = usecase.ExtractBlock(t.Context(), inbound.ExtractBlockRequest{ChainID: "1", Number: 20000000}); e != nil {
		t.Fatal(e)
	}
}
