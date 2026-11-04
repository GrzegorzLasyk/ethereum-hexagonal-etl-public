//go:build postgresintegration && perfbaseline

// performance_seed_test.go exercises bounded synthetic/authentic offline source setup for
// measurements in the outbound adapter layer.

package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/baseline"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/fixturecapture"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/fixture"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/performance"
	"github.com/jackc/pgx/v5/pgxpool"
)

// offlineRoundTripper adapts captured or synthetic responses to HTTP transport without a network
// fallback.
type offlineRoundTripper func(*http.Request) (*http.Response, error)

// RoundTrip implements the test HTTP transport with controlled responses/failures; it never
// selects a live Ethereum endpoint.
func (f offlineRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// No socket, default HTTP transport, endpoint configuration or network fallback.
func syntheticSource(t *testing.T, start uint64, count int) outbound.BlockSource {
	t.Helper()
	receiptsByHash := map[string][]byte{}
	for n := start; n < start+uint64(count); n++ {
		_, receipts := fixture.Block(n, false, true)
		for i, r := range receipts {
			receiptsByHash[fixture.TxHash(n, false, i)] = r
		}
	}
	transport := offlineRoundTripper(func(r *http.Request) (*http.Response, error) {
		var q struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if json.NewDecoder(r.Body).Decode(&q) != nil {
			return nil, fmt.Errorf("invalid offline request")
		}
		var raw []byte
		switch q.Method {
		case "eth_chainId":
			raw = []byte(`"0x1"`)
		case "eth_getBlockByNumber":
			var hex string
			if len(q.Params) != 2 || json.Unmarshal(q.Params[0], &hex) != nil || len(hex) < 3 {
				return nil, fmt.Errorf("invalid offline block")
			}
			n, err := strconv.ParseUint(hex[2:], 16, 64)
			if err != nil || n < start || n >= start+uint64(count) {
				return nil, fmt.Errorf("offline height outside range")
			}
			raw, _ = fixture.Block(n, false, true)
		case "eth_getTransactionReceipt":
			var hash string
			if len(q.Params) != 1 || json.Unmarshal(q.Params[0], &hash) != nil {
				return nil, fmt.Errorf("invalid offline receipt")
			}
			// Receipt identities are deterministic; lookup does not create or mutate history.
			raw = receiptsByHash[hash]
		default:
			return nil, fmt.Errorf("unexpected offline method")
		}
		if raw == nil {
			return nil, fmt.Errorf("missing offline response")
		}
		body := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, q.ID, raw))
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	c, e := jsonrpc.New(jsonrpc.Config{URL: "https://offline.invalid", ChainID: "1", Provider: "synthetic-stage11", RequestTimeout: time.Second, MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxResponseBytes: 16 << 20, MaxSnapshotBytes: 64 << 20}, &http.Client{Transport: transport})
	if e != nil {
		t.Fatal(e)
	}
	return fixedProvenanceSource{c}
}

// TestSeedPerformance acquires the selected bounded offline dataset into Extractor storage and
// writes correlated timing evidence.
func TestSeedPerformance(t *testing.T) {
	s, err := performance.ReadScenario()
	if err != nil || os.Getenv("HANDOFF_TEST_ISOLATED") != "1" {
		t.Fatal("requires explicit isolated baseline runner")
	}
	var source outbound.BlockSource
	if s.Dataset == "authentic-mainnet" {
		m, e := fixturecapture.LoadManifest(s.Manifest)
		if e != nil {
			t.Fatal(e)
		}
		for _, candidate := range m.Candidates {
			if candidate.ID == s.Case {
				source, e = fixturecapture.NewReplaySource(t.Context(), s.Artifact, candidate)
				if e != nil {
					t.Fatal(e)
				}
			}
		}
		if source == nil {
			t.Fatal("unverified authentic candidate")
		}
	} else {
		source = syntheticSource(t, s.Start, s.Blocks)
	}
	cfg, e := pgxpool.ParseConfig(os.Getenv("EXTRACTOR_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal("invalid own database")
	}
	cfg.MaxConns = 5
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	probe, e := baseline.Configure(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer probe.Close()
	p, e := pgxpool.NewWithConfig(t.Context(), cfg)
	if e != nil {
		t.Fatal("database unavailable")
	}
	defer p.Close()
	probe.Attach(p)
	defer probe.Close()
	var role string
	if p.QueryRow(t.Context(), "SELECT current_user").Scan(&role) != nil || role != "extractor_runtime" {
		t.Fatal("wrong database role")
	}
	store, e := New(p, "1", s.Start)
	if e != nil {
		t.Fatal(e)
	}
	u, e := usecases.NewExtractBlock("1", s.Start, source, store)
	if e != nil {
		t.Fatal(e)
	}
	duplicate := os.Getenv("ETL_BASELINE_DUPLICATE") == "1"
	var report performance.Seed
	first := s.Start
	if duplicate {
		first += uint64(s.Blocks) - 1
	}
	for n := first; n < s.Start+uint64(s.Blocks); n++ {
		started := time.Now()
		result, e := u.ExtractBlock(t.Context(), inbound.ExtractBlockRequest{ChainID: "1", Number: n})
		duration := time.Since(started).Seconds()
		if e != nil || result.AlreadyStored != duplicate {
			t.Fatal("offline extraction or duplicate invariant failed", e)
		}
		report.Events = append(report.Events, performance.SeedEvent{EventID: result.EventID, Started: started, DurationSeconds: duration})
	}
	// Counts refer to durable exact source data, outside extraction timing.
	var receipts, logs int
	if err := p.QueryRow(t.Context(), `SELECT COALESCE(sum(cardinality(raw_receipts)),0)::bigint,COALESCE(sum(octet_length(raw_block)+(SELECT COALESCE(sum(octet_length(r)),0) FROM unnest(raw_receipts) r)),0)::bigint FROM extractor.raw_blocks`).Scan(&receipts, &report.PayloadBytes); err != nil {
		t.Fatal("source counts unavailable")
	}
	rows, e := p.Query(t.Context(), "SELECT raw_receipts FROM extractor.raw_blocks")
	if e != nil {
		t.Fatal("source receipt read")
	}
	for rows.Next() {
		var raw [][]byte
		if rows.Scan(&raw) != nil {
			t.Fatal("source receipts")
		}
		for _, b := range raw {
			var r struct{ Logs []json.RawMessage }
			if json.Unmarshal(b, &r) != nil {
				t.Fatal("receipt decode")
			}
			logs += len(r.Logs)
		}
	}
	if rows.Err() != nil {
		t.Fatal("source receipts unavailable")
	}
	rows.Close()
	report.Transactions = receipts
	report.Receipts = receipts
	report.Logs = logs
	report.DuplicateVerified = duplicate
	name := "source.json"
	if duplicate {
		name = "duplicate-source.json"
	}
	if performance.ExclusiveJSON(filepath.Join(s.Directory, name), report) != nil {
		t.Fatal("cannot write source evidence")
	}
}

// TestSyntheticBaselineOfflineSource checks deterministic contiguous synthetic acquisition uses
// only the local replay transport.
func TestSyntheticBaselineOfflineSource(t *testing.T) {
	source := syntheticSource(t, 100, 3)
	chain, err := source.ChainID(t.Context())
	if err != nil || chain != "1" {
		t.Fatal("offline chain validation", err)
	}
	for n := uint64(100); n < 103; n++ {
		got, err := source.Block(t.Context(), n)
		if err != nil {
			t.Fatal(err)
		}
		block, receipts := fixture.Block(n, false, true)
		if !bytes.Equal(got.RawBlock, block) || len(got.RawReceipts) != len(receipts) || len(got.Transactions) != 2 || got.Header.ID.Number != n || got.Provider != "synthetic-stage11" {
			t.Fatal("offline source changed data")
		}
		for i := range receipts {
			if !bytes.Equal(got.RawReceipts[i], receipts[i]) {
				t.Fatal("receipt changed")
			}
		}
		if err := got.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.Block(t.Context(), 103); err == nil {
		t.Fatal("out-of-range block accepted")
	}
}
