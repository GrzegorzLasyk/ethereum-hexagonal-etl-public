//go:build postgresintegration

// rpc_reliability_integration_test.go exercises checkpoint preservation after source acquisition
// failures in the outbound adapter layer.

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/inbound/indexer"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/fixture"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testRPCCheckpointPreservation injects local source errors/budget exhaustion and checks
// PostgreSQL progress and Outbox stay at the last durable boundary.
func testRPCCheckpointPreservation(t *testing.T, pool *pgxpool.Pool) {
	ctx := t.Context()
	store, err := New(pool, "1", 100)
	if err != nil {
		t.Fatal(err)
	}
	provider := fixture.New(101).Handler()
	makeClient := func(t *testing.T, handler http.Handler, budget uint64) *jsonrpc.Client {
		t.Helper()
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		c, err := jsonrpc.New(jsonrpc.Config{URL: server.URL, ChainID: "1", Provider: "offline", RequestTimeout: time.Second, MaxAttempts: 2, MaxRequests: budget, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, MaxResponseBytes: 1 << 20, MaxSnapshotBytes: 4 << 20}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	seedClient := makeClient(t, provider, 100)
	e, err := seedClient.Block(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, outbound.CommitExtraction{Extraction: e}); err != nil {
		t.Fatal(err)
	}
	// Compare complete service-owned rows, including delivery state and raw bytes.
	state := func() []byte {
		var data []byte
		err := pool.QueryRow(ctx, `SELECT jsonb_build_array(
		(SELECT jsonb_agg(to_jsonb(b) ORDER BY block_number,block_hash) FROM extractor.raw_blocks b WHERE chain_id='1'),
		(SELECT jsonb_agg(to_jsonb(o) ORDER BY event_id) FROM extractor.outbox_events o WHERE chain_id='1'),
		(SELECT jsonb_agg(to_jsonb(a) ORDER BY block_number) FROM extractor.active_blocks a WHERE chain_id='1'),
		(SELECT jsonb_agg(to_jsonb(c)) FROM extractor.extraction_checkpoints c WHERE chain_id='1'),
		(SELECT jsonb_agg(to_jsonb(r)) FROM extractor.rewinds r WHERE chain_id='1'))`).Scan(&data)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	before := state()
	end := uint64(101)
	options := usecases.IndexOptions{Chain: "1", Start: 100, End: &end, RecoveryDepth: 8, Batch: 1, Limits: domain.CapacityLimits{Events: 100, Bytes: 1 << 30, DatabaseBytes: 1 << 50}}
	for _, tc := range []struct {
		name   string
		code   int
		budget uint64
		want   error
	}{
		{"401", 401, 100, outbound.ErrSourcePermanent}, {"402", 402, 100, outbound.ErrSourcePermanent}, {"403", 403, 100, outbound.ErrSourcePermanent},
		{"503", 503, 100, nil}, {"wrong chain", 0, 100, domain.ErrChainMismatch},
		{"missing block", 0, 100, domain.ErrUnavailable}, {"missing receipt", 0, 100, domain.ErrUnavailable},
		{"invalid receipt", 0, 100, domain.ErrInvalidData}, {"budget during receipts", 0, 9, outbound.ErrRPCBudgetExceeded},
		{"cancel during receipts", 0, 100, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.code != 0 {
					w.WriteHeader(tc.code)
					fmt.Fprint(w, "private-provider-detail")
					return
				}
				raw, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(bytes.NewReader(raw))
				var q struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if err := json.Unmarshal(raw, &q); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				result := ""
				switch {
				case tc.name == "wrong chain":
					result = `"0x2"`
				case tc.name == "missing block" && q.Method == "eth_getBlockByNumber" && len(q.Params) == 2 && string(q.Params[1]) == "true":
					result = "null"
				case strings.Contains(tc.name, "receipt") && q.Method == "eth_getTransactionReceipt":
					switch tc.name {
					case "missing receipt":
						result = "null"
					case "invalid receipt":
						result = `{"logs":null}`
					case "cancel during receipts":
						cancel()
						<-r.Context().Done()
						return
					}
				}
				if result != "" {
					fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, q.ID, result)
					return
				}
				provider.ServeHTTP(w, r)
			})
			client := makeClient(t, handler, tc.budget)
			u, err := usecases.NewIndex(options, client, client, store)
			if err != nil {
				t.Fatal(err)
			}
			worker, err := indexer.New(u, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond, time.Minute, 2)
			if err != nil {
				t.Fatal(err)
			}
			err = worker.Run(call)
			if tc.name == "cancel during receipts" {
				if err != nil || !errors.Is(call.Err(), context.Canceled) {
					t.Fatal("cancellation did not propagate", err)
				}
			} else if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatal("wrong failure", err)
			}
			if !bytes.Equal(before, state()) {
				t.Fatal("RPC failure changed checkpoint/history/outbox")
			}
			if tc.code >= 401 && tc.code <= 403 && client.Requests() != 1 {
				t.Fatal("permanent failure retried")
			}
			if tc.budget == 9 && client.Requests() != 9 {
				t.Fatal("request budget accounting")
			}
		})
	}
	// A fresh client/run resumes the same durable cursor after source recovery.
	restarted, err := New(pool, "1", 100)
	if err != nil {
		t.Fatal(err)
	}
	client := makeClient(t, provider, 100)
	u, err := usecases.NewIndex(options, client, client, restarted)
	if err != nil {
		t.Fatal(err)
	}
	r, err := u.Cycle(ctx)
	if err != nil || r.Processed != 1 {
		t.Fatal("restart did not progress", r, err)
	}
	cp, err := restarted.Checkpoint(ctx, "1")
	if err != nil || cp.Block.Number != 101 || cp.Revision != 0 {
		t.Fatal("restart changed lineage", cp, err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM extractor.outbox_events WHERE chain_id='1'`).Scan(&count); err != nil || count != 2 {
		t.Fatal("lost/duplicate outbox intent", count, err)
	}
}
