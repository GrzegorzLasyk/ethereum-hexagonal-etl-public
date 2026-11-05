// receipts_test.go exercises bounded concurrent receipt fetching with indexed results and joined
// cancellation in the outbound adapter layer.

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
	synthetic "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/test/fixture"
)

// Synthetic test-only workload, not modified authentic Ethereum evidence.
func receiptBatch(t *testing.T, n int) ([]byte, [][]byte) {
	t.Helper()
	block, receipts := synthetic.Block(100, false, true)
	var b map[string]any
	if json.Unmarshal(block, &b) != nil {
		t.Fatal("fixture block")
	}
	template, _ := json.Marshal(b["transactions"].([]any)[0])
	transactions := make([]any, 0, n)
	out := make([][]byte, n)
	for i := range n {
		var tx, r map[string]any
		_ = json.Unmarshal(template, &tx)
		_ = json.Unmarshal(receipts[0], &r)
		hash, index := fmt.Sprintf("0x%064x", i+1), fmt.Sprintf("0x%x", i)
		tx["hash"] = hash
		tx["transactionIndex"] = index
		tx["nonce"] = index
		r["transactionHash"] = hash
		r["transactionIndex"] = index
		r["cumulativeGasUsed"] = fmt.Sprintf("0x%x", (i+1)*25000)
		for _, v := range r["logs"].([]any) {
			log := v.(map[string]any)
			log["transactionHash"] = hash
			log["transactionIndex"] = index
			log["logIndex"] = index
		}
		transactions = append(transactions, tx)
		out[i], _ = json.MarshalIndent(r, "", "  ")
	}
	b["transactions"] = transactions
	b["gasUsed"] = fmt.Sprintf("0x%x", n*25000)
	block, _ = json.MarshalIndent(b, "", " ")
	return block, out
}

// receiptHook controls a receipt response, HTTP status and retry header at a chosen transaction
// index.
type receiptHook func(context.Context, int) ([]byte, int, string, error)

// offlineReceiptClient injects an in-memory receipt transport/hook into the real client to control
// completion order without external RPC.
func offlineReceiptClient(t *testing.T, cfg Config, block []byte, receipts [][]byte, hook receiptHook) *Client {
	t.Helper()
	client, e := New(cfg, &http.Client{Transport: budgetTransport(func(r *http.Request) (*http.Response, error) {
		var q wireRequest
		if json.NewDecoder(r.Body).Decode(&q) != nil {
			return nil, ErrProtocol
		}
		raw := []byte(`"0x1"`)
		code, after := 200, ""
		switch q.Method {
		case "eth_chainId":
		case "eth_getBlockByNumber":
			raw = block
		case "eth_getTransactionReceipt":
			var hash string
			if len(q.Params) != 1 || json.Unmarshal(q.Params[0], &hash) != nil {
				return nil, ErrProtocol
			}
			i := -1
			for j := range receipts {
				if hash == fmt.Sprintf("0x%064x", j+1) {
					i = j
					break
				}
			}
			if i < 0 {
				return nil, ErrProtocol
			}
			raw = receipts[i]
			if hook != nil {
				var err error
				raw, code, after, err = hook(r.Context(), i)
				if err != nil {
					return nil, err
				}
			}
		default:
			return nil, ErrProtocol
		}
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, q.ID, raw)
		return &http.Response{StatusCode: code, Header: http.Header{"Retry-After": []string{after}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})})
	if e != nil {
		t.Fatal(e)
	}
	return client
}

// TestConcurrentReceiptOrderingAndRawBytes forces out-of-order completion and compares
// transaction-indexed receipts and exact raw bytes.
func TestConcurrentReceiptOrderingAndRawBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block, receipts := receiptBatch(t, 12)
		cfg := testConfig("https://offline.invalid")
		cfg.ReceiptConcurrency = 4
		cfg.RequestTimeout = time.Minute
		var active, peak atomic.Int32
		var mu sync.Mutex
		var order []int
		c := offlineReceiptClient(t, cfg, block, receipts, func(ctx context.Context, i int) ([]byte, int, string, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); current > old; old = peak.Load() {
				if peak.CompareAndSwap(old, current) {
					break
				}
			}
			// Virtual server latency deliberately completes later indexes first.
			if e := wait(ctx, time.Duration(4-i%4)*time.Second); e != nil {
				return nil, 0, "", e
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			return receipts[i], 200, "", nil
		})
		got, e := c.Block(t.Context(), 100)
		if e != nil {
			t.Fatal(e)
		}
		if peak.Load() != 4 || active.Load() != 0 || len(order) != 12 || order[0] == 0 {
			t.Fatal("no bounded out-of-order overlap", peak.Load(), order)
		}
		if !bytes.Equal(got.RawBlock, block) || len(got.RawReceipts) != 12 || c.Requests() != 15 {
			t.Fatal("incomplete batch")
		}
		for i, r := range got.RawReceipts {
			if !bytes.Equal(r, receipts[i]) || got.Receipts[i].TransactionIndex != uint64(i) {
				t.Fatal("receipt bytes/order changed", i)
			}
		}
	})
}

// TestConcurrentReceiptSharedBudget requires competing receipt workers to share the actual HTTP-
// attempt limit.
func TestConcurrentReceiptSharedBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block, receipts := receiptBatch(t, 24)
		cfg := testConfig("https://offline.invalid")
		cfg.ReceiptConcurrency = 8
		cfg.MaxRequests = 7
		var calls atomic.Int32
		c := offlineReceiptClient(t, cfg, block, receipts, func(ctx context.Context, i int) ([]byte, int, string, error) { calls.Add(1); return nil, 503, "", nil })
		got, e := c.Block(t.Context(), 100)
		if !errors.Is(e, outbound.ErrRPCBudgetExceeded) || len(got.RawBlock) != 0 || len(got.RawReceipts) != 0 || c.Requests() != 7 || calls.Load() != 5 {
			t.Fatal("budget/partial batch", e, c.Requests(), calls.Load())
		}
		if _, e = c.ChainID(t.Context()); !errors.Is(e, outbound.ErrRPCBudgetExceeded) || c.Requests() != 7 {
			t.Fatal("budget reset")
		}
	})
}

// TestConcurrentReceiptCancellationJoinsWorkers holds workers in flight, injects
// failure/cancellation and verifies the batch joins before returning.
func TestConcurrentReceiptCancellationJoinsWorkers(t *testing.T) {
	for _, mode := range []string{"permanent", "canceled", "deadline", "request timeout", "missing"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				block, receipts := receiptBatch(t, 20)
				cfg := testConfig("https://offline.invalid")
				cfg.ReceiptConcurrency = 2
				cfg.RequestTimeout = time.Hour
				if mode == "request timeout" {
					cfg.RequestTimeout = time.Second
					cfg.MaxAttempts = 1
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if mode == "deadline" {
					var end context.CancelFunc
					ctx, end = context.WithTimeout(ctx, time.Minute)
					defer end()
				}
				second := make(chan struct{})
				var calls, active atomic.Int32
				c := offlineReceiptClient(t, cfg, block, receipts, func(call context.Context, i int) ([]byte, int, string, error) {
					calls.Add(1)
					active.Add(1)
					defer active.Add(-1)
					if i == 1 {
						close(second)
						<-call.Done()
						return nil, 0, "", call.Err()
					}
					if i != 0 {
						return nil, 0, "", fmt.Errorf("pending work started after failure")
					}
					<-second
					switch mode {
					case "permanent":
						return nil, 401, "", nil
					case "missing":
						return []byte("null"), 200, "", nil
					case "canceled":
						cancel()
					}
					<-call.Done()
					return nil, 0, "", call.Err()
				})
				got, e := c.Block(ctx, 100)
				want := error(context.Canceled)
				switch mode {
				case "permanent":
					want = outbound.ErrSourcePermanent
				case "deadline", "request timeout":
					want = context.DeadlineExceeded
				case "missing":
					want = domain.ErrUnavailable
				}
				if !errors.Is(e, want) || len(got.RawBlock) != 0 || active.Load() != 0 || calls.Load() != 2 {
					t.Fatal("workers not joined or classification lost", e, active.Load(), calls.Load())
				}
				// synctest also rejects surviving blocked goroutines when this test returns.
			})
		})
	}
}

// TestConcurrentReceiptGlobalPacingAndCooldown checks that all receipt workers obey one request
// schedule and shared Retry-After pause.
func TestConcurrentReceiptGlobalPacingAndCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block, receipts := receiptBatch(t, 8)
		cfg := testConfig("https://offline.invalid")
		cfg.ReceiptConcurrency = 4
		cfg.RequestTimeout = time.Minute
		cfg.MinRequestInterval = time.Second
		cfg.MaxBackoff = 10 * time.Second
		var mu sync.Mutex
		var starts []time.Time
		attempts := make([]int, len(receipts))
		var limitedAt time.Time
		c := offlineReceiptClient(t, cfg, block, receipts, func(ctx context.Context, i int) ([]byte, int, string, error) {
			mu.Lock()
			defer mu.Unlock()
			now := time.Now()
			starts = append(starts, now)
			attempts[i]++
			if len(starts) == 1 {
				limitedAt = now
				return nil, 429, "5", nil
			}
			return receipts[i], 200, "", nil
		})
		if _, e := c.Block(t.Context(), 100); e != nil {
			t.Fatal(e)
		}
		if len(starts) != 9 || starts[1].Sub(limitedAt) < 5*time.Second || c.Requests() != 12 {
			t.Fatal("shared Retry-After bypassed", starts, c.Requests())
		}
		for i := 1; i < len(starts); i++ {
			if starts[i].Sub(starts[i-1]) < time.Second {
				t.Fatal("global pacing bypassed")
			}
		}
	})
}

// TestConcurrentReceiptValidationFailures injects invalid or incomplete receipt data and rejects
// the entire batch rather than accepting partial membership.
func TestConcurrentReceiptValidationFailures(t *testing.T) {
	for _, kind := range []string{"index", "hash", "block", "cumulative", "snapshot"} {
		t.Run(kind, func(t *testing.T) {
			block, receipts := receiptBatch(t, 8)
			cfg := testConfig("https://offline.invalid")
			cfg.ReceiptConcurrency = 4
			if kind == "snapshot" {
				cfg.MaxResponseBytes = int64(len(block) + 256)
				cfg.MaxSnapshotBytes = int64(len(block) + len(receipts[0]))
			} else {
				var r map[string]any
				_ = json.Unmarshal(receipts[2], &r)
				switch kind {
				case "index":
					r["transactionIndex"] = "0x0"
				case "hash":
					r["transactionHash"] = fmt.Sprintf("0x%064x", 99)
				case "block":
					r["blockHash"] = fmt.Sprintf("0x%064x", 99)
				case "cumulative":
					r["cumulativeGasUsed"] = "0x1"
				}
				receipts[2], _ = json.Marshal(r)
			}
			c := offlineReceiptClient(t, cfg, block, receipts, nil)
			got, e := c.Block(t.Context(), 100)
			if e == nil || len(got.RawBlock) != 0 || len(got.RawReceipts) != 0 {
				t.Fatal("invalid/partial batch escaped", e)
			}
			if kind == "snapshot" && !errors.Is(e, ErrSnapshotTooLarge) {
				t.Fatal(e)
			}
		})
	}
}

// TestReceiptConcurrencyBounds checks the conservative receipt default and supported upper bound
// before workers can start.
func TestReceiptConcurrencyBounds(t *testing.T) {
	for _, n := range []int{-1, 9} {
		cfg := testConfig("https://offline.invalid")
		cfg.ReceiptConcurrency = n
		if _, e := New(cfg, nil); !errors.Is(e, ErrConfig) {
			t.Fatal("unbounded workers", n, e)
		}
	}
	c := newClient(t, testConfig("https://offline.invalid"))
	if c.config.ReceiptConcurrency != 1 {
		t.Fatal("unexpected default")
	}
}

// batchStore records attempted extraction commits and checkpoint state to detect partial-batch
// persistence.
type batchStore struct {
	checkpoint domain.Checkpoint
	commits    int
}

// Checkpoint reads simulated durable progress for extraction/cursor assertions.
func (s *batchStore) Checkpoint(context.Context, domain.ChainID) (*domain.Checkpoint, error) {
	return &s.checkpoint, nil
}

// Commit simulates the application persistence boundary and records committed identity/progress
// for assertions; it is not a production database.
func (s *batchStore) Commit(context.Context, outbound.CommitExtraction) (outbound.CommitResult, error) {
	s.commits++
	return outbound.CommitResult{}, nil
}

// TestConcurrentReceiptFailureNeverCommits connects a failing concurrent source to the extraction
// use case and asserts persistence is not called.
func TestConcurrentReceiptFailureNeverCommits(t *testing.T) {
	for _, budget := range []bool{false, true} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			block, receipts := receiptBatch(t, 8)
			cfg := testConfig("https://offline.invalid")
			cfg.ReceiptConcurrency = 4
			if budget {
				cfg.MaxRequests = 4
			}
			client := offlineReceiptClient(t, cfg, block, receipts, func(ctx context.Context, i int) ([]byte, int, string, error) {
				if i == 1 {
					return []byte("null"), 200, "", nil
				}
				return receipts[i], 200, "", nil
			})
			previous := domain.Checkpoint{Block: domain.BlockID{ChainID: "1", Number: 99, Hash: domain.Hash{1}}, Start: 99, Revision: 7}
			store := &batchStore{checkpoint: previous}
			u, e := usecases.NewExtractBlock("1", 99, client, store)
			if e != nil {
				t.Fatal(e)
			}
			_, e = u.ExtractBlock(t.Context(), inbound.ExtractBlockRequest{ChainID: "1", Number: 100})
			want := domain.ErrUnavailable
			if budget {
				want = outbound.ErrRPCBudgetExceeded
			}
			if !errors.Is(e, want) || store.commits != 0 || store.checkpoint != previous {
				t.Fatal("failed source changed checkpoint/outbox commit boundary", e, store.commits)
			}
		})
	}
}

// TestConcurrentReceiptCancelPacingAndRetry cancels workers waiting on shared pacing/retry paths
// and checks that pending requests stop.
func TestConcurrentReceiptCancelPacingAndRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block, receipts := receiptBatch(t, 8)
		cfg := testConfig("https://offline.invalid")
		cfg.ReceiptConcurrency = 8
		cfg.MinRequestInterval = time.Second
		cfg.MaxBackoff = 5 * time.Second
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var calls atomic.Int32
		client := offlineReceiptClient(t, cfg, block, receipts, func(context.Context, int) ([]byte, int, string, error) {
			calls.Add(1)
			time.AfterFunc(time.Second, cancel)
			return nil, 429, "3", nil
		})
		got, e := client.Block(ctx, 100)
		if !errors.Is(e, context.Canceled) || len(got.RawBlock) != 0 || calls.Load() != 1 || client.Requests() != 3 {
			t.Fatal("queued workers retried after cancellation", e, calls.Load(), client.Requests())
		}
	})
}
