package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

const fixtureNumber uint64 = 20000000

type wireRequest struct {
	Version string            `json:"jsonrpc"`
	ID      uint64            `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

func testConfig(endpoint string) Config {
	return Config{
		URL: endpoint, ChainID: "1", Provider: "fixture",
		RequestTimeout: time.Second, MaxAttempts: 3,
		InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
		MaxResponseBytes: 1 << 20, MaxSnapshotBytes: 4 << 20,
	}
}

func newClient(t *testing.T, config Config) *Client {
	t.Helper()
	c, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

func reply(w http.ResponseWriter, id uint64, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, id, raw)
}

type fixtureServer struct {
	server   *httptest.Server
	block    []byte
	receipts [2][]byte
	mu       sync.Mutex
	requests []wireRequest
}

func serveFixtures(t *testing.T, block []byte, receipts [2][]byte) *fixtureServer {
	t.Helper()
	f := &fixtureServer{block: block, receipts: receipts}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect HTTP method/content type")
			w.WriteHeader(400)
			return
		}
		var request wireRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		f.requests = append(f.requests, request)
		f.mu.Unlock()
		switch request.Method {
		case "eth_chainId":
			reply(w, request.ID, []byte(`"0x1"`))
		case "eth_getBlockByNumber":
			reply(w, request.ID, f.block)
		case "eth_getTransactionReceipt":
			var hash string
			if len(request.Params) != 1 || json.Unmarshal(request.Params[0], &hash) != nil {
				w.WriteHeader(400)
				return
			}
			switch hash {
			case "0x" + strings.Repeat("20", 32):
				reply(w, request.ID, f.receipts[0])
			case "0x" + strings.Repeat("21", 32):
				reply(w, request.ID, f.receipts[1])
			default:
				w.WriteHeader(400)
			}
		default:
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func TestBlockPreservesSourceAndBuildsMetadata(t *testing.T) {
	block := fixture(t, "block")
	receipts := [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")}
	f := serveFixtures(t, block, receipts)
	c := newClient(t, testConfig(f.server.URL))
	before := time.Now()
	e, err := c.Block(context.Background(), fixtureNumber)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(e.RawBlock, block) || !bytes.Equal(e.RawReceipts[0], receipts[0]) ||
		!bytes.Equal(e.RawReceipts[1], receipts[1]) {
		t.Fatal("raw result bytes changed")
	}
	if e.Provider != "fixture" || e.ExtractedAt.Before(before) ||
		e.Header.ID.ChainID != "1" || e.Header.ID.Number != fixtureNumber {
		t.Fatal("incorrect provenance")
	}
	if len(e.Transactions) != 2 || len(e.Receipts) != 2 || len(e.Receipts[0].Logs) != 1 ||
		len(e.Receipts[1].Logs) != 0 {
		t.Fatal("missing transaction/receipt/log")
	}
	log := e.Receipts[0].Logs[0]
	if !bytes.Equal(log.Data, []byte{0, 1, 2}) || log.Address != (domain.Address{0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd, 0xdd}) {
		t.Fatal("incorrect log bytes")
	}
	f.mu.Lock()
	requests := append([]wireRequest(nil), f.requests...)
	f.mu.Unlock()
	if len(requests) != 5 {
		t.Fatalf("requests = %d", len(requests))
	}
	if requests[0].Method != "eth_chainId" || requests[4].Method != "eth_chainId" {
		t.Fatal("missing chain checks")
	}
	if string(requests[1].Params[0]) != `"0x1312d00"` || string(requests[1].Params[1]) != "true" {
		t.Fatal("did not request full transactions")
	}
	for i, r := range requests {
		if r.Version != "2.0" || r.ID == 0 {
			t.Fatal("invalid request envelope")
		}
		if i > 0 && r.ID == requests[i-1].ID {
			t.Fatal("RPC calls shared IDs")
		}
	}
	// Caller ownership: another fetch cannot overwrite the previous snapshot.
	e.RawBlock[0] = '!'
	again, err := c.Block(context.Background(), fixtureNumber)
	if err != nil || !bytes.Equal(again.RawBlock, block) {
		t.Fatal("snapshot buffers shared")
	}
}

func mutate(t *testing.T, raw []byte, fn func(map[string]any)) []byte {
	t.Helper()
	var o map[string]any
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	fn(o)
	result, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func firstTx(o map[string]any) map[string]any  { return o["transactions"].([]any)[0].(map[string]any) }
func firstLog(o map[string]any) map[string]any { return o["logs"].([]any)[0].(map[string]any) }

func TestBlockRejectsPartialOrConflictingData(t *testing.T) {
	tests := []struct {
		name, resource string
		change         func(map[string]any)
		want           error
	}{
		{"missing hash", "block", func(o map[string]any) { delete(o, "hash") }, domain.ErrInvalidData},
		{"null number", "block", func(o map[string]any) { o["number"] = nil }, domain.ErrInvalidData},
		{"wrong height", "block", func(o map[string]any) { o["number"] = "0x1" }, domain.ErrBlockConflict},
		{"missing header field", "block", func(o map[string]any) { delete(o, "stateRoot") }, domain.ErrInvalidData},
		{"missing transaction list", "block", func(o map[string]any) { delete(o, "transactions") }, domain.ErrInvalidData},
		{"hashes instead of full transactions", "block", func(o map[string]any) { o["transactions"] = []any{"0x" + strings.Repeat("20", 32)} }, domain.ErrInvalidData},
		{"quantity overflow", "block", func(o map[string]any) { o["timestamp"] = "0x10000000000000000" }, domain.ErrInvalidData},
		{"noncanonical quantity", "block", func(o map[string]any) { o["gasLimit"] = "0x00" }, domain.ErrInvalidData},
		{"null input", "block", func(o map[string]any) { firstTx(o)["input"] = nil }, domain.ErrInvalidData},
		{"bad address", "block", func(o map[string]any) { firstTx(o)["from"] = "0x00" }, domain.ErrInvalidData},
		{"transaction chain", "block", func(o map[string]any) { firstTx(o)["chainId"] = "0x2" }, domain.ErrChainMismatch},
		{"transaction block", "block", func(o map[string]any) { firstTx(o)["blockHash"] = "0x" + strings.Repeat("ff", 32) }, domain.ErrBlockConflict},
		{"transaction index", "block", func(o map[string]any) { firstTx(o)["transactionIndex"] = "0x1" }, domain.ErrInvalidData},
		{"unknown type", "block", func(o map[string]any) { firstTx(o)["type"] = "0x7" }, domain.ErrInvalidData},
		{"missing access list", "block", func(o map[string]any) { delete(firstTx(o), "accessList") }, domain.ErrInvalidData},
		{"missing fee", "block", func(o map[string]any) { delete(firstTx(o), "maxFeePerGas") }, domain.ErrInvalidData},
		{"priority exceeds fee", "block", func(o map[string]any) { firstTx(o)["maxPriorityFeePerGas"] = "0xffffffffff" }, domain.ErrInvalidData},
		{"wrong typed parity", "block", func(o map[string]any) { firstTx(o)["v"] = "0x1" }, domain.ErrInvalidData},
		{"receipt fork", "receipt_0", func(o map[string]any) { o["blockHash"] = "0x" + strings.Repeat("ff", 32) }, domain.ErrBlockConflict},
		{"wrong receipt tx", "receipt_0", func(o map[string]any) { o["transactionHash"] = "0x" + strings.Repeat("ff", 32) }, domain.ErrInvalidData},
		{"receipt sender", "receipt_0", func(o map[string]any) { o["from"] = "0x" + strings.Repeat("bb", 20) }, domain.ErrInvalidData},
		{"receipt recipient", "receipt_0", func(o map[string]any) { o["to"] = nil }, domain.ErrInvalidData},
		{"missing status", "receipt_0", func(o map[string]any) { delete(o, "status") }, domain.ErrInvalidData},
		{"invalid status", "receipt_0", func(o map[string]any) { o["status"] = "0x2" }, domain.ErrInvalidData},
		{"both outcomes", "receipt_0", func(o map[string]any) { o["root"] = "0x" + strings.Repeat("ff", 32) }, domain.ErrInvalidData},
		{"missing logs", "receipt_0", func(o map[string]any) { delete(o, "logs") }, domain.ErrInvalidData},
		{"null logs", "receipt_0", func(o map[string]any) { o["logs"] = nil }, domain.ErrInvalidData},
		{"bad cumulative gas", "receipt_0", func(o map[string]any) { o["cumulativeGasUsed"] = "0x1" }, domain.ErrInvalidData},
		{"missing effective price", "receipt_0", func(o map[string]any) { delete(o, "effectiveGasPrice") }, domain.ErrInvalidData},
		{"log fork", "receipt_0", func(o map[string]any) { firstLog(o)["blockHash"] = "0x" + strings.Repeat("ff", 32) }, domain.ErrBlockConflict},
		{"removed log", "receipt_0", func(o map[string]any) { firstLog(o)["removed"] = true }, domain.ErrBlockConflict},
		{"log gap", "receipt_0", func(o map[string]any) { firstLog(o)["logIndex"] = "0x1" }, domain.ErrInvalidData},
		{"null topic list", "receipt_0", func(o map[string]any) { firstLog(o)["topics"] = nil }, domain.ErrInvalidData},
		{"malformed data", "receipt_0", func(o map[string]any) { firstLog(o)["data"] = "0x0" }, domain.ErrInvalidData},
		{"metadata null removed", "receipt_0", func(o map[string]any) { firstLog(o)["removed"] = nil }, domain.ErrInvalidData},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := fixture(t, "block")
			receipts := [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")}
			if tt.resource == "block" {
				block = mutate(t, block, tt.change)
			} else {
				receipts[0] = mutate(t, receipts[0], tt.change)
			}
			f := serveFixtures(t, block, receipts)
			e, err := newClient(t, testConfig(f.server.URL)).Block(context.Background(), fixtureNumber)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if len(e.RawBlock) != 0 || len(e.Receipts) != 0 {
				t.Fatal("returned partial snapshot")
			}
		})
	}
}

func TestEmptyBlockAndHistoricalReceipt(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(fmt.Sprint(historical), func(t *testing.T) {
			block := mutate(t, fixture(t, "block"), func(o map[string]any) {
				if !historical {
					o["transactions"] = []any{}
					o["gasUsed"] = "0x0"
					return
				}
				delete(o, "baseFeePerGas")
				tx := firstTx(o)
				tx["type"] = "0x0"
				tx["gasPrice"] = "0x1"
				tx["v"] = "0x1b"
				delete(tx, "chainId")
				delete(tx, "yParity")
			})
			receipts := [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")}
			if historical {
				for i := range receipts {
					receipts[i] = mutate(t, receipts[i], func(o map[string]any) {
						o["type"] = "0x0"
						delete(o, "status")
						delete(o, "effectiveGasPrice")
						o["root"] = "0x" + strings.Repeat("fe", 32)
					})
				}
			}
			f := serveFixtures(t, block, receipts)
			e, err := newClient(t, testConfig(f.server.URL)).Block(context.Background(), fixtureNumber)
			if err != nil {
				t.Fatal(err)
			}
			if !historical && (e.Transactions == nil || e.Receipts == nil || e.RawReceipts == nil || len(e.Receipts) != 0) {
				t.Fatal("empty collections lost")
			}
		})
	}
}

func TestTypedTransactionVariants(t *testing.T) {
	for _, kind := range []string{"0x1", "0x3", "0x4"} {
		t.Run(kind, func(t *testing.T) {
			block := mutate(t, fixture(t, "block"), func(o map[string]any) {
				tx := firstTx(o)
				tx["type"] = kind
				tx["gasPrice"] = "0x77359400"
				tx["maxFeePerBlobGas"] = "0x1"
				tx["blobVersionedHashes"] = []any{"0x01" + strings.Repeat("20", 31)}
				tx["authorizationList"] = []any{map[string]any{"chainId": "0x0", "nonce": "0x0", "address": "0x" + strings.Repeat("aa", 20), "yParity": "0x1", "r": "0x1", "s": "0x2"}}
			})
			r := mutate(t, fixture(t, "receipt_0"), func(o map[string]any) {
				o["type"] = kind
				if kind == "0x3" {
					o["blobGasUsed"] = "0x20000"
					o["blobGasPrice"] = "0x1"
				}
			})
			f := serveFixtures(t, block, [2][]byte{r, fixture(t, "receipt_1")})
			if _, err := newClient(t, testConfig(f.server.URL)).Block(context.Background(), fixtureNumber); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPRetriesAndCredentialRedaction(t *testing.T) {
	for _, tt := range []struct{ status, attempts int }{
		{429, 3}, {408, 3}, {500, 3}, {502, 3}, {503, 3}, {504, 3},
		{400, 1}, {401, 1}, {402, 1}, {403, 1}, {404, 1}, {501, 1},
	} {
		t.Run(fmt.Sprint(tt.status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
				fmt.Fprint(w, "private-key-in-error-body")
			}))
			defer server.Close()
			c := newClient(t, testConfig(server.URL+"/v3/private-path-key?token=private-query-key"))
			_, err := c.ChainID(context.Background())
			var status *HTTPError
			if !errors.As(err, &status) || status.Status != tt.status || int(calls.Load()) != tt.attempts {
				t.Fatalf("calls=%d error=%v", calls.Load(), err)
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), server.URL) {
				t.Fatal("credentials leaked")
			}
		})
	}
}

func TestRPCErrorsAndMalformedResponses(t *testing.T) {
	for _, tt := range []struct {
		name, body     string
		want           error
		code, attempts int
	}{
		{"rate limited", `"error":{"code":429,"message":"private-secret"}`, nil, 429, 3},
		{"limit code", `"error":{"code":-32005,"message":"private-secret"}`, nil, -32005, 3},
		{"invalid params", `"error":{"code":-32602,"message":"private-secret"}`, nil, -32602, 1},
		{"daily quota", `"error":{"code":402,"message":"private-secret"}`, nil, 402, 1},
		{"null", `"result":null`, domain.ErrUnavailable, 0, 1},
		{"both", `"result":"0x1","error":{"code":429,"message":"bad"}`, ErrProtocol, 0, 1},
		{"null error", `"error":null`, ErrProtocol, 0, 1},
		{"missing result", `"unknown":true`, ErrProtocol, 0, 1},
		{"wrong shape", `"result":{}`, domain.ErrInvalidData, 0, 1},
		{"duplicate", `"result":"0x1","result":"0x2"`, ErrProtocol, 0, 1},
		{"null code", `"error":{"code":null,"message":"private-secret"}`, ErrProtocol, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var count atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				var req wireRequest
				json.NewDecoder(r.Body).Decode(&req)
				fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,%s}`, req.ID, tt.body)
			}))
			defer server.Close()
			_, err := newClient(t, testConfig(server.URL)).ChainID(context.Background())
			if tt.want != nil {
				if !errors.Is(err, tt.want) {
					t.Fatalf("error=%v", err)
				}
			} else {
				var rpc *RPCError
				if !errors.As(err, &rpc) || rpc.Code != tt.code {
					t.Fatalf("error=%v", err)
				}
			}
			if int(count.Load()) != tt.attempts || strings.Contains(err.Error(), "private") {
				t.Fatalf("attempts=%d error=%v", count.Load(), err)
			}
		})
	}
}

func TestRetryRecoveryAndRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		name, after string
		wantCalls   int
	}{
		{"recover", "", 2}, {"retry-after-zero", "0", 2},
		{"long-server-delay", "120", 1}, {"huge-server-delay", "9223372036854775807", 1},
		{"server-date", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat), 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req wireRequest
				json.NewDecoder(r.Body).Decode(&req)
				if calls.Add(1) == 1 {
					w.Header().Set("Retry-After", tt.after)
					w.WriteHeader(429)
					return
				}
				reply(w, req.ID, []byte(`"0x1"`))
			}))
			defer server.Close()
			id, err := newClient(t, testConfig(server.URL)).ChainID(context.Background())
			if calls.Load() != int32(tt.wantCalls) {
				t.Fatalf("calls=%d", calls.Load())
			}
			if tt.wantCalls == 2 && (err != nil || id != "1") {
				t.Fatalf("id=%s err=%v", id, err)
			}
			if tt.wantCalls == 1 && !errors.Is(err, outbound.ErrRetryDeferred) {
				t.Fatal("ignored server cooldown")
			}
		})
	}
}

func TestTimeoutAndCancellation(t *testing.T) {
	t.Run("request timeout", func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			calls.Add(1)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}))
		defer server.Close()
		defer close(release)
		config := testConfig(server.URL)
		config.RequestTimeout = 20 * time.Millisecond
		config.MaxAttempts = 2
		_, err := newClient(t, config).ChainID(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 2 {
			t.Fatalf("error=%v calls=%d", err, calls.Load())
		}
	})
	t.Run("cancel during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(429)
			cancel()
		}))
		defer server.Close()
		_, err := newClient(t, testConfig(server.URL)).ChainID(ctx)
		if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("error=%v calls=%d", err, calls.Load())
		}
	})
	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		c := newClient(t, testConfig("http://127.0.0.1:1/private-secret"))
		_, err := c.ChainID(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("cancel pacing wait", func(t *testing.T) {
		c := newClient(t, testConfig("http://127.0.0.1:1"))
		c.nextRequest = time.Now().Add(time.Hour)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := c.pace(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}

func TestResponseAndSnapshotBounds(t *testing.T) {
	t.Run("body limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat("x", 1001)) }))
		defer server.Close()
		config := testConfig(server.URL)
		config.MaxResponseBytes = 1000
		if _, err := newClient(t, config).ChainID(context.Background()); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatal(err)
		}
	})
	t.Run("aggregate", func(t *testing.T) {
		block := fixture(t, "block")
		f := serveFixtures(t, block, [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")})
		config := testConfig(f.server.URL)
		config.MaxResponseBytes = int64(len(block) + 100)
		config.MaxSnapshotBytes = config.MaxResponseBytes
		e, err := newClient(t, config).Block(context.Background(), fixtureNumber)
		if !errors.Is(err, ErrSnapshotTooLarge) || len(e.RawBlock) != 0 {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("null receipt", func(t *testing.T) {
		f := serveFixtures(t, fixture(t, "block"), [2][]byte{[]byte("null"), fixture(t, "receipt_1")})
		e, err := newClient(t, testConfig(f.server.URL)).Block(context.Background(), fixtureNumber)
		if !errors.Is(err, domain.ErrUnavailable) || len(e.RawBlock) != 0 {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestChainMismatchAndRedirects(t *testing.T) {
	t.Run("wrong chain", func(t *testing.T) {
		var count atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			var req wireRequest
			json.NewDecoder(r.Body).Decode(&req)
			reply(w, req.ID, []byte(`"0xaa36a7"`))
		}))
		defer server.Close()
		_, err := newClient(t, testConfig(server.URL)).Block(context.Background(), fixtureNumber)
		if !errors.Is(err, domain.ErrChainMismatch) || count.Load() != 1 {
			t.Fatalf("error=%v calls=%d", err, count.Load())
		}
	})
	t.Run("redirect blocked", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
		defer target.Close()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
		defer server.Close()
		_, err := newClient(t, testConfig(server.URL+"/private-secret")).ChainID(context.Background())
		if err == nil || targetCalls.Load() != 0 || strings.Contains(err.Error(), "private") {
			t.Fatalf("redirect error=%v calls=%d", err, targetCalls.Load())
		}
	})
}

func TestInvalidConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Config)
	}{
		{"missing URL", func(c *Config) { c.URL = "" }},
		{"bad URL", func(c *Config) { c.URL = "https://%private-secret" }},
		{"userinfo", func(c *Config) { c.URL = "https://user:private-secret@example.com" }},
		{"remote plain HTTP", func(c *Config) { c.URL = "http://example.com/private-secret" }},
		{"fragment", func(c *Config) { c.URL = "https://example.com/#private-secret" }},
		{"chain", func(c *Config) { c.ChainID = "" }},
		{"provider URL", func(c *Config) { c.Provider = "https://private-secret" }},
		{"timeout", func(c *Config) { c.RequestTimeout = 0 }},
		{"attempts", func(c *Config) { c.MaxAttempts = 0 }},
		{"initial delay", func(c *Config) { c.InitialBackoff = 0 }},
		{"max delay", func(c *Config) { c.MaxBackoff = 0 }},
		{"pacing", func(c *Config) { c.MinRequestInterval = -1 }},
		{"response size", func(c *Config) { c.MaxResponseBytes = 0 }},
		{"snapshot size", func(c *Config) { c.MaxSnapshotBytes = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testConfig("https://mainnet.infura.io/v3/private-secret")
			tt.change(&c)
			_, err := New(c, nil)
			if !errors.Is(err, ErrConfig) || strings.Contains(err.Error(), "private-secret") {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
