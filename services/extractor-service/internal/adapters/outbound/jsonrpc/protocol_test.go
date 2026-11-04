// protocol_test.go exercises JSON-RPC envelope integrity and safe transport errors in the outbound
// adapter layer.

package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// TestInvalidEnvelopes checks malformed JSON-RPC envelope versions, identifiers and result/error
// combinations fail acquisition.
func TestInvalidEnvelopes(t *testing.T) {
	for _, body := range []string{
		`{`, `[]`, `null`,
		`{"jsonrpc":"1.0","id":1,"result":"0x1"}`,
		`{"jsonrpc":"2.0","id":2,"result":"0x1"}`,
		`{"jsonrpc":"2.0","id":"1","result":"0x1"}`,
		`{"jsonrpc":"2.0","id":null,"result":"0x1"}`,
		`{"jsonrpc":"2.0","id":1,"result":"0x1"} {}`,
		`{"jsonrpc":"2.0","id":1,"result":{"nested":{"x":1,"x":2}}}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":null}}`,
		`{"jsonrpc":"2.0","id":1,"result":` + strings.Repeat("[", 65) + `0` + strings.Repeat("]", 65) + `}`,
	} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			_, err := newClient(t, testConfig(server.URL)).ChainID(context.Background())
			if !errors.Is(err, ErrProtocol) || calls.Load() != 1 {
				t.Fatalf("error=%v calls=%d", err, calls.Load())
			}
		})
	}
}

// failingTransport counts injected transport failures without making external HTTP requests.
type failingTransport struct{ calls atomic.Int32 }

// RoundTrip implements the test HTTP transport with controlled responses/failures; it never
// selects a live Ethereum endpoint.
func (f *failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	f.calls.Add(1)
	return nil, errors.New("private-secret in transport error")
}

// TestTransportFailureRedaction checks wrapped HTTP/transport failures preserve safe
// classification without private URL details.
func TestTransportFailureRedaction(t *testing.T) {
	transport := &failingTransport{}
	c, err := New(testConfig("https://example.com/v3/private-secret"), &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ChainID(context.Background())
	if !errors.Is(err, ErrTransport) || strings.Contains(err.Error(), "private") || transport.calls.Load() != 3 {
		t.Fatalf("error=%v attempts=%d", err, transport.calls.Load())
	}
}

// TestChainChangeDuringSnapshot changes the reported chain between RPC calls and rejects the
// mixed-source snapshot.
func TestChainChangeDuringSnapshot(t *testing.T) {
	f := serveFixtures(t, fixture(t, "block"), [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")})
	var chainCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req wireRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		if req.Method == "eth_chainId" && chainCalls.Add(1) == 2 {
			reply(w, req.ID, []byte(`"0x2"`))
			return
		}
		payload, _ := json.Marshal(req)
		// Reuse the controlled fixture handler; no second network request.
		forward := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
		forward.Header.Set("Content-Type", "application/json")
		f.server.Config.Handler.ServeHTTP(w, forward)
	}))
	defer proxy.Close()
	e, err := newClient(t, testConfig(proxy.URL)).Block(context.Background(), fixtureNumber)
	if !errors.Is(err, domain.ErrChainMismatch) || len(e.RawBlock) != 0 || chainCalls.Load() != 2 {
		t.Fatalf("error=%v chain checks=%d", err, chainCalls.Load())
	}
}

// TestConcurrentRequestsHaveUniqueIDsAndPacing checks unique JSON-RPC request IDs and one pacing
// gate under concurrent callers.
func TestConcurrentRequestsHaveUniqueIDsAndPacing(t *testing.T) {
	const count = 5
	var mu sync.Mutex
	ids := make(map[uint64]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req wireRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		if ids[req.ID] {
			t.Error("duplicate request ID")
		}
		ids[req.ID] = true
		mu.Unlock()
		reply(w, req.ID, []byte(`"0x1"`))
	}))
	defer server.Close()
	config := testConfig(server.URL)
	config.MinRequestInterval = 10 * time.Millisecond
	c := newClient(t, config)
	start := time.Now()
	var group sync.WaitGroup
	for range count {
		group.Go(func() {
			if _, err := c.ChainID(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != count || time.Since(start) < (count-1)*config.MinRequestInterval {
		t.Fatal("requests were lost or pacing was bypassed")
	}
}
