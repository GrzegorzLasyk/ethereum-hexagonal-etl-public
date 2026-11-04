// server_test.go exercises loopback HTTP routes and controlled local server lifecycle in the
// offline test and diagnostic infrastructure.

package fixture

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	etlv1 "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/gen/etl/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// request builds a valid deterministic application/transport request so one invariant can be
// varied per case.
func request(t *testing.T, s *Server, path, body string) []byte {
	t.Helper()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", path, bytes.NewBufferString(body)))
	if w.Code >= 300 {
		t.Fatalf("request failed: %d", w.Code)
	}
	return w.Body.Bytes()
}

// TestDeterministicChainAndReorg checks reproducible local blocks and a controlled branch switch
// without external Ethereum data.
func TestDeterministicChainAndReorg(t *testing.T) {
	s := New(103)
	read := func(tag string, full bool) json.RawMessage {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 7, "method": "eth_getBlockByNumber", "params": []any{tag, full}})
		var response struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
		}
		if json.Unmarshal(request(t, s, "/", string(body)), &response) != nil || response.ID != 7 {
			t.Fatal("invalid envelope")
		}
		return response.Result
	}
	a := read("0x66", true)
	if !bytes.Equal(a, read("0x66", true)) {
		t.Fatal("non-deterministic fixture")
	}
	if string(read("0x68", true)) != "null" {
		t.Fatal("future block available")
	}
	request(t, s, "/control", `{"fork":true,"head":104}`)
	b := read("0x66", true)
	if bytes.Equal(a, b) {
		t.Fatal("fork did not change identity")
	}
	var block struct {
		Hash, ParentHash string
		Transactions     []struct{ Hash, BlockHash, BlockNumber string }
	}
	if json.Unmarshal(b, &block) != nil || block.Hash != Hash(102, true) || block.ParentHash != Hash(101, false) || len(block.Transactions) != 2 {
		t.Fatal("fork is not anchored at retained ancestor")
	}
	for _, tx := range block.Transactions {
		if tx.BlockHash != block.Hash || tx.BlockNumber != "0x66" {
			t.Fatal("transaction membership")
		}
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 8, "method": "eth_getTransactionReceipt", "params": []string{tx.Hash}})
		var receipt struct {
			Result struct{ BlockHash, TransactionHash string }
		}
		if json.Unmarshal(request(t, s, "/", string(body)), &receipt) != nil || receipt.Result.BlockHash != block.Hash || receipt.Result.TransactionHash != tx.Hash {
			t.Fatal("receipt membership")
		}
	}
	var header struct{ Transactions []string }
	if json.Unmarshal(read("latest", false), &header) != nil || len(header.Transactions) != 2 {
		t.Fatal("header request must return transaction hashes")
	}
}

// TestInvalidControlAndUnknownRPC rejects unsupported local fixture controls/methods instead of
// silently mutating state or contacting a provider.
func TestInvalidControlAndUnknownRPC(t *testing.T) {
	s := New(100)
	for _, body := range []string{`{"head":99}`, `{"head":111}`, `{"unknown":true}`, `invalid`} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/control", bytes.NewBufferString(body)))
		if w.Code != 400 || s.Snapshot().Head != 100 {
			t.Fatal("invalid control changed state")
		}
	}
	var response struct{ Error struct{ Code int } }
	if json.Unmarshal(request(t, s, "/", `{"jsonrpc":"2.0","id":1,"method":"unknown","params":[]}`), &response) != nil || response.Error.Code != -32601 {
		t.Fatal("unknown RPC silently accepted")
	}
}

// rawClient simulates durable receipt/failure for the fixture ACK-loss proxy.
type rawClient struct {
	fail     bool
	accepted int
}

// SubmitRawBlock implements the controlled gRPC peer/client callback so publication and ACK
// failures can be tested deterministically.
func (c *rawClient) SubmitRawBlock(_ context.Context, _ *etlv1.SubmitRawBlockRequest, _ ...grpc.CallOption) (*etlv1.SubmitRawBlockResponse, error) {
	if c.fail {
		return nil, status.Error(codes.Unavailable, "receiver offline")
	}
	c.accepted++
	s := etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_DURABLY_ACCEPTED
	if c.accepted > 1 {
		s = etlv1.AcceptanceStatus_ACCEPTANCE_STATUS_ALREADY_ACCEPTED
	}
	return &etlv1.SubmitRawBlockResponse{Acceptance: &etlv1.DurableAcceptance{Status: s}}, nil
}

// TestLostACKRequiresRealAcceptance requires the proxy to forward and receive durable acceptance
// before simulating response loss.
func TestLostACKRequiresRealAcceptance(t *testing.T) {
	s := New(100)
	request(t, s, "/control", `{"drop_raw":true}`)
	c := &rawClient{fail: true}
	p := &RawProxy{Fixture: s, Client: c}
	r := &etlv1.SubmitRawBlockRequest{}
	if _, err := p.SubmitRawBlock(t.Context(), r); err == nil || s.Snapshot().RawLost != 0 {
		t.Fatal("fault consumed before receiver accepted")
	}
	c.fail = false
	if _, err := p.SubmitRawBlock(t.Context(), r); status.Code(err) != codes.Unavailable || c.accepted != 1 || s.Snapshot().RawLost != 1 {
		t.Fatal("ACK must be lost after acceptance")
	}
	if _, err := p.SubmitRawBlock(t.Context(), r); err != nil || s.Snapshot().RawDuplicates != 1 || s.Snapshot().Changed {
		t.Fatal("duplicate did not recover")
	}
}
