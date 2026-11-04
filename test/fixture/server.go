// server.go implements loopback HTTP routes and controlled local server lifecycle in the offline
// test and diagnostic infrastructure.

package fixture

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// State captures deterministic fixture head/branch and accepted-ACK fault counters.
type State struct {
	Head                  uint64 `json:"head"`
	Fork                  bool   `json:"fork"`
	RawLost               int    `json:"raw_lost"`
	TransformedLost       int    `json:"transformed_lost"`
	RawDuplicates         int    `json:"raw_duplicates"`
	TransformedDuplicates int    `json:"transformed_duplicates"`
	Changed               bool   `json:"changed_event"`
}

// Server owns synchronized synthetic chain and fault state; it is local test infrastructure, not
// an Ethereum provider.
type Server struct {
	mu                       sync.Mutex
	state                    State
	dropRaw, dropTransformed bool
	seen                     map[string][32]byte
}

// New creates a deterministic local chain/control state without contacting Ethereum.
func New(head uint64) *Server {
	return &Server{state: State{Head: head}, seen: make(map[string][32]byte)}
}

// Snapshot copies fixture control state under its lock so callers cannot mutate the live server
// state.
func (s *Server) Snapshot() State { s.mu.Lock(); defer s.mu.Unlock(); return s.state }

// Handler registers the local fixture RPC/control handlers without starting an HTTP listener.
func (s *Server) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	m.HandleFunc("GET /control", func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(s.Snapshot()) })
	m.HandleFunc("POST /control", s.control)
	m.HandleFunc("POST /", s.rpc)
	return m
}

// control validates local fault/head/branch controls before mutating fixture state; invalid
// requests do not silently change the chain.
func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	var c struct {
		Head            *uint64 `json:"head"`
		Fork            *bool   `json:"fork"`
		DropRaw         bool    `json:"drop_raw"`
		DropTransformed bool    `json:"drop_transformed"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || c.Head != nil && (*c.Head < Start || *c.Head > Last) {
		http.Error(w, "invalid control", 400)
		return
	}
	s.mu.Lock()
	if c.Head != nil {
		s.state.Head = *c.Head
	}
	if c.Fork != nil {
		s.state.Fork = *c.Fork
	}
	if c.DropRaw {
		s.dropRaw = true
	}
	if c.DropTransformed {
		s.dropTransformed = true
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// rpc serves the supported synthetic Ethereum methods and configured failures, without contacting
// a real provider.
func (s *Server) rpc(w http.ResponseWriter, r *http.Request) {
	var q struct {
		Version string            `json:"jsonrpc"`
		ID      json.RawMessage   `json:"id"`
		Method  string            `json:"method"`
		Params  []json.RawMessage `json:"params"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&q) != nil || q.Version != "2.0" {
		http.Error(w, "invalid RPC", 400)
		return
	}
	state := s.Snapshot()
	var result any
	code := 0
	switch q.Method {
	case "eth_chainId":
		result = "0x1"
	case "eth_getBlockByNumber":
		var tag string
		var full bool
		if len(q.Params) != 2 || json.Unmarshal(q.Params[0], &tag) != nil || json.Unmarshal(q.Params[1], &full) != nil {
			code = -32602
			break
		}
		n := state.Head
		if tag != "latest" {
			var e error
			n, e = strconv.ParseUint(strings.TrimPrefix(tag, "0x"), 16, 64)
			if e != nil || !strings.HasPrefix(tag, "0x") {
				code = -32602
				break
			}
		}
		if n >= Start && n <= state.Head {
			result, _ = Block(n, state.Fork, full)
		}
	case "eth_getTransactionReceipt":
		var hash string
		if len(q.Params) != 1 || json.Unmarshal(q.Params[0], &hash) != nil {
			code = -32602
			break
		}
		for n := Start; n <= state.Head; n++ {
			for i := 0; i < 2; i++ {
				if hash == TxHash(n, state.Fork, i) {
					_, receipts := Block(n, state.Fork, true)
					result = receipts[i]
				}
			}
		}
	default:
		code = -32601
	}
	w.Header().Set("Content-Type", "application/json")
	response := map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": result}
	if code != 0 {
		delete(response, "result")
		response["error"] = map[string]any{"code": code, "message": "fixture RPC error"}
	}
	_ = json.NewEncoder(w).Encode(response)
}
