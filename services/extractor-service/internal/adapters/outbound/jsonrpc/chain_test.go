// chain_test.go exercises provider head/header observations for ordered extraction and recovery in
// the outbound adapter layer.

package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// TestChainObservations checks numbered/head observation mapping through controlled RPC responses.
func TestChainObservations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		number uint64
		head   bool
	}{
		{"latest", fixtureNumber, true},
		{"numbered", fixtureNumber, false},
		{"genesis", 0, false},
		{"maximum height", math.MaxUint64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := mutate(t, fixture(t, "block"), func(o map[string]any) {
				o["number"] = fmt.Sprintf("0x%x", tc.number)
				if tc.number == 0 {
					o["parentHash"] = "0x" + strings.Repeat("00", 32)
				}
			})
			f := serveFixtures(t, raw, [2][]byte{})
			c := newClient(t, testConfig(f.server.URL))
			var h domain.Header
			var err error
			if tc.head {
				h, err = c.Head(context.Background())
			} else {
				h, err = c.Header(context.Background(), tc.number)
			}
			if err != nil {
				t.Fatal(err)
			}
			o, _ := objectOf(raw)
			id, _ := blockReference(o, "1", "number", "hash")
			parent, _ := hashField(o, "parentHash")
			timestamp, _ := uintField(o, "timestamp")
			if h != (domain.Header{ID: id, ParentHash: parent, Timestamp: timestamp}) {
				t.Fatalf("unexpected header: %+v", h)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.requests) != 3 || f.requests[0].Method != "eth_chainId" || f.requests[2].Method != "eth_chainId" {
				t.Fatalf("expected chain checks around one header call: %+v", f.requests)
			}
			tag := fmt.Sprintf("0x%x", tc.number)
			if tc.head {
				tag = "latest"
			}
			request := f.requests[1]
			if request.Method != "eth_getBlockByNumber" || len(request.Params) != 2 || string(request.Params[0]) != fmt.Sprintf("%q", tag) || string(request.Params[1]) != "false" {
				t.Fatalf("unexpected header request: %+v", request)
			}
		})
	}
}

// TestHeaderRejectsUnavailableOrInvalidIdentity rejects null or malformed numbered headers instead
// of fabricating usable ancestry.
func TestHeaderRejectsUnavailableOrInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"missing hash", func(o map[string]any) { delete(o, "hash") }},
		{"zero hash", func(o map[string]any) { o["hash"] = "0x" + strings.Repeat("00", 32) }},
		{"null height", func(o map[string]any) { o["number"] = nil }},
		{"wrong height", func(o map[string]any) { o["number"] = "0x1" }},
		{"overflow", func(o map[string]any) { o["number"] = "0x10000000000000000" }},
		{"missing parent", func(o map[string]any) { delete(o, "parentHash") }},
		{"zero non-genesis parent", func(o map[string]any) { o["parentHash"] = "0x" + strings.Repeat("00", 32) }},
		{"missing timestamp", func(o map[string]any) { delete(o, "timestamp") }},
		{"noncanonical timestamp", func(o map[string]any) { o["timestamp"] = "0x00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := serveFixtures(t, mutate(t, fixture(t, "block"), tc.change), [2][]byte{})
			c := newClient(t, testConfig(f.server.URL))
			h, err := c.Header(context.Background(), fixtureNumber)
			if !errors.Is(err, domain.ErrInvalidData) || h != (domain.Header{}) {
				t.Fatalf("header=%+v error=%v", h, err)
			}
		})
	}
	for _, tc := range []struct {
		name, raw string
		want      error
	}{
		{"missing block", "null", domain.ErrUnavailable},
		{"duplicate field", `{"number":"0x1","number":"0x2"}`, ErrProtocol},
		{"non-object", `[]`, domain.ErrInvalidData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := serveFixtures(t, []byte(tc.raw), [2][]byte{})
			c := newClient(t, testConfig(f.server.URL))
			for _, head := range []bool{false, true} {
				var h domain.Header
				var err error
				if head {
					h, err = c.Head(context.Background())
				} else {
					h, err = c.Header(context.Background(), fixtureNumber)
				}
				if !errors.Is(err, tc.want) || h != (domain.Header{}) {
					t.Fatalf("header=%+v error=%v", h, err)
				}
			}
		})
	}
}

// TestHeadRechecksChainAndPropagatesCancellation requires head observation to preserve
// chain/cancellation validation across its RPC calls.
func TestHeadRechecksChainAndPropagatesCancellation(t *testing.T) {
	raw := fixture(t, "block")
	var checks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request wireRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Method == "eth_chainId" {
			chain := `"0x1"`
			if checks.Add(1) > 1 {
				chain = `"0x2"`
			}
			reply(w, request.ID, []byte(chain))
			return
		}
		reply(w, request.ID, raw)
	}))
	defer server.Close()
	c := newClient(t, testConfig(server.URL))
	if h, err := c.Head(context.Background()); !errors.Is(err, domain.ErrChainMismatch) || h != (domain.Header{}) {
		t.Fatalf("chain switch: header=%+v error=%v", h, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Head(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled head: %v", err)
	}
	if _, err := c.Header(ctx, fixtureNumber); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled header: %v", err)
	}
	if checks.Load() != 2 {
		t.Fatal("cancelled calls reached provider")
	}
}
