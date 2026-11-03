//go:build live

package jsonrpc

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveMainnetBlock is opt-in, uses provider credits, and never persists data.
// These explicit budgets are for this smoke test, not service defaults.
func TestLiveMainnetBlock(t *testing.T) {
	cfg, number, duration, err := smokeSettings(os.Getenv)
	if err != nil {
		t.Fatal(err)
	} // Explicit invocation must fail, never silently skip.
	c, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	started := time.Now()
	defer func() { t.Logf("rpc_attempts=%d duration_ms=%d", c.Requests(), time.Since(started).Milliseconds()) }()
	e, err := c.Block(ctx, number)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Logf("chain=%s block=%d hash=%x transactions=%d receipts=%d",
		e.Header.ID.ChainID, e.Header.ID.Number, e.Header.ID.Hash, len(e.Transactions), len(e.Receipts))
}
