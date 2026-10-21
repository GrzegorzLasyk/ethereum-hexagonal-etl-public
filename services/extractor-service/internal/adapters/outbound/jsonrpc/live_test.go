//go:build live

package jsonrpc

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestLiveMainnetBlock is opt-in, uses provider credits, and never persists data.
// These explicit budgets are for this smoke test, not service defaults.
func TestLiveMainnetBlock(t *testing.T) {
	endpoint := os.Getenv("ETHEREUM_RPC_URL")
	if endpoint == "" {
		t.Skip("set ETHEREUM_RPC_URL to enable the live smoke test")
	}
	number, err := strconv.ParseUint(os.Getenv("ETHEREUM_SMOKE_BLOCK"), 10, 64)
	if err != nil {
		t.Fatal("ETHEREUM_SMOKE_BLOCK must be an explicit decimal block number")
	}
	c, err := New(Config{
		URL: endpoint, ChainID: "1", Provider: "infura-mainnet",
		RequestTimeout: 15 * time.Second, MaxAttempts: 3,
		InitialBackoff: 500 * time.Millisecond, MaxBackoff: 5 * time.Second,
		MinRequestInterval: 200 * time.Millisecond,
		MaxResponseBytes:   8 << 20, MaxSnapshotBytes: 64 << 20,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
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
