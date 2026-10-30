package jsonrpc

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Shared by the tagged smoke test and ordinary offline gate tests. No I/O.
func smokeSettings(get func(string) string) (Config, uint64, time.Duration, error) {
	var cfg Config
	if get("CI") != "" || get("GITHUB_ACTIONS") != "" || get("ETHEREUM_LIVE_MAINNET_OPT_IN") != "true" {
		return cfg, 0, 0, fmt.Errorf("live smoke requires explicit Mainnet opt-in outside CI")
	}
	endpoint := get("ETHEREUM_RPC_URL")
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || u.Scheme != "https" || u.Host != "mainnet.infura.io" || u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.Path, "/v3/") || strings.TrimPrefix(u.Path, "/v3/") == "" {
		return cfg, 0, 0, fmt.Errorf("live smoke requires a private Infura Mainnet HTTPS endpoint")
	}
	n, err := strconv.ParseUint(get("ETHEREUM_SMOKE_BLOCK"), 10, 64)
	if err != nil {
		return cfg, 0, 0, fmt.Errorf("ETHEREUM_SMOKE_BLOCK must be explicit decimal uint64")
	}
	max, err := strconv.ParseUint(get("ETHEREUM_SMOKE_MAX_REQUESTS"), 10, 64)
	if err != nil || max == 0 || max > 100 {
		return cfg, 0, 0, fmt.Errorf("ETHEREUM_SMOKE_MAX_REQUESTS must be between 1 and 100")
	}
	duration, err := time.ParseDuration(get("ETHEREUM_SMOKE_TIMEOUT"))
	if err != nil || duration < time.Millisecond || duration > 2*time.Minute {
		return cfg, 0, 0, fmt.Errorf("ETHEREUM_SMOKE_TIMEOUT must be between 1ms and 2m")
	}
	cfg = Config{URL: endpoint, ChainID: "1", Provider: "infura-mainnet", RequestTimeout: 15 * time.Second, MaxAttempts: 3, MaxRequests: max, InitialBackoff: 500 * time.Millisecond, MaxBackoff: 5 * time.Second, MinRequestInterval: 200 * time.Millisecond, MaxResponseBytes: 8 << 20, MaxSnapshotBytes: 64 << 20}
	return cfg, n, duration, nil
}

func TestLiveSmokeGateOffline(t *testing.T) {
	settings := map[string]string{"ETHEREUM_LIVE_MAINNET_OPT_IN": "true", "ETHEREUM_RPC_URL": "https://mainnet.infura.io/v3/offline-placeholder", "ETHEREUM_SMOKE_BLOCK": "42", "ETHEREUM_SMOKE_MAX_REQUESTS": "20", "ETHEREUM_SMOKE_TIMEOUT": "1m"}
	get := func(k string) string { return settings[k] }
	cfg, number, duration, err := smokeSettings(get)
	if err != nil || cfg.MaxRequests != 20 || cfg.ChainID != "1" || number != 42 || duration != time.Minute {
		t.Fatal("valid gate rejected", err)
	}
	for _, tc := range []struct{ k, v string }{
		{"CI", "true"}, {"GITHUB_ACTIONS", "true"}, {"CI", "false"},
		{"ETHEREUM_LIVE_MAINNET_OPT_IN", ""}, {"ETHEREUM_LIVE_MAINNET_OPT_IN", "false"},
		{"ETHEREUM_RPC_URL", ""}, {"ETHEREUM_RPC_URL", "http://mainnet.infura.io/v3/private-secret"}, {"ETHEREUM_RPC_URL", "https://other.invalid/v3/private-secret"}, {"ETHEREUM_RPC_URL", "https://private-secret@mainnet.infura.io/v3/key"},
		{"ETHEREUM_SMOKE_BLOCK", ""}, {"ETHEREUM_SMOKE_BLOCK", "-1"}, {"ETHEREUM_SMOKE_BLOCK", "18446744073709551616"},
		{"ETHEREUM_SMOKE_BLOCK", "latest"}, {"ETHEREUM_SMOKE_BLOCK", "pending"}, {"ETHEREUM_SMOKE_BLOCK", "safe"}, {"ETHEREUM_SMOKE_BLOCK", "finalized"}, {"ETHEREUM_SMOKE_BLOCK", "0xf4240"},
		{"ETHEREUM_SMOKE_MAX_REQUESTS", ""}, {"ETHEREUM_SMOKE_MAX_REQUESTS", "0"},
		{"ETHEREUM_SMOKE_MAX_REQUESTS", "101"}, {"ETHEREUM_SMOKE_MAX_REQUESTS", "1000"}, {"ETHEREUM_SMOKE_MAX_REQUESTS", "18446744073709551615"}, {"ETHEREUM_SMOKE_MAX_REQUESTS", "18446744073709551616"}, {"ETHEREUM_SMOKE_MAX_REQUESTS", "-1"},
		{"ETHEREUM_SMOKE_TIMEOUT", ""}, {"ETHEREUM_SMOKE_TIMEOUT", "0s"}, {"ETHEREUM_SMOKE_TIMEOUT", "3m"},
		{"ETHEREUM_SMOKE_TIMEOUT", "2m1ns"}, {"ETHEREUM_SMOKE_TIMEOUT", "999us"},
	} {
		t.Run(tc.k+tc.v, func(t *testing.T) {
			old := settings[tc.k]
			settings[tc.k] = tc.v
			defer func() { settings[tc.k] = old }()
			if _, _, _, err := smokeSettings(get); err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatal("unsafe smoke gate")
			}
		})
	}
	for _, tc := range []struct {
		requests uint64
		duration time.Duration
	}{
		{1, time.Millisecond},
		{100, 2 * time.Minute},
	} {
		t.Run(fmt.Sprintf("boundaries/%d/%s", tc.requests, tc.duration), func(t *testing.T) {
			settings["ETHEREUM_SMOKE_MAX_REQUESTS"] = strconv.FormatUint(tc.requests, 10)
			settings["ETHEREUM_SMOKE_TIMEOUT"] = tc.duration.String()
			settings["ETHEREUM_SMOKE_BLOCK"] = "1000000"
			cfg, number, duration, err := smokeSettings(get)
			if err != nil || cfg.MaxRequests != tc.requests || duration != tc.duration || number != 1000000 {
				t.Fatalf("boundary changed: requests=%d block=%d duration=%s err=%v", cfg.MaxRequests, number, duration, err)
			}
		})
	}
}

func TestLiveSmokeExplicitBlockOffline(t *testing.T) {
	settings := map[string]string{"ETHEREUM_LIVE_MAINNET_OPT_IN": "true", "ETHEREUM_RPC_URL": "https://mainnet.infura.io/v3/offline-placeholder", "ETHEREUM_SMOKE_BLOCK": strconv.FormatUint(fixtureNumber, 10), "ETHEREUM_SMOKE_MAX_REQUESTS": "100", "ETHEREUM_SMOKE_TIMEOUT": "2m"}
	cfg, number, _, err := smokeSettings(func(k string) string { return settings[k] })
	if err != nil {
		t.Fatal(err)
	}
	f := serveFixtures(t, fixture(t, "block"), [2][]byte{fixture(t, "receipt_0"), fixture(t, "receipt_1")})
	// Replace only the endpoint and pacing for this local, deterministic wire test.
	cfg.URL = f.server.URL
	cfg.MinRequestInterval = 0
	c := newClient(t, cfg)
	e, err := c.Block(t.Context(), number)
	if err != nil || e.Header.ID.Number != number || len(e.Receipts) != 2 || c.Requests() != 5 {
		t.Fatalf("explicit block acquisition failed: attempts=%d err=%v", c.Requests(), err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 5 {
		t.Fatalf("unexpected requests: %d", len(f.requests))
	}
	for i, method := range []string{"eth_chainId", "eth_getBlockByNumber", "eth_getTransactionReceipt", "eth_getTransactionReceipt", "eth_chainId"} {
		if f.requests[i].Method != method {
			t.Fatalf("request %d: got %s, want %s", i, f.requests[i].Method, method)
		}
	}
	params := f.requests[1].Params
	if len(params) != 2 || string(params[0]) != fmt.Sprintf("\"0x%x\"", number) || string(params[1]) != "true" {
		t.Fatalf("explicit block/full transactions replaced: %s", params)
	}
}
