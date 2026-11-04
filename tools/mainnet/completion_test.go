// completion_test.go exercises bounded Mainnet target validation and no-RPC durable completion
// inspection in the local operator tooling.

package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/etlverify"
)

// TestLiveE2EGuards checks the private final workflow restricts chain, block, request and duration
// bounds before execution.
func TestLiveE2EGuards(t *testing.T) {
	c := configuration(t)
	for k, v := range map[string]string{"EXTRACTOR_START_BLOCK": "1000000", "EXTRACTOR_END_BLOCK": "1000000", "EXTRACTOR_MAX_RPC_REQUESTS": "100", "EXTRACTOR_RUN_TIMEOUT": "2m", "EXTRACTOR_MAINNET_OPT_IN": "true", "EXTRACTOR_CONFIRMATION_DEPTH": "0", "EXTRACTOR_RECOVERY_DEPTH": "1", "EXTRACTOR_RPC_URL": "https://example.invalid/secret"} {
		c[k] = v
	}
	for _, tc := range []struct{ k, v string }{{"EXTRACTOR_START_BLOCK", ""}, {"EXTRACTOR_END_BLOCK", "1000001"}, {"EXTRACTOR_MAX_RPC_REQUESTS", "101"}, {"EXTRACTOR_RUN_TIMEOUT", "2m1ns"}, {"MAINNET_VERIFY_TIMEOUT", "2m1ns"}, {"EXTRACTOR_MAINNET_OPT_IN", "false"}} {
		previous := c[tc.k]
		c[tc.k] = tc.v
		called := false
		if err := run(t.Context(), "e2e", c, nil, func(context.Context, []string, []string) ([]byte, error) { called = true; return nil, nil }); err == nil || called {
			t.Fatal("unsafe live command admitted", tc.k)
		}
		c[tc.k] = previous
	}
	if _, err := completionBounds(c, true, []string{"CI=false"}); err == nil {
		t.Fatal("CI bypass")
	}
	if _, err := completionBounds(c, false, []string{"CI=true"}); err != nil {
		t.Fatal("read-only verifier incorrectly gated", err)
	}
}

// TestCompletionCommandsUseOwnRolesAndNoRPC inspects operator commands for per-schema runtime
// credentials and absence of new Ethereum acquisition.
func TestCompletionCommandsUseOwnRolesAndNoRPC(t *testing.T) {
	c := configuration(t)
	c["EXTRACTOR_START_BLOCK"] = "1000000"
	c["EXTRACTOR_END_BLOCK"] = "1000000"
	seen := map[string]bool{}
	err := run(t.Context(), "verify", c, nil, func(ctx context.Context, args, env []string) ([]byte, error) {
		if containsArg(args, "run") || containsArg(args, "up") {
			t.Fatal("verification started acquisition")
		}
		if !containsArg(args, "psql") {
			return nil, nil
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 3*time.Second {
			t.Fatal("unbounded observation")
		}
		var service, sql string
		for i, a := range args {
			if a == "-U" {
				service = strings.TrimSuffix(args[i+1], "_runtime")
			}
			if a == "-c" {
				sql = args[i+1]
			}
		}
		if !strings.Contains(sql, "BEGIN READ ONLY") {
			t.Fatal("missing read-only guard")
		}
		found := false
		for _, e := range env {
			if e == "PGPASSWORD="+c[strings.ToUpper(service)+"_RUNTIME_PASSWORD"] {
				found = true
			}
		}
		if !found {
			t.Fatal("incorrect role secret")
		}
		for k, v := range c {
			if strings.HasSuffix(k, "_PASSWORD") && strings.Contains(strings.Join(args, " "), v) {
				t.Fatal("credential in arguments")
			}
		}
		seen[service] = true
		states := map[string]string{}
		if service != "extractor" {
			states["inbox"] = "completed"
		}
		if service != "loader" {
			states["outbox"] = "delivered"
		}
		return json.Marshal(etlverify.Snapshot{Ready: true, States: states, RawDigest: "r", PayloadDigest: "p", Provenance: "a", Members: []string{"tx1", "tx2"}})
	})
	if err != nil || len(seen) != 3 {
		t.Fatal("completion failed", err)
	}
}

// TestE2EAcquisitionFailureIsNotRetried inspects final operator orchestration to prevent
// automatically starting a second live acquisition after failure.
func TestE2EAcquisitionFailureIsNotRetried(t *testing.T) {
	c := configuration(t)
	for k, v := range map[string]string{"EXTRACTOR_START_BLOCK": "1000000", "EXTRACTOR_END_BLOCK": "1000000", "EXTRACTOR_MAX_RPC_REQUESTS": "100", "EXTRACTOR_RUN_TIMEOUT": "2m", "EXTRACTOR_MAINNET_OPT_IN": "true", "EXTRACTOR_CONFIRMATION_DEPTH": "0", "EXTRACTOR_RECOVERY_DEPTH": "1", "EXTRACTOR_RPC_URL": "https://example.invalid/private-secret"} {
		c[k] = v
	}
	calls := 0
	err := run(t.Context(), "e2e", c, nil, func(ctx context.Context, args, env []string) ([]byte, error) {
		if containsArg(args, "psql") {
			t.Fatal("success verification after failed acquisition")
		}
		if containsArg(args, "run") {
			calls++
			return []byte("private-secret"), context.DeadlineExceeded
		}
		return nil, nil
	})
	if err == nil || calls != 1 || strings.Contains(err.Error(), "private-secret") {
		t.Fatal("unsafe acquisition failure", err, calls)
	}
}
