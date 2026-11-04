// recovery_test.go exercises explicit bounded restart, replay, repair and reindex operator
// commands in the local operator tooling.

package main

import (
	"context"
	"strings"
	"testing"
)

// TestRecoveryCommandsOffline inspects bounded repair/replay/drain command construction without
// executing live RPC.
func TestRecoveryCommandsOffline(t *testing.T) {
	hash := strings.Repeat("a", 64)
	raw := "raw-block/v1/1/1000000/" + hash
	transformed := "transformed-block/v1/1/1000000/" + hash
	for _, args := range [][]string{{"replay", "config", raw, "repair-1", "3"}, {"repair", "config", "transformer", "inbox", raw, "repair-2", "3"}, {"repair", "config", "transformer", "outbox", transformed, "repair-3", "3"}, {"repair", "config", "loader", "inbox", transformed, "repair-4", "3"}} {
		r, err := parseRecovery(args)
		if err != nil {
			t.Fatal(err)
		}
		c := configuration(t)
		calls := 0
		err = run(t.Context(), args[0], c, nil, func(_ context.Context, a, env []string) ([]byte, error) {
			if containsArg(a, "run") {
				calls++
				joined := strings.Join(a, " ")
				if !strings.Contains(joined, "--no-deps") || !strings.Contains(joined, r.event) || !strings.Contains(joined, r.operation) {
					t.Fatal("missing isolation/audit")
				}
				if r.service == "extractor" && !strings.Contains(joined, "EXTRACTOR_RPC_URL=") {
					t.Fatal("replay can acquire")
				}
			}
			return nil, nil
		}, r)
		if err != nil || calls != 1 {
			t.Fatal(err, calls)
		}
	}
	for _, args := range [][]string{{"repair", "c", "loader", "outbox", transformed, "r", "1"}, {"repair", "c", "transformer", "inbox", transformed, "r", "1"}, {"replay", "c", raw, "r", "1001"}, {"reindex", "c", ""}} {
		if _, err := parseRecovery(args); err == nil {
			t.Fatal("invalid recovery accepted")
		}
	}
	c := configuration(t)
	c["EXTRACTOR_START_BLOCK"] = "1000000"
	c["EXTRACTOR_END_BLOCK"] = "1000000"
	if err := run(t.Context(), "drain", c, nil, func(_ context.Context, args, env []string) ([]byte, error) {
		if containsArg(args, "run") && !strings.Contains(strings.Join(args, " "), "EXTRACTOR_MODE=drain") {
			t.Fatal("not offline drain")
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := run(t.Context(), "reindex", c, nil, func(context.Context, []string, []string) ([]byte, error) { called = true; return nil, nil }, recoveryRequest{operation: "bounded-reindex"}); err == nil || called {
		t.Fatal("reindex bypasses live opt-in")
	}
}

// TestRestartNeverAcquiresAndRefusesActiveRun requires restart to touch only retained
// dependencies/consumers and refuse interfering with an active acquisition.
func TestRestartNeverAcquiresAndRefusesActiveRun(t *testing.T) {
	for _, active := range []bool{false, true} {
		c := configuration(t)
		stopped := false
		migrations := 0
		err := run(t.Context(), "restart", c, nil, func(_ context.Context, args, env []string) ([]byte, error) {
			if args[0] == "ps" && active {
				return []byte("running-container"), nil
			}
			if containsArg(args, "stop") {
				stopped = true
			}
			if containsArg(args, "run") {
				last := args[len(args)-1]
				if !strings.HasPrefix(last, "migrate-") {
					t.Fatal("restart invoked a service operation", last)
				}
				migrations++
			}
			return nil, nil
		})
		if active {
			if err == nil || stopped {
				t.Fatal("active acquisition interrupted", err)
			}
		} else if err != nil || !stopped || migrations != 3 {
			t.Fatal("incomplete restart", err, migrations)
		}
	}
}
