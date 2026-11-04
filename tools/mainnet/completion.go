// completion.go implements bounded Mainnet target validation and no-RPC durable completion
// inspection in the local operator tooling.

// Package main implements private bounded Mainnet setup, preflight, acquisition, no-RPC completion
// inspection and audited recovery commands; acquisition requires explicit opt-in.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/etlverify"
)

// completionBounds checks the fixed one-block live verification range and finite operation
// deadlines before building commands.
func completionBounds(c map[string]string, live bool, parent []string) (time.Duration, error) {
	if c["EXTRACTOR_START_BLOCK"] != "1000000" || c["EXTRACTOR_END_BLOCK"] != "1000000" {
		return 0, errors.New("Stage 13.4A requires explicit range 1000000..1000000")
	}
	raw := c["MAINNET_VERIFY_TIMEOUT"]
	if raw == "" {
		raw = "2m"
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout < time.Millisecond || timeout > 2*time.Minute {
		return 0, errors.New("MAINNET_VERIFY_TIMEOUT must be 1ms..2m")
	}
	if live {
		for _, e := range parent {
			k, v, _ := strings.Cut(e, "=")
			if (k == "CI" || k == "GITHUB_ACTIONS") && v != "" {
				return 0, errors.New("live E2E is forbidden in CI")
			}
		}
		requests, err := strconv.ParseUint(c["EXTRACTOR_MAX_RPC_REQUESTS"], 10, 64)
		duration, e := time.ParseDuration(c["EXTRACTOR_RUN_TIMEOUT"])
		if err != nil || e != nil || requests < 1 || requests > 100 || duration < time.Millisecond || duration > 2*time.Minute {
			return 0, errors.New("live E2E requires 1..100 RPC attempts and at most 2m acquisition including drain")
		}
	}
	return timeout, nil
}

// The operator holds three credentials, but each observation authenticates as
// exactly one runtime role and queries only that role's schema. Never a service.
func verifyCompletion(ctx context.Context, c map[string]string, parent, base []string, invoke executor, timeout time.Duration) error {
	query := func(ctx context.Context, service, sql string) ([]byte, error) {
		env := environment(parent, map[string]string{})
		// Compose interpolation still needs generated credentials; the psql process
		// authenticates with its owning runtime credential via environment, never argv.
		env = environment(env, c)
		env = append(env, "PGPASSWORD="+c[strings.ToUpper(service)+"_RUNTIME_PASSWORD"])
		args := append(append([]string{}, base...), "exec", "-T", "-e", "PGPASSWORD", "postgres", "psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-U", service+"_runtime", "-d", "ethereum_etl", "-c", "BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms'; "+sql+"; COMMIT;")
		return invoke(ctx, args, env)
	}
	report, err := etlverify.Wait(ctx, query, etlverify.MainnetBlock, timeout, 250*time.Millisecond)
	fmt.Fprintln(os.Stdout, report.Summary())
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "PASS: Mainnet block 1000000 complete; raw/checkpoint, both durable handoffs, 2 transactions, 2 receipts, 1 log, canonical_status=unknown")
	return nil
}
