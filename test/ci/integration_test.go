// integration_test.go checks missing/skipped integration PASS markers through a controlled shell
// runner in the CI orchestration tests; it does not open a database or start Docker.

// Package ci_test checks required integration-gate execution markers with controlled child
// commands; these tests do not start PostgreSQL.
package ci_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestIntegrationGate checks the CI runner cannot silently skip required execution.
//
// Exercise the real shell gate with a fake runner: no Docker or live RPC.
func TestIntegrationGate(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repository")
	}
	script := filepath.Join(filepath.Dir(file), "../../scripts/ci-integration.sh")
	for _, tc := range []struct {
		name, suite, output, exit, storage string
		pass                               bool
	}{
		{"extractor", "extractor-postgres", "--- PASS: TestPostgresIntegration (1s)\n", "0", "", true},
		{"transformer", "transformer-postgres", "--- PASS: TestPostgresIntegration (1s)\n", "0", "", true},
		{"loader", "loader-postgres", "--- PASS: TestPostgresIntegration (1s)\n", "0", "", true},
		{"handoff", "handoff-postgres", "--- PASS: TestSeedHandoff (1s)\n--- PASS: TestHandoff (1s)\n", "0", "", true},
		{"fixture", "fixture-e2e", "--- PASS: TestFixtureE2E (1s)\n", "0", "", true},
		{"fixture storage enabled", "fixture-e2e", "--- PASS: TestFixtureE2E (1s)\n    --- PASS: TestFixtureE2E/storage_release (1s)\n", "0", "1", true},
		{"fixture storage missing", "fixture-e2e", "--- PASS: TestFixtureE2E (1s)\n", "0", "1", false},
		{"storage", "storage-release", "--- PASS: TestFixtureE2E (1s)\n    --- PASS: TestFixtureE2E/storage_release (1s)\n", "0", "", true},
		{"missing storage", "storage-release", "--- PASS: TestFixtureE2E (1s)\n", "0", "", false},
		{"skip", "extractor-postgres", "--- SKIP: TestPostgresIntegration (0s)\nPASS\n", "0", "", false},
		{"subtest skip", "extractor-postgres", "    --- SKIP: TestPostgresIntegration/case (0s)\n--- PASS: TestPostgresIntegration (1s)\n", "0", "", false},
		{"no tests", "extractor-postgres", "testing: warning: no tests to run\nPASS\n", "0", "", false},
		{"wrong test", "fixture-e2e", "--- PASS: TestHandoff (1s)\n", "0", "", false},
		{"missing seed", "handoff-postgres", "--- PASS: TestSeedHandoff (1s)\n", "0", "", false},
		{"runner failure after pass", "extractor-postgres", "--- PASS: TestPostgresIntegration (1s)\ncleanup failed\n", "1", "", false},
		{"unavailable Docker", "extractor-postgres", "cannot connect to Docker daemon\n", "1", "", false},
		{"invalid suite", "unknown", "", "0", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := t.TempDir()
			fake := "#!/bin/sh\nprintf '%s' \"$CI_GATE_TEST_OUTPUT\"\nexit \"$CI_GATE_TEST_EXIT\"\n"
			if err := os.WriteFile(filepath.Join(bin, "go"), []byte(fake), 0700); err != nil {
				t.Fatal(err)
			}
			// Allow process startup on busy CI hosts; timeout is always a test
			// failure, including cases that expect the gate to reject a runner.
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", script, tc.suite)
			cmd.WaitDelay = 3 * time.Second
			// The final offline wrapper enables storage verification globally.
			// Each fake-runner case must select its own expected gate contract.
			for _, entry := range os.Environ() {
				if !strings.HasPrefix(entry, "PATH=") && !strings.HasPrefix(entry, "CI_GATE_TEST_") && !strings.HasPrefix(entry, "ETL_STORAGE_VERIFY=") {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "CI_GATE_TEST_OUTPUT="+tc.output, "CI_GATE_TEST_EXIT="+tc.exit, "ETL_STORAGE_VERIFY="+tc.storage)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("gate timed out: %v\n%s", ctx.Err(), output)
			}
			if (err == nil) != tc.pass {
				t.Fatalf("expected pass=%v, err=%v\n%s", tc.pass, err, output)
			}
		})
	}
}
