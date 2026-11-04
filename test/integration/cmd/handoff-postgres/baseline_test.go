// baseline_test.go exercises explicit opt-in benchmark configuration and isolated scenario
// orchestration in the offline test and diagnostic infrastructure.

package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestBaselineGuards rejects missing opt-in, unsafe outputs and unsupported/unbounded benchmark
// scenarios before resource creation.
func TestBaselineGuards(t *testing.T) {
	for _, args := range [][]string{{}, {"--opt-in", "--out", "x", "--workers", "3"}, {"--opt-in", "--out", "x", "--blocks", "0"}, {"--opt-in", "--out", "x", "--repeat", "0"}, {"--opt-in", "--out", "x", "--workers", "1,1"}} {
		if _, err := baselineFlags(args); err == nil {
			t.Fatal("invalid flags accepted")
		}
	}
	t.Setenv("CI", "true")
	if startBaseline(t.Context(), []string{"--opt-in", "--out", "must-not-create"}) == nil {
		t.Fatal("CI admitted")
	}
	if _, err := os.Stat("must-not-create"); !os.IsNotExist(err) {
		t.Fatal("CI guard wrote files")
	}
}

// TestBaselineRunnerNoRaceOrRegression verifies benchmark orchestration selects measurement
// binaries rather than accidentally running regressions or race instrumentation.
func TestBaselineRunnerNoRaceOrRegression(t *testing.T) {
	var phases []string
	err := runCase(t.Context(), t.TempDir(), nil, 15432, func(_ context.Context, c command) error {
		phases = append(phases, c.phase)
		if strings.Contains(strings.Join(c.args, " "), "-race") || c.expected == "TestHandoff" || c.expected == "TestSeedHandoff" {
			t.Fatal("regression path in performance mode")
		}
		return nil
	}, nil, &baselineRun{"private-spec", "baseline.test"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(phases, " "), "offline performance baseline") {
		t.Fatal(phases)
	}
}

// TestBaselineLoaderBatchSize checks the direct-write and supported bounded batch selections
// forwarded to benchmark processes.
func TestBaselineLoaderBatchSize(t *testing.T) {
	for _, v := range []string{"0", "257", "bad"} {
		if _, err := baselineFlags([]string{"--opt-in", "--out", "unused", "--loader-batch-size", v}); err == nil {
			t.Fatal("invalid batch size accepted")
		}
	}
	for _, v := range []int{1, 64, 256} {
		o, err := baselineFlags([]string{"--opt-in", "--out", "unused", "--loader-batch-size", strconv.Itoa(v)})
		if err != nil || o.loaderBatchSize != v {
			t.Fatal("batch tuning lost", err)
		}
	}
}
