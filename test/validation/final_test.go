// final_test.go exercises offline/performance/live phase guards using controlled child commands in
// the offline test and diagnostic infrastructure.

// Package validation_test provides offline/performance/live phase guards using controlled child
// commands in the offline test and diagnostic infrastructure.
package validation_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestFinalValidationPhaseIsolation checks explicit offline/performance/live phase separation.
//
// Only shell orchestration runs. Every Go/Docker-capable command is a local stub.
func TestFinalValidationPhaseIsolation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		ci, fail string
		ok       bool
		want     string
	}{
		{"missing opt in", []string{"offline", "fixtures"}, "", "", false, ""},
		{"unknown", []string{"all", "--opt-in", "fixtures"}, "", "", false, ""},
		{"offline", []string{"offline", "--opt-in", "fixtures"}, "", "", true, "offline 1 1 1 fixtures"},
		{"performance", []string{"performance", "--opt-in", "new-report", "--dataset", "synthetic"}, "", "", true, "--baseline --opt-in --out new-report"},
		{"live CI", []string{"live", "--opt-in", "private.env"}, "true", "", false, ""},
		{"live", []string{"live", "--opt-in", "private.env"}, "", "", true, "./tools/mainnet verify private.env"},
		{"live failed", []string{"live", "--opt-in", "private.env"}, "", "e2e", false, "./tools/mainnet e2e private.env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			scripts := filepath.Join(root, "scripts")
			bin := filepath.Join(root, "bin")
			if err := os.MkdirAll(scripts, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(bin, 0700); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile("../../scripts/validate-final.sh")
			if err != nil {
				t.Fatal(err)
			}
			write := func(path, content string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(content), 0700); err != nil {
					t.Fatal(err)
				}
			}
			write(filepath.Join(scripts, "validate-final.sh"), string(data))
			write(filepath.Join(scripts, "validate-stage13.sh"), `#!/usr/bin/env bash
printf 'offline %s %s %s %s\n' "$ETL_FINAL_VALIDATION" "$ETL_STORAGE_VERIFY" "$ETL_TEST_RETAIN" "$1" >> "$CALL_LOG"
`)
			write(filepath.Join(bin, "go"), `#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CALL_LOG"
if [[ -n ${FAIL_COMMAND:-} && "$*" == *"$FAIL_COMMAND"* ]]; then exit 1; fi
`)
			log := filepath.Join(root, "calls")
			cmd := exec.Command("bash", append([]string{filepath.Join(scripts, "validate-final.sh")}, tc.args...)...)
			cmd.Env = []string{"PATH=" + bin + ":/usr/bin:/bin", "CALL_LOG=" + log, "CI=" + tc.ci, "GITHUB_ACTIONS=", "FAIL_COMMAND=" + tc.fail}
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.ok {
				t.Fatalf("result %v: %s", err, output)
			}
			calls, _ := os.ReadFile(log)
			got := string(calls)
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatal("missing command", got)
			}
			if tc.want == "" && got != "" {
				t.Fatal("guard invoked command", got)
			}
			if (tc.name == "offline" || tc.name == "performance") && strings.Contains(got, "tools/mainnet") {
				t.Fatal("implicit live execution")
			}
			if tc.fail != "" && (strings.Contains(got, " verify ") || !strings.Contains(string(output), "FAIL live")) {
				t.Fatal("failed live continued or reported success")
			}
		})
	}
}
