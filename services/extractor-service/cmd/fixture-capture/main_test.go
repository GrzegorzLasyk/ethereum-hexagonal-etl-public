package main

import (
	"testing"
)

func TestCaptureRequiresExplicitGateBeforeNetwork(t *testing.T) {
	manifest := "../../../../test/fixtures/mainnet/v1/manifest.json"
	if err := run(t.Context(), []string{"manifest", manifest}, func(string) string { return "" }); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		nil, {"capture"}, {"capture", manifest, "unknown", t.TempDir() + "/artifact.json"},
		{"capture", manifest, "historical-smoke", t.TempDir() + "/artifact.json"},
	} {
		if run(t.Context(), args, func(string) string { return "" }) == nil {
			t.Fatal("ungated capture accepted")
		}
	}
}
