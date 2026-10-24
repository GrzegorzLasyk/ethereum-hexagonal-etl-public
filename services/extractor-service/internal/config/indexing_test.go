package config

import (
	"strings"
	"testing"
)

func indexEnvironment(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"EXTRACTOR_DATABASE_URL": "postgres://placeholder", "EXTRACTOR_CHAIN_ID": "1", "EXTRACTOR_MODE": "index", "EXTRACTOR_START_BLOCK": "10", "EXTRACTOR_END_BLOCK": "12", "EXTRACTOR_CONFIRMATION_DEPTH": "0", "EXTRACTOR_RECOVERY_DEPTH": "10", "EXTRACTOR_MAX_OUTSTANDING_EVENTS": "100", "EXTRACTOR_MAX_OUTSTANDING_BYTES": "104857600", "EXTRACTOR_MAX_DATABASE_BYTES": "1073741824", "EXTRACTOR_RPC_URL": "https://example.invalid/private-secret", "EXTRACTOR_RPC_PROVIDER": "infura-mainnet", "EXTRACTOR_OPERATION_ID": "repair-1", "EXTRACTOR_REPLAY_EVENT_ID": "raw-block/v1/1/10/placeholder",
	} {
		t.Setenv(k, v)
	}
}
func TestIndexingConfiguration(t *testing.T) {
	indexEnvironment(t)
	c, err := Load()
	if err != nil || c.Indexing.Mode != "index" || c.Indexing.Options.Confirmations != 0 || *c.Indexing.Options.End != 12 {
		t.Fatal(c.Indexing, err)
	}
	t.Setenv("EXTRACTOR_END_BLOCK", "")
	c, err = Load()
	if err != nil || c.Indexing.Options.End != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, value string }{
		{"EXTRACTOR_START_BLOCK", ""}, {"EXTRACTOR_START_BLOCK", "-1"}, {"EXTRACTOR_START_BLOCK", "18446744073709551616"},
		{"EXTRACTOR_END_BLOCK", "9"}, {"EXTRACTOR_CONFIRMATION_DEPTH", ""}, {"EXTRACTOR_RECOVERY_DEPTH", "0"}, {"EXTRACTOR_RECOVERY_DEPTH", "10001"},
		{"EXTRACTOR_MAX_OUTSTANDING_EVENTS", "0"}, {"EXTRACTOR_MAX_OUTSTANDING_BYTES", ""}, {"EXTRACTOR_MAX_DATABASE_BYTES", "18446744073709551615"},
		{"EXTRACTOR_BATCH_SIZE", "1001"}, {"EXTRACTOR_MAX_CYCLE_ERRORS", "0"}, {"EXTRACTOR_POLL_INTERVAL", "0s"}, {"EXTRACTOR_RPC_URL", "https://private-secret@host.invalid"},
		{"EXTRACTOR_RPC_PROVIDER", ""}, {"EXTRACTOR_MODE", "unknown"},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			_, err := Load()
			if err == nil {
				t.Fatal("invalid setting accepted")
			}
			if strings.Contains(err.Error(), "private-secret") {
				t.Fatal("secret exposed")
			}
		})
	}
}
func TestOperatorModes(t *testing.T) {
	indexEnvironment(t)
	t.Setenv("EXTRACTOR_MODE", "reindex")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXTRACTOR_END_BLOCK", "")
	if _, err := Load(); err == nil {
		t.Fatal("unbounded reindex")
	}
	t.Setenv("EXTRACTOR_MODE", "replay")
	t.Setenv("EXTRACTOR_RPC_URL", "")
	c, err := Load()
	if err != nil || c.Indexing.Replay.Allowance != 20 {
		t.Fatal(err)
	}
	t.Setenv("EXTRACTOR_REPLAY_ATTEMPTS", "1001")
	if _, err := Load(); err == nil {
		t.Fatal("unbounded retry budget")
	}
	t.Setenv("EXTRACTOR_REPLAY_ATTEMPTS", "2")
	t.Setenv("EXTRACTOR_OPERATION_ID", "")
	if _, err := Load(); err == nil {
		t.Fatal("missing audit ID")
	}
	t.Setenv("EXTRACTOR_MODE", "dispatch")
	if _, err := Load(); err != nil {
		t.Fatal("dispatch requires RPC config", err)
	}
}
