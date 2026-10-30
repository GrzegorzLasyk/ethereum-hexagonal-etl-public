package config

import (
	"strings"
	"testing"
	"time"
)

func TestMainnetExecutionSafety(t *testing.T) {
	indexEnvironment(t)
	for k, v := range map[string]string{
		"EXTRACTOR_ENVIRONMENT": "mainnet", "EXTRACTOR_MAINNET_OPT_IN": "true",
		"EXTRACTOR_MAX_RPC_REQUESTS": "27", "EXTRACTOR_RUN_TIMEOUT": "2m",
		"EXTRACTOR_RPC_URL": "https://example.invalid/private-secret",
	} {
		t.Setenv(k, v)
	}
	c, err := Load()
	if err != nil || c.Indexing.RPC.MaxRequests != 27 || c.Indexing.RunTimeout != 2*time.Minute {
		t.Fatal("valid bounded configuration rejected", err)
	}
	for _, tc := range []struct{ k, v string }{
		{"EXTRACTOR_ENVIRONMENT", ""}, {"EXTRACTOR_ENVIRONMENT", "local"}, {"EXTRACTOR_ENVIRONMENT", "typo"},
		{"EXTRACTOR_MAINNET_OPT_IN", ""}, {"EXTRACTOR_MAINNET_OPT_IN", "false"}, {"EXTRACTOR_MAINNET_OPT_IN", "TRUE"},
		{"EXTRACTOR_CHAIN_ID", "2"}, {"EXTRACTOR_START_BLOCK", ""}, {"EXTRACTOR_END_BLOCK", ""}, {"EXTRACTOR_END_BLOCK", "9"},
		{"EXTRACTOR_MAX_RPC_REQUESTS", ""}, {"EXTRACTOR_MAX_RPC_REQUESTS", "0"}, {"EXTRACTOR_MAX_RPC_REQUESTS", "-1"}, {"EXTRACTOR_MAX_RPC_REQUESTS", "18446744073709551616"},
		{"EXTRACTOR_RUN_TIMEOUT", ""}, {"EXTRACTOR_RUN_TIMEOUT", "0s"}, {"EXTRACTOR_RUN_TIMEOUT", "25h"},
		{"EXTRACTOR_RPC_URL", "http://127.0.0.1/private-secret"}, {"EXTRACTOR_RPC_URL", "https://private-secret@host"},
	} {
		t.Run(tc.k+"="+tc.v, func(t *testing.T) {
			t.Setenv(tc.k, tc.v)
			_, err := Load()
			if err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatal("configuration accepted or secret disclosed")
			}
		})
	}
}

func TestBoundedMainnetReindexAndOfflineDrain(t *testing.T) {
	indexEnvironment(t)
	for k, v := range map[string]string{"EXTRACTOR_ENVIRONMENT": "mainnet", "EXTRACTOR_MAINNET_OPT_IN": "true", "EXTRACTOR_MAX_RPC_REQUESTS": "100", "EXTRACTOR_RUN_TIMEOUT": "2m", "EXTRACTOR_RPC_URL": "https://example.invalid/private-secret", "EXTRACTOR_MODE": "reindex"} {
		t.Setenv(k, v)
	}
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXTRACTOR_OPERATION_ID", "")
	if _, err := Load(); err == nil {
		t.Fatal("reindex missing audit ID")
	}
	t.Setenv("EXTRACTOR_MODE", "drain")
	t.Setenv("EXTRACTOR_MAINNET_OPT_IN", "false")
	t.Setenv("EXTRACTOR_RPC_URL", "")
	c, err := Load()
	if err != nil || c.Indexing.RPC.URL != "" || c.Indexing.RunTimeout == 0 {
		t.Fatal("unsafe offline drain", err)
	}
}
