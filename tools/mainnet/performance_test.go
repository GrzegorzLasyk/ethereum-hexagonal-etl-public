package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivatePerformanceSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mainnet.env")
	if err := initialize(path); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("EXTRACTOR_RECEIPT_CONCURRENCY=2\nLOADER_SQL_BATCH_SIZE=32\nEXTRACTOR_RPC_INTERVAL=400ms\nTRANSFORMER_WORKERS=2\n")
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	c, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = run(t.Context(), "check", c, []string{"LOADER_SQL_BATCH_SIZE=256", "EXTRACTOR_RECEIPT_CONCURRENCY=8"}, func(_ context.Context, args, env []string) ([]byte, error) {
		called = true
		joined := strings.Join(env, "\n")
		if !strings.Contains(joined, "LOADER_SQL_BATCH_SIZE=32") || strings.Contains(joined, "LOADER_SQL_BATCH_SIZE=256") || !strings.Contains(joined, "EXTRACTOR_RECEIPT_CONCURRENCY=2") {
			t.Fatal("private tuning not authoritative")
		}
		if strings.Contains(strings.Join(args, " "), "run --rm") {
			t.Fatal("check acquired Ethereum")
		}
		return nil, nil
	})
	if err != nil || !called {
		t.Fatal("offline preflight", err)
	}
	for key, value := range map[string]string{"EXTRACTOR_RECEIPT_CONCURRENCY": "9", "LOADER_SQL_BATCH_SIZE": "257", "TRANSFORMER_WORKERS": "0", "LOADER_MAX_INFLIGHT_BYTES": "1", "LOADER_DB_MAX_CONNECTIONS": "33", "EXTRACTOR_RPC_INTERVAL": "0s"} {
		t.Run(key, func(t *testing.T) {
			bad := configuration(t)
			bad[key] = value
			called := false
			err := run(t.Context(), "check", bad, nil, func(context.Context, []string, []string) ([]byte, error) { called = true; return nil, nil })
			if err == nil || called {
				t.Fatal("invalid tuning reached Docker")
			}
		})
	}
}
