// main_test.go exercises command startup, explicit adapter wiring and bounded resource lifetime in
// the local operator tooling.

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// configuration builds private-looking but synthetic test configuration; it never reads a user's
// RPC credentials.
func configuration(t *testing.T) map[string]string {
	t.Helper()
	p := filepath.Join(t.TempDir(), ".env.mainnet")
	if err := initialize(p); err != nil {
		t.Fatal(err)
	}
	c, err := readConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestPrivateInitialization checks new private configuration creation and refusal to replace an
// existing file.
func TestPrivateInitialization(t *testing.T) {
	p := filepath.Join(t.TempDir(), ".env.mainnet")
	if err := initialize(p); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(p)
	if err := initialize(p); err == nil {
		t.Fatal("overwrote existing credentials")
	}
	after, _ := os.ReadFile(p)
	if string(before) != string(after) {
		t.Fatal("configuration changed")
	}
	c, err := readConfig(p)
	if err != nil || validate(c, false) != nil || validate(c, true) == nil {
		t.Fatal("unsafe initialization", err)
	}
	if configuration(t)["MAINNET_PROJECT"] == c["MAINNET_PROJECT"] {
		t.Fatal("project reused")
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readConfig(p); err == nil {
		t.Fatal("public credentials accepted")
	}
}

// TestIsolationAndExplicitAcquisition checks private project separation and that setup commands do
// not implicitly start live RPC.
func TestIsolationAndExplicitAcquisition(t *testing.T) {
	c := configuration(t)
	parent := []string{"PATH=/bin", "DOCKER_HOST=unix:///test", "COMPOSE_FILE=fixture.yaml", "COMPOSE_PROJECT_NAME=ethereum-stage11-danger", "EXTRACTOR_RPC_URL=secret-override", "PGHOST=foreign", "POSTGRES_PASSWORD=foreign"}
	for _, action := range []string{"check", "up", "stop"} {
		var commands []string
		err := run(t.Context(), action, c, parent, func(_ context.Context, args, env []string) ([]byte, error) {
			line := strings.Join(args, " ")
			commands = append(commands, line)
			if strings.Contains(strings.Join(env, "\n"), "secret-override") || strings.Contains(strings.Join(env, "\n"), "COMPOSE_") {
				t.Fatal("inherited overrides")
			}
			if args[0] == "compose" && (!strings.Contains(line, "-p "+c["MAINNET_PROJECT"]) || !strings.Contains(line, "mainnet.yaml") || !strings.Contains(line, "--env-file "+os.DevNull)) {
				t.Fatal("unisolated Compose command", line)
			}
			if strings.Contains(line, "fixture") || strings.Contains(line, "--volumes") || strings.Contains(line, "down") || strings.Contains(line, "run --rm --no-deps") {
				t.Fatal("unexpected acquisition or cleanup", line)
			}
			return nil, nil
		})
		if err != nil || len(commands) < 2 {
			t.Fatal(err)
		}
	}
	called := false
	if err := run(t.Context(), "extract", c, parent, func(context.Context, []string, []string) ([]byte, error) { called = true; return nil, nil }); err == nil || called {
		t.Fatal("RPC-capable command before opt-in")
	}
	for k, v := range map[string]string{"EXTRACTOR_MAINNET_OPT_IN": "true", "EXTRACTOR_START_BLOCK": "10", "EXTRACTOR_END_BLOCK": "12", "EXTRACTOR_CONFIRMATION_DEPTH": "1", "EXTRACTOR_RECOVERY_DEPTH": "8", "EXTRACTOR_MAX_RPC_REQUESTS": "30", "EXTRACTOR_RUN_TIMEOUT": "1m", "EXTRACTOR_RPC_URL": "https://example.invalid/private-secret"} {
		c[k] = v
	}
	var acquisitions int
	err := run(t.Context(), "extract", c, parent, func(_ context.Context, args, env []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "run --rm --no-deps --name "+c["MAINNET_PROJECT"]+"-acquisition extractor") {
			acquisitions++
		}
		return nil, nil
	})
	if err != nil || acquisitions != 1 {
		t.Fatal(err, acquisitions)
	}
	for _, key := range []string{"EXTRACTOR_CHAIN_ID", "EXTRACTOR_START_BLOCK", "EXTRACTOR_END_BLOCK", "EXTRACTOR_MAX_RPC_REQUESTS", "EXTRACTOR_RUN_TIMEOUT", "MAINNET_PROJECT"} {
		original := c[key]
		c[key] = ""
		if validate(c, true) == nil {
			t.Fatal("missing execution bound", key)
		}
		c[key] = original
	}
}

// TestRefuseForeignVolumeAndRedactFailures checks operator setup rejects storage not owned by its
// project and suppresses sensitive command failures.
func TestRefuseForeignVolumeAndRedactFailures(t *testing.T) {
	c := configuration(t)
	volume := c["MAINNET_PROJECT"] + "_mainnet_postgres_data"
	for _, labels := range []string{"", "fixture|" + c["MAINNET_PROJECT"] + "|mainnet_postgres_data", "mainnet|foreign|mainnet_postgres_data", "mainnet|" + c["MAINNET_PROJECT"] + "|mainnet_postgres_data"} {
		mutated := false
		err := run(t.Context(), "up", c, nil, func(_ context.Context, args, _ []string) ([]byte, error) {
			if args[0] == "volume" && args[1] == "ls" {
				return []byte(volume + "\n"), nil
			}
			if args[0] == "volume" && args[1] == "inspect" {
				return []byte(labels), nil
			}
			if strings.Contains(strings.Join(args, " "), "build extractor") {
				mutated = true
			}
			return nil, nil
		})
		owned := labels == "mainnet|"+c["MAINNET_PROJECT"]+"|mainnet_postgres_data"
		if (err == nil) != owned || mutated != owned {
			t.Fatal("unsafe volume admission", err)
		}
	}
	err := run(t.Context(), "check", c, nil, func(context.Context, []string, []string) ([]byte, error) {
		return []byte("secret-output"), errors.New("private-secret")
	})
	if err == nil || strings.Contains(err.Error(), "private-secret") || strings.Contains(err.Error(), "secret-output") {
		t.Fatal("unsafe command error")
	}
}
