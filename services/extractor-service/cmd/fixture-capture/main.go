// main.go implements command startup, explicit adapter wiring and bounded resource lifetime in the
// service composition/operator entry point.

// Command fixture-capture is explicit operator tooling, never started by services or CI.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/fixturecapture"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
)

// main invokes the bounded service/operator entry point and exits nonzero on startup or execution
// failure.
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		// Neither paths, configuration, provider error text nor payloads are output.
		fmt.Fprintln(os.Stderr, "fixture operation failed; check configuration, bounds and offline validation")
		os.Exit(1)
	}
	fmt.Println("fixture operation completed")
}

// run dispatches manifest/verify or explicitly opted-in capture commands, returning safe failures
// without printing credentials.
func run(ctx context.Context, args []string, get func(string) string) error {
	if len(args) == 2 && args[0] == "validate" {
		return fixturecapture.ValidateFile(ctx, args[1])
	}
	if len(args) == 2 && args[0] == "manifest" {
		_, err := fixturecapture.LoadManifest(args[1])
		return err
	}
	if len(args) != 4 || (args[0] != "capture" && args[0] != "verify") {
		return fixturecapture.ErrConfig
	}
	m, err := fixturecapture.LoadManifest(args[1])
	if err != nil {
		return err
	}
	for _, c := range m.Candidates {
		if c.ID != args[2] {
			continue
		}
		if args[0] == "verify" {
			return fixturecapture.VerifyFile(ctx, args[3], c, fixturecapture.SecretTokens(get("ETHEREUM_RPC_URL")))
		}
		cfg, duration, err := fixturecapture.Settings(get, c)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, duration)
		defer cancel()
		client, err := jsonrpc.New(cfg, nil)
		if err != nil {
			return err
		}
		return fixturecapture.Capture(ctx, client, c, args[3], fixturecapture.SecretTokens(cfg.URL))
	}
	return fixturecapture.ErrConfig
}
