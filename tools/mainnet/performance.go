package main

import (
	"errors"
	"strconv"
	"time"
)

// Explicit private-file allowlist. Inherited shell settings remain filtered.
// Absent keys preserve the service/Compose defaults and existing private files.
func performanceKey(key string) bool {
	switch key {
	case "EXTRACTOR_RECEIPT_CONCURRENCY", "LOADER_SQL_BATCH_SIZE", "EXTRACTOR_RPC_INTERVAL", "EXTRACTOR_RPC_TIMEOUT",
		"EXTRACTOR_DISPATCH_WORKERS", "TRANSFORMER_DISPATCH_WORKERS", "TRANSFORMER_WORKERS", "LOADER_WORKERS",
		"EXTRACTOR_DB_MAX_CONNECTIONS", "TRANSFORMER_DB_MAX_CONNECTIONS", "LOADER_DB_MAX_CONNECTIONS",
		"TRANSFORMER_MAX_OUTSTANDING_EVENTS", "LOADER_MAX_OUTSTANDING_EVENTS",
		"TRANSFORMER_MAX_INCOMING_REQUESTS", "LOADER_MAX_INCOMING_REQUESTS",
		"TRANSFORMER_MAX_INFLIGHT_BYTES", "LOADER_MAX_INFLIGHT_BYTES",
		"TRANSFORMER_MAX_GRPC_CONNECTIONS", "LOADER_MAX_GRPC_CONNECTIONS":
		return true
	}
	return false
}
func validatePerformance(c map[string]string) error {
	for key, raw := range c {
		if !performanceKey(key) {
			continue
		}
		if key == "EXTRACTOR_RPC_INTERVAL" || key == "EXTRACTOR_RPC_TIMEOUT" {
			d, err := time.ParseDuration(raw)
			if err != nil || d < time.Millisecond || d > 24*time.Hour {
				return errors.New("invalid Mainnet RPC timing")
			}
			continue
		}
		lo, hi := 1, 8
		switch key {
		case "LOADER_SQL_BATCH_SIZE":
			hi = 256
		case "EXTRACTOR_DB_MAX_CONNECTIONS", "TRANSFORMER_DB_MAX_CONNECTIONS", "LOADER_DB_MAX_CONNECTIONS":
			lo, hi = 2, 32
		case "TRANSFORMER_MAX_OUTSTANDING_EVENTS", "LOADER_MAX_OUTSTANDING_EVENTS":
			hi = 10000
		case "TRANSFORMER_MAX_INFLIGHT_BYTES", "LOADER_MAX_INFLIGHT_BYTES":
			lo, hi = 65<<20, 520<<20
		case "TRANSFORMER_MAX_GRPC_CONNECTIONS", "LOADER_MAX_GRPC_CONNECTIONS":
			hi = 64
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < lo || n > hi {
			return errors.New("invalid Mainnet performance setting")
		}
	}
	return nil
}
