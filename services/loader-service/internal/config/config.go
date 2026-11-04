// config.go implements validated environment configuration and conservative startup limits in the
// startup configuration boundary.

// Package config provides validated environment configuration and conservative startup limits in
// the startup configuration boundary.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/domain"
)

// Config holds validated startup identities, transport/resource limits and worker lifetime
// settings; URLs are private.
type Config struct {
	SQLBatchSize                                                                     int
	AdmissionCapacity                                                                int
	Resources                                                                        grpccontract.Resources
	Repair                                                                           *outbound.RepairRequest
	LogLevel                                                                         slog.Level
	DatabaseURL                                                                      string
	ChainID                                                                          domain.ChainID
	ListenAddress                                                                    string
	Limits                                                                           grpccontract.Limits
	Workers, MaxAttempts                                                             int
	PollInterval, ProcessTimeout, Lease, InitialBackoff, MaxBackoff, ShutdownTimeout time.Duration
}

// Load parses environment configuration and validates required identities, resource budgets and
// lifetime relationships before startup.
func Load() (Config, error) {
	cfg := Config{DatabaseURL: os.Getenv("LOADER_DATABASE_URL"), ChainID: domain.ChainID(os.Getenv("LOADER_CHAIN_ID")), ListenAddress: os.Getenv("LOADER_GRPC_ADDRESS")}
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "", "INFO":
		cfg.LogLevel = slog.LevelInfo
	case "DEBUG":
		cfg.LogLevel = slog.LevelDebug
	case "WARN":
		cfg.LogLevel = slog.LevelWarn
	case "ERROR":
		cfg.LogLevel = slog.LevelError
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL must be DEBUG, INFO, WARN, or ERROR")
	}
	if cfg.DatabaseURL == "" || !cfg.ChainID.Valid() {
		return Config{}, fmt.Errorf("LOADER_DATABASE_URL and canonical LOADER_CHAIN_ID are required")
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:50052"
	}
	host, port, err := net.SplitHostPort(cfg.ListenAddress)
	n, e := strconv.Atoi(port)
	if err != nil || e != nil || n < 1 || n > 65535 || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return Config{}, fmt.Errorf("LOADER_GRPC_ADDRESS must be a loopback IP and valid port")
	}
	ints := []struct {
		name          string
		target        *int
		def, min, max int
	}{
		{"LOADER_MAX_PAYLOAD_BYTES", &cfg.Limits.PayloadBytes, 64 << 20, 1, 64 << 20},
		{"LOADER_MAX_MESSAGE_BYTES", &cfg.Limits.MessageBytes, 65 << 20, 2, 65 << 20},
		{"LOADER_SQL_BATCH_SIZE", &cfg.SQLBatchSize, 64, 1, 256},
		{"LOADER_WORKERS", &cfg.Workers, 1, 1, 8},
		{"LOADER_MAX_ATTEMPTS", &cfg.MaxAttempts, 5, 1, 1000},
	}
	for _, v := range ints {
		value := v.def
		if text := os.Getenv(v.name); text != "" {
			n, err := strconv.Atoi(text)
			if err != nil {
				return Config{}, fmt.Errorf("invalid %s", v.name)
			}
			value = n
		}
		if value < v.min || value > v.max {
			return Config{}, fmt.Errorf("invalid %s", v.name)
		}
		*v.target = value
	}
	if err := cfg.Limits.Validate(); err != nil {
		return Config{}, err
	}
	durations := []struct {
		name   string
		target *time.Duration
		def    time.Duration
	}{
		{"LOADER_POLL_INTERVAL", &cfg.PollInterval, time.Second},
		{"LOADER_PROCESS_TIMEOUT", &cfg.ProcessTimeout, 30 * time.Second},
		{"LOADER_LEASE", &cfg.Lease, time.Minute},
		{"LOADER_INITIAL_BACKOFF", &cfg.InitialBackoff, time.Second},
		{"LOADER_MAX_BACKOFF", &cfg.MaxBackoff, time.Minute},
		{"LOADER_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout, 10 * time.Second},
	}
	for _, v := range durations {
		d := v.def
		if text := os.Getenv(v.name); text != "" {
			parsed, err := time.ParseDuration(text)
			if err != nil {
				return Config{}, fmt.Errorf("invalid %s", v.name)
			}
			d = parsed
		}
		if d < time.Millisecond || d > 24*time.Hour {
			return Config{}, fmt.Errorf("invalid %s", v.name)
		}
		*v.target = d
	}
	if cfg.Lease <= cfg.ProcessTimeout || cfg.MaxBackoff < cfg.InitialBackoff {
		return Config{}, fmt.Errorf("lease must exceed process timeout; max backoff must cover initial backoff")
	}
	cfg.Resources, err = grpccontract.LoadResources("LOADER", cfg.Limits)
	if err != nil {
		return Config{}, err
	}
	cfg.AdmissionCapacity = 128
	if text := os.Getenv("LOADER_MAX_OUTSTANDING_EVENTS"); text != "" {
		cfg.AdmissionCapacity, err = strconv.Atoi(text)
	}
	if err != nil || cfg.AdmissionCapacity < 1 || cfg.AdmissionCapacity > 10000 {
		return Config{}, fmt.Errorf("invalid LOADER_MAX_OUTSTANDING_EVENTS")
	}
	if cfg.Repair, err = loadRepair(string(cfg.ChainID)); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
