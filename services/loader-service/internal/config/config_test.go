// config_test.go exercises validated environment configuration and conservative startup limits in
// the startup configuration boundary.

package config_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/loader-service/internal/config"
)

// TestLoad checks required identities and environment parsing fail safely before startup with
// invalid settings.
func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    slog.Level
		wantErr bool
	}{
		{name: "default", value: "", want: slog.LevelInfo},
		{name: "debug", value: "DEBUG", want: slog.LevelDebug},
		{name: "info", value: "INFO", want: slog.LevelInfo},
		{name: "warn", value: "WARN", want: slog.LevelWarn},
		{name: "error", value: "ERROR", want: slog.LevelError},
		{name: "case and whitespace", value: "  debug\t", want: slog.LevelDebug},
		{name: "blank", value: " \t", want: slog.LevelInfo},
		{name: "invalid", value: "verbose", wantErr: true},
		{name: "numeric", value: "4", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setDefaults(t)
			t.Setenv("LOG_LEVEL", tt.value)
			t.Setenv("LOADER_DATABASE_URL", "postgres://placeholder")
			t.Setenv("LOADER_CHAIN_ID", "1")
			got, err := config.Load()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Load() accepted an invalid log level")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got.LogLevel != tt.want {
				t.Errorf("LogLevel = %v, want %v", got.LogLevel, tt.want)
			}
		})
	}
}

// setDefaults sets test-scoped environment values so cases do not depend on the caller's startup
// configuration.
func setDefaults(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"LOG_LEVEL", "LOADER_GRPC_ADDRESS", "LOADER_MAX_PAYLOAD_BYTES", "LOADER_MAX_MESSAGE_BYTES",
		"LOADER_WORKERS", "LOADER_MAX_ATTEMPTS", "LOADER_POLL_INTERVAL", "LOADER_PROCESS_TIMEOUT",
		"LOADER_LEASE", "LOADER_INITIAL_BACKOFF", "LOADER_MAX_BACKOFF", "LOADER_SHUTDOWN_TIMEOUT",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("LOADER_DATABASE_URL", "postgres://placeholder")
	t.Setenv("LOADER_CHAIN_ID", "1")
}

// TestProcessingConfiguration checks worker/attempt limits and requires lease duration to exceed
// the processing deadline.
func TestProcessingConfiguration(t *testing.T) {
	setDefaults(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workers != 1 || cfg.MaxAttempts != 5 || cfg.ProcessTimeout != 30*time.Second || cfg.Lease != time.Minute || cfg.ListenAddress != "127.0.0.1:50052" {
		t.Fatal("unexpected processing defaults", cfg.Workers, cfg.MaxAttempts, cfg.ProcessTimeout, cfg.Lease, cfg.ListenAddress)
	}
	for _, tt := range []struct{ name, value string }{
		{"LOADER_DATABASE_URL", ""}, {"LOADER_CHAIN_ID", ""}, {"LOADER_CHAIN_ID", "01"},
		{"LOADER_GRPC_ADDRESS", "0.0.0.0:50052"}, {"LOADER_GRPC_ADDRESS", "127.0.0.1:0"},
		{"LOADER_MAX_PAYLOAD_BYTES", "67108865"}, {"LOADER_MAX_MESSAGE_BYTES", "1"},
		{"LOADER_MAX_MESSAGE_BYTES", "67108864"}, {"LOADER_WORKERS", "0"}, {"LOADER_WORKERS", "9"},
		{"LOADER_MAX_ATTEMPTS", "1001"}, {"LOADER_MAX_ATTEMPTS", "bad"},
		{"LOADER_LEASE", "30s"}, {"LOADER_PROCESS_TIMEOUT", "2m"},
		{"LOADER_POLL_INTERVAL", "0s"}, {"LOADER_POLL_INTERVAL", "oops"},
		{"LOADER_MAX_BACKOFF", "500ms"}, {"LOADER_SHUTDOWN_TIMEOUT", "25h"},
	} {
		t.Run(tt.name+"="+tt.value, func(t *testing.T) {
			setDefaults(t)
			t.Setenv(tt.name, tt.value)
			if _, err := config.Load(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	t.Run("explicit settings", func(t *testing.T) {
		setDefaults(t)
		t.Setenv("LOADER_GRPC_ADDRESS", "[::1]:50053")
		t.Setenv("LOADER_WORKERS", "8")
		t.Setenv("LOADER_MAX_ATTEMPTS", "1")
		t.Setenv("LOADER_POLL_INTERVAL", "10ms")
		t.Setenv("LOADER_CHAIN_ID", "18446744073709551616")
		cfg, err := config.Load()
		if err != nil || cfg.Workers != 8 || cfg.MaxAttempts != 1 || cfg.PollInterval != 10*time.Millisecond || cfg.ChainID != "18446744073709551616" {
			t.Fatal("explicit configuration lost", err)
		}
	})
}
