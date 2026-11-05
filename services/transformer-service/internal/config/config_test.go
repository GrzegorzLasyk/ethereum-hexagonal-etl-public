// config_test.go exercises validated environment configuration and conservative startup limits in
// the startup configuration boundary.

package config_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/transformer-service/internal/config"
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
			t.Setenv("TRANSFORMER_DATABASE_URL", "postgres://placeholder")
			t.Setenv("TRANSFORMER_CHAIN_ID", "1")
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
		"LOG_LEVEL", "TRANSFORMER_GRPC_ADDRESS", "TRANSFORMER_MAX_PAYLOAD_BYTES", "TRANSFORMER_MAX_MESSAGE_BYTES",
		"TRANSFORMER_WORKERS", "TRANSFORMER_MAX_ATTEMPTS", "TRANSFORMER_POLL_INTERVAL", "TRANSFORMER_PROCESS_TIMEOUT",
		"TRANSFORMER_LEASE", "TRANSFORMER_INITIAL_BACKOFF", "TRANSFORMER_MAX_BACKOFF", "TRANSFORMER_SHUTDOWN_TIMEOUT",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("TRANSFORMER_DATABASE_URL", "postgres://placeholder")
	t.Setenv("TRANSFORMER_CHAIN_ID", "1")
}

// TestProcessingConfiguration checks worker/attempt limits and requires lease duration to exceed
// the processing deadline.
func TestProcessingConfiguration(t *testing.T) {
	setDefaults(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workers != 1 || cfg.MaxAttempts != 5 || cfg.ProcessTimeout != 30*time.Second || cfg.Lease != time.Minute || cfg.ListenAddress != "127.0.0.1:50051" {
		t.Fatal("unexpected processing defaults", cfg.Workers, cfg.MaxAttempts, cfg.ProcessTimeout, cfg.Lease, cfg.ListenAddress)
	}
	for _, tt := range []struct{ name, value string }{
		{"TRANSFORMER_DATABASE_URL", ""}, {"TRANSFORMER_CHAIN_ID", ""}, {"TRANSFORMER_CHAIN_ID", "01"},
		{"TRANSFORMER_GRPC_ADDRESS", "0.0.0.0:50051"}, {"TRANSFORMER_GRPC_ADDRESS", "127.0.0.1:0"},
		{"TRANSFORMER_MAX_PAYLOAD_BYTES", "67108865"}, {"TRANSFORMER_MAX_MESSAGE_BYTES", "1"},
		{"TRANSFORMER_MAX_MESSAGE_BYTES", "67108864"}, {"TRANSFORMER_WORKERS", "0"}, {"TRANSFORMER_WORKERS", "9"},
		{"TRANSFORMER_MAX_ATTEMPTS", "1001"}, {"TRANSFORMER_MAX_ATTEMPTS", "bad"},
		{"TRANSFORMER_LEASE", "30s"}, {"TRANSFORMER_PROCESS_TIMEOUT", "2m"},
		{"TRANSFORMER_POLL_INTERVAL", "0s"}, {"TRANSFORMER_POLL_INTERVAL", "oops"},
		{"TRANSFORMER_MAX_BACKOFF", "500ms"}, {"TRANSFORMER_SHUTDOWN_TIMEOUT", "25h"},
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
		t.Setenv("TRANSFORMER_GRPC_ADDRESS", "[::1]:50052")
		t.Setenv("TRANSFORMER_WORKERS", "8")
		t.Setenv("TRANSFORMER_MAX_ATTEMPTS", "1")
		t.Setenv("TRANSFORMER_POLL_INTERVAL", "10ms")
		t.Setenv("TRANSFORMER_CHAIN_ID", "18446744073709551616")
		cfg, err := config.Load()
		if err != nil || cfg.Workers != 8 || cfg.MaxAttempts != 1 || cfg.PollInterval != 10*time.Millisecond || cfg.ChainID != "18446744073709551616" {
			t.Fatal("explicit configuration lost", err)
		}
	})
}
