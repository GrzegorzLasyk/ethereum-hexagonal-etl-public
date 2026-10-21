package config_test

import (
	"log/slog"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/loader-service/internal/config"
)

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
			t.Setenv("LOG_LEVEL", tt.value)
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
