package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config contains settings used by the service skeleton.
type Config struct {
	LogLevel slog.Level
}

// Load reads and validates startup configuration from the environment.
func Load() (Config, error) {
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "", "INFO":
		return Config{LogLevel: slog.LevelInfo}, nil
	case "DEBUG":
		return Config{LogLevel: slog.LevelDebug}, nil
	case "WARN":
		return Config{LogLevel: slog.LevelWarn}, nil
	case "ERROR":
		return Config{LogLevel: slog.LevelError}, nil
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL must be DEBUG, INFO, WARN, or ERROR")
	}
}
