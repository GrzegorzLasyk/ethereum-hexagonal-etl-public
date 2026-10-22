package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// Config starts the dispatcher for committed snapshots; acquisition scheduling
// remains a separate stage and is not silently enabled here.
type Config struct {
	LogLevel    slog.Level
	DatabaseURL string
	ChainID     domain.ChainID
	Limits      grpccontract.Limits
	Dispatch    DispatchConfig
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("EXTRACTOR_DATABASE_URL"), ChainID: domain.ChainID(os.Getenv("EXTRACTOR_CHAIN_ID")), Limits: grpccontract.DefaultLimits()}
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "", "INFO":
		c.LogLevel = slog.LevelInfo
	case "DEBUG":
		c.LogLevel = slog.LevelDebug
	case "WARN":
		c.LogLevel = slog.LevelWarn
	case "ERROR":
		c.LogLevel = slog.LevelError
	default:
		return c, fmt.Errorf("LOG_LEVEL must be DEBUG, INFO, WARN, or ERROR")
	}
	if c.DatabaseURL == "" || c.ChainID.Validate() != nil {
		return c, fmt.Errorf("EXTRACTOR_DATABASE_URL and canonical EXTRACTOR_CHAIN_ID are required")
	}
	for _, v := range []struct {
		name   string
		target *int
	}{{"EXTRACTOR_MAX_PAYLOAD_BYTES", &c.Limits.PayloadBytes}, {"EXTRACTOR_MAX_MESSAGE_BYTES", &c.Limits.MessageBytes}} {
		if raw := os.Getenv(v.name); raw != "" {
			n, e := strconv.Atoi(raw)
			if e != nil {
				return c, fmt.Errorf("invalid %s", v.name)
			}
			*v.target = n
		}
	}
	if err := c.Limits.Validate(); err != nil {
		return c, err
	}
	var err error
	c.Dispatch, err = loadDispatch()
	return c, err
}
