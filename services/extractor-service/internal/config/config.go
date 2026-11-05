// config.go implements validated environment configuration and conservative startup limits in the
// startup configuration boundary.

// Package config provides validated environment configuration and conservative startup limits in
// the startup configuration boundary.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/grpccontract"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// Config explicitly selects dispatch, indexing or an operator command.
type Config struct {
	PoolConnections int
	LogLevel        slog.Level
	DatabaseURL     string
	ChainID         domain.ChainID
	Limits          grpccontract.Limits
	Dispatch        DispatchConfig
	Indexing        IndexingConfig
}

// Load parses environment configuration and validates required identities, resource budgets and
// lifetime relationships before startup.
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
	c.PoolConnections = 12
	if raw := os.Getenv("EXTRACTOR_DB_MAX_CONNECTIONS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 2 || n > 32 {
			return c, fmt.Errorf("invalid EXTRACTOR_DB_MAX_CONNECTIONS")
		}
		c.PoolConnections = n
	}
	var err error
	c.Dispatch, err = loadDispatch()
	if err != nil {
		return c, err
	}
	c.Indexing, err = loadIndexing(c)
	return c, err
}
