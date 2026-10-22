package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/grpccontract"
)

type DispatchConfig struct {
	Target                                                               string
	Workers, MaxAttempts                                                 int
	PollInterval, Timeout, RPCTimeout, Lease, InitialBackoff, MaxBackoff time.Duration
}

func loadDispatch() (DispatchConfig, error) {
	c := DispatchConfig{Target: os.Getenv("TRANSFORMER_DISPATCH_TARGET")}
	if c.Target == "" {
		c.Target = "127.0.0.1:50052"
	}
	if !grpccontract.LocalAddress(c.Target) {
		return c, fmt.Errorf("TRANSFORMER_DISPATCH_TARGET requires a loopback IP and valid port")
	}
	for _, v := range []struct {
		name     string
		target   *int
		def, max int
	}{
		{"WORKERS", &c.Workers, 1, 8}, {"MAX_ATTEMPTS", &c.MaxAttempts, 20, 1000},
	} {
		n := v.def
		if raw := os.Getenv("TRANSFORMER_DISPATCH_" + v.name); raw != "" {
			parsed, e := strconv.Atoi(raw)
			if e != nil {
				return c, fmt.Errorf("invalid dispatcher %s", v.name)
			}
			n = parsed
		}
		if n < 1 || n > v.max {
			return c, fmt.Errorf("invalid dispatcher %s", v.name)
		}
		*v.target = n
	}
	for _, v := range []struct {
		name   string
		target *time.Duration
		def    time.Duration
	}{
		{"POLL_INTERVAL", &c.PollInterval, time.Second}, {"TIMEOUT", &c.Timeout, 30 * time.Second},
		{"RPC_TIMEOUT", &c.RPCTimeout, 5 * time.Second}, {"LEASE", &c.Lease, time.Minute},
		{"INITIAL_BACKOFF", &c.InitialBackoff, time.Second}, {"MAX_BACKOFF", &c.MaxBackoff, time.Minute},
	} {
		d := v.def
		if raw := os.Getenv("TRANSFORMER_DISPATCH_" + v.name); raw != "" {
			parsed, e := time.ParseDuration(raw)
			if e != nil {
				return c, fmt.Errorf("invalid dispatcher %s", v.name)
			}
			d = parsed
		}
		if d < time.Millisecond || d > 24*time.Hour {
			return c, fmt.Errorf("invalid dispatcher %s", v.name)
		}
		*v.target = d
	}
	if c.Lease <= c.Timeout || c.Timeout <= c.RPCTimeout || c.MaxBackoff < c.InitialBackoff {
		return c, fmt.Errorf("dispatch requires lease > timeout > RPC timeout and max backoff >= initial backoff")
	}
	return c, nil
}
