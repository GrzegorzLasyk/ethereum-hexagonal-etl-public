package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/usecases"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

type IndexingConfig struct {
	Mode          string
	RunTimeout    time.Duration
	DrainTimeout  time.Duration
	Options       usecases.IndexOptions
	RPC           jsonrpc.Config
	Poll, Timeout time.Duration
	MaxErrors     int
	OperationID   string
	Replay        outbound.ReplayRequest
}

func loadIndexing(c Config) (IndexingConfig, error) {
	x := IndexingConfig{Mode: os.Getenv("EXTRACTOR_MODE"), OperationID: os.Getenv("EXTRACTOR_OPERATION_ID")}
	if x.Mode == "" {
		x.Mode = "dispatch"
	}
	if x.Mode == "dispatch" {
		return x, nil
	}
	if x.Mode == "drain" {
		var err error
		x.Options.Chain = c.ChainID
		if x.Options.Start, err = uintSetting("EXTRACTOR_START_BLOCK", 0, true); err != nil {
			return x, err
		}
		end, err := uintSetting("EXTRACTOR_END_BLOCK", 0, true)
		if err != nil || end < x.Options.Start {
			return x, fmt.Errorf("drain requires explicit ordered range")
		}
		x.Options.End = &end
		x.DrainTimeout, err = durationSetting("EXTRACTOR_DRAIN_TIMEOUT", 30*time.Second)
		if err != nil || x.DrainTimeout > 2*time.Minute {
			return x, fmt.Errorf("invalid EXTRACTOR_DRAIN_TIMEOUT")
		}
		x.RunTimeout = x.DrainTimeout + 10*time.Second
		return x, nil
	}

	if x.Mode != "index" && x.Mode != "reindex" && x.Mode != "replay" {
		return x, fmt.Errorf("EXTRACTOR_MODE must be dispatch, index, reindex or replay")
	}
	if x.Mode == "replay" || x.Mode == "reindex" {
		if err := domain.ValidateOperationID(x.OperationID); err != nil {
			return x, fmt.Errorf("invalid EXTRACTOR_OPERATION_ID")
		}
	}
	if x.Mode == "replay" {
		n, err := uintSetting("EXTRACTOR_REPLAY_ATTEMPTS", 20, false)
		if err != nil || n < 1 || n > 1000 {
			return x, fmt.Errorf("invalid EXTRACTOR_REPLAY_ATTEMPTS")
		}
		x.Replay = outbound.ReplayRequest{ID: x.OperationID, EventID: os.Getenv("EXTRACTOR_REPLAY_EVENT_ID"), Allowance: int(n)}
		if x.Replay.EventID == "" {
			return x, fmt.Errorf("EXTRACTOR_REPLAY_EVENT_ID is required")
		}
		return x, nil
	}
	o := usecases.IndexOptions{Chain: c.ChainID}
	var err error
	if o.Start, err = uintSetting("EXTRACTOR_START_BLOCK", 0, true); err != nil {
		return x, err
	}
	if raw := os.Getenv("EXTRACTOR_END_BLOCK"); raw != "" {
		end, e := uintSetting("EXTRACTOR_END_BLOCK", 0, true)
		if e != nil {
			return x, e
		}
		o.End = &end
	}
	if x.Mode == "reindex" && o.End == nil {
		return x, fmt.Errorf("reindex requires EXTRACTOR_END_BLOCK")
	}
	if o.Confirmations, err = uintSetting("EXTRACTOR_CONFIRMATION_DEPTH", 0, true); err != nil {
		return x, err
	}
	if o.RecoveryDepth, err = uintSetting("EXTRACTOR_RECOVERY_DEPTH", 0, true); err != nil {
		return x, err
	}
	batch, err := uintSetting("EXTRACTOR_BATCH_SIZE", 1, false)
	if err != nil || batch < 1 || batch > 1000 {
		return x, fmt.Errorf("invalid EXTRACTOR_BATCH_SIZE")
	}
	o.Batch = int(batch)
	for _, v := range []struct {
		name string
		dst  *int64
	}{
		{"EXTRACTOR_MAX_OUTSTANDING_EVENTS", &o.Limits.Events}, {"EXTRACTOR_MAX_OUTSTANDING_BYTES", &o.Limits.Bytes}, {"EXTRACTOR_MAX_DATABASE_BYTES", &o.Limits.DatabaseBytes},
	} {
		n, e := uintSetting(v.name, 0, true)
		if e != nil {
			return x, e
		}
		if n == 0 || n > 1<<63-1 {
			return x, fmt.Errorf("invalid %s", v.name)
		}
		*v.dst = int64(n)
	}
	if err = o.Validate(); err != nil {
		return x, err
	}
	x.Options = o
	if x.DrainTimeout, err = durationSetting("EXTRACTOR_DRAIN_TIMEOUT", 30*time.Second); err != nil {
		return x, err
	}
	if x.DrainTimeout > 2*time.Minute {
		return x, fmt.Errorf("EXTRACTOR_DRAIN_TIMEOUT must not exceed 2m")
	}
	if x.Poll, err = durationSetting("EXTRACTOR_POLL_INTERVAL", 3*time.Second); err != nil {
		return x, err
	}
	if x.Timeout, err = durationSetting("EXTRACTOR_CYCLE_TIMEOUT", 5*time.Minute); err != nil {
		return x, err
	}
	failures, err := uintSetting("EXTRACTOR_MAX_CYCLE_ERRORS", 5, false)
	if err != nil || failures < 1 || failures > 1000 {
		return x, fmt.Errorf("invalid EXTRACTOR_MAX_CYCLE_ERRORS")
	}
	x.MaxErrors = int(failures)
	x.RPC = jsonrpc.Config{URL: os.Getenv("EXTRACTOR_RPC_URL"), Provider: os.Getenv("EXTRACTOR_RPC_PROVIDER"), ChainID: c.ChainID, MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: 30 * time.Second, MaxSnapshotBytes: int64(c.Limits.PayloadBytes), MaxResponseBytes: int64(min(16<<20, c.Limits.PayloadBytes))}
	concurrency, e := uintSetting("EXTRACTOR_RECEIPT_CONCURRENCY", jsonrpc.DefaultReceiptConcurrency, false)
	if e != nil || concurrency < 1 || concurrency > jsonrpc.MaxReceiptConcurrency {
		return x, fmt.Errorf("EXTRACTOR_RECEIPT_CONCURRENCY must be 1..8")
	}
	x.RPC.ReceiptConcurrency = int(concurrency)
	if x.RPC.RequestTimeout, err = durationSetting("EXTRACTOR_RPC_TIMEOUT", 10*time.Second); err != nil {
		return x, err
	}
	if x.RPC.MinRequestInterval, err = durationSetting("EXTRACTOR_RPC_INTERVAL", 200*time.Millisecond); err != nil {
		return x, err
	}
	if err = loadExecutionSafety(c, &x); err != nil {
		return x, err
	}
	// Constructor validates URL/provider and all transport budgets without I/O.
	if _, err = jsonrpc.New(x.RPC, nil); err != nil {
		return x, err
	}
	return x, nil
}
func uintSetting(name string, def uint64, required bool) (uint64, error) {
	raw := os.Getenv(name)
	if raw == "" {
		if required {
			return 0, fmt.Errorf("%s is required", name)
		}
		return def, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return n, nil
}
func durationSetting(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < time.Millisecond || d > 24*time.Hour {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return d, nil
}
