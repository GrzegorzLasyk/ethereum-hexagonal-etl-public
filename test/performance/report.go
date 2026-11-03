// Package performance defines the offline baseline's versioned, secret-free report.
// It is test tooling, not a shared service domain or production scheduler.
package performance

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/baseline"
)

type Scenario struct {
	LoaderBatchSize int    `json:"loader_sql_batch_size"`
	Dataset         string `json:"dataset"` // authentic-mainnet or synthetic-stage11
	Workers         int    `json:"workers_per_inbox_and_dispatcher"`
	Blocks          int    `json:"blocks"`
	Start           uint64 `json:"start_height"`
	Warmup          bool   `json:"warmup_excluded"`
	Repetition      int    `json:"repetition"`
	Artifact        string `json:"-"`
	Manifest        string `json:"-"`
	Case            string `json:"manifest_case,omitempty"`
	Digest          string `json:"artifact_sha256,omitempty"`
	Directory       string `json:"-"`
	Binaries        string `json:"-"`
}

// WireSpec intentionally carries local paths only in a private runner-owned file.
type WireSpec struct {
	Scenario
	ArtifactPath, ManifestPath, OutputDirectory, BinaryDirectory string
}

func ReadScenario() (Scenario, error) {
	if os.Getenv("ETL_BASELINE_OPT_IN") != "1" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return Scenario{}, errors.New("baseline requires explicit local opt-in")
	}
	data, e := os.ReadFile(os.Getenv("ETL_BASELINE_SCENARIO"))
	if e != nil {
		return Scenario{}, errors.New("missing baseline scenario")
	}
	var w WireSpec
	if json.Unmarshal(data, &w) != nil {
		return Scenario{}, errors.New("invalid baseline scenario")
	}
	s := w.Scenario
	if s.LoaderBatchSize == 0 {
		s.LoaderBatchSize = 64
	} // older private specs used defaults
	s.Artifact = w.ArtifactPath
	s.Manifest = w.ManifestPath
	s.Directory = w.OutputDirectory
	s.Binaries = w.BinaryDirectory
	return s, s.Validate()
}
func (s Scenario) Validate() error {
	if s.LoaderBatchSize < 0 || s.LoaderBatchSize > 256 {
		return errors.New("invalid Loader batch size")
	}
	if s.Workers != 1 && s.Workers != 2 && s.Workers != 4 && s.Workers != 8 {
		return errors.New("workers must be 1,2,4,8")
	}
	if s.Blocks < 1 || s.Blocks > 1000 || s.Directory == "" || s.Binaries == "" {
		return errors.New("invalid bounded baseline scenario")
	}
	switch s.Dataset {
	case "authentic-mainnet":
		if s.Blocks != 1 || s.Artifact == "" || s.Manifest == "" || len(s.Digest) != 64 || s.Case == "" || (s.Start != 1000000 && s.Start != 12965000 && s.Start != 22431084) {
			return errors.New("authentic heights require independent verified runs")
		}
	case "synthetic-stage11":
		if s.Start != 100 || s.Artifact != "" {
			return errors.New("invalid deterministic synthetic range")
		}
	default:
		return errors.New("unknown dataset")
	}
	return nil
}
func ExclusiveJSON(path string, v any) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return errors.New("cannot create exclusive baseline report")
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

type SeedEvent struct {
	EventID         string    `json:"event_id"`
	Started         time.Time `json:"started_at"`
	DurationSeconds float64   `json:"duration_seconds"`
}
type Seed struct {
	Events                       []SeedEvent `json:"events"`
	Transactions, Receipts, Logs int
	PayloadBytes                 int64
	DuplicateVerified            bool `json:"duplicate_verified"`
}
type Sample struct {
	At         time.Time         `json:"at"`
	Checkpoint *uint64           `json:"checkpoint"`
	Completed  int               `json:"completed_blocks"`
	Metrics    map[string]string `json:"prometheus"`
}
type RPC struct {
	Attempts     int                   `json:"attempts"`
	Errors       int                   `json:"errors"`
	PayloadBytes int64                 `json:"protobuf_message_bytes"`
	Latency      baseline.Distribution `json:"durable_ack_latency"`
}
type Report struct {
	Version               string                           `json:"report_version"`
	Status                string                           `json:"status"`
	Scenario              Scenario                         `json:"scenario"`
	Environment           map[string]any                   `json:"environment"`
	MeasuredSeconds       float64                          `json:"measured_seconds"`
	BlocksPerSecond       *float64                         `json:"verified_blocks_per_second"`
	TransactionsPerSecond *float64                         `json:"verified_transactions_per_second"`
	E2ELatency            baseline.Distribution            `json:"completion_observation_latency"`
	ExtractionLatency     baseline.Distribution            `json:"offline_extraction_commit_latency"`
	Operations            map[string]baseline.Distribution `json:"operation_latency_from_slog_milliseconds"`
	RPC                   map[string]RPC                   `json:"grpc"`
	Processes             map[string]json.RawMessage       `json:"process_probes"`
	PostgreSQL            map[string]any                   `json:"postgresql"`
	Samples               []Sample                         `json:"samples"`
	Seed                  Seed                             `json:"source"`
	Integrity             map[string]bool                  `json:"integrity"`
	Limitations           []string                         `json:"limitations"`
}

func (r *Report) Accept(elapsed time.Duration, blocks, txs int) error {
	if elapsed <= 0 || blocks != r.Scenario.Blocks || txs < 0 || len(r.Integrity) == 0 {
		return errors.New("invalid completed baseline")
	}
	for _, ok := range r.Integrity {
		if !ok {
			return errors.New("incomplete baseline cannot count throughput")
		}
	}
	r.MeasuredSeconds = elapsed.Seconds()
	b, t := float64(blocks)/elapsed.Seconds(), float64(txs)/elapsed.Seconds()
	r.BlocksPerSecond = &b
	r.TransactionsPerSecond = &t
	r.Status = "passed"
	return nil
}
