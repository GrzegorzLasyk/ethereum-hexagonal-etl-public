// Package fixturecapture is offline fixture tooling, not a service persistence path.
// It stores the exact JSON-RPC result bytes defined by ADR 0001/0002.
package fixturecapture

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"slices"
)

var ErrFixture = errors.New("invalid or incomplete fixture")

var hashPattern = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

var Features = []string{"type_0", "type_1", "type_2", "type_3", "type_4", "failed_receipt", "contract_creation", "event_logs"}
var integrityChecks = []string{"sha256_raw_results", "pinned_chain_number_hash", "complete_ordered_receipts", "counts_and_coverage", "production_adapter_validation"}

type Counts struct {
	Transactions int  `json:"transactions"`
	Receipts     *int `json:"receipts"`
	Logs         *int `json:"logs"`
}

// Explorer-verified is selection evidence only; raw coverage requires an artifact.
type Coverage struct {
	Feature         string `json:"feature"`
	State           string `json:"state"`
	TransactionHash string `json:"transaction_hash,omitempty"`
	Evidence        string `json:"evidence"`
}

type Candidate struct {
	ID          string     `json:"id"`
	ChainID     string     `json:"chain_id"`
	BlockNumber *uint64    `json:"block_number"`
	BlockHash   string     `json:"block_hash"`
	Counts      Counts     `json:"counts"`
	Provenance  string     `json:"provenance"`
	Method      string     `json:"acquisition_method"`
	Purpose     string     `json:"purpose"`
	Coverage    []Coverage `json:"coverage"`
	State       string     `json:"state"`
}

type Manifest struct {
	Version         int         `json:"version"`
	IntegrityChecks []string    `json:"integrity_checks"`
	Candidates      []Candidate `json:"candidates"`
	Pending         []Coverage  `json:"pending"`
}

func knownFeature(s string) bool {
	for _, f := range Features {
		if f == s {
			return true
		}
	}
	return false
}

func (c Candidate) Validate() error {
	if !idPattern.MatchString(c.ID) || c.ChainID != "1" || c.BlockNumber == nil || !hashPattern.MatchString(c.BlockHash) || c.BlockHash == "0x0000000000000000000000000000000000000000000000000000000000000000" || c.Counts.Transactions < 0 || c.Counts.Transactions > 997 || c.Provenance == "" || c.Purpose == "" || c.Method != "jsonrpc-block-and-all-receipts" || c.State != "capture_pending" {
		return ErrFixture
	}
	if c.Counts.Receipts != nil && *c.Counts.Receipts != c.Counts.Transactions || c.Counts.Logs != nil && *c.Counts.Logs < 0 {
		return ErrFixture
	}
	seen := map[string]bool{}
	for _, v := range c.Coverage {
		if !knownFeature(v.Feature) || seen[v.Feature] || v.State != "explorer_verified" || !hashPattern.MatchString(v.TransactionHash) || v.Evidence == "" {
			return ErrFixture
		}
		seen[v.Feature] = true
	}
	return nil
}

func LoadManifest(path string) (Manifest, error) {
	var m Manifest
	f, err := os.Open(path)
	if err != nil {
		return m, ErrFixture
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return m, ErrFixture
	}
	if err := decode(io.LimitReader(f, 1<<20), &m); err != nil {
		return Manifest{}, ErrFixture
	}
	if m.Version != 1 || len(m.Candidates) == 0 || !slices.Equal(m.IntegrityChecks, integrityChecks) {
		return Manifest{}, ErrFixture
	}
	seen, covered := map[string]bool{}, map[string]bool{}
	for _, c := range m.Candidates {
		if c.Validate() != nil || seen[c.ID] {
			return Manifest{}, ErrFixture
		}
		seen[c.ID] = true
		for _, v := range c.Coverage {
			covered[v.Feature] = true
		}
	}
	for _, v := range m.Pending {
		if !knownFeature(v.Feature) || v.State != "pending" || v.TransactionHash != "" || v.Evidence == "" {
			return Manifest{}, ErrFixture
		}
		covered[v.Feature] = true
	}
	for _, f := range Features {
		if !covered[f] {
			return Manifest{}, ErrFixture
		}
	}
	return m, nil
}

func decode(r io.Reader, value any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return ErrFixture
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrFixture
	}
	return nil
}
