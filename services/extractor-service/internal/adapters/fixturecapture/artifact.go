// artifact.go implements exclusive capture publication and offline raw-byte/integrity verification
// in the Extractor fixture/operator adapter.

package fixturecapture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/domain"
)

// Artifact preserves raw result bytes inside a versioned capture envelope.
//
// Byte slices use JSON base64: unlike RawMessage, encoding them cannot compact,
// reformat or discard unknown fields from the original RPC result bytes.
type Artifact struct {
	Version     int                 `json:"version"`
	Candidate   Candidate           `json:"candidate"`
	Provider    string              `json:"provider"`
	AcquiredAt  time.Time           `json:"acquired_at"`
	Attempts    uint64              `json:"rpc_attempts"`
	Counts      Counts              `json:"counts"`
	Coverage    map[string][]string `json:"observed_coverage"`
	SHA256      []string            `json:"sha256_block_then_receipts"`
	RawBlock    []byte              `json:"raw_block_base64"`
	RawReceipts [][]byte            `json:"raw_receipts_base64"`
}

// Source is the bounded block-acquisition dependency used by capture, including actual attempt
// accounting.
type Source interface {
	// Block returns a complete ordered raw snapshot for the height or an error; partial receipt
	// batches are not valid results.
	Block(context.Context, uint64) (domain.Extraction, error)
	// Requests reports actual source request attempts for the bounded capture artifact.
	Requests() uint64
}

// digests computes SHA-256 identities of the retained raw block and ordered receipts without
// reserializing their contents.
func digests(block []byte, receipts [][]byte) []string {
	d := []string{fmt.Sprintf("%x", sha256.Sum256(block))}
	for _, r := range receipts {
		d = append(d, fmt.Sprintf("%x", sha256.Sum256(r)))
	}
	return d
}

// dimensions derives transaction-type, receipt-outcome, creation and log coverage from decoded
// source data.
func dimensions(e domain.Extraction) (Counts, map[string][]string, error) {
	var block struct {
		Transactions []struct {
			Hash, Type string
			To         *string
		}
	}
	if json.Unmarshal(e.RawBlock, &block) != nil {
		return Counts{}, nil, ErrFixture
	}
	coverage := make(map[string][]string)
	logs, receipts := 0, len(e.RawReceipts)
	for i, tx := range block.Transactions {
		kind := tx.Type
		if kind == "" {
			kind = "0x0"
		} // Pre-EIP-2718 legacy provider responses.
		if len(kind) != 3 || kind[:2] != "0x" || kind[2] < '0' || kind[2] > '4' {
			return Counts{}, nil, ErrFixture
		}
		feature := "type_" + kind[2:]
		coverage[feature] = append(coverage[feature], tx.Hash)
		var r struct {
			Status          string
			ContractAddress *string
			Logs            []json.RawMessage
		}
		if i >= len(e.RawReceipts) || json.Unmarshal(e.RawReceipts[i], &r) != nil {
			return Counts{}, nil, ErrFixture
		}
		if r.Status == "0x0" {
			coverage["failed_receipt"] = append(coverage["failed_receipt"], tx.Hash)
		}
		if tx.To == nil && r.ContractAddress != nil {
			coverage["contract_creation"] = append(coverage["contract_creation"], tx.Hash)
		}
		if len(r.Logs) > 0 {
			coverage["event_logs"] = append(coverage["event_logs"], tx.Hash)
		}
		logs += len(r.Logs)
	}
	return Counts{Transactions: len(block.Transactions), Receipts: &receipts, Logs: &logs}, coverage, nil
}

// Capture runs once. The composition root supplies a fresh, bounded RPC adapter
// and deadline. No checkpoint, database, dispatcher or downstream service exists
// on this tooling path. Failure leaves existing output untouched.
func Capture(ctx context.Context, source Source, c Candidate, path string, secrets []string) error {
	if c.Validate() != nil || source.Requests() != 0 {
		return ErrFixture
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return ErrFixture
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil || !parent.IsDir() {
		return ErrFixture
	}
	e, err := source.Block(ctx, *c.BlockNumber)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	counts, coverage, err := dimensions(e)
	if err != nil {
		return err
	}
	a := Artifact{Version: 1, Candidate: c, Provider: "infura-mainnet", AcquiredAt: e.ExtractedAt, Attempts: source.Requests(), Counts: counts, Coverage: coverage, SHA256: digests(e.RawBlock, e.RawReceipts), RawBlock: e.RawBlock, RawReceipts: e.RawReceipts}
	if err := a.Validate(ctx); err != nil {
		return err
	}
	// Never serialize endpoint configuration. Also refuse accidental credential
	// echoes in otherwise valid unknown provider fields; do not redact source bytes.
	for _, raw := range append([][]byte{a.RawBlock}, a.RawReceipts...) {
		if containsSecret(raw, secrets) {
			return ErrFixture
		}
	}
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return ErrFixture
	}
	if containsSecret(data, secrets) {
		return ErrFixture
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// Exclusive creation prevents accidental replacement. An interrupted write is
	// retained for inspection and cannot pass ValidateFile; no automatic deletion.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrFixture
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return ErrFixture
	}
	return ctx.Err()
}

// Check decoded strings as well as literal bytes, including JSON-escaped echoes.
func containsSecret(data []byte, secrets []string) bool {
	match := func(s string) bool {
		// Offline verification must reject endpoint URLs even without credentials
		// in its environment. Explorer provenance links are not RPC endpoints.
		if strings.Contains(strings.ToLower(s), "infura.io") {
			return true
		}
		for _, secret := range secrets {
			if secret != "" && strings.Contains(s, secret) {
				return true
			}
		}
		return false
	}
	if match(string(data)) {
		return true
	}
	var value any
	if json.Unmarshal(data, &value) != nil {
		return true
	}
	var visit func(any) bool
	visit = func(v any) bool {
		switch x := v.(type) {
		case string:
			return match(x)
		case []any:
			for _, child := range x {
				if visit(child) {
					return true
				}
			}
		case map[string]any:
			for key, child := range x {
				if match(key) || visit(child) {
					return true
				}
			}
		}
		return false
	}
	return visit(value)
}

// ValidateFile validates a captured artifact offline and returns its observed coverage; it does
// not fetch replacement data.
func ValidateFile(ctx context.Context, path string) error {
	a, err := readArtifact(path)
	if err != nil {
		return err
	}
	return a.Validate(ctx)
}

// VerifyFile anchors the artifact to the independently reviewed selection,
// rather than trusting a possibly changed candidate embedded in the artifact.
// It is entirely offline; secrets are optional tokens, never persisted.
func VerifyFile(ctx context.Context, path string, expected Candidate, secrets []string) error {
	_, err := readVerified(ctx, path, expected, secrets)
	return err
}

// readVerified loads an artifact, validates its manifest binding and source integrity, and returns
// the checked in-memory data.
func readVerified(ctx context.Context, path string, expected Candidate, secrets []string) (Artifact, error) {
	a, err := readArtifact(path)
	if err != nil {
		return Artifact{}, err
	}
	if expected.Validate() != nil || !reflect.DeepEqual(a.Candidate, expected) {
		return Artifact{}, ErrFixture
	}
	if err := a.Validate(ctx); err != nil {
		return Artifact{}, err
	}
	if err := a.checkSecrets(secrets); err != nil {
		return Artifact{}, err
	}
	return a, nil
}

// readArtifact reads and decodes a bounded capture file before any replay or digest validation.
func readArtifact(path string) (Artifact, error) {
	var a Artifact
	f, err := os.Open(path)
	if err != nil {
		return a, ErrFixture
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 96<<20 {
		return a, ErrFixture
	}
	if decode(f, &a) != nil {
		return Artifact{}, ErrFixture
	}
	return a, nil
}

// checkSecrets rejects credential/endpoint traces in a capture rather than writing or reporting
// sensitive source bytes.
func (a Artifact) checkSecrets(secrets []string) error {
	for _, raw := range append([][]byte{a.RawBlock}, a.RawReceipts...) {
		if containsSecret(raw, secrets) {
			return ErrFixture
		}
	}
	metadata, err := json.Marshal(a)
	if err != nil || containsSecret(metadata, secrets) {
		return ErrFixture
	}
	return nil
}

// Validate checks pinned identity, raw digests, complete receipt membership and recorded coverage
// using offline production decoding.
func (a Artifact) Validate(ctx context.Context) error {
	if a.Version != 1 || a.Candidate.Validate() != nil || a.Provider != "infura-mainnet" || a.AcquiredAt.IsZero() || a.Attempts < uint64(a.Candidate.Counts.Transactions)+3 || a.Attempts > 1000 || len(a.RawReceipts) != a.Candidate.Counts.Transactions {
		return ErrFixture
	}
	total := 0
	for _, raw := range append([][]byte{a.RawBlock}, a.RawReceipts...) {
		if len(raw) == 0 || len(raw) > 8<<20 {
			return ErrFixture
		}
		total += len(raw)
	}
	if total > 64<<20 || !reflect.DeepEqual(a.SHA256, digests(a.RawBlock, a.RawReceipts)) {
		return ErrFixture
	}
	e, err := replay(ctx, a)
	if err != nil {
		return ErrFixture
	}
	if e.Header.ID.ChainID != "1" || fmt.Sprintf("0x%x", e.Header.ID.Hash) != a.Candidate.BlockHash || !bytes.Equal(e.RawBlock, a.RawBlock) || !reflect.DeepEqual(e.RawReceipts, a.RawReceipts) {
		return ErrFixture
	}
	counts, coverage, err := dimensions(e)
	if err != nil || !reflect.DeepEqual(counts, a.Counts) || !reflect.DeepEqual(coverage, a.Coverage) || counts.Transactions != a.Candidate.Counts.Transactions {
		return ErrFixture
	}
	if a.Candidate.Counts.Receipts != nil && *a.Candidate.Counts.Receipts != *counts.Receipts || a.Candidate.Counts.Logs != nil && *a.Candidate.Counts.Logs != *counts.Logs {
		return ErrFixture
	}
	for _, expected := range a.Candidate.Coverage {
		found := false
		for _, hash := range coverage[expected.Feature] {
			if hash == expected.TransactionHash {
				found = true
			}
		}
		if !found {
			return ErrFixture
		}
	}
	return a.checkSecrets(nil)
}

// localTransport implements HTTP transport as an injected function for network-free captured
// response replay.
type localTransport func(*http.Request) (*http.Response, error)

// RoundTrip replays only captured methods/identities in memory and rejects unsupported requests
// without a network fallback.
func (t localTransport) RoundTrip(r *http.Request) (*http.Response, error) { return t(r) }

// Validation reuses the production decoder with an in-memory-only transport.
// There is no fallback network dialer, no fixture server and no live credential.
func replay(ctx context.Context, a Artifact) (domain.Extraction, error) {
	next := 0
	transport := localTransport(func(r *http.Request) (*http.Response, error) {
		var q struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if json.NewDecoder(r.Body).Decode(&q) != nil {
			return nil, ErrFixture
		}
		var raw []byte
		switch q.Method {
		case "eth_chainId":
			raw = []byte(`"0x1"`)
		case "eth_getBlockByNumber":
			raw = a.RawBlock
		case "eth_getTransactionReceipt":
			if next >= len(a.RawReceipts) {
				return nil, ErrFixture
			}
			raw = a.RawReceipts[next]
			next++
		default:
			return nil, ErrFixture
		}
		body := append([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":`, q.ID)), raw...)
		body = append(body, '}')
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	cfg := jsonrpc.Config{URL: "https://fixture.invalid", ChainID: "1", Provider: "offline-fixture", RequestTimeout: time.Second, MaxAttempts: 1, MaxRequests: uint64(len(a.RawReceipts)) + 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxResponseBytes: (8 << 20) + 1024, MaxSnapshotBytes: 64 << 20}
	c, err := jsonrpc.New(cfg, &http.Client{Transport: transport})
	if err != nil {
		return domain.Extraction{}, err
	}
	return c.Block(ctx, *a.Candidate.BlockNumber)
}
