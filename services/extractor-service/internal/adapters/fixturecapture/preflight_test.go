// preflight_test.go exercises fixture capture safety guards, identity pins and no-overwrite
// behavior in the Extractor fixture/operator adapter.

package fixturecapture

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// writeArtifact writes the test's controlled artifact so integrity checks can exercise intentional
// tampering.
func writeArtifact(t *testing.T, path string, a Artifact) {
	t.Helper()
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

// TestPreflightPinsAndBudgets checks capture candidates, explicit historical identity and bounded
// attempt/time settings before acquisition.
func TestPreflightPinsAndBudgets(t *testing.T) {
	m, err := LoadManifest("../../../../../test/fixtures/mainnet/v1/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id     string
		number uint64
		tx     int
		max    uint64
		hash   string
	}{
		{"historical-smoke", 1000000, 2, 100, "0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e"},
		{"pectra-blob-dynamic-fee", 22431084, 95, 300, "0x50c8cab760b2948349c590461b166773c45d8f4858cccf5a43025ab2960152e8"},
		{"london-access-list", 12965000, 259, 800, "0x9b83c12c69edb74f6c8dd5d052765c1adf940e320bd1291696e6fa07829eee71"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			found := false
			for _, c := range m.Candidates {
				if c.ID != tc.id {
					continue
				}
				found = true
				if *c.BlockNumber != tc.number || c.BlockHash != tc.hash || c.Counts.Transactions != tc.tx {
					t.Fatal("reviewed selection changed")
				}
				env := settings()
				env["ETHEREUM_CAPTURE_MAX_REQUESTS"] = fmt.Sprint(tc.max)
				cfg, d, err := Settings(func(k string) string { return env[k] }, c)
				if err != nil || d != 2*time.Minute || cfg.MaxRequests != tc.max || cfg.ChainID != "1" || cfg.MaxAttempts != 3 || cfg.RequestTimeout != 15*time.Second {
					t.Fatal("capture bounds changed", err)
				}
			}
			if !found {
				t.Fatal("missing reviewed selection")
			}
		})
	}
}

// TestVerifyAnchorsSelectionAndNeverUsesNetwork requires offline verification to bind the artifact
// to its reviewed manifest candidate without source discovery.
func TestVerifyAnchorsSelectionAndNeverUsesNetwork(t *testing.T) {
	// All tests in this package are sequential. Any accidental fallback to the
	// default HTTP transport fails before network access, including proxy use.
	old := http.DefaultTransport
	http.DefaultTransport = localTransport(func(*http.Request) (*http.Response, error) {
		t.Error("offline replay attempted network transport")
		return nil, ErrFixture
	})
	t.Cleanup(func() { http.DefaultTransport = old })
	c, e := synthetic(t)
	path := filepath.Join(t.TempDir(), "fixture.json")
	if err := Capture(t.Context(), &fakeSource{e: e}, c, path, nil); err != nil {
		t.Fatal(err)
	}
	if err := VerifyFile(t.Context(), path, c, nil); err != nil {
		t.Fatal(err)
	}
	a := loadArtifact(t, path)
	for i, r := range e.RawReceipts {
		if !bytes.Equal(r, a.RawReceipts[i]) {
			t.Fatal("raw receipt changed")
		}
	}
	if !bytes.Equal(e.RawBlock, a.RawBlock) {
		t.Fatal("raw block changed")
	}
	// Self-consistency alone is not enough: changing selection metadata must fail
	// when independently compared with the reviewed manifest.
	a.Candidate.Purpose = "different selection"
	writeArtifact(t, path, a)
	if err := ValidateFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if VerifyFile(t.Context(), path, c, nil) == nil {
		t.Fatal("embedded selection trusted instead of manifest")
	}
}

// TestOfflineArtifactSecretChecks rejects endpoint/credential traces in local artifacts without
// making provider requests.
func TestOfflineArtifactSecretChecks(t *testing.T) {
	c, e := synthetic(t)
	path := filepath.Join(t.TempDir(), "fixture.json")
	if err := Capture(t.Context(), &fakeSource{e: e}, c, path, nil); err != nil {
		t.Fatal(err)
	}
	base := loadArtifact(t, path)
	for _, value := range []string{`https://mainnet.infura.io/v3/unrecognized-key`, `https:\/\/mainnet.infura.io\/v3\/unrecognized-key`, `private\u002dsecret`} {
		t.Run(value, func(t *testing.T) {
			a := base
			a.RawBlock = append([]byte(`{"unexpected":"`+value+`",`), base.RawBlock[1:]...)
			a.SHA256 = digests(a.RawBlock, a.RawReceipts)
			writeArtifact(t, path, a)
			if value[0] == 'p' {
				if err := VerifyFile(t.Context(), path, c, nil); err != nil {
					t.Fatal("valid JSON must pass without the optional known-key check", err)
				}
			}
			if VerifyFile(t.Context(), path, c, SecretTokens("https://mainnet.infura.io/v3/private-secret")) == nil {
				t.Fatal("secret/endpoint in decoded raw bytes accepted")
			}
		})
	}
}

// TestOfflineTransactionReceiptAndLogIntegrity mutates captured membership, indexes and types to
// verify offline rejection of inconsistent source data.
func TestOfflineTransactionReceiptAndLogIntegrity(t *testing.T) {
	c, e := synthetic(t)
	path := filepath.Join(t.TempDir(), "fixture.json")
	if err := Capture(t.Context(), &fakeSource{e: e}, c, path, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, from, to string
		rehash         bool
	}{
		{"receipt identity", `"transactionHash": "0x2020`, `"transactionHash": "0x2121`, true},
		{"receipt index", `"transactionIndex": "0x0"`, `"transactionIndex": "0x1"`, true},
		{"receipt type", `"type": "0x2"`, `"type": "0x1"`, true},
		{"log order", `"logIndex": "0x0"`, `"logIndex": "0x1"`, true},
		{"removed log", `"removed": false`, `"removed": true`, true},
		{"log bytes checksum", `"data": "0x000102"`, `"data": "0x000103"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := loadArtifact(t, path)
			changed := bytes.Replace(a.RawReceipts[0], []byte(tc.from), []byte(tc.to), 1)
			if bytes.Equal(changed, a.RawReceipts[0]) {
				t.Fatal("mutation did not apply")
			}
			a.RawReceipts[0] = changed
			if tc.rehash {
				a.SHA256 = digests(a.RawBlock, a.RawReceipts)
			}
			if a.Validate(t.Context()) == nil {
				t.Fatal("invalid source membership/order accepted")
			}
		})
	}
	// Add a second synthetic log, then prove the valid ordering passes before
	// reversing it. Recompute digests to exercise the production log validator.
	a := loadArtifact(t, path)
	var receipt map[string]any
	if err := json.Unmarshal(a.RawReceipts[0], &receipt); err != nil {
		t.Fatal(err)
	}
	logs := receipt["logs"].([]any)
	second := map[string]any{}
	for k, v := range logs[0].(map[string]any) {
		second[k] = v
	}
	second["logIndex"] = "0x1"
	logs = append(logs, second)
	receipt["logs"] = logs
	a.RawReceipts[0], _ = json.Marshal(receipt)
	a.SHA256 = digests(a.RawBlock, a.RawReceipts)
	*a.Counts.Logs = 2
	if err := a.Validate(t.Context()); err != nil {
		t.Fatal("ordered logs rejected", err)
	}
	logs[0], logs[1] = logs[1], logs[0]
	a.RawReceipts[0], _ = json.Marshal(receipt)
	a.SHA256 = digests(a.RawBlock, a.RawReceipts)
	if a.Validate(t.Context()) == nil {
		t.Fatal("reversed logs accepted")
	}
}

// racingSource creates a competing capture output during acquisition to test exclusive publication
// without overwrite.
type racingSource struct {
	*fakeSource
	path string
}

// Block returns or fails a controlled complete source snapshot so acquisition/persistence
// boundaries can be exercised.
func (s racingSource) Block(ctx context.Context, n uint64) (domain.Extraction, error) {
	if err := os.WriteFile(s.path, []byte("existing data"), 0600); err != nil {
		return domain.Extraction{}, err
	}
	return s.fakeSource.Block(ctx, n)
}

// TestCaptureNeverReplacesExistingOrRacedFiles races destination creation and requires capture to
// preserve an existing artifact instead of overwriting it.
func TestCaptureNeverReplacesExistingOrRacedFiles(t *testing.T) {
	c, e := synthetic(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "fixture.json")
	if err := Capture(t.Context(), racingSource{&fakeSource{e: e}, path}, c, path, nil); err == nil {
		t.Fatal("racing file replaced")
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "existing data" {
		t.Fatal("existing content damaged")
	}
	link := filepath.Join(dir, "symlink.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	s := &fakeSource{e: e}
	if Capture(t.Context(), s, c, link, nil) == nil || s.attempts != 0 {
		t.Fatal("symlink accepted or acquisition started")
	}
}
