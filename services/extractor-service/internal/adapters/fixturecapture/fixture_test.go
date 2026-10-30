package fixturecapture

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/adapters/outbound/jsonrpc"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/application/ports/outbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// All raw data in these tests is the existing explicitly synthetic dataset.
func synthetic(t *testing.T) (Candidate, domain.Extraction) {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile("../outbound/jsonrpc/testdata/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		return bytes.TrimSpace(b)
	}
	n := uint64(20000000)
	block := read("block")
	var identity struct{ Hash string }
	if err := json.Unmarshal(block, &identity); err != nil {
		t.Fatal(err)
	}
	c := Candidate{ID: "synthetic-only", ChainID: "1", BlockNumber: &n, BlockHash: identity.Hash, Counts: Counts{Transactions: 2}, Provenance: "existing synthetic unit test data; never Mainnet evidence", Method: "jsonrpc-block-and-all-receipts", Purpose: "test fixture tooling offline", State: "capture_pending"}
	e, err := replay(t.Context(), Artifact{Candidate: c, RawBlock: block, RawReceipts: [][]byte{read("receipt_0"), read("receipt_1")}})
	if err != nil {
		t.Fatal(err)
	}
	return c, e
}

func settings() map[string]string {
	return map[string]string{"ETHEREUM_CAPTURE_OPT_IN": "true", "ETHEREUM_RPC_URL": "https://mainnet.infura.io/v3/private-secret", "ETHEREUM_CAPTURE_MAX_REQUESTS": "100", "ETHEREUM_CAPTURE_TIMEOUT": "2m"}
}

func TestSettingsOffline(t *testing.T) {
	c, _ := synthetic(t)
	for _, tc := range []struct{ k, v string }{
		{"ETHEREUM_CAPTURE_OPT_IN", ""}, {"ETHEREUM_CAPTURE_OPT_IN", "false"}, {"CI", "false"}, {"GITHUB_ACTIONS", "true"},
		{"ETHEREUM_RPC_URL", ""}, {"ETHEREUM_RPC_URL", "https://other.invalid/private-secret"}, {"ETHEREUM_RPC_URL", "http://mainnet.infura.io/v3/private-secret"}, {"ETHEREUM_RPC_URL", "https://mainnet.infura.io/v3/private-secret?key=x"},
		{"ETHEREUM_RPC_URL", "https://mainnet.infura.io/v3/private-secret/extra"}, {"ETHEREUM_RPC_URL", "https://mainnet.infura.io/v3/private-secret?"}, {"ETHEREUM_RPC_URL", "https://mainnet.infura.io/v3/%70rivate-secret"},
		{"ETHEREUM_CAPTURE_MAX_REQUESTS", ""}, {"ETHEREUM_CAPTURE_MAX_REQUESTS", "4"}, {"ETHEREUM_CAPTURE_MAX_REQUESTS", "1001"}, {"ETHEREUM_CAPTURE_MAX_REQUESTS", "18446744073709551616"},
		{"ETHEREUM_CAPTURE_TIMEOUT", ""}, {"ETHEREUM_CAPTURE_TIMEOUT", "2m1ns"}, {"ETHEREUM_CAPTURE_TIMEOUT", "0s"},
	} {
		t.Run(tc.k+tc.v, func(t *testing.T) {
			env := settings()
			env[tc.k] = tc.v
			_, _, err := Settings(func(k string) string { return env[k] }, c)
			if err == nil || strings.Contains(err.Error(), "private-secret") {
				t.Fatal("unsafe config accepted or leaked")
			}
		})
	}
	for _, max := range []string{"5", "1000"} {
		env := settings()
		env["ETHEREUM_CAPTURE_MAX_REQUESTS"] = max
		cfg, d, err := Settings(func(k string) string { return env[k] }, c)
		if err != nil || cfg.ChainID != "1" || d != 2*time.Minute || fmt.Sprint(cfg.MaxRequests) != max {
			t.Fatal("valid bounds rejected", err)
		}
	}
	for _, mutate := range []func(*Candidate){func(c *Candidate) { c.BlockNumber = nil }, func(c *Candidate) { c.BlockHash = "" }, func(c *Candidate) { c.ChainID = "2" }} {
		bad := c
		mutate(&bad)
		env := settings()
		if _, _, err := Settings(func(k string) string { return env[k] }, bad); err == nil {
			t.Fatal("unpinned selection accepted")
		}
	}
}

type fakeSource struct {
	e        domain.Extraction
	attempts uint64
}

func (s *fakeSource) Requests() uint64 { return s.attempts }
func (s *fakeSource) Block(ctx context.Context, n uint64) (domain.Extraction, error) {
	s.attempts = 5
	if n != s.e.Header.ID.Number {
		return domain.Extraction{}, ErrFixture
	}
	return s.e, ctx.Err()
}

func loadArtifact(t *testing.T, path string) Artifact {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var a Artifact
	if err := json.Unmarshal(b, &a); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestCaptureAndOfflineIntegrity(t *testing.T) {
	c, e := synthetic(t)
	path := filepath.Join(t.TempDir(), "artifact.json")
	if err := Capture(t.Context(), &fakeSource{e: e}, c, path, []string{"private-secret"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFile(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	a := loadArtifact(t, path)
	if !bytes.Equal(a.RawBlock, e.RawBlock) || !bytes.Equal(a.RawReceipts[0], e.RawReceipts[0]) || a.Counts.Receipts == nil || *a.Counts.Receipts != 2 || *a.Counts.Logs != 1 {
		t.Fatal("bytes/counts changed")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("private permissions")
	}
	source := &fakeSource{e: e}
	if Capture(t.Context(), source, c, path, nil) == nil || source.attempts != 0 {
		t.Fatal("existing artifact overwritten or RPC started")
	}
	for name, mutate := range map[string]func(*Artifact){
		"version":         func(a *Artifact) { a.Version = 2 },
		"checksum":        func(a *Artifact) { a.SHA256[0] = strings.Repeat("0", 64) },
		"missing receipt": func(a *Artifact) { a.RawReceipts = a.RawReceipts[:1] },
		"order": func(a *Artifact) {
			a.RawReceipts[0], a.RawReceipts[1] = a.RawReceipts[1], a.RawReceipts[0]
			a.SHA256 = digests(a.RawBlock, a.RawReceipts)
		},
		"counts":            func(a *Artifact) { a.Counts.Transactions++ },
		"chain":             func(a *Artifact) { a.Candidate.ChainID = "2" },
		"hash pin":          func(a *Artifact) { a.Candidate.BlockHash = "0x" + strings.Repeat("a", 64) },
		"number pin":        func(a *Artifact) { *a.Candidate.BlockNumber++ },
		"invented coverage": func(a *Artifact) { a.Coverage["type_4"] = []string{"invented"} },
		"unproven selection": func(a *Artifact) {
			a.Candidate.Coverage = []Coverage{{Feature: "type_4", State: "explorer_verified", TransactionHash: fmt.Sprintf("0x%x", e.Transactions[0].Hash), Evidence: "synthetic claim"}}
		},
		"unknown data corruption": func(a *Artifact) {
			a.RawBlock = bytes.Replace(a.RawBlock, []byte(`"gasUsed"`), []byte(`"unknownGasUsed"`), 1)
			a.SHA256 = digests(a.RawBlock, a.RawReceipts)
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := loadArtifact(t, path)
			mutate(&a)
			if a.Validate(t.Context()) == nil {
				t.Fatal("corrupt artifact accepted")
			}
		})
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "partial.json"), []byte(`{"version":1`), 0600); err != nil {
		t.Fatal(err)
	}
	if ValidateFile(t.Context(), filepath.Join(filepath.Dir(path), "partial.json")) == nil {
		t.Fatal("partial file accepted")
	}
}

func TestSecretsAndCancellationDoNotPublish(t *testing.T) {
	c, e := synthetic(t)
	for _, secret := range []bool{false, true} {
		t.Run(fmt.Sprint(secret), func(t *testing.T) {
			e := e
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if secret {
				e.RawBlock = append([]byte(`{"extension":"private-secret",`), e.RawBlock[1:]...)
			} else {
				cancel()
			}
			path := filepath.Join(t.TempDir(), "artifact.json")
			if Capture(ctx, &fakeSource{e: e}, c, path, []string{"private-secret"}) == nil {
				t.Fatal("unsafe capture succeeded")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("failed capture published output")
			}
		})
	}
}

func TestEscapedCredentialEcho(t *testing.T) {
	c, e := synthetic(t)
	e.RawBlock = append([]byte(`{"extension":"private\u002dsecret",`), e.RawBlock[1:]...)
	path := filepath.Join(t.TempDir(), "artifact.json")
	if Capture(t.Context(), &fakeSource{e: e}, c, path, []string{"private-secret"}) == nil {
		t.Fatal("escaped credential stored")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("credential artifact published")
	}
}

func TestBoundedAdapterCaptureOffline(t *testing.T) {
	c, e := synthetic(t)
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			dir := t.TempDir()
			synctest.Test(t, func(t *testing.T) {
				env := settings()
				env["ETHEREUM_CAPTURE_MAX_REQUESTS"] = "5"
				env["ETHEREUM_CAPTURE_TIMEOUT"] = "1s"
				cfg, d, err := Settings(func(k string) string { return env[k] }, c)
				if err != nil {
					t.Fatal(err)
				}
				calls, next := 0, 0
				client, err := jsonrpc.New(cfg, &http.Client{Transport: localTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					if timeout {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					var q struct {
						ID     uint64
						Method string
					}
					if json.NewDecoder(r.Body).Decode(&q) != nil {
						return nil, ErrFixture
					}
					if calls == 1 {
						return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
					}
					var raw []byte
					switch q.Method {
					case "eth_chainId":
						raw = []byte(`"0x1"`)
					case "eth_getBlockByNumber":
						raw = e.RawBlock
					case "eth_getTransactionReceipt":
						raw = e.RawReceipts[next]
						next++
					default:
						return nil, ErrFixture
					}
					body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":%s}`, q.ID, raw)
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
				})})
				if err != nil {
					t.Fatal(err)
				}
				// Keep the real retry policy/pacing; virtual time makes it deterministic.
				if !timeout {
					d = 2 * time.Minute
				}
				ctx, cancel := context.WithTimeout(t.Context(), d)
				defer cancel()
				path := filepath.Join(dir, "artifact.json")
				err = Capture(ctx, client, c, path, nil)
				want := outbound.ErrRPCBudgetExceeded
				wantCalls := 5
				if timeout {
					want = context.DeadlineExceeded
					wantCalls = 1
				}
				if !errors.Is(err, want) || calls != wantCalls || client.Requests() != uint64(wantCalls) {
					t.Fatalf("err=%v calls=%d counter=%d", err, calls, client.Requests())
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("failed capture published output")
				}
			})
		})
	}
}

func TestVersionedSelectionManifest(t *testing.T) {
	m, err := LoadManifest("../../../../../test/fixtures/mainnet/v1/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Candidates) != 3 || len(m.Pending) != 2 {
		t.Fatal("review updated selection coverage")
	}
	for name, mutate := range map[string]func(*Manifest){
		"version":          func(m *Manifest) { m.Version = 2 },
		"duplicate":        func(m *Manifest) { m.Candidates = append(m.Candidates, m.Candidates[0]) },
		"missing coverage": func(m *Manifest) { m.Pending = nil },
		"invented capture": func(m *Manifest) { m.Candidates[0].State = "captured" },
		"missing number":   func(m *Manifest) { m.Candidates[0].BlockNumber = nil },
		"integrity policy": func(m *Manifest) { m.IntegrityChecks = nil },
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := json.Marshal(m)
			var changed Manifest
			if json.Unmarshal(b, &changed) != nil {
				t.Fatal("clone")
			}
			mutate(&changed)
			b, _ = json.Marshal(changed)
			path := filepath.Join(t.TempDir(), "manifest.json")
			if os.WriteFile(path, b, 0600) != nil {
				t.Fatal("write")
			}
			if _, err := LoadManifest(path); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}
