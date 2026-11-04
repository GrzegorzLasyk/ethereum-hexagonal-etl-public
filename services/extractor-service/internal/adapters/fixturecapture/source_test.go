// source_test.go exercises network-free replay of verified fixture artifacts in the Extractor
// fixture/operator adapter.

package fixturecapture

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/services/extractor-service/internal/domain"
)

// TestReplaySourceOffline checks verified historical heights replay through production decoding
// with exact bytes and no external transport.
func TestReplaySourceOffline(t *testing.T) {
	c, e := synthetic(t)
	path := filepath.Join(t.TempDir(), "fixture.json")
	if err := Capture(t.Context(), &fakeSource{e: e}, c, path, nil); err != nil {
		t.Fatal(err)
	}
	original := http.DefaultTransport
	http.DefaultTransport = localTransport(func(*http.Request) (*http.Response, error) {
		t.Error("network fallback used")
		return nil, errors.New("network forbidden")
	})
	t.Cleanup(func() { http.DefaultTransport = original })
	s, err := NewReplaySource(t.Context(), path, c)
	if err != nil {
		t.Fatal(err)
	}
	a := loadArtifact(t, path)
	for i := 0; i < 2; i++ {
		got, err := s.Block(t.Context(), *c.BlockNumber)
		if err != nil || !bytes.Equal(got.RawBlock, a.RawBlock) || got.Provider != a.Provider || !got.ExtractedAt.Equal(a.AcquiredAt) || len(got.RawReceipts) != len(a.RawReceipts) {
			t.Fatal("replay changed raw data/provenance", err)
		}
		for j := range got.RawReceipts {
			if !bytes.Equal(got.RawReceipts[j], a.RawReceipts[j]) {
				t.Fatal("receipt changed")
			}
		}
		got.RawBlock[0] = '!' // A consumer cannot mutate the retained snapshot.
	}
	if _, err := s.Block(t.Context(), *c.BlockNumber+1); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatal("unknown height accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ChainID(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("chain cancellation ignored")
	}
	if _, err := s.Block(ctx, *c.BlockNumber); !errors.Is(err, context.Canceled) {
		t.Fatal("block cancellation ignored", err)
	}
	bad := c
	bad.BlockHash = "0x0000000000000000000000000000000000000000000000000000000000000000"
	if _, err := NewReplaySource(t.Context(), path, bad); err == nil {
		t.Fatal("manifest mismatch accepted")
	}
	if _, err := NewReplaySource(t.Context(), path+"-missing", c); err == nil {
		t.Fatal("missing file accepted")
	}
}
