//go:build postgresintegration

// mainnet_seed_test.go exercises authentic offline fixture acquisition into isolated Extractor
// storage in the outbound adapter layer.

package postgres

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/adapters/fixturecapture"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/ports/inbound"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/services/extractor-service/internal/application/usecases"
)

// TestSeedRealFixture seeds one manifest-verified noncontiguous height in isolation.
//
// Only the explicit isolated real-data runner selects this test. Missing inputs
// are failures, never a silent skip or a synthetic substitution.
func TestSeedRealFixture(t *testing.T) {
	if os.Getenv("HANDOFF_TEST_ISOLATED") != "1" || os.Getenv("REAL_FIXTURE_ARTIFACT") == "" {
		t.Fatal("requires isolated --mainnet-fixtures runner")
	}
	path := os.Getenv("REAL_FIXTURE_ARTIFACT")
	data, err := os.ReadFile(path)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != os.Getenv("REAL_FIXTURE_SHA256") {
		t.Fatal("artifact missing or changed after verification")
	}
	m, err := fixturecapture.LoadManifest(os.Getenv("REAL_FIXTURE_MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	var selected *fixturecapture.Candidate
	for _, c := range m.Candidates {
		if c.ID == os.Getenv("REAL_FIXTURE_CASE") {
			selected = &c
		}
	}
	if selected == nil {
		t.Fatal("unknown manifest selection")
	}
	source, err := fixturecapture.NewReplaySource(t.Context(), path, *selected)
	if err != nil {
		t.Fatal(err)
	}
	pool := integrationPool(t, "EXTRACTOR_TEST_DATABASE_URL", "extractor_runtime")
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM extractor.raw_blocks").Scan(&count); err != nil || count != 0 {
		t.Fatal("requires a fresh isolated database")
	}
	n := *selected.BlockNumber
	store, err := New(pool, "1", n)
	if err != nil {
		t.Fatal(err)
	}
	u, err := usecases.NewExtractBlock("1", n, source, store)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r, err := u.ExtractBlock(t.Context(), inbound.ExtractBlockRequest{ChainID: "1", Number: n})
		if err != nil || r.AlreadyStored != (i == 1) {
			t.Fatal("extraction/idempotency failed", err)
		}
	}
	e, err := source.Block(t.Context(), n)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	var receipts [][]byte
	var provider, acquired string
	if err := pool.QueryRow(t.Context(), "SELECT raw_block,raw_receipts,provider,extracted_at FROM extractor.raw_blocks").Scan(&raw, &receipts, &provider, &acquired); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, e.RawBlock) || len(receipts) != len(e.RawReceipts) || provider != e.Provider || acquired != e.ExtractedAt.Format(time.RFC3339Nano) {
		t.Fatal("raw bytes or provenance changed")
	}
	for i := range receipts {
		if !bytes.Equal(receipts[i], e.RawReceipts[i]) {
			t.Fatal("receipt bytes changed", i)
		}
	}
	var height, start, hash string
	var revision int
	if err := pool.QueryRow(t.Context(), "SELECT block_number::text,start_number::text,encode(block_hash,'hex'),revision FROM extractor.extraction_checkpoints").Scan(&height, &start, &hash, &revision); err != nil {
		t.Fatal(err)
	}
	if height != fmt.Sprint(n) || start != height || "0x"+hash != selected.BlockHash || revision != 0 {
		t.Fatal("invalid isolated checkpoint")
	}
	for _, table := range []string{"raw_blocks", "outbox_events", "active_blocks"} {
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM extractor."+table).Scan(&count); err != nil || count != 1 {
			t.Fatal("unexpected durable count", table)
		}
	}
	var id, status string
	if err := pool.QueryRow(t.Context(), "SELECT event_id,status FROM extractor.outbox_events").Scan(&id, &status); err != nil || id != e.Header.ID.EventID() || status != "pending" {
		t.Fatal("handoff intent missing or changed")
	}
	t.Logf("verified authentic seed block=%d transactions=%d receipts=%d; exact bytes/provenance, isolated checkpoint and duplicate extraction PASS", n, len(e.Transactions), len(e.Receipts))
}
