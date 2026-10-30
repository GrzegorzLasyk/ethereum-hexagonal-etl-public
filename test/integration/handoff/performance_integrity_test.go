//go:build postgresintegration && perfbaseline

package handoff_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/fixture"
	"github.com/GrzegorzLasyk/ethereum-hexagonal-etl/test/performance"
	"github.com/jackc/pgx/v5/pgxpool"
)

func assertBaselineData(t *testing.T, s performance.Scenario, pools map[string]*pgxpool.Pool, seed *performance.Seed) {
	t.Helper()
	ex, tr, lo := pools["extractor"], pools["transformer"], pools["loader"]
	totalTx, totalLogs := 0, 0
	for n := s.Start; n < s.Start+uint64(s.Blocks); n++ {
		r := new(realInput)
		if s.Dataset == "authentic-mainnet" {
			data, e := os.ReadFile(s.Artifact)
			if e != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != s.Digest || json.Unmarshal(data, r) != nil {
				t.Fatal("authentic artifact changed")
			}
		} else {
			var receipts []json.RawMessage
			r.RawBlock, receipts = fixture.Block(n, false, true)
			for _, receipt := range receipts {
				r.RawReceipts = append(r.RawReceipts, receipt)
			}
			r.Provider = "synthetic-stage11"
			r.AcquiredAt = time.Unix(100, 123).UTC()
		}
		var e error
		r.block, e = decodeObject(r.RawBlock)
		if e != nil {
			t.Fatal(e)
		}
		for _, v := range r.block["transactions"].([]any) {
			r.transactions = append(r.transactions, v.(map[string]any))
		}
		for _, raw := range r.RawReceipts {
			receipt, e := decodeObject(raw)
			if e != nil {
				t.Fatal(e)
			}
			r.receipts = append(r.receipts, receipt)
			for _, v := range receipt["logs"].([]any) {
				r.logs = append(r.logs, v.(map[string]any))
			}
		}
		totalTx += len(r.transactions)
		totalLogs += len(r.logs)
		hash := strings.TrimPrefix(r.block["hash"].(string), "0x")
		suffix := fmt.Sprintf("/v1/1/%d/%s", n, hash)
		rawID, normalizedID := "raw-block"+suffix, "transformed-block"+suffix
		for _, c := range []struct {
			p     *pgxpool.Pool
			table string
		}{{ex, "extractor.raw_blocks"}, {tr, "transformer.inbox_events"}} {
			var block []byte
			var receipts [][]byte
			var provider, at string
			if e := c.p.QueryRow(t.Context(), "SELECT raw_block,raw_receipts,provider,extracted_at FROM "+c.table+" WHERE block_number=$1", fmt.Sprint(n)).Scan(&block, &receipts, &provider, &at); e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(block, r.RawBlock) || len(receipts) != len(r.RawReceipts) || provider != r.Provider || at != r.AcquiredAt.Format(time.RFC3339Nano) {
				t.Fatal("raw data/provenance mismatch")
			}
			for i := range receipts {
				if !bytes.Equal(receipts[i], r.RawReceipts[i]) {
					t.Fatal("receipt bytes changed")
				}
			}
		}
		for _, c := range []struct {
			p    *pgxpool.Pool
			q    string
			args []any
		}{
			{ex, "SELECT count(*) FROM extractor.outbox_events WHERE event_id=$1 AND status='delivered'", []any{rawID}},
			{ex, "SELECT count(*) FROM extractor.active_blocks WHERE block_number=$1 AND encode(block_hash,'hex')=$2", []any{fmt.Sprint(n), hash}},
			{tr, "SELECT count(*) FROM transformer.inbox_events WHERE event_id=$1 AND status='completed'", []any{rawID}},
			{tr, "SELECT count(*) FROM transformer.outbox_events WHERE event_id=$1 AND status='delivered'", []any{normalizedID}},
			{tr, "SELECT count(*) FROM transformer.transformed_blocks WHERE event_id=$1 AND source_event_id=$2", []any{normalizedID, rawID}},
			{lo, "SELECT count(*) FROM loader.inbox_events WHERE event_id=$1 AND source_event_id=$2 AND status='completed'", []any{normalizedID, rawID}},
			{lo, "SELECT count(*) FROM loader.blocks WHERE event_id=$1 AND canonical_status='unknown' AND NOT height_conflict", []any{normalizedID}},
		} {
			var count int
			if e := c.p.QueryRow(t.Context(), c.q, c.args...).Scan(&count); e != nil || count != 1 {
				t.Fatal("durable correlation mismatch", e)
			}
		}
		var transformed, loaded []byte
		var transformedAt time.Time
		var loadedAt, provider, extractedAt string
		if e := tr.QueryRow(t.Context(), "SELECT payload,transformed_at FROM transformer.transformed_blocks WHERE event_id=$1", normalizedID).Scan(&transformed, &transformedAt); e != nil {
			t.Fatal(e)
		}
		if e := lo.QueryRow(t.Context(), "SELECT payload,transformed_at,provider,extracted_at FROM loader.inbox_events WHERE event_id=$1", normalizedID).Scan(&loaded, &loadedAt, &provider, &extractedAt); e != nil {
			t.Fatal(e)
		}
		if !bytes.Equal(transformed, loaded) || loadedAt != transformedAt.UTC().Format(time.RFC3339Nano) || provider != r.Provider || extractedAt != r.AcquiredAt.Format(time.RFC3339Nano) {
			t.Fatal("normalized payload/provenance changed")
		}
		for _, table := range []struct {
			name, order string
			records     []map[string]any
		}{{"blocks", "block_number", []map[string]any{r.block}}, {"transactions", "transaction_index", r.transactions}, {"receipts", "transaction_index", r.receipts}, {"logs", "log_index", r.logs}} {
			rows, e := lo.Query(t.Context(), "SELECT to_jsonb(t) FROM loader."+table.name+" t WHERE block_number=$1 ORDER BY "+table.order, fmt.Sprint(n))
			if e != nil {
				t.Fatal(e)
			}
			i := 0
			for rows.Next() {
				var data []byte
				if rows.Scan(&data) != nil {
					t.Fatal("invalid persisted row")
				}
				stored, e := decodeObject(data)
				if e != nil || i >= len(table.records) {
					t.Fatal("unexpected normalized row")
				}
				if e := compareRecord(table.name, table.records[i], stored); e != nil {
					t.Fatal(e)
				}
				i++
			}
			e = rows.Err()
			rows.Close()
			if e != nil || i != len(table.records) {
				t.Fatal("incomplete normalized records", e)
			}
		}
		if n == s.Start+uint64(s.Blocks)-1 {
			var count int
			if e := ex.QueryRow(t.Context(), "SELECT count(*) FROM extractor.extraction_checkpoints WHERE chain_id='1' AND start_number=$1 AND block_number=$2 AND encode(block_hash,'hex')=$3 AND revision=0", fmt.Sprint(s.Start), fmt.Sprint(n), hash).Scan(&count); e != nil || count != 1 {
				t.Fatal("ordered checkpoint mismatch", e)
			}
		}
	}
	for _, c := range []struct {
		service, table string
		count          int
	}{{"extractor", "raw_blocks", s.Blocks}, {"extractor", "active_blocks", s.Blocks}, {"extractor", "outbox_events", s.Blocks}, {"transformer", "inbox_events", s.Blocks}, {"transformer", "outbox_events", s.Blocks}, {"transformer", "transformed_blocks", s.Blocks}, {"loader", "inbox_events", s.Blocks}, {"loader", "blocks", s.Blocks}, {"loader", "transactions", totalTx}, {"loader", "receipts", totalTx}, {"loader", "logs", totalLogs}} {
		if queryInt(t, pools[c.service], "SELECT count(*) FROM "+c.service+"."+c.table) != c.count {
			t.Fatal("duplicate or missing history", c.table)
		}
	}
	var duplicate performance.Seed
	data, e := os.ReadFile(filepath.Join(s.Directory, "duplicate-source.json"))
	if e != nil || json.Unmarshal(data, &duplicate) != nil || !duplicate.DuplicateVerified {
		t.Fatal("missing source deduplication proof")
	}
	if seed.Transactions != totalTx || seed.Receipts != totalTx || seed.Logs != totalLogs {
		t.Fatal("source measurement counts mismatch")
	}
}
