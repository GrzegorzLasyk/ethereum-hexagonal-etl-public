//go:build postgresintegration

package handoff_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type realInput struct {
	RawBlock                     []byte    `json:"raw_block_base64"`
	RawReceipts                  [][]byte  `json:"raw_receipts_base64"`
	Provider                     string    `json:"provider"`
	AcquiredAt                   time.Time `json:"acquired_at"`
	block                        map[string]any
	transactions, receipts, logs []map[string]any
}

func readRealInput(t *testing.T) *realInput {
	t.Helper()
	path := os.Getenv("REAL_FIXTURE_ARTIFACT")
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(data)) != os.Getenv("REAL_FIXTURE_SHA256") {
		t.Fatal("verified artifact missing or changed")
	}
	r := new(realInput)
	if json.Unmarshal(data, r) != nil {
		t.Fatal("invalid artifact")
	}
	r.block, err = decodeObject(r.RawBlock)
	if err != nil {
		t.Fatal("invalid block")
	}
	txs, ok := r.block["transactions"].([]any)
	if !ok {
		t.Fatal("missing transactions")
	}
	for _, v := range txs {
		tx, ok := v.(map[string]any)
		if !ok {
			t.Fatal("incomplete transaction")
		}
		r.transactions = append(r.transactions, tx)
	}
	for _, raw := range r.RawReceipts {
		receipt, err := decodeObject(raw)
		if err != nil {
			t.Fatal("invalid receipt")
		}
		r.receipts = append(r.receipts, receipt)
		logs, ok := receipt["logs"].([]any)
		if !ok {
			t.Fatal("missing logs")
		}
		for _, v := range logs {
			log, ok := v.(map[string]any)
			if !ok {
				t.Fatal("invalid log")
			}
			r.logs = append(r.logs, log)
		}
	}
	if len(r.transactions) != len(r.receipts) {
		t.Fatal("incomplete receipts")
	}
	return r
}
func assertRealPipeline(t *testing.T, r *realInput, ex, tr, lo *pgxpool.Pool, raw *rawProxy, normalized *transformedProxy) {
	t.Helper()
	number, err := sqlValue(r.block["number"], "s")
	if err != nil {
		t.Fatal(err)
	}
	hash := strings.TrimPrefix(r.block["hash"].(string), "0x")
	suffix := "/v1/1/" + number.(string) + "/" + hash
	rawID, transformedID := "raw-block"+suffix, "transformed-block"+suffix
	for _, check := range []struct {
		p     *pgxpool.Pool
		table string
	}{{ex, "extractor.raw_blocks"}, {tr, "transformer.inbox_events"}} {
		var block []byte
		var receipts [][]byte
		var provider, at string
		err := check.p.QueryRow(t.Context(), "SELECT raw_block,raw_receipts,provider,extracted_at FROM "+check.table).Scan(&block, &receipts, &provider, &at)
		if err != nil {
			t.Fatal("read raw persistence", err)
		}
		if !bytes.Equal(block, r.RawBlock) || len(receipts) != len(r.RawReceipts) || provider != r.Provider || at != r.AcquiredAt.Format(time.RFC3339Nano) {
			t.Fatal("raw bytes/provenance changed", check.table)
		}
		for i := range receipts {
			if !bytes.Equal(receipts[i], r.RawReceipts[i]) {
				t.Fatal("receipt bytes changed", check.table, i)
			}
		}
	}
	for _, check := range []struct {
		p    *pgxpool.Pool
		q    string
		args []any
	}{
		{ex, "SELECT count(*) FROM extractor.extraction_checkpoints WHERE chain_id='1' AND start_number=$1 AND block_number=$1 AND encode(block_hash,'hex')=$2 AND revision=0", []any{number, hash}},
		{ex, "SELECT count(*) FROM extractor.outbox_events WHERE event_id=$1 AND status='delivered'", []any{rawID}},
		{ex, "SELECT count(*) FROM extractor.active_blocks WHERE block_number=$1 AND encode(block_hash,'hex')=$2", []any{number, hash}},
		{tr, "SELECT count(*) FROM transformer.inbox_events WHERE event_id=$1 AND status='completed'", []any{rawID}},
		{tr, "SELECT count(*) FROM transformer.outbox_events WHERE event_id=$1 AND status='delivered'", []any{transformedID}},
		{tr, "SELECT count(*) FROM transformer.transformed_blocks WHERE event_id=$1 AND source_event_id=$2", []any{transformedID, rawID}},
		{lo, "SELECT count(*) FROM loader.inbox_events WHERE event_id=$1 AND source_event_id=$2 AND status='completed'", []any{transformedID, rawID}},
		{lo, "SELECT count(*) FROM loader.blocks WHERE event_id=$1 AND canonical_status='unknown' AND NOT height_conflict AND parent_link='unknown'", []any{transformedID}},
	} {
		var count int
		if err := check.p.QueryRow(t.Context(), check.q, check.args...).Scan(&count); err != nil {
			t.Fatal("durable identity assertion", err)
		}
		if count != 1 {
			t.Fatal("durable identity/state mismatch")
		}
	}
	var transformedPayload, loadedPayload []byte
	var trAt time.Time
	var loAt, provider, extractedAt string
	if err := tr.QueryRow(t.Context(), "SELECT payload,transformed_at FROM transformer.transformed_blocks").Scan(&transformedPayload, &trAt); err != nil {
		t.Fatal(err)
	}
	if err := lo.QueryRow(t.Context(), "SELECT payload,transformed_at,provider,extracted_at FROM loader.inbox_events").Scan(&loadedPayload, &loAt, &provider, &extractedAt); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(transformedPayload, loadedPayload) || trAt.UTC().Format(time.RFC3339Nano) != loAt || provider != r.Provider || extractedAt != r.AcquiredAt.Format(time.RFC3339Nano) {
		t.Fatal("normalized handoff changed payload/provenance")
	}
	raw.mu.Lock()
	wireRaw := raw.first
	raw.mu.Unlock()
	normalized.mu.Lock()
	wireNormalized := normalized.first
	normalized.mu.Unlock()
	if wireRaw == nil || wireNormalized == nil || wireRaw.Metadata.EventId != rawID || wireNormalized.Metadata.EventId != transformedID || wireNormalized.SourceEventId != rawID || !bytes.Equal(wireNormalized.PayloadJson, transformedPayload) || !bytes.Equal(wireRaw.Payload.BlockJson, r.RawBlock) {
		t.Fatal("wire correlation or payload mismatch")
	}
	if len(wireRaw.Payload.Receipts.Json) != len(r.RawReceipts) {
		t.Fatal("wire receipts incomplete")
	}
	for i, v := range wireRaw.Payload.Receipts.Json {
		if !bytes.Equal(v, r.RawReceipts[i]) {
			t.Fatal("wire receipt changed", i)
		}
	}
	for _, table := range []struct {
		name, order string
		records     []map[string]any
	}{
		{"blocks", "block_number", []map[string]any{r.block}}, {"transactions", "transaction_index", r.transactions}, {"receipts", "transaction_index", r.receipts}, {"logs", "log_index", r.logs},
	} {
		rows, err := lo.Query(t.Context(), "SELECT to_jsonb(t) FROM loader."+table.name+" t ORDER BY "+table.order)
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			stored, err := decodeObject(data)
			if err != nil {
				rows.Close()
				t.Fatal("invalid stored row")
			}
			if i >= len(table.records) {
				rows.Close()
				t.Fatal("extra row")
			}
			if err := compareRecord(table.name, table.records[i], stored); err != nil {
				rows.Close()
				t.Fatalf("row %d: %v", i, err)
			}
			i++
		}
		err = rows.Err()
		rows.Close()
		if err != nil || i != len(table.records) {
			t.Fatal("incomplete stored records", table.name, err)
		}
	}
	types := map[string]int{}
	failed, roots, creations := 0, 0, 0
	for _, tx := range r.transactions {
		kind := "legacy-absent"
		if v, ok := tx["type"]; ok {
			kind = fmt.Sprint(v)
		}
		types[kind]++
	}
	for _, receipt := range r.receipts {
		if receipt["status"] == "0x0" {
			failed++
		}
		if receipt["root"] != nil {
			roots++
		}
		if receipt["contractAddress"] != nil {
			creations++
		}
	}
	t.Logf("authentic block=%s transactions=%d receipts=%d logs=%d types=%v failed=%d historical_roots=%d contract_creations=%d; all normalized fields, raw bytes, provenance and correlation PASS", number, len(r.transactions), len(r.receipts), len(r.logs), types, failed, roots, creations)
}
