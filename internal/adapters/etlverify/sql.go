// sql.go implements owning-role diagnostic queries for complete pipeline verification in the
// shared technical support layer, outside service domains.

package etlverify

import (
	"fmt"
	"strings"
)

// All interpolation is validated numeric/hex target data. Queries are one local
// MVCC statement per role; there is no cross-service transaction or join.
func sqlFor(service string, t Target) string {
	rawID := fmt.Sprintf("raw-block/v1/1/%d/%s", t.Number, t.Hash)
	transformedID := fmt.Sprintf("transformed-block/v1/1/%d/%s", t.Number, t.Hash)
	identity := fmt.Sprintf("chain_id='1' AND block_number=%d AND encode(block_hash,'hex')='%s'", t.Number, t.Hash)
	provenance := "encode(sha256(convert_to(provider||'/'||extracted_at,'UTF8')),'hex')"
	rawDigest := `encode(sha256(raw_block),'hex')||'/'||array_to_string(ARRAY(SELECT encode(sha256(v),'hex') FROM unnest(raw_receipts) v),'/')`
	rawValid := fmt.Sprintf(`document->>'hash'='0x%s'
 AND document->>'number'='0x%x'
 AND jsonb_array_length(document->'transactions')=%d
 AND cardinality(raw_receipts)=%d
 AND (SELECT coalesce(sum(jsonb_array_length(convert_from(v,'UTF8')::jsonb->'logs')),0) FROM unnest(raw_receipts) v)=%d
 AND NOT EXISTS(SELECT 1 FROM unnest(raw_receipts) WITH ORDINALITY AS receipts(v,n)
 WHERE convert_from(v,'UTF8')::jsonb->>'transactionHash' IS DISTINCT FROM document->'transactions'->(n::int-1)->>'hash'
 OR convert_from(v,'UTF8')::jsonb->>'blockHash' IS DISTINCT FROM '0x%s'
 OR convert_from(v,'UTF8')::jsonb->>'blockNumber' IS DISTINCT FROM '0x%x'
 OR convert_from(v,'UTF8')::jsonb->>'transactionIndex' IS DISTINCT FROM '0x'||to_hex(n-1))`, t.Hash, t.Number, t.Transactions, t.Receipts, t.Logs, t.Hash, t.Number)
	members := `(SELECT jsonb_agg(v->>'hash' ORDER BY n) FROM jsonb_array_elements(document->'transactions') WITH ORDINALITY a(v,n))`
	source := func(query, table string) string {
		// Parsing a full block for every receipt makes large snapshots quadratic.
		// Materialize the decoded document once without rewriting stored bytes.
		return "WITH source AS MATERIALIZED (SELECT *,convert_from(raw_block,'UTF8')::jsonb AS document FROM " + table + " WHERE " + identity + ") " + strings.ReplaceAll(query, table, "source")
	}
	switch service {
	case "extractor":
		return source(fmt.Sprintf(`SELECT jsonb_build_object(
 'ready', (SELECT count(*)=1 AND coalesce(bool_and(%s),false) FROM extractor.raw_blocks WHERE %s)
 AND (SELECT count(*)=1 FROM extractor.extraction_checkpoints WHERE %s AND start_number=%d)
 AND (SELECT count(*)=1 FROM extractor.active_blocks WHERE %s),
 'states',jsonb_build_object('outbox',coalesce((SELECT status FROM extractor.outbox_events WHERE event_id='%s'),'missing')),
 'stuck',coalesce((SELECT status='sending' AND lease_until<=clock_timestamp() FROM extractor.outbox_events WHERE event_id='%s'),false),
 'raw_digest',(SELECT %s FROM extractor.raw_blocks WHERE %s),
 'provenance',(SELECT %s FROM extractor.raw_blocks WHERE %s),
 'members',(SELECT %s FROM extractor.raw_blocks WHERE %s))`, rawValid, identity, identity, t.Number, identity, rawID, rawID, rawDigest, identity, provenance, identity, members, identity), "extractor.raw_blocks")
	case "transformer":
		return source(fmt.Sprintf(`SELECT jsonb_build_object(
 'ready',(SELECT count(*)=1 AND coalesce(bool_and(%s),false) FROM transformer.inbox_events WHERE %s AND event_id='%s')
 AND (SELECT count(*)=1 FROM transformer.transformed_blocks WHERE event_id='%s' AND source_event_id='%s'),
 'states',jsonb_build_object('inbox',coalesce((SELECT status FROM transformer.inbox_events WHERE event_id='%s'),'missing'),
 'outbox',coalesce((SELECT status FROM transformer.outbox_events WHERE event_id='%s'),'missing')),
 'stuck',coalesce((SELECT status='processing' AND lease_until<=clock_timestamp() FROM transformer.inbox_events WHERE event_id='%s'),false)
 OR coalesce((SELECT status='sending' AND lease_until<=clock_timestamp() FROM transformer.outbox_events WHERE event_id='%s'),false),
 'raw_digest',(SELECT %s FROM transformer.inbox_events WHERE event_id='%s'),
 'payload_digest',(SELECT encode(sha256(payload),'hex') FROM transformer.transformed_blocks WHERE event_id='%s'),
 'provenance',(SELECT %s FROM transformer.inbox_events WHERE event_id='%s'),
 'members',(SELECT %s FROM transformer.inbox_events WHERE event_id='%s'))`, rawValid, identity, rawID, transformedID, rawID, rawID, transformedID, rawID, transformedID, rawDigest, rawID, transformedID, provenance, rawID, members, rawID), "transformer.inbox_events")
	case "loader":
		return fmt.Sprintf(`SELECT jsonb_build_object(
 'ready',(SELECT count(*)=1 FROM loader.inbox_events WHERE %s AND event_id='%s' AND source_event_id='%s')
 AND (SELECT count(*)=1 FROM loader.blocks WHERE %s AND event_id='%s' AND canonical_status='unknown' AND NOT height_conflict)
 AND (SELECT count(*)=%d FROM loader.transactions WHERE %s)
 AND (SELECT count(*)=%d FROM loader.receipts WHERE %s)
 AND (SELECT count(*)=%d FROM loader.logs WHERE %s),
 'states',jsonb_build_object('inbox',coalesce((SELECT status FROM loader.inbox_events WHERE event_id='%s'),'missing')),
 'stuck',coalesce((SELECT status='processing' AND lease_until<=clock_timestamp() FROM loader.inbox_events WHERE event_id='%s'),false),
 'payload_digest',(SELECT encode(sha256(payload),'hex') FROM loader.inbox_events WHERE event_id='%s'),
 'provenance',(SELECT %s FROM loader.inbox_events WHERE event_id='%s'),
 'members',(SELECT jsonb_agg('0x'||encode(transaction_hash,'hex') ORDER BY transaction_index) FROM loader.transactions WHERE %s))`, identity, transformedID, rawID, identity, transformedID, t.Transactions, identity, t.Receipts, identity, t.Logs, identity, transformedID, transformedID, transformedID, provenance, transformedID, identity)
	}
	return ""
}
