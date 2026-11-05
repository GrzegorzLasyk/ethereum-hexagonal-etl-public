// readiness_policy.go implements required schema objects, migration identities and runtime
// privilege policy in the outbound adapter layer.

package postgres

import "github.com/GrzegorzLasyk/ethereum-hexagonal-etl-public/internal/adapters/dbreadiness"

// readinessPolicy is the binary compatibility contract. Update alongside migrations.
var readinessPolicy = dbreadiness.Policy{Schema: "extractor", Tables: []dbreadiness.Table{
	{Name: "raw_blocks", Columns: []string{"chain_id", "block_number", "block_hash", "parent_hash", "raw_block", "raw_receipts", "metadata", "provider", "extracted_at", "stored_at"}, Types: []string{"text", "numeric(20,0)", "bytea", "bytea", "bytea", "bytea[]", "jsonb", "text", "text", "timestamp with time zone"}, Updates: []string{}, Insert: true, Delete: false, View: false},
	{Name: "outbox_events", Columns: []string{"event_id", "chain_id", "block_number", "block_hash", "created_at", "status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "delivered_at", "last_error_code"}, Types: []string{"text", "text", "numeric(20,0)", "bytea", "timestamp with time zone", "text", "integer", "integer", "timestamp with time zone", "timestamp with time zone", "timestamp with time zone", "text"}, Updates: []string{"status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "delivered_at", "last_error_code"}, Insert: true, Delete: false, View: false},
	{Name: "extraction_checkpoints", Columns: []string{"chain_id", "block_number", "block_hash", "updated_at", "revision", "start_number"}, Types: []string{"text", "numeric(20,0)", "bytea", "timestamp with time zone", "bigint", "numeric(20,0)"}, Updates: []string{"block_number", "block_hash", "updated_at", "revision"}, Insert: true, Delete: false, View: false},
	{Name: "active_blocks", Columns: []string{"chain_id", "block_number", "block_hash"}, Types: []string{"text", "numeric(20,0)", "bytea"}, Updates: []string{}, Insert: true, Delete: true, View: false},
	{Name: "rewinds", Columns: []string{"chain_id", "previous_revision", "from_number", "from_hash", "to_number", "to_hash", "created_at"}, Types: []string{"text", "bigint", "numeric(20,0)", "bytea", "numeric(20,0)", "bytea", "timestamp with time zone"}, Updates: []string{}, Insert: true, Delete: false, View: false},
	{Name: "reindex_jobs", Columns: []string{"chain_id", "operation_id", "start_number", "end_number", "last_number", "last_hash", "created_at", "updated_at"}, Types: []string{"text", "text", "numeric(20,0)", "numeric(20,0)", "numeric(20,0)", "bytea", "timestamp with time zone", "timestamp with time zone"}, Updates: []string{"last_number", "last_hash", "updated_at"}, Insert: true, Delete: false, View: false},
	{Name: "replays", Columns: []string{"chain_id", "operation_id", "event_id", "allowance", "fenced_attempt", "created_at"}, Types: []string{"text", "text", "text", "integer", "integer", "timestamp with time zone"}, Updates: []string{}, Insert: true, Delete: false, View: false},
	{Name: "schema_readiness", Columns: []string{"version", "checksum"}, Types: []string{"text", "text"}, Updates: []string{}, Insert: false, Delete: false, View: true},
}, Indexes: []string{"outbox_pending", "outbox_expired", "raw_blocks_pkey", "outbox_events_pkey", "extraction_checkpoints_pkey", "active_blocks_pkey", "rewinds_pkey", "reindex_jobs_pkey", "replays_pkey"}, Migrations: map[string]string{
	"0001_schema_privileges.sql":  "5df63707fb2eb55d41267f93756f5039e110927a98025db5761af882bedde7c2",
	"0002_extraction_storage.sql": "70f6b3a754484c72c655520c583316e07fbb43549e2293fa450a58afc9cb85c1",
	"0003_outbox_delivery.sql":    "f064ffe2da7820049c7679157c017b6d61a90150ae1639de47c7bec4e76f5d93",
	"0004_active_branch.sql":      "5eb7392bbfb9e5b052125c729bdc3b65b4d6b16c29d8a3895019f671bad91335",
	"0005_readiness_metadata.sql": "8dbfbef6348fef429d51e99cc723d930f2c80f48bec4bee97967fbdb2d316221",
}}
