package postgres

import "github.com/GrzegorzLasyk/ethereum-hexagonal-etl/internal/adapters/dbreadiness"

// readinessPolicy is the binary compatibility contract. Update alongside migrations.
var readinessPolicy = dbreadiness.Policy{Schema: "transformer", Tables: []dbreadiness.Table{
	{Name: "inbox_events", Columns: []string{"event_id", "chain_id", "block_number", "block_hash", "parent_hash", "event_version", "payload_version", "provider", "extracted_at", "raw_block", "raw_receipts", "accepted_at", "status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "last_error_code", "completed_at"}, Types: []string{"text", "text", "numeric(20,0)", "bytea", "bytea", "integer", "integer", "text", "text", "bytea", "bytea[]", "timestamp with time zone", "text", "integer", "integer", "timestamp with time zone", "timestamp with time zone", "text", "timestamp with time zone"}, Updates: []string{"status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "last_error_code", "completed_at"}, Insert: true, Delete: false, View: false},
	{Name: "transformed_blocks", Columns: []string{"event_id", "source_event_id", "payload_version", "payload", "transformed_at"}, Types: []string{"text", "text", "integer", "bytea", "timestamp with time zone"}, Updates: []string{}, Insert: true, Delete: false, View: false},
	{Name: "outbox_events", Columns: []string{"event_id", "created_at", "status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "delivered_at", "last_error_code"}, Types: []string{"text", "timestamp with time zone", "text", "integer", "integer", "timestamp with time zone", "timestamp with time zone", "timestamp with time zone", "text"}, Updates: []string{"status", "attempts", "max_attempts", "next_attempt_at", "lease_until", "delivered_at", "last_error_code"}, Insert: true, Delete: false, View: false},
	{Name: "recovery_operations", Columns: []string{"chain_id", "operation_id", "queue", "event_id", "allowance", "fenced_attempt", "created_at"}, Types: []string{"text", "text", "text", "text", "integer", "integer", "timestamp with time zone"}, Updates: []string{}, Insert: true},
	{Name: "schema_readiness", Columns: []string{"version", "checksum"}, Types: []string{"text", "text"}, Updates: []string{}, Insert: false, Delete: false, View: true},
}, Indexes: []string{"inbox_outstanding", "outbox_outstanding", "recovery_operations_pkey", "inbox_pending", "inbox_expired", "outbox_pending", "outbox_expired", "inbox_events_pkey", "transformed_blocks_pkey", "outbox_events_pkey"}, Migrations: map[string]string{
	"0006_admission_indexes.sql":           "a727eb16dd8341e58ae4c32717d51eb80eb2e2d5c5f52de5ca1c205eafcdf181",
	"0005_recovery_operations.sql":         "02661917c943f6df4bd714842052a876a5462fa2813f2d6bb7e9fbfefc75da06",
	"0001_schema_privileges.sql":           "ea47b832d2853912dc8d45b6dc43b647b6053a5722d49b8b5ece9e4b1e52f14b",
	"0002_inbox_transformation_outbox.sql": "0ddc680fb0fbd1f7aff9abab07a78e1cb6266b8677a92c6d206ef0e2a94ac802",
	"0003_outbox_delivery.sql":             "69dacd270f632c92460710d4d25c25dc7bba1b1bcfad48a4db2a3f93f86cba8f",
	"0004_readiness_metadata.sql":          "a2cc865aedb617d8eb6085231ac44a183c62dfe8ce8131fd2d153e3ddc8a93f5",
}}
