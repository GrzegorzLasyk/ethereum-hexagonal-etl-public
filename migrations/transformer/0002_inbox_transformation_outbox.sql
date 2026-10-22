-- Applied only by transformer_migrator. Payload/provenance columns are immutable
-- to transformer_runtime; only processing state can be updated.
CREATE TABLE transformer.inbox_events (
 event_id text PRIMARY KEY,
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 parent_hash bytea NOT NULL CHECK (octet_length(parent_hash)=32),
 event_version integer NOT NULL CHECK (event_version=1),
 payload_version integer NOT NULL CHECK (payload_version=1),
 provider text NOT NULL,
 extracted_at text NOT NULL,
 raw_block bytea NOT NULL CHECK (octet_length(raw_block)>0),
 raw_receipts bytea[] NOT NULL,
 accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','completed','failed')),
 attempts integer NOT NULL DEFAULT 0 CHECK (attempts>=0),
 max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 1000),
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_until timestamptz,
 last_error_code text,
 completed_at timestamptz,
 CHECK ((status='processing')=(lease_until IS NOT NULL)),
 CHECK ((status='completed')=(completed_at IS NOT NULL))
);
CREATE INDEX inbox_pending ON transformer.inbox_events (chain_id,next_attempt_at,accepted_at,event_id) WHERE status='pending';
CREATE INDEX inbox_expired ON transformer.inbox_events (chain_id,lease_until) WHERE status='processing';

-- Versioned document storage, not Loader's normalized relational query tables.
CREATE TABLE transformer.transformed_blocks (
 event_id text PRIMARY KEY,
 source_event_id text NOT NULL UNIQUE REFERENCES transformer.inbox_events(event_id),
 payload_version integer NOT NULL CHECK (payload_version=1),
 payload bytea NOT NULL CHECK (octet_length(payload)>0 AND octet_length(payload)<=67108864),
 transformed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- All rows are pending intents. Dispatcher/delivery state belongs to Stage 9.
CREATE TABLE transformer.outbox_events (
 event_id text PRIMARY KEY REFERENCES transformer.transformed_blocks(event_id),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

GRANT SELECT,INSERT ON transformer.inbox_events,transformer.transformed_blocks,transformer.outbox_events TO transformer_runtime;
GRANT UPDATE (status,attempts,next_attempt_at,lease_until,last_error_code,completed_at)
 ON transformer.inbox_events TO transformer_runtime;
