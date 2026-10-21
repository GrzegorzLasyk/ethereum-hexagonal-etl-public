-- Accepted extraction lineage only: competing hashes are rejected, not promoted.
-- numeric(20,0) retains the complete uint64 height range used by the port.
CREATE TABLE extractor.raw_blocks (
    chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
    block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    parent_hash bytea NOT NULL CHECK (octet_length(parent_hash) = 32),
    raw_block bytea NOT NULL CHECK (octet_length(raw_block) > 0),
    raw_receipts bytea[] NOT NULL,
    metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
    provider text NOT NULL CHECK (length(btrim(provider)) > 0),
    -- RFC3339Nano preserves source provenance beyond PostgreSQL's microseconds.
    extracted_at text NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (chain_id, block_number, block_hash),
    UNIQUE (chain_id, block_number)
);

CREATE TABLE extractor.outbox_events (
    event_id text PRIMARY KEY,
    chain_id text NOT NULL,
    block_number numeric(20,0) NOT NULL,
    block_hash bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (chain_id, block_number, block_hash),
    FOREIGN KEY (chain_id, block_number, block_hash)
        REFERENCES extractor.raw_blocks (chain_id, block_number, block_hash),
    CHECK (event_id = 'raw-block/v1/' || chain_id || '/' || block_number::text || '/' || encode(block_hash, 'hex'))
);

CREATE TABLE extractor.extraction_checkpoints (
    chain_id text PRIMARY KEY,
    block_number numeric(20,0) NOT NULL,
    block_hash bytea NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (chain_id, block_number, block_hash)
        REFERENCES extractor.raw_blocks (chain_id, block_number, block_hash),
    FOREIGN KEY (chain_id, block_number, block_hash)
        REFERENCES extractor.outbox_events (chain_id, block_number, block_hash)
);

REVOKE ALL ON extractor.raw_blocks, extractor.outbox_events,
    extractor.extraction_checkpoints FROM PUBLIC;
GRANT SELECT, INSERT ON extractor.raw_blocks, extractor.outbox_events,
    extractor.extraction_checkpoints TO extractor_runtime;
GRANT UPDATE (block_number, block_hash, updated_at)
    ON extractor.extraction_checkpoints TO extractor_runtime;
-- No DELETE/TRUNCATE, raw-data UPDATE, delivery-state UPDATE or sequence grants.
-- Every outbox row is a pending intent; dispatch/ACK state belongs to later stages.
