-- ADR 0004: immutable observations, separate provider-relative active branch.
ALTER TABLE extractor.raw_blocks DROP CONSTRAINT raw_blocks_chain_id_block_number_key;
ALTER TABLE extractor.extraction_checkpoints
 ADD COLUMN revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 ADD COLUMN start_number numeric(20,0);
UPDATE extractor.extraction_checkpoints c SET start_number=(
 SELECT min(block_number) FROM extractor.raw_blocks b WHERE b.chain_id=c.chain_id);
ALTER TABLE extractor.extraction_checkpoints ALTER COLUMN start_number SET NOT NULL;
ALTER TABLE extractor.extraction_checkpoints ADD CHECK(start_number BETWEEN 0 AND block_number);
CREATE TABLE extractor.active_blocks (
 chain_id text NOT NULL, block_number numeric(20,0) NOT NULL, block_hash bytea NOT NULL,
 PRIMARY KEY(chain_id,block_number),
 FOREIGN KEY(chain_id,block_number,block_hash) REFERENCES extractor.outbox_events(chain_id,block_number,block_hash)
);
INSERT INTO extractor.active_blocks SELECT chain_id,block_number,block_hash FROM extractor.raw_blocks;
CREATE TABLE extractor.rewinds (
 chain_id text NOT NULL, previous_revision bigint NOT NULL CHECK(previous_revision>=0),
 from_number numeric(20,0) NOT NULL, from_hash bytea NOT NULL,
 to_number numeric(20,0) NOT NULL, to_hash bytea NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(chain_id,previous_revision),
 CHECK(to_number<from_number),
 FOREIGN KEY(chain_id,from_number,from_hash) REFERENCES extractor.raw_blocks(chain_id,block_number,block_hash),
 FOREIGN KEY(chain_id,to_number,to_hash) REFERENCES extractor.raw_blocks(chain_id,block_number,block_hash)
);
CREATE TABLE extractor.reindex_jobs (
 chain_id text NOT NULL, operation_id text NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128),
 start_number numeric(20,0) NOT NULL CHECK(start_number>=0),
 end_number numeric(20,0) NOT NULL CHECK(end_number<=18446744073709551615 AND end_number>=start_number),
 last_number numeric(20,0), last_hash bytea,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(chain_id,operation_id),
 CHECK((last_number IS NULL)=(last_hash IS NULL)),
 CHECK(last_number BETWEEN start_number AND end_number),
 FOREIGN KEY(chain_id,last_number,last_hash) REFERENCES extractor.outbox_events(chain_id,block_number,block_hash)
);
-- Attempts are monotonic fencing tokens, including across operator replay.
ALTER TABLE extractor.outbox_events DROP CONSTRAINT outbox_events_max_attempts_check;
ALTER TABLE extractor.outbox_events ADD CHECK(max_attempts>=1);
CREATE TABLE extractor.replays (
 chain_id text NOT NULL, operation_id text NOT NULL CHECK(length(operation_id) BETWEEN 1 AND 128),
 event_id text NOT NULL REFERENCES extractor.outbox_events(event_id),
 allowance integer NOT NULL CHECK(allowance BETWEEN 1 AND 1000),
 fenced_attempt integer NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(chain_id,operation_id)
);
REVOKE ALL ON extractor.active_blocks,extractor.rewinds,extractor.reindex_jobs,extractor.replays FROM PUBLIC;
GRANT SELECT,INSERT,DELETE ON extractor.active_blocks TO extractor_runtime;
GRANT SELECT,INSERT ON extractor.rewinds,extractor.reindex_jobs,extractor.replays TO extractor_runtime;
GRANT UPDATE(last_number,last_hash,updated_at) ON extractor.reindex_jobs TO extractor_runtime;
GRANT UPDATE(revision) ON extractor.extraction_checkpoints TO extractor_runtime;
