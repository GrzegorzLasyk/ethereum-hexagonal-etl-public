-- Applied only by loader_migrator. Payload/provenance columns are immutable
-- to loader_runtime; only processing state can be updated.
CREATE TABLE loader.inbox_events (
 event_id text PRIMARY KEY,
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 parent_hash bytea NOT NULL CHECK (octet_length(parent_hash)=32),
 event_version integer NOT NULL CHECK (event_version=1),
 payload_version integer NOT NULL CHECK (payload_version=1),
 provider text NOT NULL,
 extracted_at text NOT NULL,
 payload bytea NOT NULL CHECK (octet_length(payload)>0 AND octet_length(payload)<=67108864),
 source_event_id text NOT NULL,
 transformed_at text NOT NULL,
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
CREATE INDEX inbox_pending ON loader.inbox_events (chain_id,next_attempt_at,accepted_at,event_id) WHERE status='pending';
CREATE INDEX inbox_expired ON loader.inbox_events (chain_id,lease_until) WHERE status='processing';

CREATE TABLE loader.blocks (
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 parent_hash bytea NOT NULL CHECK (octet_length(parent_hash)=32),
 timestamp numeric(20,0) NOT NULL CHECK (timestamp BETWEEN 0 AND 18446744073709551615),
 nonce bytea NOT NULL CHECK (octet_length(nonce)=8),
 sha3_uncles bytea NOT NULL CHECK (octet_length(sha3_uncles)=32),
 logs_bloom bytea NOT NULL CHECK (octet_length(logs_bloom)=256),
 transactions_root bytea NOT NULL CHECK (octet_length(transactions_root)=32),
 state_root bytea NOT NULL CHECK (octet_length(state_root)=32),
 receipts_root bytea NOT NULL CHECK (octet_length(receipts_root)=32),
 miner bytea NOT NULL CHECK (octet_length(miner)=20),
 difficulty numeric(78,0) NOT NULL CHECK (difficulty BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 extra_data bytea NOT NULL,
 gas_limit numeric(20,0) NOT NULL CHECK (gas_limit BETWEEN 0 AND 18446744073709551615),
 gas_used numeric(20,0) NOT NULL CHECK (gas_used BETWEEN 0 AND 18446744073709551615),
 mix_hash bytea NOT NULL CHECK (octet_length(mix_hash)=32),
 size numeric(78,0) NOT NULL CHECK (size BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 uncles bytea[] NOT NULL,
 total_difficulty numeric(78,0) CHECK (total_difficulty BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 base_fee_per_gas numeric(78,0) CHECK (base_fee_per_gas BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 withdrawals_root bytea CHECK (octet_length(withdrawals_root)=32),
 blob_gas_used numeric(20,0) CHECK (blob_gas_used BETWEEN 0 AND 18446744073709551615),
 excess_blob_gas numeric(20,0) CHECK (excess_blob_gas BETWEEN 0 AND 18446744073709551615),
 parent_beacon_block_root bytea CHECK (octet_length(parent_beacon_block_root)=32),
 requests_hash bytea CHECK (octet_length(requests_hash)=32),
 present_fields text[] NOT NULL,
 event_id text NOT NULL UNIQUE REFERENCES loader.inbox_events(event_id),
 loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (chain_id,block_number,block_hash)
);

CREATE TABLE loader.transactions (
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 transaction_hash bytea NOT NULL CHECK (octet_length(transaction_hash)=32),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 transaction_index numeric(20,0) NOT NULL CHECK (transaction_index BETWEEN 0 AND 18446744073709551615),
 from_address bytea NOT NULL CHECK (octet_length(from_address)=20),
 to_address bytea CHECK (octet_length(to_address)=20),
 nonce numeric(20,0) NOT NULL CHECK (nonce BETWEEN 0 AND 18446744073709551615),
 value numeric(78,0) NOT NULL CHECK (value BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 gas numeric(20,0) NOT NULL CHECK (gas BETWEEN 0 AND 18446744073709551615),
 input_data bytea NOT NULL,
 r numeric(78,0) NOT NULL CHECK (r BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 s numeric(78,0) NOT NULL CHECK (s BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 v numeric(78,0) CHECK (v BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 y_parity numeric(20,0) CHECK (y_parity BETWEEN 0 AND 18446744073709551615),
 type numeric(20,0) CHECK (type BETWEEN 0 AND 18446744073709551615),
 signature_chain_id text CHECK (signature_chain_id ~ '^(0|[1-9][0-9]*)$'),
 gas_price numeric(78,0) CHECK (gas_price BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 max_fee_per_gas numeric(78,0) CHECK (max_fee_per_gas BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 max_priority_fee_per_gas numeric(78,0) CHECK (max_priority_fee_per_gas BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 access_list jsonb CHECK (jsonb_typeof(access_list)='array'),
 max_fee_per_blob_gas numeric(78,0) CHECK (max_fee_per_blob_gas BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 blob_versioned_hashes bytea[],
 authorization_list jsonb CHECK (jsonb_typeof(authorization_list)='array'),
 present_fields text[] NOT NULL,
 FOREIGN KEY (chain_id,block_number,block_hash) REFERENCES loader.blocks(chain_id,block_number,block_hash),
 PRIMARY KEY (chain_id,block_number,block_hash,transaction_hash),
 UNIQUE (chain_id,block_number,block_hash,transaction_index),
 UNIQUE (chain_id,block_number,block_hash,transaction_hash,transaction_index)
);

CREATE TABLE loader.receipts (
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 transaction_hash bytea NOT NULL CHECK (octet_length(transaction_hash)=32),
 transaction_index numeric(20,0) NOT NULL CHECK (transaction_index BETWEEN 0 AND 18446744073709551615),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 from_address bytea NOT NULL CHECK (octet_length(from_address)=20),
 to_address bytea CHECK (octet_length(to_address)=20),
 contract_address bytea CHECK (octet_length(contract_address)=20),
 cumulative_gas_used numeric(20,0) NOT NULL CHECK (cumulative_gas_used BETWEEN 0 AND 18446744073709551615),
 gas_used numeric(20,0) NOT NULL CHECK (gas_used BETWEEN 0 AND 18446744073709551615),
 logs_bloom bytea NOT NULL CHECK (octet_length(logs_bloom)=256),
 type numeric(20,0) CHECK (type BETWEEN 0 AND 18446744073709551615),
 effective_gas_price numeric(78,0) CHECK (effective_gas_price BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 blob_gas_used numeric(20,0) CHECK (blob_gas_used BETWEEN 0 AND 18446744073709551615),
 blob_gas_price numeric(78,0) CHECK (blob_gas_price BETWEEN 0 AND 115792089237316195423570985008687907853269984665640564039457584007913129639935),
 status numeric(20,0) CHECK (status BETWEEN 0 AND 18446744073709551615),
 root bytea CHECK (octet_length(root)=32),
 present_fields text[] NOT NULL,
 FOREIGN KEY (chain_id,block_number,block_hash) REFERENCES loader.blocks(chain_id,block_number,block_hash),
 PRIMARY KEY (chain_id,block_number,block_hash,transaction_hash),
 UNIQUE (chain_id,block_number,block_hash,transaction_hash,transaction_index),
 FOREIGN KEY (chain_id,block_number,block_hash,transaction_hash,transaction_index) REFERENCES loader.transactions(chain_id,block_number,block_hash,transaction_hash,transaction_index),
 CHECK ((status IS NOT NULL) <> (root IS NOT NULL)),
 CHECK (status IN (0,1))
);

CREATE TABLE loader.logs (
 chain_id text NOT NULL CHECK (chain_id ~ '^(0|[1-9][0-9]*)$'),
 block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
 block_number numeric(20,0) NOT NULL CHECK (block_number BETWEEN 0 AND 18446744073709551615),
 transaction_hash bytea NOT NULL CHECK (octet_length(transaction_hash)=32),
 transaction_index numeric(20,0) NOT NULL CHECK (transaction_index BETWEEN 0 AND 18446744073709551615),
 log_index numeric(20,0) NOT NULL CHECK (log_index BETWEEN 0 AND 18446744073709551615),
 address bytea NOT NULL CHECK (octet_length(address)=20),
 topics bytea[] NOT NULL,
 data bytea NOT NULL,
 removed boolean,
 block_timestamp numeric(20,0) CHECK (block_timestamp BETWEEN 0 AND 18446744073709551615),
 present_fields text[] NOT NULL,
 FOREIGN KEY (chain_id,block_number,block_hash) REFERENCES loader.blocks(chain_id,block_number,block_hash),
 PRIMARY KEY (chain_id,block_number,block_hash,log_index),
 FOREIGN KEY (chain_id,block_number,block_hash,transaction_hash,transaction_index) REFERENCES loader.receipts(chain_id,block_number,block_hash,transaction_hash,transaction_index),
 CHECK (cardinality(topics)<=4)
);

CREATE INDEX transactions_hash ON loader.transactions(chain_id,transaction_hash);
CREATE INDEX transactions_sender ON loader.transactions(chain_id,from_address,block_number);
CREATE INDEX transactions_recipient ON loader.transactions(chain_id,to_address,block_number) WHERE to_address IS NOT NULL;
CREATE INDEX receipts_transaction ON loader.receipts(chain_id,transaction_hash);
CREATE INDEX logs_transaction ON loader.logs(chain_id,transaction_hash);
CREATE INDEX logs_address ON loader.logs(chain_id,address,block_number);
CREATE INDEX logs_topic0 ON loader.logs(chain_id,(topics[1]),block_number) WHERE cardinality(topics)>0;

GRANT SELECT,INSERT ON loader.inbox_events,loader.blocks,loader.transactions,loader.receipts,loader.logs TO loader_runtime;
GRANT UPDATE (status,attempts,next_attempt_at,lease_until,last_error_code,completed_at) ON loader.inbox_events TO loader_runtime;
