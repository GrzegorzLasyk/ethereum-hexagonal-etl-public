-- Stage 13.5: explicit service-owned recovery, immutable audit and fencing.
ALTER TABLE transformer.inbox_events DROP CONSTRAINT inbox_events_max_attempts_check;
ALTER TABLE transformer.inbox_events ADD CHECK(max_attempts>=1);
GRANT UPDATE(max_attempts) ON transformer.inbox_events TO transformer_runtime;
ALTER TABLE transformer.outbox_events DROP CONSTRAINT outbox_events_max_attempts_check;
ALTER TABLE transformer.outbox_events ADD CHECK(max_attempts>=1);
CREATE TABLE transformer.recovery_operations (
 chain_id text NOT NULL, operation_id text NOT NULL CHECK(operation_id ~ '^[A-Za-z0-9_.-]{1,128}$'),
 queue text NOT NULL CHECK(queue IN ('inbox','outbox')),
 event_id text NOT NULL, allowance integer NOT NULL CHECK(allowance BETWEEN 1 AND 1000),
 fenced_attempt integer NOT NULL CHECK(fenced_attempt>=1),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(chain_id,operation_id)
);
REVOKE ALL ON transformer.recovery_operations FROM PUBLIC;
GRANT SELECT,INSERT ON transformer.recovery_operations TO transformer_runtime;
