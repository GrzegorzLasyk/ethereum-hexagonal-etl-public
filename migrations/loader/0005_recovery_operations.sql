-- Stage 13.5: explicit service-owned recovery, immutable audit and fencing.
ALTER TABLE loader.inbox_events DROP CONSTRAINT inbox_events_max_attempts_check;
ALTER TABLE loader.inbox_events ADD CHECK(max_attempts>=1);
GRANT UPDATE(max_attempts) ON loader.inbox_events TO loader_runtime;
CREATE TABLE loader.recovery_operations (
 chain_id text NOT NULL, operation_id text NOT NULL CHECK(operation_id ~ '^[A-Za-z0-9_.-]{1,128}$'),
 queue text NOT NULL CHECK(queue IN ('inbox')),
 event_id text NOT NULL, allowance integer NOT NULL CHECK(allowance BETWEEN 1 AND 1000),
 fenced_attempt integer NOT NULL CHECK(fenced_attempt>=1),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(chain_id,operation_id)
);
REVOKE ALL ON loader.recovery_operations FROM PUBLIC;
GRANT SELECT,INSERT ON loader.recovery_operations TO loader_runtime;
