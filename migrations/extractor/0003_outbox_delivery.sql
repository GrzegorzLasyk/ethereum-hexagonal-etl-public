-- Delivery state only; source snapshots and event identities remain immutable.
ALTER TABLE extractor.outbox_events
 ADD COLUMN status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','sending','delivered','failed')),
 ADD COLUMN attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 ADD COLUMN max_attempts integer CHECK(max_attempts BETWEEN 1 AND 1000),
 ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN lease_until timestamptz,
 ADD COLUMN delivered_at timestamptz,
 ADD COLUMN last_error_code text,
 ADD CHECK ((status='sending')=(lease_until IS NOT NULL)),
 ADD CHECK ((status='delivered')=(delivered_at IS NOT NULL));
CREATE INDEX outbox_pending ON extractor.outbox_events(next_attempt_at,created_at,event_id) WHERE status='pending';
CREATE INDEX outbox_expired ON extractor.outbox_events(lease_until) WHERE status='sending';
GRANT UPDATE(status,attempts,max_attempts,next_attempt_at,lease_until,delivered_at,last_error_code)
 ON extractor.outbox_events TO extractor_runtime;
