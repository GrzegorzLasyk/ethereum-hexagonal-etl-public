-- Bounded admission probes never scan completed blockchain history.
CREATE INDEX inbox_outstanding ON loader.inbox_events(event_id) WHERE status <> 'completed';
