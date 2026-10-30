-- Bounded admission probes never scan completed blockchain history.
CREATE INDEX inbox_outstanding ON transformer.inbox_events(event_id) WHERE status <> 'completed';
CREATE INDEX outbox_outstanding ON transformer.outbox_events(event_id) WHERE status <> 'delivered';
