-- Runtime may inspect migration identity, never mutate migration history.
-- The owner executes this simple projection with its own SELECT privilege.
CREATE VIEW extractor.schema_readiness WITH (security_barrier=true) AS
 SELECT version,checksum FROM extractor.schema_migrations;
REVOKE ALL ON extractor.schema_readiness FROM PUBLIC;
GRANT SELECT ON extractor.schema_readiness TO extractor_runtime;
