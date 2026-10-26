-- Runtime may inspect migration identity, never mutate migration history.
-- The owner executes this simple projection with its own SELECT privilege.
CREATE VIEW loader.schema_readiness WITH (security_barrier=true) AS
 SELECT version,checksum FROM loader.schema_migrations;
REVOKE ALL ON loader.schema_readiness FROM PUBLIC;
GRANT SELECT ON loader.schema_readiness TO loader_runtime;
