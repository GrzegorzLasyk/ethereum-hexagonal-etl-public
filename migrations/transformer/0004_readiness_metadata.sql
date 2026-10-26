-- Runtime may inspect migration identity, never mutate migration history.
-- The owner executes this simple projection with its own SELECT privilege.
CREATE VIEW transformer.schema_readiness WITH (security_barrier=true) AS
 SELECT version,checksum FROM transformer.schema_migrations;
REVOKE ALL ON transformer.schema_readiness FROM PUBLIC;
GRANT SELECT ON transformer.schema_readiness TO transformer_runtime;
