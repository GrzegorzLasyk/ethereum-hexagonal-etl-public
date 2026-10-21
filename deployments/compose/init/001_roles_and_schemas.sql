-- First-volume bootstrap only; service migrations never run as postgres.
\set ON_ERROR_STOP on
BEGIN;
REVOKE ALL ON DATABASE ethereum_etl FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

\getenv password EXTRACTOR_MIGRATOR_PASSWORD
CREATE ROLE extractor_migrator LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO extractor_migrator;
ALTER ROLE extractor_migrator IN DATABASE ethereum_etl SET search_path = pg_catalog, extractor;

\getenv password EXTRACTOR_RUNTIME_PASSWORD
CREATE ROLE extractor_runtime LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO extractor_runtime;
ALTER ROLE extractor_runtime IN DATABASE ethereum_etl SET search_path = pg_catalog, extractor;

CREATE SCHEMA extractor AUTHORIZATION extractor_migrator;
REVOKE ALL ON SCHEMA extractor FROM PUBLIC;

\getenv password TRANSFORMER_MIGRATOR_PASSWORD
CREATE ROLE transformer_migrator LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO transformer_migrator;
ALTER ROLE transformer_migrator IN DATABASE ethereum_etl SET search_path = pg_catalog, transformer;

\getenv password TRANSFORMER_RUNTIME_PASSWORD
CREATE ROLE transformer_runtime LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO transformer_runtime;
ALTER ROLE transformer_runtime IN DATABASE ethereum_etl SET search_path = pg_catalog, transformer;

CREATE SCHEMA transformer AUTHORIZATION transformer_migrator;
REVOKE ALL ON SCHEMA transformer FROM PUBLIC;

\getenv password LOADER_MIGRATOR_PASSWORD
CREATE ROLE loader_migrator LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO loader_migrator;
ALTER ROLE loader_migrator IN DATABASE ethereum_etl SET search_path = pg_catalog, loader;

\getenv password LOADER_RUNTIME_PASSWORD
CREATE ROLE loader_runtime LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
GRANT CONNECT ON DATABASE ethereum_etl TO loader_runtime;
ALTER ROLE loader_runtime IN DATABASE ethereum_etl SET search_path = pg_catalog, loader;

CREATE SCHEMA loader AUTHORIZATION loader_migrator;
REVOKE ALL ON SCHEMA loader FROM PUBLIC;

COMMIT;
