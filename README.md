# Ethereum Hexagonal ETL

An incrementally developed Ethereum ETL system in Go, with independently bounded Extractor, Transformer, and Loader services using Hexagonal Architecture.

Stage 1 provides three executable service skeletons with environment configuration, structured logs, and signal-driven shutdown. They do not yet extract, transform, or load data, open network listeners, or connect to external infrastructure.

Stage 2 adds local PostgreSQL, isolated service schemas, migration credentials and tooling, and database permission tests. The Go services remain unchanged and do not connect to PostgreSQL yet.

The approved MVP uses gRPC with Protocol Buffers for transport and service-owned PostgreSQL schemas with transactional outbox/inbox storage for durable, at-least-once handoff. A receiver will acknowledge an event only after its inbox transaction commits. See [ADR 0001](docs/adr/0001-mvp-transport-and-storage.md), [PLAN.md](PLAN.md), and [AGENTS.md](AGENTS.md) for decisions and staged implementation.

## Prerequisites

Use Go 1.27.3 as declared in the root `go.mod`. Run the following commands from the repository root. Stage 1 uses only the standard library and requires no database, Ethereum provider, broker, or Docker environment.

## Build

```sh
mkdir -p bin
go build -o bin/extractor-service ./services/extractor-service/cmd/extractor
go build -o bin/transformer-service ./services/transformer-service/cmd/transformer
go build -o bin/loader-service ./services/loader-service/cmd/loader
```

Build outputs under `bin/` are ignored by Git. There is one root Go module and no nested service modules.

## Run and stop

Run each command in a separate terminal:

```sh
./bin/extractor-service
./bin/transformer-service
./bin/loader-service
```

Alternatively, run individual services from source:

```sh
go run ./services/extractor-service/cmd/extractor
go run ./services/transformer-service/cmd/transformer
go run ./services/loader-service/cmd/loader
```

Each service validates configuration, writes a JSON startup log to stderr, and waits for cancellation. Stop it with Ctrl+C (SIGINT) or send SIGTERM to the service process. Both signals cause a shutdown log and exit status 0. Invalid configuration causes a JSON error log and exit status 1 before startup.

Services can start independently in any order. There are no workers or external resources to drain at this stage. The root Hello placeholder has been replaced by the three service entry points; use the paths above instead of `go run .`.

## Configuration

Each service loads its own configuration from the environment at startup; `.env` files are not loaded automatically.

| Variable | Default | Accepted values | Purpose |
| --- | --- | --- | --- |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` | Minimum severity for JSON logs on stderr |

Values are case-insensitive and surrounding whitespace is ignored. Empty or whitespace-only values select `INFO`; other values fail startup. Lifecycle messages have INFO severity and are suppressed at WARN or ERROR. Every log includes the service name. No provider or database settings are needed yet.

```sh
LOG_LEVEL=DEBUG ./bin/extractor-service
```

## Stage 1 health checks

Health verification currently means process liveness only: with `LOG_LEVEL=INFO`, check the `service skeleton started` log (including `etl_enabled=false`), verify the process remains alive, and confirm it exits cleanly on SIGINT or SIGTERM. This does not imply ETL readiness or connectivity. No HTTP/gRPC health endpoint or background health polling is implemented. Dependency-aware health/readiness checks belong to the later infrastructure and full-system stages.

For a manual SIGTERM smoke check, start a built binary in the background, confirm its startup log and running PID, then stop that PID:

```sh
LOG_LEVEL=INFO ./bin/extractor-service &
service_pid=$!
kill -0 "$service_pid"
# After observing the startup log:
kill -TERM "$service_pid"
wait "$service_pid"
```

Use a built binary for PID-based signal checks; `go run` also involves a Go tool process.

## Package boundaries

The implemented structure is deliberately small:

```text
services/
  extractor-service/
    cmd/extractor/main.go
    internal/config/config.go
    internal/config/config_test.go
  transformer-service/
    cmd/transformer/main.go
    internal/config/config.go
    internal/config/config_test.go
  loader-service/
    cmd/loader/main.go
    internal/config/config.go
    internal/config/config_test.go
```

Each `main.go` is a composition root for its service: it loads configuration, constructs its logger, and manages process cancellation. Each service owns its `internal/` packages; services do not import one another's internals. The small configuration implementations remain service-owned so their settings can evolve independently.

As functionality is introduced, use these boundaries from the agreed layout:

- `internal/domain/`: pure entities, invariants, value objects, and domain errors, independent of infrastructure.
- `internal/application/ports/inbound/`: interfaces exposing application capabilities to inbound adapters.
- `internal/application/ports/outbound/`: small interfaces describing infrastructure capabilities needed by use cases.
- `internal/application/usecases/`: application orchestration using domain types and ports. Business workflows belong here, not in `main.go`, handlers, or repositories.
- `internal/adapters/inbound/`: CLI, worker, or transport triggers translating external input into application requests.
- `internal/adapters/outbound/`: implementations of application ports for JSON-RPC, storage, or publishing.
- `internal/config/`: service startup configuration, kept outside domain and application logic.

Dependencies point inward: adapters depend on application contracts; application depends on domain and its ports; domain has no transport, database, or framework dependencies. Composition roots wire concrete implementations explicitly. Generated protobuf and storage DTOs stay outside domain and use cases. There are no speculative domain types, ports, adapters, use cases, or empty packages in Stage 1.

## Verification

```sh
gofmt -w services/*/cmd/*/*.go services/*/internal/config/*.go
go test ./...
go vet ./...
```

Also run the three build commands above. Unit tests cover default, supported, normalized, and invalid log-level configuration for each service. Entry-point packages currently have no unit tests; process startup and signal shutdown can be checked using the built binaries. No integration infrastructure is required, and these checks do not establish ETL behavior coverage.

## Local PostgreSQL (Stage 2)

Requires Docker Engine/Desktop and Docker Compose with support for `up --wait`. PostgreSQL is pinned to `postgres:17.11-bookworm`. All clients and scripts run in this image; host installations of `psql`, Bash 4+, or a migration framework are unnecessary.

```text
deployments/compose/
  compose.yaml
  init/001_roles_and_schemas.sql
  scripts/migrate.sh
migrations/
  extractor/0001_schema_privileges.sql
  transformer/0001_schema_privileges.sql
  loader/0001_schema_privileges.sql
test/integration/postgres.sh
.env.example
```

This is infrastructure tooling outside the services' domain/application boundaries. No database adapter, ETL table, outbox, inbox, or protobuf contract is added at this stage.

### Configuration and startup

From the repository root, create a private local configuration if one does not already exist:

```sh
cp -n .env.example .env
chmod 600 .env
```

Fill in the seven password values with distinct, nonempty local secrets. The example intentionally leaves passwords empty, so Compose refuses to start until they are set. `.env` and `.env.*` are ignored by Git; only `.env.example` is tracked. Do not commit generated credentials. Compose reads this file; the Go service skeletons still use only `LOG_LEVEL`.

| Variable | Used for |
| --- | --- |
| `POSTGRES_PASSWORD` | Local PostgreSQL administrator and initial role/schema bootstrap |
| `EXTRACTOR_MIGRATOR_PASSWORD` / `EXTRACTOR_RUNTIME_PASSWORD` | Extractor migration and runtime logins |
| `TRANSFORMER_MIGRATOR_PASSWORD` / `TRANSFORMER_RUNTIME_PASSWORD` | Transformer migration and runtime logins |
| `LOADER_MIGRATOR_PASSWORD` / `LOADER_RUNTIME_PASSWORD` | Loader migration and runtime logins |
| `POSTGRES_PORT` | Host port, default `5432`; use another port if occupied |

Use this shell helper for the remaining commands, or expand it to the full Compose command:

```sh
dc() {
  docker compose --env-file .env -f deployments/compose/compose.yaml "$@"
}
dc config --quiet
dc up -d --wait --wait-timeout 120 postgres
dc ps
```

`config --quiet` validates without printing resolved credentials. Avoid sharing plain `docker compose config` or container environment dumps: the local tooling uses environment variables for passwords. This setup assumes a trusted developer machine; Docker administrators can inspect container environments. It is not a production secret-management or TLS deployment.

PostgreSQL listens on `127.0.0.1:${POSTGRES_PORT}` on the host. The single application database is always `ethereum_etl`; PostgreSQL's standard maintenance/template databases also exist. A Compose-managed named volume, `postgres_data`, mounted at `/var/lib/postgresql/data`, retains data across container removal/recreation. The default project is `ethereum-etl`; use a separate `-p` project name and port for isolated tests.

The healthcheck authenticates over TCP and executes `SELECT 1` against `ethereum_etl`. It establishes database availability, not migration completion or ETL readiness. Apply the migrations explicitly after startup.

### Ownership and permissions

| Schema | Migration login / schema owner | Runtime login |
| --- | --- | --- |
| `extractor` | `extractor_migrator` | `extractor_runtime` |
| `transformer` | `transformer_migrator` | `transformer_runtime` |
| `loader` | `loader_migrator` | `loader_runtime` |

The first-volume bootstrap runs as the PostgreSQL administrator to create the six logins and assign schema ownership. Service migrations subsequently authenticate as the corresponding migrator, never as the administrator. Each migration container receives only its own migration password. Never use migration/admin credentials in an application service.

All six service logins have no superuser, role/database creation, replication, bypass-RLS, inherited membership, or membership in other service roles. Database-level PUBLIC privileges and PUBLIC access to the `public` schema are revoked. Service roles receive CONNECT, but no database CREATE or TEMP privileges. The migrator owns only its service schema. Runtime receives USAGE on its own schema and no CREATE, ownership, or access to migration metadata. Neither role can access another service's schema or assume its roles. PostgreSQL system catalog visibility is not a promise of metadata secrecy; the isolation boundary protects service objects and data.

There are no blanket runtime table grants. When application tables are introduced in their planned stages, their service-owned migrations must grant only the needed operations and sequence privileges explicitly. New functions and types do not receive PUBLIC access by default. The global default revokes are intentional: schema-local revokes cannot remove privileges granted globally. See the [PostgreSQL default-privilege documentation](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html).

### Apply migrations

```sh
dc run --rm migrate-extractor
dc run --rm migrate-transformer
dc run --rm migrate-loader
```

These one-shot tools belong to the `tools` profile; invoking them explicitly starts them and waits for a healthy PostgreSQL dependency. A normal `dc up -d postgres` starts only PostgreSQL. Application services are not containerized in Stage 2.

Each schema has its own `schema_migrations` metadata table containing the filename, SHA-256 checksum, and application timestamp. This is the only persistent table created in Stage 2. The runner:

1. Requires the owning migrator login and the `ethereum_etl` database.
2. Acquires a per-service transaction-level advisory lock, with a 30-second lock timeout.
3. Applies lexically sorted `NNNN_description.sql` files and records their checksums in the same local transaction.
4. Skips matching applied files without changing their timestamps and rejects altered applied files.
5. Rolls back the entire service batch, including version records, if any statement fails.

Different services have separate transactions and can migrate independently. Repeating the three commands is safe. Runners for the same service serialize. Migration SQL is trusted, reviewed deployment code: use schema-qualified names, append new fixed-width versions, and do not put transaction-control commands, nontransactional operations, or `psql` connection-changing commands in migration files. Keep applied files present and immutable; the runner checks files present on disk, not missing historical files. There is no automatic down migration; correct a deployed schema with a new forward migration. Transaction-incompatible migrations require a separately reviewed strategy when needed.

The bootstrap script runs only when the volume is first initialized, as specified by the [official PostgreSQL image](https://github.com/docker-library/docs/blob/master/postgres/README.md). Restarting a populated volume does not rerun it, change passwords, or repair manual privilege changes. Editing `.env` alone does not rotate stored database passwords. Coordinate password changes using an authenticated administrator and update the matching local configuration. If initial bootstrap fails, inspect logs and repair the cause; do not assume a restart will finish a partially initialized volume. Do not delete a volume containing needed data.

### Database verification

After applying the migrations:

```sh
dc run --rm verify
```

This Stage 2 integration suite uses actual authenticated TCP connections, with no administrator password. It verifies all six roles, schema ownership, database restrictions, cross-schema read/write/DDL denial, blocked role escalation, runtime DDL and migration-metadata denial, explicit own-schema CRUD grants, sequence/function isolation, and rejection of wrong passwords. It also checks repeated/concurrent migrations, checksum mismatch rejection, and transaction rollback on an intentionally failing migration batch.

The test temporarily creates uniquely named probe tables/functions, grants narrowly scoped probe access, and removes these objects on exit. It uses temporary copies of migration files for failure tests and does not edit committed migrations. Run it on the local Stage 2 database, not production: its final assertion expects only migration metadata in each service schema and must evolve when later stages introduce tables. A forcibly killed test may require cleanup of its `stage2_probe_*` objects by the owning migrator.

To verify persistence across container replacement, after a successful test:

```sh
dc down
dc up -d --wait --wait-timeout 120 postgres
dc run --rm verify
```

`down` preserves the named volume, so the second run should report existing migrations as already applied. Database integration tests are opt-in and separate from `go test ./...`; ordinary Go tests need no Docker access.

### Shutdown and recovery

```sh
dc stop postgres
dc up -d --wait --wait-timeout 120 postgres
# Remove containers and the project network while retaining the database:
dc down
```

PostgreSQL receives a shutdown signal and has a 30-second grace period. For a deliberate reset of disposable local data only, `dc down --volumes` also deletes the named database volume and all data. Do not use that command for routine shutdown. Back up needed data before any reset or major PostgreSQL version change.
