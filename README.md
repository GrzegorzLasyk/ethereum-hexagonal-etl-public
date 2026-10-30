# Ethereum Hexagonal ETL

An incrementally developed Ethereum ETL system in Go, with independently bounded Extractor, Transformer, and Loader services using Hexagonal Architecture.

Stage 1 introduced three executable service skeletons with environment configuration, structured logs, and signal-driven shutdown. All three now run the persistence and handoff workflows described below.

Stage 2 adds local PostgreSQL, isolated service schemas, migration credentials and tooling, and database permission tests.

Stage 3 adds Extractor domain rules, ports, and a single-block extraction use case tested with in-memory fakes. It is not wired into the executable until real adapters are implemented in their planned stages. See [ADR 0002](docs/adr/0002-ethereum-source-data.md) for the source dataset and durable processing boundary.

Stage 4 adds the Extractor's Ethereum JSON-RPC outbound adapter, controlled HTTP tests, and an optional Infura Mainnet smoke test. It returns complete source snapshots through `BlockSource`; the executable still does not run extraction or persist data.

Stage 5 adds Extractor-owned PostgreSQL storage for raw snapshots, outbox intents, and checkpoints through `ExtractionStore`. The source and storage adapters are individually usable; Stage 5 itself adds neither acquisition scheduling nor dispatch; Stage 9 wires the dispatcher below.

Stage 6 adds versioned protobuf contracts, reproducible generation, and inbound gRPC receiver adapters for Transformer and Loader. Service-owned application ports contain only plain Go types. A receiver constructed without an acceptance implementation returns `Unimplemented`. See [contracts, generation and limits](docs/architecture/protobuf-contracts.md).

Stage 7 wires the Transformer gRPC receiver to a durable PostgreSQL Inbox and bounded processing workers. Valid source data becomes a deterministic JSON v1 document and an Outbox intent in one local transaction. Duplicate acceptance, retries and restart recovery work without Loader availability. No outgoing dispatcher is implemented in Stage 7. See [Transformer operation](#transformer-operation-stage-7) and [data/recovery semantics](docs/architecture/transformer-stage7.md).

Stage 8 adds Loader-owned durable Inbox acceptance, leased processing workers and atomic normalized block/transaction/receipt/log persistence. Competing block versions are retained with explicit conflict observations; all versions have unknown canonicality ([ADR 0003](docs/adr/0003-loader-conflict-observation.md)). See [Loader operation](#loader-operation-stage-8) and [data/recovery semantics](docs/architecture/loader-stage8.md).

Stage 9 connects Extractor → Transformer → Loader with leased Outbox dispatchers,
stable IDs and verified durable gRPC ACKs. Deterministic tests cover offline
recipients, lost ACKs and sender crashes. See [handoff operation](docs/architecture/handoff-stage9.md).

Stage 10 adds ordered/ranged and follow-head acquisition, explicit confirmation
depth, bounded provider-relative reorg recovery, backpressure, replay and reindex.
Historical observations remain immutable; active Extractor membership is separate.
Apply the new migration before starting the new Extractor. See the
[Stage 10 operator runbook](docs/architecture/extraction-stage10.md) and
[accepted ADR 0004](docs/adr/0004-ordered-extraction-reorg-recovery.md).

Stage 11 adds a complete local Compose environment, deterministic Ethereum RPC
fixtures and an isolated E2E runner covering outages, ambiguous ACKs, restarts,
replay and reorg recovery. See the [local setup and test runbook](docs/architecture/local-stage11.md).

The approved MVP uses gRPC with Protocol Buffers for transport and service-owned PostgreSQL schemas with transactional outbox/inbox storage for durable, at-least-once handoff. A receiver will acknowledge an event only after its inbox transaction commits. See [ADR 0001](docs/adr/0001-mvp-transport-and-storage.md), [PLAN.md](PLAN.md), and [AGENTS.md](AGENTS.md) for decisions and staged implementation.

## Isolated Mainnet preparation (Stage 13.1)

Stage 13.1 adds a separate retained Compose environment and bounded Extractor
execution. Setup starts PostgreSQL and the two consumers without calling Ethereum:

```sh
mkdir -m 700 .mainnet
go run ./tools/mainnet init .mainnet/local.env
go run ./tools/mainnet check .mainnet/local.env
go run ./tools/mainnet up .mainnet/local.env
# Retains all data and private configuration:
go run ./tools/mainnet stop .mainnet/local.env
```

Actual acquisition requires a separately invoked `extract` command, explicit
Mainnet consent, chain 1, start/end blocks, confirmation/recovery depth and total
request/time budgets. No endpoint or live consent is supplied by initialization.
Never reuse fixture storage or print resolved Compose configuration. See the
[Stage 13 runbook](docs/architecture/mainnet-stage13.md) for all settings, accounting,
isolation and remaining limitations. Stage 13.2A adds [offline RPC reliability](docs/architecture/rpc-reliability-stage13.md).
The user reports Stage 13.2B smoke PASS (block 1000000, two transactions, two
receipts, five requests). Stage 13.3A prepares [Mainnet fixture selection and bounded capture](docs/architecture/mainnet-fixtures-stage13.md):
versioned metadata, explicit gaps, raw-byte integrity and offline validation.
No real fixture has been captured; Stages 13.3B–13.5 remain pending.

Stage 13.3B [offline preflight and ordered capture commands](docs/architecture/mainnet-capture-stage13.md)
are prepared. Use `fixture-capture verify MANIFEST CASE ARTIFACT` to validate
against the reviewed selection before any next capture. It performs no network
access and also checks known credentials if the private endpoint is in the
environment. Live capture still requires separate explicit authorization.

Validate fixture selection without network access:

```sh
go run ./services/extractor-service/cmd/fixture-capture manifest test/fixtures/mainnet/v1/manifest.json
```

Capture is a separate, explicitly authorized command, rejected in CI. Its
maximum is 1000 actual attempts for one pinned block and a two-minute operation
deadline; the existing smoke maximum remains 100 attempts. Unknown coverage
(currently type 4 and top-level contract creation) remains pending in the manifest.

## Prerequisites

Use Go 1.27.3 as declared in the root `go.mod`. Run the following commands from the repository root. Ordinary unit tests need no infrastructure. Running services requires migrated PostgreSQL; integration runners require Docker Engine/Desktop and Compose. No live Ethereum provider or broker is needed for deterministic tests.

## Build

For the complete local pipeline, start with the [Stage 11 fixture runbook](docs/architecture/local-stage11.md).
The isolated verification command builds and starts PostgreSQL, all three services
and a local Ethereum RPC fixture, injects failures/reorgs, verifies persisted data
and cleans up its temporary project:

```sh
go run ./test/integration/cmd/fixture-e2e
```

It needs Docker Desktop/Engine and Go, supports native Apple Silicon, and makes
no real Infura calls. The runbook also describes a persistent developer setup.

```sh
mkdir -p bin
go build -o bin/extractor-service ./services/extractor-service/cmd/extractor
go build -o bin/transformer-service ./services/transformer-service/cmd/transformer
go build -o bin/loader-service ./services/loader-service/cmd/loader
```

Build outputs under `bin/` are ignored by Git. There is one root Go module and no nested service modules.

## CI and architecture gates (Stage 12.1)

[CI](.github/workflows/ci.yml) runs for pull requests and pushes to `main`, with
two independent Ubuntu 24.04 jobs. Both use the Go version from `go.mod`;
checkout/setup actions are pinned to commits. The workflow has only
`contents: read`, does not retain checkout credentials and uses no repository
secrets, Infura credentials, deployment or image publishing. Fixture images are
built locally inside the integration runner. There are no path-based exclusions.

**Quality** verifies gofmt without changing files, architectural boundaries,
uncached unit/race tests, vet, and Buf formatting/lint/generated-code consistency.
**Integration** requires a reachable Docker daemon and Compose v2, then runs the
existing isolated Extractor, Transformer, Loader, Stage 9 handoff and Stage 11
fixture runners, followed by Stage 12.5 backup/restore and storage measurements.
Runners generate private credentials, apply/check migrations and permissions,
and run tagged tests with race detection. Ordinary runs clean up their own
resources; storage verification and `ETL_TEST_RETAIN=1` retain them. Missing infrastructure, runner/cleanup failure, a skipped test or a
missing expected top-level PASS fails the gate. Later steps may be skipped after
a failure, but the job cannot succeed. No `continue-on-error` is used.

Run the same gates locally from the repository root (Bash, Go and Docker required):

```sh
# Must print no paths; include untracked Go files when checking local edits.
git ls-files -z --cached --others --exclude-standard '*.go' | xargs -0 gofmt -l
go test -count=1 -v ./test/architecture
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
sh scripts/generate-proto.sh --check

docker info > /dev/null
docker compose version
bash scripts/ci-integration.sh extractor-postgres
bash scripts/ci-integration.sh transformer-postgres
bash scripts/ci-integration.sh loader-postgres
bash scripts/ci-integration.sh handoff-postgres
bash scripts/ci-integration.sh fixture-e2e
bash scripts/ci-integration.sh storage-release
git diff --check
```

Ordinary `go test ./...` does not execute tagged database/E2E suites. Calling
tagged tests without their runner configuration can skip them; the CI wrapper
explicitly rejects that outcome. It also requires the Stage 9 seed test and the Stage 12.5 storage subtest to pass.
Its negative tests simulate skipped/missing tests, unavailable Docker and failed
cleanup without starting infrastructure. Runner logs are already redacted;
temporary wrapper logs are removed on normal exit. Abrupt runner/host termination
can still interrupt cleanup; hosted CI workers are disposable.

`test/architecture` parses every production Go file in the three services,
including build-tagged/platform-specific files, and their service-owned SQL
migrations. Core imports permit only explicitly reviewed standard-library
packages and inward dependencies within the same service. This rejects SQL,
HTTP, gRPC, protobuf, broker clients, adapters/config and unreviewed helpers in
domain/application; domain cannot import application. All production service
layers reject imports from another service. Tests exercise forbidden and valid
imports, quoted schema names, joins/writes, sequence/function references and
constant SQL concatenation (including package constants across files).

The schema check detects explicit foreign schema references in Go string
expressions and migrations. It is not a SQL parser or a complete analysis of
runtime-built SQL, aliases or `search_path`. Test harnesses and administrator
bootstrap scripts are intentionally outside the service ownership scan. Actual
PostgreSQL permission tests provide the complementary enforcement boundary;
static checks do not replace database privileges or code review.

To make CI mandatory for merging, a repository administrator must configure
the `Quality` and `Integration` checks in branch protection/rulesets after the
workflow has run. Adding this file does not change repository settings or prove
a hosted run passed. Fork PR execution may require approval under repository
policy. Cold runs require access to Go modules, pinned tools and container
registries. Stage 12.1 introduces no deployment or publishing. Stage 12.2 logging
is described below; Stage 12.3 adds local metrics and Stage 12.4 adds local schema
readiness. Stage 12.5 adds storage verification and the local MVP release checklist.

## Structured logging and correlation (Stage 12.2)

All three executables emit JSON `slog` records to stderr, controlled by their
existing log-level configuration (default `INFO`). See the
[logging contract and troubleshooting guide](docs/architecture/logging-stage12.md)
for field meanings, safe failure codes and examples.

Every record has `service`, `operation`, `event_id`, `source_event_id`, `chain_id`,
`block_number`, `block_hash`, `attempt`, `outcome`, `error_code`, and `duration_ms`.
Unavailable metadata is `null`; no new event IDs or protobuf fields are introduced.
Raw IDs correlate Extractor delivery with Transformer processing; the existing
`source_event_id` links transformed delivery and Loader processing back to the raw
observation. `accepted` means a durable Inbox ACK, while `completed` means the
local processing transaction succeeded. `unknown` / `commit_unknown` requires
checking durable state or retrying the same identity, not assuming rollback.

```sh
docker compose --env-file .env.fixture -p ethereum-fixture -f deployments/compose/compose.yaml -f deployments/compose/fixture.yaml --profile fixture logs --no-color --no-log-prefix extractor transformer loader
bash scripts/ci-integration.sh fixture-e2e
```

The fixture E2E verifies actual JSON logs across both hops, retries, duplicate
acceptance after ACK loss, replay and reorg. Unit tests cover ambiguous commits,
failed operations, cancellation after success and redaction. Logs exclude payloads,
receipts, provider labels, DSNs, endpoints and underlying error messages. They are
operational evidence, not durable audit storage. No metrics or readiness changes
are part of Stage 12.2.

## Prometheus metrics (Stage 12.3)

Each service exposes local `GET /metrics` on its existing `*_HEALTH_ADDRESS`
listener. The fixture uses ports 8091 (Extractor), 8092 (Transformer), and 8093
(Loader), accessible inside the fixture network namespace. `/live` and `/ready`
retain their existing semantics. No Prometheus/Grafana stack is deployed.

Using `dc_fixture` from the [local runbook](docs/architecture/local-stage11.md):

```sh
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8091/metrics
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8092/metrics
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8093/metrics
bash scripts/ci-integration.sh fixture-e2e
```

Inspect `etl_database_snapshot_success` before trusting queue/checkpoint gauges.
A database failure leaves HTTP 200 for process telemetry, sets snapshot success
to 0 and **omits database-derived gauges**, rather than presenting healthy zeros.
Counters reset with the process; queue/checkpoint gauges come from durable state.
See the [metric reference](docs/architecture/metrics-stage12.md) for units, fixed
labels, head freshness, backlog/age definitions, restart semantics and limitations.

## Storage and release readiness (Stage 12.5)

The [operations index](docs/architecture/operations-stage12.md) consolidates
startup, upgrades, incident diagnosis, failed Inbox/Outbox recovery, retention and
archiving. History and deduplication records remain retained indefinitely; no
automatic deletion or archival eviction is implemented.

```sh
# Full fixture E2E, fresh-cluster backup/restore and measured critical SQL:
bash scripts/ci-integration.sh storage-release
# Preserve isolated regression resources instead of deleting their volumes:
ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres
```

Storage verification retains both isolated PostgreSQL volumes, a private archive,
checksums, EXPLAIN JSON plans and configuration, printing their locations. It refuses
a populated restore target. It never targets the developer's persistent database.
The measurement clone contains synthetic SQL-shape data; do not start ETL against it.
See [backup/restore](docs/architecture/backup-restore-stage12.md),
[actual measurements](docs/architecture/storage-measurements-stage12.md), and the
[MVP release checklist/security limits](docs/architecture/release-stage12.md).
See the [exact validation results](docs/architecture/validation-stage12.md).
This is a trusted-local MVP, not approval for production deployment.

## Schema readiness (Stage 12.4)

`/ready` now checks the owning PostgreSQL database, required tables/columns/types,
indexes, exact migration versions/checksums and effective runtime privileges.
It returns an empty 503 on failure, timeout or shutdown; it never exposes SQL,
credentials or database diagnostics. `/live` and `/metrics` remain independent.
Health listeners still accept only literal loopback IP addresses.

Existing databases must apply the new service-owned migrations before this binary
can become ready: Extractor `0005_readiness_metadata.sql`, Transformer and Loader
`0004_readiness_metadata.sql`. They add only a read-only version/checksum view;
no event or business storage changes. In the persistent fixture environment:

```sh
dc_fixture run --rm migrate-extractor
dc_fixture run --rm migrate-transformer
dc_fixture run --rm migrate-loader
dc_fixture up --build -d --wait --wait-timeout 120
```

See [readiness checks, upgrades and limitations](docs/architecture/readiness-stage12.md).
Readiness does not call downstream services or the Ethereum provider, migrate or
repair the database, pause workers, or declare the ETL caught up.

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

Each service validates configuration and writes JSON logs to stderr. All three require their database settings and applied migrations below. Stop a service with Ctrl+C (SIGINT) or send SIGTERM to the service process. Both signals cause a shutdown log and exit status 0. Invalid configuration causes a JSON error log and exit status 1 before startup.

Services can start independently in any order; all require PostgreSQL but do not require downstream services online at startup. They cancel and join workers before closing connections/pools; Transformer and Loader also drain incoming RPCs within a configured grace period. Interrupted processing remains recoverable through persisted leases. Use the service paths above instead of `go run .`.

## Configuration

Each service loads its own configuration from the environment at startup; `.env` files are not loaded automatically.

| Variable | Default | Accepted values | Purpose |
| --- | --- | --- | --- |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` | Minimum severity for JSON logs on stderr |

Log levels are case-insensitive and surrounding whitespace is ignored. Empty or whitespace-only values select `INFO`; other values fail startup. Lifecycle messages have INFO severity and are suppressed at WARN or ERROR. Every log includes the service name. Every service requires database and chain settings. Extractor/Transformer dispatch configuration is documented in [Stage 9](docs/architecture/handoff-stage9.md).

```sh
LOG_LEVEL=DEBUG ./bin/extractor-service
```

## Process health checks

Optional `<SERVICE>_HEALTH_ADDRESS` variables enable loopback-only `/live` and
`/ready` HTTP endpoints. Readiness requires a responsive owning database after
startup; liveness follows process cancellation. The Stage 11 Compose overlay
configures and checks these endpoints. Neither readiness nor startup logs imply
that downstream work has completed; inspect Inbox/Outbox progress separately.
See [health and shutdown semantics](docs/architecture/local-stage11.md#networking-readiness-and-shutdown).

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
    internal/domain/
      block.go
      checkpoint.go
      block_test.go
    internal/application/
      ports/inbound/extract_block.go
      ports/outbound/extraction.go
      usecases/extract_block.go
      usecases/extract_block_test.go
    internal/adapters/outbound/jsonrpc/
      client.go
      decode.go
      fields.go
      client_test.go
      protocol_test.go
      live_test.go
      testdata/
    internal/adapters/outbound/postgres/
      store.go
      snapshot.go
      store_test.go
      integration_test.go
    internal/config/config.go
    internal/config/config_test.go
  transformer-service/
    cmd/transformer/main.go
    internal/domain/                       # typed entities and invariants
    internal/application/ports/inbound/    # acceptance and processing
    internal/application/ports/outbound/   # Inbox, Queue, Decoder, Encoder
    internal/application/usecases/         # durable acceptance and processing
    internal/adapters/inbound/grpc/
    internal/adapters/inbound/worker/
    internal/adapters/outbound/ethereumjson/
    internal/adapters/outbound/jsondocument/
    internal/adapters/outbound/postgres/
    internal/config/
  loader-service/
    cmd/loader/main.go
    internal/domain/
    internal/application/ports/inbound/
    internal/application/ports/outbound/
    internal/application/usecases/
    internal/adapters/inbound/grpc/
    internal/adapters/inbound/worker/
    internal/adapters/outbound/jsondocument/
    internal/adapters/outbound/postgres/
    internal/config/
```

Each `main.go` is a composition root for its service: it loads configuration, constructs its logger, and manages process cancellation. Each service owns its `internal/` packages; services do not import one another's internals. The small configuration implementations remain service-owned so their settings can evolve independently.

All three services use the domain, application ports, and use-case boundaries below. Other packages are introduced only when their planned functionality needs them:

- `internal/domain/`: pure entities, invariants, value objects, and domain errors, independent of infrastructure.
- `internal/application/ports/inbound/`: interfaces exposing application capabilities to inbound adapters.
- `internal/application/ports/outbound/`: small interfaces describing infrastructure capabilities needed by use cases.
- `internal/application/usecases/`: application orchestration using domain types and ports. Business workflows belong here, not in `main.go`, handlers, or repositories.
- `internal/adapters/inbound/`: CLI, worker, or transport triggers translating external input into application requests.
- `internal/adapters/outbound/`: implementations of application ports for JSON-RPC, storage, or publishing.
- `internal/config/`: service startup configuration, kept outside domain and application logic.

Dependencies point inward: adapters depend on application contracts; application depends on domain and its ports; domain has no transport, database, or framework dependencies. Composition roots wire concrete implementations explicitly. Generated protobuf and storage DTOs stay outside domain and use cases. Test fakes are defined only in `_test.go` files; no fake infrastructure is wired into a binary and no empty adapter packages are created.

## Transformer operation (Stage 7)

Start PostgreSQL and apply migrations using the [local PostgreSQL instructions](#local-postgresql-stage-2). Transformer never runs migrations itself. Supply a private `TRANSFORMER_DATABASE_URL` using **transformer_runtime**, database `ethereum_etl` and the configured port. Startup rejects migrator/admin logins. Keep credential-bearing URLs out of tracked files and shared logs.

```sh
# TRANSFORMER_DATABASE_URL must already be set in the process environment.
export TRANSFORMER_CHAIN_ID=1
go run ./services/transformer-service/cmd/transformer
```

The approved development source is Ethereum Mainnet (`1`). Transformer makes no provider calls; incoming envelopes must match its explicit chain. `SubmitRawBlock` persists exact bytes before ACK. Semantic failures such as missing receipts become failed Inbox events asynchronously. Identical re-delivery retains content and state; conflicting duplicates receive `AlreadyExists`.

| Environment variable | Default | Constraint / meaning |
| --- | --- | --- |
| `TRANSFORMER_DATABASE_URL` | Required | Private connection string; runtime role only |
| `TRANSFORMER_CHAIN_ID` | Required | Canonical nonnegative decimal string |
| `TRANSFORMER_GRPC_ADDRESS` | `127.0.0.1:50051` | Loopback IP, port 1–65535; local plaintext listener |
| `TRANSFORMER_MAX_PAYLOAD_BYTES` | `67108864` | Positive, at most 64 MiB; raw aggregate and normalized output |
| `TRANSFORMER_MAX_MESSAGE_BYTES` | `68157440` | Greater than payload limit, at most 65 MiB |
| `TRANSFORMER_WORKERS` | `1` | 1–8 concurrent processing calls |
| `TRANSFORMER_MAX_ATTEMPTS` | `5` | 1–1000; persisted at acceptance, includes crashed attempts |
| `TRANSFORMER_POLL_INTERVAL` | `1s` | Delay after idle queue or infrastructure error |
| `TRANSFORMER_PROCESS_TIMEOUT` | `30s` | Context deadline per processing call |
| `TRANSFORMER_LEASE` | `1m` | Must exceed processing timeout |
| `TRANSFORMER_INITIAL_BACKOFF` | `1s` | Initial retry delay with equal jitter |
| `TRANSFORMER_MAX_BACKOFF` | `1m` | At least initial backoff |
| `TRANSFORMER_SHUTDOWN_TIMEOUT` | `10s` | RPC shutdown grace period |

Durations use Go duration syntax, between 1 ms and 24 h. The pool has `workers + 4` connections, a 5 s connect timeout and a 10 s SQL statement timeout. Compose reads its selected environment file; the Go executable does not. The [Stage 11 overlay](docs/architecture/local-stage11.md) provides full local service deployment. Non-loopback deployment and transport authentication/TLS remain later work.

Owned tables are `transformer.inbox_events`, `transformer.transformed_blocks` and `transformer.outbox_events`. The runtime can update only Inbox processing and Outbox delivery columns. Status (`pending`, `processing`, `completed`, `failed`), attempts, lease, next attempt and safe error code expose progress. Workers reclaim expired leases after restart; failed input remains retained. Stage 9 dispatches these intents and records delivery status. See [normalized JSON and recovery semantics](docs/architecture/transformer-stage7.md).

### Transformer verification

Fixture-based tests require neither Docker nor live Ethereum:

```sh
go test ./...
go test -race ./services/transformer-service/...
go vet ./...
```

With Docker Engine/Desktop and Compose available, run:

```sh
go run ./test/integration/cmd/transformer-postgres
# Extractor regression tests against the current migrations:
go run ./test/integration/cmd/extractor-postgres
```

The Transformer runner creates a unique disposable Compose project, private random credentials and a loopback port. It ignores the project's `.env`, verifies all migrations and permissions, then executes:

```sh
go test -tags postgresintegration -race -run '^TestPostgresIntegration$' -count=1 -timeout=3m -v ./services/transformer-service/internal/adapters/outbound/postgres
```

It privately supplies `TRANSFORMER_TEST_DATABASE_URL`, `TRANSFORMER_TEST_MIGRATOR_URL` and `TRANSFORMER_TEST_ISOLATED=1`. Tagged tests skip without URLs and refuse a configured database without the isolation marker. They truncate disposable Transformer tables and install temporary failure triggers; never point them at needed data. Cleanup removes only the runner's project/volume and credential file, including on cancellation. For supervisors signaling one PID, build/run the runner binary directly, as described for Extractor below.

Database coverage includes durable ACK, exact-byte duplicates/conflicts, concurrent claims, expired leases/fencing, restart, poison input, retry scheduling/exhaustion, rollback, ambiguous commits, permissions and cancellation. Fixture tests cover deterministic decimal-string output, transaction types 0–4, access lists, blob/authorization fields, historical roots, null/absent fields, empty arrays and malformed/incomplete data. Worker tests cover concurrency bounds, deadlines, polling and joined shutdown.

## Loader operation (Stage 8)

Apply all Loader migrations, including `0002_inbox_normalized_storage.sql` and
`0003_conflict_observations.sql`, through the existing
migration runner (`dc run --rm migrate-loader` after defining `dc` below).
Configure a private `LOADER_DATABASE_URL` for `loader_runtime` in `ethereum_etl`
and explicitly set `LOADER_CHAIN_ID=1` for the approved Mainnet source:

```sh
# LOADER_DATABASE_URL is supplied privately through the environment.
export LOADER_CHAIN_ID=1
go run ./services/loader-service/cmd/loader
```

Loader listens on `127.0.0.1:50052` by default. It durably accepts transformed
JSON v1 before ACK, then validates and atomically writes normalized query records
and Inbox completion. Exact duplicates retain the first content; conflicting
content under the same event ID is rejected. Invalid semantic payloads remain
in failed Inbox state. Stage 9 now connects this receiver to the Transformer dispatcher.

Different hashes at the same chain/height retain separate blocks and child records.
Every competing version has `height_conflict=true` and `canonical_status='unknown'`.
Successful Inbox completion and a matching parent do not establish canonicality.
Extractor retains competing hashes and performs bounded recovery of its separate
active branch (ADR 0004). Loader preserves conflicting observations without
automatically selecting a canonical branch.

[Loader documentation](docs/architecture/loader-stage8.md) lists all `LOADER_*`
configuration settings, schema types, permission boundaries, restart semantics
and limitations. The Go executable does not load Compose's `.env` or migrate.

```sh
go test ./...
go test -race ./services/loader-service/...
go vet ./...
# Docker Engine/Desktop and Compose required; uses its own disposable database:
go run ./test/integration/cmd/loader-postgres
```

The runner applies and verifies all service migrations and permissions, runs
Loader PostgreSQL tests with `-race`, then removes its own resources. It supplies
private `LOADER_TEST_*` settings; never point those tests at needed data.

## Reliable handoff operation (Stage 9)

Apply all service migrations, including the new Extractor/Transformer
`0003_outbox_delivery.sql`. Extractor now requires private `EXTRACTOR_DATABASE_URL`
(`extractor_runtime` in `ethereum_etl`) and explicit `EXTRACTOR_CHAIN_ID=1`.
Transformer and Loader keep their existing runtime database/chain settings.

Default targets are `EXTRACTOR_DISPATCH_TARGET=127.0.0.1:50051` and
`TRANSFORMER_DISPATCH_TARGET=127.0.0.1:50052`. Configure compatible payload limits.
Each sender defaults to one worker, 20 attempts, a 5 s RPC timeout and a 1 min
lease. See [all dispatch settings, transaction boundaries and backlog queries](docs/architecture/handoff-stage9.md).

Start Loader, Transformer and Extractor in separate terminals; other orders also
work within retry budgets. Extractor dispatches already-committed snapshots;
continuous acquisition is available through the explicit Stage 10 `index` mode.
Failed events stay retained and are not reset by restart. Stage 10 adds explicit
Extractor replay/reindex commands; downstream canonical-chain reconciliation
remains outside scope.

```sh
# Isolated deterministic handoff test, including database and gRPC failures:
go run ./test/integration/cmd/handoff-postgres
```

## Single-block extraction contract (Stage 3)

Construct `usecases.NewExtractBlock(chainID, startBlock, source, store)` with an explicit chain ID and starting height. It implements the inbound `BlockExtractor` port. Requests carry `ChainID` and `Number`; chain IDs are canonical decimal strings, not RPC hex strings. There is no default network/provider and no new CLI command at this stage.

The source port verifies chain identity and returns one complete `domain.Extraction`: block identity/header metadata, ordered transaction and receipt membership, receipt logs, raw block and receipt bytes, provider label, and extraction time. The raw block contains full transaction objects. Full field normalization belongs to Transformer; RPC parsing and raw-to-metadata consistency checks belong to the Stage 4 source adapter. The domain does not parse JSON. The exact acquisition fields, historical optionality, and trace exclusion are recorded in ADR 0002.

`ExtractBlock` validates the request, source chain, checkpoint position, source completeness, and parent/hash continuity. With no checkpoint it accepts only the configured starting height; otherwise it accepts the immediate child or a retry of the checkpoint height. A different hash at that height, a parent mismatch, a gap, or an older request returns an error. Detection does not choose a new canonical chain or repair a reorganization.

Persistence is a single outbound operation that must atomically commit raw data, one outbox intent, and the checkpoint. It receives the expected prior checkpoint for a transactional concurrency check. Checkpoints mean extraction and handoff intent are durable, not that downstream processing is complete. Stable event IDs derive from chain ID, block height, and hash. Idempotent retry keeps the original snapshot/event rather than replacing provenance. There is no separate checkpoint setter, network publish, internal retry loop, range scheduler, or real persistence adapter here.

The same context reaches every I/O port, and cancellation is checked between calls and before commit. Wrapped errors retain their identity for `errors.Is`. Failure during a real commit can be ambiguous; retrying the same block identity must resolve it without creating a second logical event. Cancellation received after a reported successful commit does not undo that success.

Unit tests cover complete and empty blocks, invalid/missing metadata, wrong chains, transaction/receipt/log consistency, checkpoint gaps and uint64 overflow, parent/hash conflicts, cancellation boundaries, source failures/timeouts, persistence failure, concurrent checkpoint change, duplicate retries, restart, and a lost response after commit. Fakes demonstrate the application contract; they do not prove PostgreSQL durability or JSON-RPC correctness. Those require the adapter stages.

## Verification

```sh
gofmt -w services/*/cmd/*/*.go services/*/internal/config/*.go
gofmt -w services/extractor-service/internal/domain/*.go services/extractor-service/internal/application/ports/inbound/*.go services/extractor-service/internal/application/ports/outbound/*.go services/extractor-service/internal/application/usecases/*.go
gofmt -w services/extractor-service/internal/adapters/outbound/jsonrpc/*.go
gofmt -w services/extractor-service/internal/adapters/outbound/postgres/*.go
go test ./...
go vet ./...
```

Also run the three build commands above. Alongside the Extractor tests described above, unit tests cover default, supported, normalized, and invalid log-level configuration for each service. Entry-point and port-only packages have no unit tests of their own; process startup and signal shutdown can be checked using the built binaries. The Go unit tests require neither a live Ethereum provider nor database/Docker access; they do not establish full-pipeline behavior coverage.

## Ethereum JSON-RPC source (Stage 4)

The adapter in `services/extractor-service/internal/adapters/outbound/jsonrpc` implements the existing `BlockSource` port. `Block(ctx, number)` verifies `eth_chainId`, calls `eth_getBlockByNumber` with full transactions, retrieves each transaction's receipt with `eth_getTransactionReceipt`, validates its logs, and rechecks the chain. Receipts/logs are fetched separately from the block; no additional `eth_getLogs` scan is needed. Successful acquisition returns exact raw JSON result bytes plus typed identity metadata. It performs no database writes or checkpoint updates.

Construct it with `jsonrpc.New(config, httpClient)` inside the Extractor service boundary; `nil` uses a standard HTTP client. The constructor validates all settings. The current binary loads only `LOG_LEVEL`; it does not consume RPC environment variables or invoke the adapter. Stage 4 does not add fake persistence to make the executable appear functional.

| `jsonrpc.Config` field | Meaning / constraint |
| --- | --- |
| `URL` | Secret-bearing endpoint; HTTPS required except loopback HTTP for tests; userinfo and fragments rejected, redirects disabled |
| `ChainID` | Explicit canonical decimal ID; use `"1"` for Ethereum Mainnet |
| `Provider` | Non-secret provenance label, e.g. `infura-mainnet`; 1–64 letters, digits, underscores, dots or hyphens, starting with a letter/digit |
| `RequestTimeout` | Positive timeout per attempt, including response body reads |
| `MaxAttempts` | Positive total attempts per RPC call, including the first |
| `InitialBackoff`, `MaxBackoff` | Positive initial exponential delay and ceiling; equal jitter, ceiling at least initial delay |
| `MinRequestInterval` | Nonnegative pacing interval across all requests/retries on this client; zero disables pacing |
| `MaxResponseBytes` | Positive maximum bytes per HTTP response, including the envelope/error body; less than MaxInt64 |
| `MaxSnapshotBytes` | Maximum combined raw block and receipt result bytes; at least `MaxResponseBytes` |

There are no implicit network or resource-budget defaults. Supply a caller context deadline for the whole block as well as the per-attempt timeout. Response limits apply to bytes read from the HTTP client (decompressed for standard automatic gzip handling). Aggregate limits bound retained raw results, not exact heap usage. Oversize, malformed, missing, or conflicting data fails the whole snapshot; no truncated/partial extraction is returned.

Retries cover transport failures/timeouts, HTTP 408/429/500/502/503/504, and RPC codes 429/-32005. Other responses, including authentication failures and Infura daily-credit exhaustion (402), fail immediately. Retry delays use exponential backoff with jitter. A valid `Retry-After` is respected within `MaxBackoff`; a longer server delay returns the error immediately for later rescheduling. Request pacing is per client, not account-wide credit accounting. See [Infura rate limits](https://docs.infura.io/how-to/avoid-rate-limiting/) and [credit costs](https://docs.infura.io/get-started/pricing/credit-cost/).

### Secure Infura configuration and optional live test

Create an Infura Ethereum Mainnet endpoint using the [official setup guide](https://docs.infura.io/get-started/infura/). Its URL has the form `https://mainnet.infura.io/v3/<YOUR-API-KEY>`. Store the complete URL privately in `ETHEREUM_RPC_URL`, using a secret store, IDE private environment, or the ignored local `.env` file (mode 600). Do not put a real URL in tracked files, command history, logs, or screenshots. Do not enable shell tracing while loading secrets. Errors deliberately omit URLs, provider message text and response bodies; never log the `Config` value or an HTTP client's request dump. Use a non-secret provider label.

Any live smoke execution requires **separate authorization**. The user reports
the Stage 13.2B smoke passed for block 1000000 (2 transactions, 2 receipts, 5 requests).
That run is not repeated by the fixture infrastructure work. Stage
13.2A prepares and validates its gate offline; it does not run it. Ordinary tests
and CI do not enable the `live` build tag. The gate also refuses any nonempty
`CI` or `GITHUB_ACTIONS` variable, and CI explicitly disables opt-in.

After authorization, privately supply all five settings from `.env.example`:
`ETHEREUM_LIVE_MAINNET_OPT_IN=true`, `ETHEREUM_RPC_URL`, an explicit decimal
`ETHEREUM_SMOKE_BLOCK`, `ETHEREUM_SMOKE_MAX_REQUESTS` between 1 and 100, and
`ETHEREUM_SMOKE_TIMEOUT` between 1ms and 2m. Missing/invalid configuration fails
an explicitly invoked test; it never silently skips. No `.env` is loaded automatically.

The smoke accepts only the Infura Mainnet HTTPS host, expects chain ID 1, and
reads one block with all receipts without writing snapshots or database state.
The request ceiling counts every HTTP attempt, including retries and chain checks.
Fixed per-attempt bounds remain 15s, three attempts, 500ms initial/5s maximum
backoff, 200ms pacing, 8 MiB per response and 64 MiB aggregate raw snapshot.
It reports only identity, counts, attempted requests and duration; failure to
collect the complete block is failure, never partial success or a finality claim.

The separately authorized live command would be:

```sh
go test -tags live -run '^TestLiveMainnetBlock$' -count=1 -timeout=2m -v ./services/extractor-service/internal/adapters/outbound/jsonrpc
```

The runner rejects request budgets above 100 and never substitutes `latest` for
the selected decimal block. The two-minute operation deadline starts before
acquisition; it excludes Go compilation. See the [Stage 13.2B preflight](docs/architecture/rpc-reliability-stage13.md#stage-132b-preflight--live-execution-pending)
for the verified two-transaction block 1000000, private-input command and limits.

Compile the opt-in test without contacting a provider:

```sh
go test -tags live -run '^$' ./services/extractor-service/internal/adapters/outbound/jsonrpc
```

### Coverage and limitations

Ordinary tests use `httptest` and synthetic fixtures; they need local loopback sockets but no external network or secrets. They cover full/empty blocks, transaction types 0–4, historical receipts, raw byte preservation, metadata consistency, wrong/changing chains, malformed envelopes, duplicate JSON keys, null data, rate limits, bounded retries, cancellation/timeouts, response/snapshot limits, redirect prevention, credential redaction, and concurrent request pacing/IDs.

Receipt calls are sequential and unbatched: `N + 3` RPC calls per block before retries, plus the use case's own chain check. No archive entitlement or throughput is assumed. Unknown fields are retained; unknown transaction types fail explicitly. Fork-dependent fields are checked when present and where implied by transaction type; there is no complete fork-activation calendar. Validation checks source shape and consistency, not signatures, header/trie roots, or consensus. Conflicting receipt/log hashes fail, but a consistent snapshot is not proof of finality or continued canonicality. Reorg reconciliation, durable storage, outbox/gRPC delivery, and range/follow-head workers belong to later stages.

## Extractor PostgreSQL persistence (Stage 5)

The following describes the original Stage 5 boundary. Stage 10 extends it with
separate active membership, revision-fenced recovery and operator jobs; see the
[updated persistence semantics](docs/architecture/extraction-stage10.md).
Height-only uniqueness and first-lineage-only behavior no longer apply after
migration 0004. Historical source bytes and event identities remain immutable.

Migration `migrations/extractor/0002_extraction_storage.sql` is owned and applied by `extractor_migrator`. The runtime adapter uses `extractor_runtime` in `ethereum_etl`. Run the existing Compose migration command after PostgreSQL becomes healthy:

```sh
docker compose --env-file .env -f deployments/compose/compose.yaml run --rm migrate-extractor
```

Reapplication is safe through the existing checksum/version runner; do not execute a previously applied migration file directly or edit its contents. Transformer and Loader receive no new tables or business logic.

| Extractor table | Stored data and access |
| --- | --- |
| `raw_blocks` | Full identity `(chain_id, block_number, block_hash)`, parent hash, exact raw block bytes and ordered raw receipt bytes, versioned typed metadata, provider and original extraction time. Runtime: SELECT/INSERT only. |
| `outbox_events` | Stable `raw-block/v1/<chain>/<number>/<hash>` event ID and a foreign key to the immutable local snapshot, plus creation time. Runtime: SELECT/INSERT and restricted delivery-state UPDATE. Stage 9 records leases, retries and durable ACKs. |
| `extraction_checkpoints` | One last committed block identity per configured chain and update time, with foreign keys to both the source snapshot and outbox intent. Runtime: SELECT/INSERT and UPDATE of position/hash/time only. |

Raw JSON is stored in `bytea` / ordered `bytea[]`, preserving whitespace, unknown fields, and numeric spelling. Only adapter-owned typed metadata uses versioned `jsonb`. Source extraction time is RFC3339Nano text so PostgreSQL's microsecond timestamp precision cannot discard its nanoseconds; database write times use `timestamptz`. Heights use bounded `numeric(20,0)` to retain the port's complete uint64 range. Chain IDs remain canonical decimal text. Primary/unique indexes support identity, height, event and checkpoint lookup; there are no speculative delivery-state indexes.

`raw_blocks` stores the accepted extraction lineage, not proof of consensus canonicality. Alongside its full-identity primary key, a unique chain/height constraint prevents silently retaining competing accepted hashes. The adapter explicitly detects conflicting hashes and parent hashes and stops. It does not overwrite data, rewind progress, promote an alternate fork, or label a block finalized. Reconciliation and finality policy remain separate decisions.

The outbox reference is strictly local to Extractor storage. It retains everything needed to construct a future outgoing message without duplicating raw JSON in a second table. It does not authorize another service to read Extractor tables or decide payload-versus-reference gRPC contracts. No delivery/ACK state, dispatcher, automatic retries, or retention cleanup is added in Stage 5.

### Adapter construction and dependencies

Use `postgres.New(pool, chainID, startBlock)` from within the Extractor's service boundary, passing the same chain and start as `usecases.NewExtractBlock`. The pool is a `pgxpool.Pool`; the constructor borrows it and the caller must close it on shutdown. Create it with explicit runtime credentials, connection limits and connect timeout; pass deadlines through every application call. Keep connection strings in a private environment/secret store and never print configuration or raw driver connection errors. Production TLS configuration belongs to deployment; the disposable loopback test uses `sslmode=disable` only locally.

The adapter implements the unchanged `ExtractionStore` port. Its `Load(ctx, blockID)` method also retrieves the original snapshot by full identity for local verification; it is not a new inter-service API. SQL uses schema-qualified names and bind parameters. `pgx/v5` is the one direct new dependency: Go's standard library does not provide a PostgreSQL driver. Driver, pool, SQL and storage DTOs stay outside domain/application packages. The service binary does not yet read a database URL or wire an extraction worker.

### Atomicity, concurrency and recovery

Each commit starts one Read Committed transaction and takes a transaction-scoped advisory lock for the configured chain, including when no checkpoint row exists. Different writers then compare the actual checkpoint against `ExpectedCheckpoint` **inside** that transaction. The checkpoint write additionally uses a conditional update/insert. A lock-hash collision only serializes unrelated chains; no process-local mutex is used for correctness. All adapter writers must follow this protocol; migration/admin SQL is trusted and must not manually alter live progress.

For a new block, the transaction inserts raw data, inserts exactly one stable outbox intent, then advances the checkpoint, and commits all three together. Initial extraction must match configured start. Subsequent extraction must be the immediate child with the expected parent hash. Stale expectations, missing intermediate blocks, conflicting identities and overflow cannot advance progress. Any pre-commit failure rolls back partial writes. The runtime cannot update raw snapshots, delete any of these records, manipulate migration metadata, perform DDL, or access another service schema.

An exact stored identity with a matching parent is an idempotent success (`AlreadyStored`), even if another writer has since advanced the checkpoint. It preserves the first raw bytes, provider, extraction time and event; no checkpoint regression or second event occurs. A stored row without its expected durable intent/progress is reported as corruption rather than treated as successful recovery. The use case still controls scheduling: it only accepts the current checkpoint or its immediate child, while the store can safely resolve an older in-flight duplicate without changing progress.

`AlreadyStored` confirms durable **block identity**, not equality of complete snapshots. A valid retry can contain different raw block/receipt bytes, typed metadata or provenance; these do not replace the first snapshot and are not certified as equivalent. Domain validation and the parent-hash check still apply. Storage serialization happens only for a new block, so replacement provenance that is never written cannot prevent identity-based recovery. Raw-to-metadata correspondence and provider trust remain source-adapter responsibilities; the store does not reparse Ethereum JSON or cryptographically verify block contents.

On restart, read `Checkpoint`: nil means no committed extraction and the configured start is next; otherwise resume at the immediate child, subject to the uint64 boundary. A successful commit means source data and handoff intent are durable, not that downstream processing finished. An error from `COMMIT` may mean the server committed but its response was lost. The adapter exposes `ErrCommitUnknown` conservatively, performs no automatic retry, and never claims rollback based on a transport failure. Retry **the same full block identity and event**, even with the old expected checkpoint: it either returns `AlreadyStored` or commits once if the first transaction rolled back. If the chain now supplies a different hash, stop for explicit reconciliation. A fresh valid context is needed after cancellation. Cancellation after an acknowledged successful commit does not undo success.

### PostgreSQL integration tests

The reproducible suite requires Go, Docker Engine/Desktop and Compose. Its small Go CLI uses only the standard library. It creates a uniquely named temporary Compose project, cryptographically random credentials in a temporary mode-600 file, and an available loopback port. It never uses your `.env` or an existing database. Run from the repository root:

```sh
go run ./test/integration/cmd/extractor-postgres
```

The CLI clears inherited Compose, PostgreSQL and integration database settings while preserving Go toolchain and Docker connection settings. It passes credentials through the private environment file and test-process environment, never command-line arguments. Child output is buffered per step and generated passwords/connection URLs are redacted before printing; output above 4 MiB is omitted entirely. Errors identify the failed step and exit status without dumping arguments or environments.

Cleanup runs on success, failure, SIGINT and SIGTERM with its own 60-second deadline, even if the test context was cancelled. It removes only this run's containers, network and volume, then deletes the credential file. A cleanup failure remains an error and reports the generated project name for recovery. SIGKILL, a host crash or an unavailable Docker daemon can still require manual cleanup. For process supervisors that send SIGTERM to one PID, build and run the CLI directly (the `go run` wrapper does not forward every signal):

```sh
go build -o bin/extractor-postgres ./test/integration/cmd/extractor-postgres
./bin/extractor-postgres
```

The runner validates Compose, starts a healthy PostgreSQL, runs the existing migration/permission suite (including repeated/concurrent application, checksums and rollback), then runs:

```sh
go test -tags postgresintegration -race -run '^TestPostgresIntegration$' -count=1 -timeout=3m -v ./services/extractor-service/internal/adapters/outbound/postgres
```

The runner supplies private `EXTRACTOR_TEST_DATABASE_URL` and `EXTRACTOR_TEST_MIGRATOR_URL` variables only to the test process. Do not point these at production: tests insert fixtures and temporarily install failure-injection triggers using the migrator role. Application writes always use the runtime role. Without these settings the tagged tests skip; ordinary `go test ./...` runs storage encoding/validation tests without PostgreSQL.

CLI unit tests also run without Docker. They check cleanup after each failed step, cancellation and cleanup errors, temporary file permissions/removal, environment isolation, unique project names and redaction of secrets split across output writes.

Integration cases cover failures before each of the three writes (initial and continuation), complete rollback and retry, duplicates with preserved provenance, stable event IDs, concurrent identical/forked writers, stale expectations, gaps/regression, hash/parent conflicts, uint64 maximum height, recovery through a fresh pool, cancellation during lock contention, and runtime permission denial. Ambiguous outcomes are injected around a real PostgreSQL COMMIT, both before and after it, followed by a resolving retry. This deterministically tests acknowledgement loss semantics; it is not a network fault proxy or machine power-loss test.

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

Fill in the seven password values with distinct, nonempty local secrets. The example intentionally leaves passwords empty, so Compose refuses to start until they are set. `.env` and `.env.*` are ignored by Git; only `.env.example` is tracked. Do not commit generated credentials. Compose reads this file; Transformer and Loader separately require their process environment settings above.

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

All six service logins have no superuser, role/database creation, replication, bypass-RLS, inherited membership, or membership in other service roles. Database-level PUBLIC privileges and PUBLIC access to the `public` schema are revoked. Service roles receive CONNECT, but no database CREATE or TEMP privileges. The migrator owns only its service schema. Runtime receives USAGE on its own schema and no CREATE or ownership. Stage 12.4 grants read-only access to version/checksum through an owning-schema `schema_readiness` view; the migration table remains private. Neither role can access another service's schema or assume its roles. PostgreSQL system catalog visibility is not a promise of metadata secrecy; the isolation boundary protects service objects and data.

There are no blanket runtime table grants. Stages 5, 7 and 8 grant only the documented operations on service-owned tables; future service-owned migrations must likewise grant operations explicitly. New functions and types do not receive PUBLIC access by default. The global default revokes are intentional: schema-local revokes cannot remove privileges granted globally. See the [PostgreSQL default-privilege documentation](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html).

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

The test temporarily creates uniquely named probe tables/functions, grants narrowly scoped probe access, and removes these objects on exit. It uses temporary copies of migration files for failure tests and does not edit committed migrations. Run it on the local development database, not production: its final assertion checks the exact current inventory (migration metadata plus the seven Extractor, three Transformer and five Loader tables). A forcibly killed test may require cleanup of its `stage2_probe_*` objects by the owning migrator.

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

### Authentic Mainnet fixtures: offline ETL regression (Stage 13.3C)

With the three previously captured files available locally and Docker Desktop running:

```sh
go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b
```

This explicit local mode verifies the pinned manifest, replays original JSON through
production adapters and checks the complete PostgreSQL/gRPC pipeline. Each height
uses a separate retained database/checkpoint; no live RPC or new capture occurs.
Missing private artifacts fail clearly. Normal unit tests and CI use synthetic
fixtures. See [assertions, verified coverage, retention and CI publication policy](docs/architecture/mainnet-offline-stage13.md)
and [actual validation results](docs/architecture/validation-stage13-3c.md).

### Live Mainnet E2E preparation (Stage 13.4A)

Finite extraction now drains its Outbox within `EXTRACTOR_DRAIN_TIMEOUT` (default
30s, maximum 2m, also within the whole-run budget). The read-only command below
checks the pinned block 1000000 across all three owning runtime roles; checkpoint
advancement alone cannot pass:

```sh
go run ./tools/mainnet verify .mainnet/stage13.env
```

The guarded `e2e` command is prepared for separately authorized live execution:
1000000..1000000, at most 100 actual RPC attempts, 2m acquisition including drain,
then at most 2m completion observation. It is forbidden in normal CI. No live
execution was performed during implementation. See the [safe configuration, commands
and failure semantics](docs/architecture/mainnet-e2e-stage13.md).

### Mainnet recovery and final acceptance (Stage 13.5)

Use the [Stage 13 operations runbook](docs/architecture/operations-stage13.md) for
bounded offline drain, audited repair of failed Transformer/Loader queues,
Extractor replay, restart, reorg limitations and the exact user-only live command.
Forward migrations add immutable service-owned recovery audits; run the existing
Mainnet `up` command before using the new binaries. No live RPC runs automatically.

One comprehensive offline validation, preserving all test volumes and artifacts:

```sh
bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
```

[Validation results and remaining live acceptance](docs/architecture/validation-stage13-final.md)
are recorded separately. Missing private fixtures or Docker fail explicitly.

### Offline performance baseline (Stage 14.1)

Explicit local benchmark; no live Ethereum RPC and no ordinary CI dependency:

```sh
go run ./test/integration/cmd/handoff-postgres --baseline --opt-in \
  --dataset synthetic --blocks 3 --workers 1 --repeat 1 --warmup 0 \
  --out .mainnet/baseline-smoke
```

The output directory must be new. Each sample uses its own disposable Compose
PostgreSQL project and host Go processes. Existing fixtures and volumes are never
reused or deleted. See [datasets, 1/2/4/8 worker matrix, measurement scope and report
format](docs/architecture/performance-stage14.md) before interpreting results.

### Resource limits and backpressure (Stage 14.2)

Transformer and Loader default to 128 outstanding events, two incoming gRPC slots,
a 130 MiB serialized-message reservation budget, 16 connections and a 12-connection
PostgreSQL pool. Transformer retains capacity until its Outbox is delivered.
Exact accepted duplicates remain acknowledgeable at capacity; new identities get
retryable overload. Oversized payloads remain permanent errors, with no truncation.

Apply owning migrations `0006_admission_indexes.sql` before starting updated
Transformer/Loader binaries. Existing isolated runners handle these automatically.
See [resource configuration, Compose limits, metrics and outage behavior](docs/architecture/backpressure-stage14.md).
Worker concurrency remains bounded at 1..8; no automatic retry allowance reset or
history deletion is introduced.

### Concurrent receipt fetching (Stage 14.3)

`EXTRACTOR_RECEIPT_CONCURRENCY=1` preserves the conservative default; explicit
values 2..8 enable concurrent receipts within one block. Block heights stay
sequential. All workers share the RPC budget, pacing and Retry-After cooldown;
failed batches cannot advance checkpoints or create Outbox work.

See [ordering, cancellation, memory overhead and offline tests](docs/architecture/receipts-stage14.md).
This setting is passed through the existing fixture and isolated Mainnet Compose
files; changing it does not authorize live extraction.

### Batched Loader persistence (Stage 14.4)

`LOADER_SQL_BATCH_SIZE=64` groups existing INSERTs in one atomic block transaction.
Accepted values are 1..256; `1` selects direct writes for comparison. A fixed 1 MiB
estimated coalescing budget bounds batches; larger individual rows remain intact
and are sent alone. Per-chain serialization, Inbox fencing, rollback, idempotency
and `canonical_status=unknown` are preserved. No migration is needed.

See [batching guarantees, tests and offline before/after commands](docs/architecture/loader-batching-stage14.md).
The baseline runner accepts `--loader-batch-size 1|64` (or any value in 1..256),
records the setting and measures batch calls/durations. Database execution and
throughput comparisons remain deferred; fewer submissions are not a measured
performance claim.

### Final validation and release preparation (Stage 14.5)

Implementation scope is frozen to the three-service Ethereum ETL. The next step
is comprehensive validation, not additional features. Start with the
[final configuration audit and release checklist](docs/architecture/release-stage14.md).

```sh
# Explicit offline acceptance; never starts live Ethereum acquisition.
bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
```

Separate `performance --opt-in` and `live --opt-in` phases are documented there;
no command automatically runs all phases. The live phase additionally requires the
private Mainnet opt-in, block1000000, <=100 attempts and <=2m acquisition. Do not
confuse prepared workflows, prior-stage checks or an advanced checkpoint with
final acceptance. Minimal Stage 14.5 checks do not establish production readiness.
