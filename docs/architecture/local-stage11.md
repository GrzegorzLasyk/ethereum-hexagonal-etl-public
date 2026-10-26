# Local complete ETL with deterministic fixtures (Stage 11)

Current operations, storage preservation, failed-work limitations and release
checks are consolidated in the [Stage 12.5 runbook](operations-stage12.md).
Earlier exclusions below describe the original stage's scope, not missing current
health, metrics, dispatch or local deployment.

Stage 11 runs PostgreSQL, the real Extractor/Transformer/Loader binaries and a
synthetic Ethereum JSON-RPC server in Docker Compose. It reuses the existing
PostgreSQL Compose definition, migrator scripts, service-owned schemas and
regression runners. There are no new dependencies, migrations, domain models or
transport contracts. ADR 0003 and ADR 0004 remain unchanged.

## One-command verification

Install the Go version in `go.mod` and Docker Desktop with Compose v2. Start
Docker Desktop, then run from the repository root:

```sh
go run ./test/integration/cmd/fixture-e2e
```

The runner builds the image, creates a unique `ethereum-stage11-*` Compose
project with two ephemeral loopback ports and seven random credentials, applies
and verifies migrations/permissions, starts all services, and executes the
scenario below. It uses a private temporary environment file (0600), ignores
inherited database/Compose/fixture settings, redacts generated secrets, and
removes only its own containers, network, volume and credential file on success,
failure or handled cancellation. Image/build cache is retained. No user `.env`
is sourced and no existing developer database is used.

Every phase has a deadline. A failed assertion or cleanup returns a nonzero exit
code. If Docker becomes unavailable or the runner is forcibly killed, cleanup
may require `docker compose -p <reported-project> ... down --volumes`; identify
the exact reported project before removing anything. Never use global pruning.

macOS Apple Silicon uses native Linux arm64 images/binaries through Docker
Desktop; Linux amd64 uses its native architecture. No forced `platform`, Rosetta,
host networking or host-installed PostgreSQL is required. First build needs
registry and Go module access and takes longer; fixtures require no Infura key
or live Ethereum connection. Allow a few GB of Docker storage for build layers.

## Persistent developer environment

Generate a new ignored configuration file. The initializer refuses to overwrite
an existing file and creates seven distinct random hexadecimal passwords:

```sh
go run ./test/integration/cmd/fixture-e2e --init .env.fixture

dc_fixture() {
  docker compose --env-file .env.fixture -p ethereum-fixture \
    -f deployments/compose/compose.yaml \
    -f deployments/compose/fixture.yaml --profile fixture "$@"
}

dc_fixture up --build -d --wait --wait-timeout 120
dc_fixture ps
dc_fixture logs --tail=50 extractor transformer loader
```

Run these commands from the repository root. If a host port is occupied, change
`POSTGRES_PORT` (default 5432) or `FIXTURE_PORT` (18545) in `.env.fixture` before
starting. Generated hexadecimal passwords can be inserted safely into the
database URLs in the overlay; arbitrary replacement passwords would need URL
encoding. Keep the environment file private. Do not print the expanded Compose
configuration because it contains credentials; use `config --quiet` to validate.

Migrators run with separate owning roles before their service starts. Services
receive only their own runtime URL; the fixture receives no database credentials.
Receivers need their own database and listener, not an online downstream service.
Each service remains independently stoppable/restartable:

```sh
dc_fixture stop transformer
dc_fixture start transformer
dc_fixture restart postgres
```

The fixture starts at head 104. Extractor starts at 100, follows the head and
requires one descendant. It eventually commits blocks 100–103 and waits. Expect
4 Loader blocks, 8 transactions, 8 receipts and 4 logs after handoff drains:

```sh
dc_fixture exec -T postgres sh -c '
  PGPASSWORD="$LOADER_RUNTIME_PASSWORD" psql -X -w -h 127.0.0.1 \
    -U loader_runtime -d ethereum_etl -c "
      SELECT canonical_status, count(*) FROM loader.blocks GROUP BY canonical_status;
      SELECT count(*) FROM loader.transactions;
      SELECT count(*) FROM loader.receipts;
      SELECT count(*) FROM loader.logs;"'
```

All four blocks have `canonical_status=unknown`. These are synthetic chain-ID-1
fixtures with provider label `stage11-fixture`, not observed Mainnet blocks.
Keep this project/database separate from any live-provider environment.

To stop while retaining data, or explicitly remove this developer dataset:

```sh
dc_fixture down
# Destructive only to this named developer project's retained database:
dc_fixture down --volumes
```

PostgreSQL initializes passwords only for a new volume. Preserve `.env.fixture`
when retaining the volume. Fixture control state is in memory; recreating the
fixture restores `FIXTURE_HEAD` and branch A. After experimenting with reorgs,
either restore the matching fixture state or intentionally reset this disposable
dataset. A regressed provider head correctly stops automatic recovery.

## Networking, readiness and shutdown

`fixture.yaml` is an opt-in overlay; the original PostgreSQL-only setup and
Stage 5–9 runners remain usable unchanged. It adds no non-loopback transport
exception: the three application containers use `network_mode: service:fixture`.
They retain separate processes, filesystems and credentials while sharing one
local network namespace. Their existing loopback gRPC listeners work unchanged.
The namespace reaches PostgreSQL through the Compose network. Only PostgreSQL
and fixture HTTP are published, bound to host `127.0.0.1`; gRPC and health ports
are not published. Recreate the application containers if recreating the fixture
network namespace.

This is a trusted local test environment, not a production deployment or network
isolation boundary between services. The control API intentionally permits test
faults. Do not expose it remotely. The fixture gRPC proxies forward calls directly;
they have no queue, storage or broker semantics.

Optional environment variables `EXTRACTOR_HEALTH_ADDRESS`,
`TRANSFORMER_HEALTH_ADDRESS`, `LOADER_HEALTH_ADDRESS` enable loopback-only HTTP
health listeners. Unset means disabled, retaining the earlier executable behavior.
The overlay sets ports 8091, 8092 and 8093 respectively:

- `/live`: process lifecycle has not been canceled.
- `/ready`: startup wiring succeeded and the owning database passes bounded
  connectivity, service schema, migration and runtime privilege checks (Stage 12.4);
  returns 503 during outages, schema incompatibility, privilege drift or shutdown.
  See the [readiness upgrade guide](readiness-stage12.md).

Readiness is not evidence of a drained queue, valid source head, downstream
availability or completed ETL. Observe persistent progress separately. Docker
checks `/ready` every two seconds. An unhealthy status does not automatically
restart a service; finite retry budgets and explicit failure states still apply.
Compose grants 30 seconds for service shutdown. SIGTERM cancels workers, drains
gRPC within the configured grace period, and closes database pools after workers
finish. The E2E checks clean exit codes for normal service stops.

## Fixture and fault controls

`test/fixture` constructs deterministic full blocks with two transactions, two
receipts and one log each, separately served via `eth_getTransactionReceipt`.
`eth_chainId` returns `0x1`; `eth_getBlockByNumber` accepts numbered/latest tags
and full-transaction or hash-only responses. Missing/future blocks return null;
unknown methods return an RPC error. Heights are bounded to 100–110. Transaction
value `9007199254740993` tests exact integer preservation. Header hashes are
deterministic labels, not valid consensus hashes; no consensus validation is
claimed.

The host fixture URL defaults to `http://127.0.0.1:18545`. Adapt it if changing
`FIXTURE_PORT`:

```sh
curl --fail http://127.0.0.1:18545/control
curl --fail -X POST http://127.0.0.1:18545/control \
  -H 'Content-Type: application/json' -d '{"head":105,"fork":true}'
```

Branch B shares 100–101 with branch A and replaces heights >=102. Extractor
rewinds from 103 to retained ancestor 101 and resumes through 104. All historical
source/event data remains; the resulting dataset contains 7 observations and
7 Loader candidates, with 5 active Extractor members. Four Loader candidates
(two versions each at 102 and 103) have `height_conflict=true`. Every candidate
keeps `canonical_status=unknown` per ADR 0003.

`{"drop_raw":true,"drop_transformed":true}` arms one lost ACK at each proxy.
The proxy first receives a real successful response from the downstream service,
then returns `Unavailable` to the sender. It never fabricates acceptance or
changes a payload. Subsequent retries receive duplicate ACKs. `/control` exposes
lost/duplicate counters and whether a repeated event changed content. Controls
and diagnostics exist only in the test fixture binary, not production services.

## E2E assertions and regression commands

The isolated runner starts the fixture at head 100 and executes:

1. Verify no shallow block was persisted; stop Transformer and Loader.
2. Advance head to 102; verify raw blocks 100–101, checkpoint 101 and retained
   retrying Extractor Outbox entries while Transformer is offline.
3. Start Transformer with an armed lost raw ACK. Verify completed transformation
   and retained retrying Transformer Outbox while Loader is offline.
4. Start Loader with an armed lost transformed ACK. Verify both duplicate ACKs,
   stable event content and complete durable handoff.
5. Stop PostgreSQL; require readiness failure while liveness remains available.
   Advance head to 104 and restart the same database. Require automatic reconnect
   and full processing through 103 without resetting storage.
6. Gracefully stop/restart all service containers; verify unchanged immutable
   source/provenance and durable progress. Execute the real Extractor replay
   command and require deduplication without extra materialized rows.
7. Switch to branch B/head 105. Require audited revision-1 rewind to 101, active
   tip 104, historical retention, and all 7 events propagated to Loader.
8. Compare exact source/receipt bytes, original provenance, transformed payloads,
   normalized identities/values/counts, conflict flags, unknown canonicality,
   cross-schema denial and graceful service shutdown.

Assertions use three separately authenticated runtime connections. The test
observer never gives a service cross-schema privileges or joins across schemas.

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go run ./test/integration/cmd/extractor-postgres
go run ./test/integration/cmd/transformer-postgres
go run ./test/integration/cmd/loader-postgres
go run ./test/integration/cmd/handoff-postgres
go run ./test/integration/cmd/fixture-e2e
git diff --check
```

Ordinary unit tests need no Docker. PostgreSQL suites use `postgresintegration`;
the new full Compose test uses `composeintegration`. Both are driven by isolated
runners and executed with `-race -count=1`. Application container binaries are
normal native builds; the existing handoff regression also exercises separate
race-instrumented service binaries. No live-provider smoke test runs by default.

Stage 11 does not add metrics, tracing, dashboards, CI, retention automation,
remote authentication/TLS, canonicality reconciliation or production hardening.
Database size remains an admission watermark; retry budgets remain bounded.
The fixture does not simulate consensus, dishonest providers or performance at
Mainnet scale. Those are not claims of this local acceptance test.
