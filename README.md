# Ethereum Hexagonal ETL

An incrementally developed Ethereum ETL system in Go, with independently bounded Extractor, Transformer, and Loader services using Hexagonal Architecture.

Stage 1 provides three executable service skeletons with environment configuration, structured logs, and signal-driven shutdown. They do not yet extract, transform, or load data, open network listeners, or connect to external infrastructure.

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
