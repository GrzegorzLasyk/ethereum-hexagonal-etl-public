# Stage 14.2 — Resource limits and end-to-end backpressure

This stage bounds admission and execution resources. It does not change payload
contracts, canonicality, checkpoints, history retention, worker scheduling or
introduce a broker. Stage 14.1 measurements remain separate; these are conservative
safety defaults, not throughput-optimal values established by a benchmark.

## Durable admission algorithm

Transformer and Loader implement admission in their service-owned PostgreSQL
Inbox adapter. The application port explicitly requires atomic capacity checking,
exact duplicate handling, and ACK only after persistence. No core package imports
SQL, gRPC or the shared technical resource/metrics helpers.

For each valid incoming event:

1. Begin a local READ COMMITTED transaction with synchronous_commit on. Take the
   owning service's transaction advisory lock (Transformer 14201, Loader 14202).
   Every process receiving for this service must use the same policy and limit.
2. Check existence by indexed event_id **without sending payload parameters**.
   For an existing identity, compare all immutable bytes and metadata. An exact
   duplicate returns AlreadyAccepted, including when its row is failed or the
   queue is full. Conflicting content returns AlreadyExists. No new work is added.
3. For a new event, count unfinished entries in one MVCC statement. Transformer
   counts Inbox status != completed **plus** Outbox status != delivered; Loader
   counts Inbox status != completed. Failed, pending and leased work all count.
   Each subquery has LIMIT capacity. Forward partial indexes cover only unfinished
   entries; completed history is not part of these admission scans. A primary-key
   lookup covers duplicates. No payload aggregation or full-history sum is needed.
4. If the sum reaches capacity, return ErrOverloaded and roll back without an
   INSERT. Otherwise INSERT the Inbox record and COMMIT before returning success.
   The advisory lock remains held through commit/rollback. An uncertain commit
   still returns an error; retry with the same event ID resolves its durable state.

The budget is **schema-global**, not per process or per chain. Serializing new
admissions prevents concurrent check-then-insert overshoot. Worker completion does
not need this lock: Transformer transfers one slot from Inbox to Outbox in its
existing single transaction, so the snapshot observes one slot before or after
that transaction. Outbox delivery only releases a slot. Loader completion releases
one slot. Failed-event repair does not release capacity. No deletion, attempt
reset or history rewrite is used to make room.

All receiver instances must run this implementation with the same capacity.
Mixed old/new binaries or direct administrator INSERTs are outside the admission
protocol. An already overfull database after upgrade or lowering a limit is not
trimmed: new identities wait for drain/repair; exact duplicates still work.

## gRPC overload and retry contract

| Condition | gRPC status | Sender behavior |
|---|---|---|
| Durable capacity or global incoming slot exhausted | Unavailable + ErrorInfo domain `ethereum-etl`, reason `RECEIVER_OVERLOADED` | Retryable `overloaded` outcome |
| Connection cap | Connection closed before handshake | Existing transient transport retry |
| Payload/message above configured size | ResourceExhausted | Permanent `oversize`; retain failed Outbox |
| Conflicting identity | AlreadyExists | Permanent conflict; never duplicate ACK |
| PostgreSQL unavailable / ambiguous acceptance | Existing Unavailable/error | Durable retry; no speculative ACK |

Both publishers preserve one RPC per dispatch attempt and disable built-in gRPC
retries. Overload uses the existing persisted attempt allowance, exponential
backoff/jitter, next_attempt_at and fencing rules. It is not an immediate inner
retry loop. Exhausted allowances become retained failed work and require the
existing audited repair/replay procedure after capacity is restored.
ResourceExhausted remains the approved permanent size signal; peers must use the
explicit Unavailable overload contract instead of repurposing that status.

## Configuration

For `TRANSFORMER_` and `LOADER_` prefixes:

| Suffix | Default | Allowed values / meaning |
|---|---|---|
| MAX_OUTSTANDING_EVENTS | 128 | 1..10000; combined Inbox+Outbox for Transformer, Inbox for Loader |
| MAX_INCOMING_REQUESTS | 2 | 1..8; global concurrent method handlers per process |
| MAX_INFLIGHT_BYTES | 136314880 (130 MiB) | At least MAX_MESSAGE_BYTES, at most 520 MiB; serialized reservation budget |
| MAX_GRPC_CONNECTIONS | 16 | 1..64 accepted connections per process |
| DB_MAX_CONNECTIONS | 12 | 2..32 per process |
| WORKERS | 1 | Existing 1..8 Inbox workers |

Extractor also supports `EXTRACTOR_DB_MAX_CONNECTIONS` (12, range 2..32).
Extractor and Transformer retain their existing `*_DISPATCH_WORKERS` (1, range
1..8), retry, timeout and lease settings. Extractor's existing explicit Outbox
count/bytes/database-size ceilings still pause acquisition, including failed rows.
No automatic follow-head or live provider behavior is introduced.

Existing MAX_PAYLOAD_BYTES (64 MiB) and MAX_MESSAGE_BYTES (65 MiB) remain unchanged
and can only be lowered. Effective incoming slots are:

```
min(MAX_INCOMING_REQUESTS, floor(MAX_INFLIGHT_BYTES / MAX_MESSAGE_BYTES))
```

A slot reserves one **maximum-size serialized message**, even for a small request.
The wrapper runs around the generated method handler before its decoder; a unary
interceptor would be too late. It does not modify generated files or schemas. A
slot is released after the handler exits, including decode/SQL errors; context
cancellation alone does not release buffers still used by a handler. Global slot
exhaustion is rejected before protobuf decoding. HTTP/2 may already have received
bounded transport buffers. Per-connection stream limits match the effective slot
count; conforming clients may wait locally for a stream slot until their deadline.
A bounded listener closes excess connections; header lists are capped at 16 KiB
and handshakes at 5 seconds. Direct in-process receiver tests bypass transport
admission intentionally; production roots wire the guarded server and listener.

This reservation is **not a heap/RSS bound**. Decoded protobuf structures, pgx
encoding, queued socket bytes, worker snapshots and normalized JSON add overhead.
Workers/dispatchers are independently bounded; a full-size snapshot can require
several copies. Lower payload limits and worker counts together for small hosts.
Slow clients can occupy slots until cancellation/connection termination; ingress
is trusted loopback, not an Internet-facing denial-of-service boundary.

Pools now have explicit hard maxima instead of concurrency-derived sizing. Defaults
allocate at most 36 service connections on the shared PostgreSQL instance; allow
additional connections for health, diagnostics, migrations and replicas. A small
pool bounds writes but can increase waiting and deadlines. Do not set all processes
or replicas to 32 without budgeting against PostgreSQL max_connections.

## Compose limits

Base PostgreSQL, the existing fixture overlay and the standalone Mainnet Compose
file use real `cpus`, `mem_limit` and equal `memswap_limit` settings (no extra swap):

| Component | CPU default | Memory default |
|---|---|---|
| PostgreSQL | 2.0 | 2g |
| Extractor / Transformer / Loader | 1.0 each | 2g each |
| Synthetic JSON-RPC fixture | 0.5 | 256m |

Override `<SERVICE>_CPUS` and `<SERVICE>_MEM_LIMIT` in the explicit Compose
environment. Go service containers also use `<SERVICE>_GO_MEMORY_LIMIT`, default
`1536MiB`, mapped to GOMEMLIMIT. It is a soft GC target, not an allocation limiter.
Invalid limits are rejected by Go/Docker; unsafe smaller operator overrides can
cause OOM. A container OOM can interrupt a transaction; durable retry/lease recovery
still governs recovery and must not be mistaken for successful completion.

Compose exposes the receiver resource variables and all worker settings. Host Go
processes, including the Stage 14.1 benchmark services, do not inherit container
CPU/RSS restrictions. Set GOMEMLIMIT explicitly for host tests if desired and record
that difference; do not compare measurements with different limits as equivalent.
Docker Desktop must have enough total RAM/CPU; these limits are ceilings, not
reservations. No new Compose services, ports or infrastructure are added.

## Outages, monitoring and recovery

When Loader stops accepting, Transformer Outbox retains undelivered work. Its
combined budget eventually rejects new Extractor deliveries. Extractor Outbox then
reaches its existing acquisition limits; dispatch continues while new acquisition
pauses. Already accepted work is never dropped. Repair failed rows and restore
capacity using the [Stage 13 runbook](operations-stage13.md); restarting alone does
not erase a failed row or grant additional retries. Completed immutable history
still grows indefinitely: admission limits are not a retention/disk quota.

Existing Prometheus endpoints add fixed series:

- `etl_admission_inflight_requests`, `etl_admission_request_capacity`;
- `etl_admission_reserved_bytes`, `etl_admission_byte_capacity` (bytes);
- `etl_admission_transport_backpressure` (all global slots occupied);
- `etl_admission_rejections_total{reason="transport|durable|connections"}`;
- `etl_overload_retries_total` (successfully scheduled durable overload retries);
- `etl_admission_outstanding_events`, `etl_admission_event_capacity`;
- `etl_admission_capacity_utilization` (ratio; can exceed 1 after lowering limits);
- `etl_admission_durable_backpressure` (outstanding >= capacity).

Process counters reset on restart; active gauges are process-local. Durable gauges
are schema-global and refreshed on the existing bounded own-schema scrape. Do not
sum duplicate schema-global gauges from replicas. Failed database snapshots omit
durable gauges, never report healthy zeros. Existing `/live`, `/ready`, `/metrics`
contracts and listener restrictions remain; full capacity is not unready and does
not depend on downstream availability. Overload logs use fixed error code
`overloaded` and retain event correlation when an event was decoded.

## Upgrade and validation

Apply `0006_admission_indexes.sql` through each owning Transformer/Loader migrator
before starting matching binaries. These ordinary CREATE INDEX migrations can block writes while building; apply them
in a maintenance window through the existing transactional migration runner.
Runtime roles gain no new privileges. Readiness
requires these indexes and migration checksums; migration inventories become 6/6
for Transformer/Loader and remain 5 for Extractor. Existing full integration
runners apply migrations automatically. Coordinate upgrades: older binaries and
readiness inventories are not rolling-upgrade compatible.

Targeted tests cover global reservation concurrency, predecode rejection, held
permits after cancellation, decode failure cleanup, connection release, configuration
bounds, overload versus oversize, finite durable retries, acceptance failures and
metrics omission during DB failures. PostgreSQL tests are wired into each existing
`TestPostgresIntegration/bounded_admission` and cover concurrent competing identities
across Store instances, duplicate ACK/conflict at capacity, failed retained work,
Transformer Inbox-to-Outbox transfer, drain recovery and ambiguous commit retries.
They are prepared for final validation, not claimed executed in this stage.

Final validation commands (explicitly deferred):

```sh
go run ./test/integration/cmd/transformer-postgres
go run ./test/integration/cmd/loader-postgres
go run ./test/integration/cmd/handoff-postgres
# Then the existing full fixture E2E and Stage 14.1 baseline matrix.
```

No live RPC execution is needed. A measured concurrency/throughput recommendation
for Stage 14.3 still requires the baseline and PostgreSQL/Compose regression.

## Implementation inventory

Stage 14.2 changes only (previous dirty Stage 13/14.1 work is preserved):

- `PLAN.md`, `README.md`, `.env.example`, `go.mod` (existing gRPC ErrorInfo dependency
  promoted from indirect to direct; no dependency version added or changed).
- `docs/architecture/backpressure-stage14.md` (new),
  `docs/architecture/performance-stage14.md` (updated baseline configuration note).
- `deployments/compose/compose.yaml`, `fixture.yaml`, `mainnet.yaml`.
- `internal/adapters/grpccontract/admission.go`, `admission_test.go` (new).
- `internal/adapters/metrics/metrics.go`; new `admission.go`, `admission_test.go`.
- `migrations/transformer/0006_admission_indexes.sql`,
  `migrations/loader/0006_admission_indexes.sql` (new).
- `test/integration/postgres.sh` (expected migration counts).

In **both** `services/transformer-service/` and `services/loader-service/`:

- `cmd/<service>/main.go`;
- `internal/config/config.go`, new `internal/config/admission_test.go`;
- `internal/application/ports/inbound/accept_block.go`;
- `internal/application/ports/outbound/processing.go`, `status.go`;
- `internal/application/usecases/usecases_test.go`;
- `internal/adapters/inbound/grpc/receiver.go`, `receiver_test.go`;
- `internal/adapters/inbound/metrics/handler.go`;
- `internal/adapters/logging/errors.go`;
- `internal/adapters/outbound/postgres/store.go`, `status.go`,
  `readiness_policy.go`, `integration_test.go`;
- new `internal/adapters/outbound/postgres/admission.go`,
  `admission_integration_test.go`.

In **both** `services/extractor-service/` and `services/transformer-service/`:

- `internal/application/ports/outbound/delivery.go`;
- `internal/application/usecases/dispatch.go`, `dispatch_test.go`;
- `internal/adapters/outbound/grpc/publisher.go`, `publisher_test.go`;
- `internal/adapters/outbound/postgres/outbox.go`;
- `internal/adapters/inbound/dispatcher/worker.go`;
- `internal/adapters/logging/errors.go`.

Also `services/extractor-service/cmd/extractor/main.go` and
`services/extractor-service/internal/config/config.go` for its pool limit.
Accepted ADRs, generated contracts, original fixture artifacts and canonicality
implementations are unchanged.

## Validation record

`go build ./...`, `go vet ./...`, gofmt verification and `git diff --check`
passed on the final implementation. Sandbox cache restrictions required approved
retries of Go commands; these were environment failures, not successful checks.

Executed targeted unit tests passed for shared gRPC resource controls, metrics,
three service configurations, receiver acceptance, both publisher classifications,
application acceptance and dispatch retries, readiness migration inventories, and
architecture boundaries. A targeted race run passed for:

```sh
go test -race -count=1 \
  -run 'TestAdmission|TestConnectionBudget|TestResourceConfiguration' \
  ./internal/adapters/grpccontract
```

Both Compose configurations passed `docker compose ... config --quiet` using only
placeholder offline environment values; no containers or RPC providers were
started. Initial YAML placement/anchor errors found by inspection/config checking
were corrected before the successful checks.

The tagged PostgreSQL packages compile with `-tags postgresintegration -run '^$'`:
this is **compilation only**, not a successful database test. Their new concurrent
admission and recovery scenarios, full handoff/E2E, complete race suite and updated
performance matrix are **NOT RUN**, intentionally deferred by the task's validation
strategy. No throughput or production capacity claim is made.
