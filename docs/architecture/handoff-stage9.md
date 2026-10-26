# Outbox dispatch and reliable gRPC handoff (Stage 9)

Current operations, storage preservation, failed-work limitations and release
checks are consolidated in the [Stage 12.5 runbook](operations-stage12.md).
Earlier exclusions below describe the original stage's scope, not missing current
health, metrics, dispatch or local deployment.

Stage 9 connects committed Extractor Outbox events to Transformer, and committed
Transformer Outbox events to Loader. It implements ADR 0001's at-least-once
handoff using the unchanged Stage 6 protobuf and Stage 7 transformed JSON v1.
Loader's unknown canonicality/conflict observations remain as specified in ADR 0003.

## Boundaries and package ownership

Each sender owns `application/ports/inbound/dispatch.go`,
`application/ports/outbound/delivery.go`, `application/usecases/dispatch.go`,
`adapters/inbound/dispatcher`, `adapters/outbound/grpc`, and its PostgreSQL
`Outbox` adapter. Publication DTOs are service-owned plain Go types. Use cases
know neither protobuf nor SQL. The polling adapter invokes the inbound port;
the use case coordinates outbound queue and publisher ports. Only the composition
roots wire these implementations. The small shared `grpccontract` additions are
transport checks, not a shared domain model.

Extractor joins its own Outbox to its immutable raw snapshots. Transformer joins
its own Outbox, transformed document and source Inbox. The wire carries exact
raw block/ordered receipt bytes or the exact persisted transformed document.
Source and transformation timestamps come from committed storage on every retry;
no dispatcher generates a new event ID, timestamp or JSON representation.

## Local transactions and acknowledgement

1. The existing source-data/checkpoint/Outbox or transformation/Inbox/Outbox
   transaction commits the intent. Dispatch never changes these boundaries.
2. A short local transaction uses `FOR UPDATE OF o SKIP LOCKED`, selects an
   eligible event for the configured chain, increments its attempt, sets a lease
   and commits before returning content to the use case.
3. The publisher calls the recipient with a deadline, outside any SQL transaction.
   It checks payload and protobuf message limits before sending. Client send and
   receive limits match the approved configurable budgets (up to 64/65 MiB).
4. Only `DURABLY_ACCEPTED` or `ALREADY_ACCEPTED` with the exact event ID permits
   success. A bare OK, absent/unknown acceptance or another ID is not an ACK.
   `AlreadyExists` is a content conflict, not a duplicate acceptance.
5. After a valid ACK, a separate local transaction checks the same attempt and
   unexpired lease before writing `delivered`. Transactions use
   `synchronous_commit=on`. Neither ACK nor `delivered` proves downstream
   processing, canonicality or finality.

There is no cross-service database access or distributed transaction. Each
process gets only its runtime role. Payloads and events remain retained after
both success and failure. Dispatch never deletes or modifies source content.

## State, retries and recovery

Both `0003_outbox_delivery.sql` migrations extend the existing Outbox without
rewriting prior migrations. Existing intents become pending with zero attempts.
Runtime receives UPDATE only on the explicitly listed delivery-state columns.
Event IDs, creation timestamps, source content and migration metadata remain
protected. Partial indexes support pending work and expired leases.

| State | Meaning |
| --- | --- |
| `pending` | Waiting for initial delivery or `next_attempt_at` |
| `sending` | Claimed with an incremented attempt and `lease_until` |
| `delivered` | Matching durable ACK was recorded locally |
| `failed` | Permanent rejection or exhausted automatic attempts; retained for investigation |

The attempt number fences later updates. A stale worker cannot acknowledge or
fail an event after another worker reclaims it. An expired worker cannot complete
even before a replacement claim. Replaying the same already-delivered attempt
is harmless. Claims and decoding of a publication do not establish delivery.

Transient failures use exponential backoff with equal jitter in
`[delay/2, delay]`, capped at the configured maximum. A dedicated RPC deadline is
shorter than the worker deadline, leaving time to persist retry state after a
network timeout. Shutdown cancellation leaves claimed work to lease recovery.
The maximum attempt count is pinned in the database at the first claim; changing
configuration does not reset or extend an event's existing budget. Each claim,
including one interrupted by a crash, consumes an attempt. A bounded cleanup
pass turns expired final attempts into retained `failed` events.

| Failure | Action |
| --- | --- |
| Recipient offline, unavailable, unimplemented, internal error, timeout or lost ACK | Retry the same content and ID within the attempt budget |
| Missing, mismatched or unknown ACK | Retry with `last_error_code=invalid_ack` |
| Invalid input, wrong chain/version, denied/authentication failure | Retain failed with `invalid` |
| Conflicting event content | Retain failed with `conflict` |
| Payload/message limit exceeded | Retain failed with `oversize`; never split or truncate |
| Claim COMMIT response lost | No RPC from that claim; recover any committed lease |
| Recipient commits, ACK lost or sender crashes | Expired lease/retry sends the same event; recipient Inbox deduplicates |
| Delivery-state COMMIT response lost | Do not write another failure state; later inspection/reclaim resolves delivered versus sending |

Restart and independent startup resume eligible work without receiver availability.
A sufficiently long outage may exhaust the finite retry budget. Restart alone
does not requeue failed events. Investigation and an explicitly reviewed recovery
operation are required; replay/reindex tooling belongs to a later stage.

## Configuration and startup

Apply all migrations with the existing Compose migrators before deploying these
binaries. Each runtime needs its private `*_DATABASE_URL` for its corresponding
runtime role in `ethereum_etl`, and explicit `*_CHAIN_ID=1`. Executables do not
read `.env` or apply migrations. Extractor now requires these settings and runs
its dispatcher; it does not start live or continuous acquisition. Existing
single-block extraction remains available through its application port.

Use each of the following suffixes with `EXTRACTOR_DISPATCH_` or
`TRANSFORMER_DISPATCH_`:

| Suffix | Default | Validation |
| --- | --- | --- |
| `TARGET` | Extractor: `127.0.0.1:50051`; Transformer: `127.0.0.1:50052` | Loopback IP and port 1–65535; IPv4/IPv6 |
| `WORKERS` | `1` | 1–8 per process |
| `MAX_ATTEMPTS` | `20` | 1–1000, pinned at first claim |
| `POLL_INTERVAL` | `1s` | Idle/error polling |
| `RPC_TIMEOUT` | `5s` | Deadline per publication |
| `TIMEOUT` | `30s` | Whole claim/publish/state-update attempt |
| `LEASE` | `1m` | Must exceed `TIMEOUT`, which must exceed `RPC_TIMEOUT` |
| `INITIAL_BACKOFF` | `1s` | Initial equal-jitter retry delay |
| `MAX_BACKOFF` | `1m` | At least initial backoff |

Durations are 1 ms–24 h. Extractor adds `EXTRACTOR_MAX_PAYLOAD_BYTES` and
`EXTRACTOR_MAX_MESSAGE_BYTES` (defaults 64/65 MiB); Transformer uses its existing
`TRANSFORMER_MAX_*` limits for both receiving and publishing. Align peer budgets.
Targets are explicit loopback addresses, with lazy connections and no readiness
RPC required at startup. This preserves the approved local unauthenticated
transport boundary. DNS/network discovery and production TLS/authentication are
not added. No new external dependency is needed.

After setting private database URLs and chain IDs, a convenient startup order is:

```sh
# Separate terminals; all migrations must already be applied.
go run ./services/loader-service/cmd/loader
go run ./services/transformer-service/cmd/transformer
go run ./services/extractor-service/cmd/extractor
```

Any order works within retry budgets. Extractor drains already-committed Outbox
intents; starting it does not itself fetch a block. SIGINT/SIGTERM cancel and
join dispatcher goroutines before closing gRPC connections and database pools.
Transformer also joins Inbox workers and drains its incoming gRPC listener using
its existing grace period. No acknowledged work lives only in memory.

## Progress and backlog queries

JSON logs expose Outbox event ID, attempt and outcome (`delivered`, `retry`,
`failed`) without payloads, credentials or remote error text. Database status,
next attempt, lease, delivered time and safe error codes expose queued and failed
work. Run each query using only that service's role (replace `extractor` with
`transformer` for its separate connection):

```sql
SELECT status, count(*), min(created_at) AS oldest_created_at
FROM extractor.outbox_events GROUP BY status ORDER BY status;

SELECT event_id, status, attempts, max_attempts, next_attempt_at,
       lease_until, delivered_at, last_error_code
FROM extractor.outbox_events
WHERE status <> 'delivered'
ORDER BY created_at, event_id;
```

A failed delivery may already exist in the receiver Inbox if all ACKs were lost.
Never equate failed sender state with absent receiver data, or mutate event content
under an existing identity to resolve a conflict.

## Verification and limitations

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./test/integration/cmd/extractor-postgres
go run ./test/integration/cmd/transformer-postgres
go run ./test/integration/cmd/loader-postgres
go run ./test/integration/cmd/handoff-postgres
git diff --check
```

Unit/gRPC tests cover acknowledgement validation, exact retries, permanent and
transient status mapping, deadlines, byte limits, cancellation, concurrency and
joined shutdown. PostgreSQL suites cover exclusive claims, leases, pinned budgets,
retry timing, exhaustion, permanent failures, ambiguous claim/delivery commits,
cancellation, immutable content and restricted permissions. Ordinary Go tests
need no PostgreSQL or live Ethereum access; tagged tests skip without runner
configuration. The existing runners execute real database tests with `-race`.

The new Go handoff runner creates a unique disposable Compose database with
private credentials, applies/verifies migrations, seeds a deterministic snapshot
through the real single-block extraction use case and local fixture JSON-RPC
server, then builds all three executables with `-race`. The test starts recipients
late, drops the first successful ACK on both links, kills/restarts each sender
while its lease is held and verifies duplicate ACKs and one final normalized
block with two transactions, two receipts and one log. Test assertions use
separate service-role connections; no service imports another service's internals.
The runner removes only its own database resources and private credential file.

This is at-least-once delivery, not exactly-once or ordered delivery. Resource
budgets bound bytes and concurrency, not total process memory. A single database
remains a common availability dependency. No new metrics endpoint, retention,
automatic failed-event replay, live-provider run, continuous extraction, reorg
reconciliation, finality, broker or full application Compose deployment is added.
There are no unresolved architectural decisions for Stage 9's local scope.

## Verification record

Final verification on 2026-10-22 passed:

- `go test ./...`
- `go test -race ./...` (including inbound/outbound gRPC and architecture tests)
- `go test -tags=postgresintegration ./...` (tagged compilation; database tests skip without runner URLs)
- `go vet ./...`
- `go run ./test/integration/cmd/extractor-postgres`
- `go run ./test/integration/cmd/transformer-postgres`
- `go run ./test/integration/cmd/loader-postgres`
- `go run ./test/integration/cmd/handoff-postgres`
- `gofmt` on changed Go files; whole-repository `gofmt -l` returned no paths.
- `git diff --check`

Each database runner used real disposable PostgreSQL, verified migrations and
permissions, ran tagged tests with race detection and completed cleanup. The
handoff test observed an already-accepted ACK on each link after sender crash
recovery and verified one final normalized dataset. No live Ethereum provider,
commit or push was used.

## File inventory

Modified files:

- `PLAN.md`
- `README.md`
- `docs/architecture/loader-stage8.md`
- `docs/architecture/protobuf-contracts.md`
- `docs/architecture/transformer-stage7.md`
- `services/extractor-service/cmd/extractor/main.go`
- `services/extractor-service/internal/adapters/outbound/postgres/integration_test.go`
- `services/extractor-service/internal/config/config.go`
- `services/extractor-service/internal/config/config_test.go`
- `services/transformer-service/cmd/transformer/main.go`
- `services/transformer-service/internal/adapters/outbound/postgres/integration_test.go`
- `services/transformer-service/internal/config/config.go`
- `test/integration/postgres.sh`

Created files:

- `docs/architecture/handoff-stage9.md`
- `internal/adapters/grpccontract/client.go`
- `migrations/extractor/0003_outbox_delivery.sql`
- `migrations/transformer/0003_outbox_delivery.sql`
- `services/extractor-service/internal/adapters/inbound/dispatcher/worker.go`
- `services/extractor-service/internal/adapters/inbound/dispatcher/worker_test.go`
- `services/extractor-service/internal/adapters/outbound/grpc/publisher.go`
- `services/extractor-service/internal/adapters/outbound/grpc/publisher_test.go`
- `services/extractor-service/internal/adapters/outbound/postgres/handoff_seed_test.go`
- `services/extractor-service/internal/adapters/outbound/postgres/outbox.go`
- `services/extractor-service/internal/adapters/outbound/postgres/outbox_integration_test.go`
- `services/extractor-service/internal/application/ports/inbound/dispatch.go`
- `services/extractor-service/internal/application/ports/outbound/delivery.go`
- `services/extractor-service/internal/application/usecases/dispatch.go`
- `services/extractor-service/internal/application/usecases/dispatch_test.go`
- `services/extractor-service/internal/config/dispatch.go`
- `services/extractor-service/internal/config/dispatch_test.go`
- `services/transformer-service/internal/adapters/inbound/dispatcher/worker.go`
- `services/transformer-service/internal/adapters/inbound/dispatcher/worker_test.go`
- `services/transformer-service/internal/adapters/outbound/grpc/publisher.go`
- `services/transformer-service/internal/adapters/outbound/grpc/publisher_test.go`
- `services/transformer-service/internal/adapters/outbound/postgres/outbox.go`
- `services/transformer-service/internal/adapters/outbound/postgres/outbox_integration_test.go`
- `services/transformer-service/internal/application/ports/inbound/dispatch.go`
- `services/transformer-service/internal/application/ports/outbound/delivery.go`
- `services/transformer-service/internal/application/usecases/dispatch.go`
- `services/transformer-service/internal/application/usecases/dispatch_test.go`
- `services/transformer-service/internal/config/dispatch.go`
- `services/transformer-service/internal/config/dispatch_test.go`
- `test/integration/cmd/handoff-postgres/main.go`
- `test/integration/cmd/handoff-postgres/main_test.go`
- `test/integration/handoff/handoff_test.go`
