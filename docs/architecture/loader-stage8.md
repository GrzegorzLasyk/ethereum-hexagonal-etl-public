# Loader Inbox and normalized persistence (Stage 8)

Current operations, storage preservation, failed-work limitations and release
checks are consolidated in the [Stage 12.5 runbook](operations-stage12.md).
Earlier exclusions below describe the original stage's scope, not missing current
health, metrics, dispatch or local deployment.

This document records its stage's persistence boundary. [Stage 9](handoff-stage9.md)
now adds Outbox transmission and executable dispatch wiring; earlier statements
about deferred dispatch describe the original stage scope.

Status: implemented, including the accepted conflict observation policy in ADR 0003.

Loader owns its domain, acceptance/processing use cases, ports, JSON decoder,
PostgreSQL adapter and polling worker. Its types follow the existing JSON v1
contract but do not import Transformer internals or a shared domain model.
The existing protobuf and gRPC receiving adapter are unchanged. No outgoing
client, Outbox dispatcher, broker or inter-service transmission is introduced.

## Acceptance and processing boundaries

`AcceptTransformedBlock` validates the envelope independently of gRPC, then
stores every immutable field and the exact JSON bytes in `loader.inbox_events`.
The transaction uses `synchronous_commit=on`; ACK is emitted only after success.
Event ID is the deduplication key. Identical content returns already accepted;
a different payload, parent, provider or timestamp under that ID is a conflict.
Original extraction/transformation timestamps use RFC3339Nano text to preserve
nanoseconds. Unknown COMMIT outcomes remain errors resolved by identical retry.
Neither ACK nor an Inbox row means that query tables are populated.

The worker claims one eligible row with `FOR UPDATE SKIP LOCKED`. It increments
the persisted attempt and installs a bounded lease. Attempt is a fencing token.
Only that attempt can complete or record failure while its lease is valid.
Expired claims are reclaimed; exhausted crashed claims become failed during a
bounded cleanup pass. Re-delivery does not reset failed or completed work.

The use case decodes the accepted document, validates the Loader domain and
checks every document identity/provenance field against its accepted envelope.
Permanent invalid, incomplete, unsupported, identity or storage conflicts become
failed input. Transient errors receive exponential backoff with equal jitter
and a bounded attempt count. Cancellation leaves the lease recoverable. A lost
COMMIT response is never followed by an unrelated failure state update.

`Complete` obtains a per-chain transaction lock, locks/fences the Inbox row,
inserts the block and all transaction, receipt and log records, updates affected
conflict observations, and marks Inbox completion in one local transaction.
The final update rechecks expiry: a lease expiring during persistence rolls back
all records. Failed writes likewise roll back everything. A completed retry of
the same attempt succeeds without rewriting data; stale attempts are rejected.
Duplicate delivery cannot create another block or repeat logical processing.
No cross-service database access or transaction is used.

## JSON v1 interpretation

The parser accepts the existing Stage 7 normalized document, not raw RPC JSON.
It requires `schema_version: 1`, exact decimal strings for quantities, lowercase
hex data of the correct width and explicit transaction/receipt/log arrays. It
rejects duplicate JSON keys, malformed UTF-8, excessive nesting, trailing input,
overflow, missing collections and incomplete or inconsistent membership.
The document's separate ordered logs are associated with their matching receipts.

Domain validation covers identities, transaction/log indexes, gas totals, types
0–4, type-dependent fee/signature fields, receipt outcomes and log membership.
It does not prove cryptographic hashes, signatures, consensus or finality.
Historical receipts retain root instead of invented status; status zero remains
a valid failed transaction. Unknown v1 fields remain available in exact Inbox
bytes; they are not interpreted as new query columns. Withdrawals, traces,
internal calls and blob sidecars remain outside the normalized dataset.

## Owned tables and query representation

| Table | Content and identity |
| --- | --- |
| `inbox_events` | Stable transformed event ID, exact document, all provenance, processing state, attempts, scheduling, lease and safe error code |
| `blocks` | Full header fields keyed by `(chain_id, block_number, block_hash)`; unique source Inbox event, load timestamp, canonical status and conflict observations |
| `transactions` | Full transaction fields keyed by chain/block identity and transaction hash; unique position within that block |
| `receipts` | One receipt per transaction within its block, including failed/historical outcomes and gas data |
| `logs` | Log identity, address, topics and data keyed by chain/block identity and global log index |

Foreign keys connect blocks to accepted events, transactions to blocks, receipts
to exact transaction hash/index and logs to exact receipt hash/index. A transaction
hash can occur in more than one block; block identity remains in every key.
Indexes support block/height lookup, transaction hashes, sender/recipient,
receipt/log transaction membership, log address and first topic by height.

All bounded uint64 values use range-checked `numeric(20,0)`; uint256 values use
range-checked `numeric(78,0)`. Chain IDs are canonical decimal text without an
implicit uint64 limit. Hashes, addresses, blooms, nonces and binary data use
`bytea`; uncle hashes, blob hashes and log topics use ordered `bytea[]`.
Access/authorization lists are explicit per-transaction JSONB columns containing
the existing normalized strings and ordered entries. They are not opaque
replacements for the block, transaction, receipt or log tables.

Each entity has `present_fields`, listing optional JSON keys that were supplied.
SQL NULL plus membership in that list means explicit null; SQL NULL without
membership means absence. Required nullable recipients are included. Empty
arrays remain empty arrays. Raw accepted JSON preserves the complete original
representation. Source provenance is queryable through the block's Inbox event.

Only `loader_migrator` applies `0002_inbox_normalized_storage.sql` and the forward
`0003_conflict_observations.sql` migration, which also backfills existing rows.
Runtime gets SELECT/INSERT on owned data and UPDATE only on Inbox processing
columns and block `height_conflict`/`parent_link` observations. It
cannot modify source bytes or normalized values, delete rows, run DDL, change
migration metadata or access other service schemas.

## Conflicting observations and canonicality

[Accepted ADR 0003](../adr/0003-loader-conflict-observation.md) preserves all valid
block candidates. Different hashes at the same chain/height remain separate
records, with `height_conflict=true` on every version. Child keys and foreign keys
include chain, height and block hash, so shared transaction hashes do not merge
receipts or logs across forks. Same-event-ID content conflicts remain rejected.

`canonical_status` is always `unknown`: for a single candidate, competing
candidates, linked children and successfully completed Inbox events alike.
The database constraint and runtime privileges prohibit canonical promotion.
Successful processing means durable normalized observations, never canonicality.

`parent_link` describes only observed neighbors: `unknown` when the previous
height is absent (also at genesis), `linked` when any candidate matches the
parent hash, and `mismatch` when candidates exist but none match. A new candidate
revisits its height and immediate children. Late parents can change an observation
to `linked` without choosing a canonical fork or propagating descendant status.
Domain functions determine these states; the PostgreSQL adapter gathers facts
and persists results within the local completion transaction. Per-chain advisory
locking prevents concurrent completions from missing each other's candidates.
Rollback also restores flags on older blocks. Claims and decoding remain concurrent;
same-chain completion serialization is a deliberate write-throughput tradeoff.

Conflicts are directly queryable, supported by a partial index:

```sql
SELECT chain_id, block_number, encode(block_hash, 'hex') AS block_hash,
       canonical_status, height_conflict, parent_link
FROM loader.blocks
WHERE height_conflict OR parent_link = 'mismatch'
ORDER BY chain_id, block_number, block_hash;
```

Extractor currently rejects competing hashes and parent discontinuity rather
than advancing its accepted extraction lineage. Loader preserves conflicting
observations received in its own Inbox. Neither policy implies canonicality.
This stage changes no Extractor behavior and adds no inter-service transmission,
automatic reconciliation or finality tracking.

## Local operation

Apply migrations with the existing Compose tooling before starting Loader.
Supply `LOADER_DATABASE_URL` through a private process environment using
`loader_runtime` and database `ethereum_etl`. Startup rejects another role/database.
Set `LOADER_CHAIN_ID=1` for the approved Mainnet source; there is no implicit chain.
The executable does not load `.env` or run migrations.

```sh
# LOADER_DATABASE_URL must already be privately configured.
export LOADER_CHAIN_ID=1
go run ./services/loader-service/cmd/loader
```

| Variable | Default | Constraint |
| --- | --- | --- |
| `LOADER_DATABASE_URL` | Required | Runtime role, database `ethereum_etl` |
| `LOADER_CHAIN_ID` | Required | Canonical nonnegative decimal string |
| `LOADER_GRPC_ADDRESS` | `127.0.0.1:50052` | Loopback IP, port 1–65535 |
| `LOADER_MAX_PAYLOAD_BYTES` | `67108864` | Positive, at most 64 MiB |
| `LOADER_MAX_MESSAGE_BYTES` | `68157440` | Greater than payload budget, at most 65 MiB |
| `LOADER_WORKERS` | `1` | 1–8 |
| `LOADER_MAX_ATTEMPTS` | `5` | 1–1000, persisted at acceptance |
| `LOADER_POLL_INTERVAL` | `1s` | Idle/error polling delay |
| `LOADER_PROCESS_TIMEOUT` | `30s` | Deadline per processing call |
| `LOADER_LEASE` | `1m` | Must exceed processing timeout |
| `LOADER_INITIAL_BACKOFF` | `1s` | Initial retry delay with equal jitter |
| `LOADER_MAX_BACKOFF` | `1m` | At least initial backoff |
| `LOADER_SHUTDOWN_TIMEOUT` | `10s` | Incoming RPC grace period |

Durations use Go syntax, between 1 ms and 24 h. LOG_LEVEL follows the existing
service convention. Pool capacity is workers + 4; connect timeout is 5 s and
SQL statement timeout 10 s. SIGINT/SIGTERM cancel and join workers, drain RPCs
within the grace period and close the pool. Accepted interrupted work remains
durable. Progress logs include safe event identity/attempt/outcome, not payloads
or driver messages. Stages 11–12.4 add local health/metrics; production transport authentication remains absent.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./test/integration/cmd/loader-postgres
git diff --check
```

The runner follows the existing Go runner pattern: unique disposable Compose
project, random private credentials, loopback port, no project `.env`, buffered
redaction and cleanup on success/failure/cancellation. It verifies all migrations
and permissions, then runs the tagged Loader tests with `-race -count=1`.
`LOADER_TEST_DATABASE_URL`, `LOADER_TEST_MIGRATOR_URL` and `LOADER_TEST_ISOLATED=1`
are supplied privately. Do not point these at needed data: tests truncate the
owned disposable tables and install fault-injection triggers. Tagged tests skip
without database URLs and reject configured databases without the isolation marker.

The fixture was generated with the actual Stage 7 encoder and includes maximum
uint256, failed transaction, a log and null/absent fields. Ordinary tests need
neither PostgreSQL nor Ethereum access. Database tests verify ACK/duplicates,
concurrent claims, restart, fencing/expiry, retry budgets, poison data, exact
numeric/provenance storage, late delivery, rollback at every write boundary,
ambiguous commits and restricted privileges. Conflict tests cover concurrent
competing hashes, shared transaction hashes with isolated receipts/logs, rejected
cross-fork foreign keys, late matching parents, unknown canonicality, rollback of
older observation flags and forward migration/backfill over existing records.

## Limits and deferred work

Byte ceilings do not bound total process memory; parsing and database buffers
allocate additional memory. Very large blocks must fit processing deadlines.
COMMIT fault injection verifies database outcomes, not host power loss. There is
no retention/deletion, failed-event replay tool, dispatch, live Ethereum test,
canonical-chain promotion or automatic reorganisation recovery in this stage.

There are no unresolved Stage 8 architectural decisions. Future canonical-chain
reconciliation and finality require a separate explicitly approved mechanism.

## Verification record

The following checks passed on the current implementation:

- `go test ./...`
- `go test -race ./...`
- `go test -race ./services/loader-service/...`
- `go vet ./...`
- `go run ./test/integration/cmd/loader-postgres`
- `go run ./test/integration/cmd/transformer-postgres`
- `go run ./test/integration/cmd/extractor-postgres`
- `gofmt -w services/loader-service test/integration/cmd/loader-postgres`
- Whole-repository `gofmt -l` inspection: no unformatted Go files.
- `git diff --check`

PostgreSQL suites used real disposable databases with race detection, verified
all migrations/permissions and cleaned up their own resources. Sandbox-blocked
cache/loopback access was rerun with approved permissions. No live Ethereum
provider, inter-service sender, commit or push was used.

## File inventory

This includes uncommitted work retained from the interrupted implementation.

Modified tracked files:

- `PLAN.md`
- `README.md`
- `docs/architecture/protobuf-contracts.md`
- `services/loader-service/cmd/loader/main.go`
- `services/loader-service/internal/config/config.go`
- `services/loader-service/internal/config/config_test.go`
- `test/integration/postgres.sh`

New files:

- `docs/adr/0003-loader-conflict-observation.md`
- `docs/architecture/loader-stage8.md`
- `migrations/loader/0002_inbox_normalized_storage.sql`
- `migrations/loader/0003_conflict_observations.sql`
- `services/loader-service/internal/adapters/inbound/worker/worker.go`
- `services/loader-service/internal/adapters/inbound/worker/worker_test.go`
- `services/loader-service/internal/adapters/outbound/jsondocument/decode.go`
- `services/loader-service/internal/adapters/outbound/jsondocument/decode_test.go`
- `services/loader-service/internal/adapters/outbound/jsondocument/fields.go`
- `services/loader-service/internal/adapters/outbound/jsondocument/testdata/README.md`
- `services/loader-service/internal/adapters/outbound/jsondocument/testdata/transformed_v1.json`
- `services/loader-service/internal/adapters/outbound/postgres/complete.go`
- `services/loader-service/internal/adapters/outbound/postgres/integration_test.go`
- `services/loader-service/internal/adapters/outbound/postgres/observations.go`
- `services/loader-service/internal/adapters/outbound/postgres/observations_integration_test.go`
- `services/loader-service/internal/adapters/outbound/postgres/records.go`
- `services/loader-service/internal/adapters/outbound/postgres/store.go`
- `services/loader-service/internal/application/ports/inbound/process.go`
- `services/loader-service/internal/application/ports/inbound/validation.go`
- `services/loader-service/internal/application/ports/outbound/processing.go`
- `services/loader-service/internal/application/usecases/accept.go`
- `services/loader-service/internal/application/usecases/process.go`
- `services/loader-service/internal/application/usecases/usecases_test.go`
- `services/loader-service/internal/domain/document.go`
- `services/loader-service/internal/domain/model.go`
- `services/loader-service/internal/domain/observation.go`
- `services/loader-service/internal/domain/observation_test.go`
- `services/loader-service/internal/domain/provenance.go`
- `services/loader-service/internal/domain/validation.go`
- `services/loader-service/internal/domain/validation_test.go`
- `services/loader-service/internal/domain/values.go`
- `test/integration/cmd/loader-postgres/main.go`
- `test/integration/cmd/loader-postgres/main_test.go`
