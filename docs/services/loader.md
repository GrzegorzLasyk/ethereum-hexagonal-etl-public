# Loader: atomic normalized persistence and historical observations

Loader is the last durable consumer. It accepts a normalized JSON v1 document,
validates it independently, and inserts relational records. It retains conflicting
block observations; it does not choose an Ethereum canonical branch. This is the
explicit policy in [ADR 0003](../adr/0003-loader-conflict-observation.md).

## Source map and boundaries

- [`cmd/loader/main.go`](../../services/loader-service/cmd/loader/main.go) wires
  owning pool, store, decoder, metrics decorator, use cases, receiver and workers.
- [`application/ports/inbound`](../../services/loader-service/internal/application/ports/inbound)
  defines `TransformedBlockAcceptor`, `Processor`, envelope/document validation and
  progress values. No generated message or SQL type crosses these contracts.
- [`application/ports/outbound`](../../services/loader-service/internal/application/ports/outbound)
  defines `Inbox`, `Queue`, `Decoder`, `RepairStore`, `StatusReader`, `ReadinessChecker`.
- [`domain`](../../services/loader-service/internal/domain) owns `Document`, normalized
  `Snapshot`, `Provenance`, exact numeric/optional values and observation rules.
- [`jsondocument`](../../services/loader-service/internal/adapters/outbound/jsondocument)
  decodes normalized JSON, not Ethereum RPC JSON. Identity is checked against the
  gRPC envelope and source provenance before storage.
- [`postgres`](../../services/loader-service/internal/adapters/outbound/postgres)
  implements admission, claims, bounded INSERT batching, observation updates and repair.
- Inbound `grpc`/`worker`/`metrics` adapters trigger work and expose diagnostics;
  outbound `metrics.Queue` decorates completion to time persistence at the port.

Loader has no outgoing pipeline Outbox: the local relational records are its result.
It must not query Transformer tables to fill in missing payload fields.

## Acceptance and one atomic block transaction

Acceptance commits `loader.inbox_events` before gRPC ACK. Existing IDs are checked
for exact envelope/payload equality before outstanding capacity; conflicting content
fails. Claim is a short transaction using due/expired leases and incremented attempts.
`ProcessOne` decodes/validates after claim and invokes `Queue.Complete`.

[`Complete`](../../services/loader-service/internal/adapters/outbound/postgres/complete.go):

1. validates envelope and document consistency;
2. begins a local transaction and obtains the chain observation advisory lock;
3. locks the Inbox row and verifies attempt/state/lease; a matching completed
   attempt can resolve a lost successful commit acknowledgement;
4. inserts the block, each transaction, corresponding receipt and its ordered logs;
5. flushes every pending batch and updates arriving-height/immediate-child observations;
6. conditionally completes the Inbox under the same lease fence and commits.

Any failure rolls back all normalized rows and observation changes, including
changes to previously stored neighbors. A later-batch failure never leaves a partial
block. An ambiguous commit returns explicit uncertainty rather than an automatic
failure mutation. Existing source fields are not overwritten to resolve conflicts.

## Tables, values and keys

| Table | Contents / identity |
|---|---|
| `loader.inbox_events` | Stable transformed event, source event, immutable document, processing state |
| `loader.blocks` | Chain/number/hash identity, normalized fields, source event and observations |
| `loader.transactions` | Full block identity plus transaction position; hash index supports lookup |
| `loader.receipts` | Corresponding transaction identity, execution fields and optional outcome/root |
| `loader.logs` | Full block/transaction identity and ordered log indexes/topics/data |
| `loader.recovery_operations` | Immutable operator repair audit |
| `schema_migrations`, `schema_readiness` | Owning migration ledger and runtime readiness projection |

[Migrations](../../migrations/loader) 0001–0006 establish privileges, Inbox/records,
observations, readiness, repair and partial admission index. Hash/address/data values
are stored without lossy float conversions. Optional JSON-presence information is
kept in modeled fields; null recipient is not a zero-address substitution.
`records.go` maps domain values into parameterized SQL; no caller-generated table
name or domain-owned SQL abstraction is required for ordinary persistence.

## Neighbor observations are not canonical selection

`domain.ObservationRange` bounds the arriving height and neighbors without uint64
wraparound. `ObserveArrival` marks all same-height competitors and revisits the
arriving height plus immediate children. A matching retained parent yields `linked`;
known unmatched parents yield `mismatch`; absent evidence yields `unknown`.
Child-before-parent arrival is allowed. A link does not make a conflicting parent
canonical. Every record remains `canonical_status=unknown`, enforced by schema and
privileges. Same-chain completion serialization prevents concurrent arrivals from
missing each other's observations; it also limits write parallelism.

## Batching and resource trade-offs

[`recordBatch`](../../services/loader-service/internal/adapters/outbound/postgres/batch.go)
belongs to one completion transaction. It groups existing INSERTs, consumes results
and always closes a batch before further SQL. It preserves the first failure.
The default count is 64, supported range 1–256; count 1 uses direct Exec. A 1 MiB
estimate bounds coalescing; a large individual row is sent alone, never truncated.
The 65535 parameter check is per statement. Batch count is not SQL statement count
or measured network round trips. See [batch methodology](../architecture/loader-batching-stage14.md).

Admission bounds unfinished Inbox rows, including failed work. Request/message
reservations bound serialized input exposure before decoding, while connection
pools and Compose/Go limits cover other resources. These limits do not bound
retained historical disk usage or prove worst-case decoded heap usage.

## Configuration, lifecycle and visibility

[`config.Load`](../../services/loader-service/internal/config/config.go) requires
`LOADER_DATABASE_URL` and canonical `LOADER_CHAIN_ID`. Defaults are loopback
`127.0.0.1:50052`, `LOADER_WORKERS=1` (1–8), `LOADER_MAX_ATTEMPTS=5`, process timeout
30s, lease 1m, poll 1s, backoff 1s–1m, shutdown 10s and `LOADER_SQL_BATCH_SIZE=64`.
Lease must exceed processing timeout. Outstanding capacity defaults to 128; pool
maximum 12; aggregate payload/message maxima are 64/65 MiB. See the
[complete tables](../reference/runtime-reference.md#loader-operation-stage-8) and
[resource settings](../architecture/backpressure-stage14.md).

The composition root bounds listener and worker shutdown. Accepted unfinished work
remains durable for lease recovery. `/ready` depends on owning DB objects and
privileges, not upstream availability. Structured logs retain transformed/raw event
correlation and safe error codes. Metrics expose backlog/age, processing/retry/failure,
admission and persistence durations; optional performance probes add SQL/pool data.

Audited repair grants a finite allowance only after diagnosing the failed item.
Invalid, incomplete, identity/unsupported/conflicting documents are permanent;
transient storage failures use capped retries. Do not change canonicality, delete
Inbox rows or rewrite historical data as a repair shortcut.

## Tests to read

- `domain/observation_test.go`: forks, missing parents and child-before-parent.
- `jsondocument/decode_test.go`: document types, values and identity validation.
- `postgres/batch_test.go`: count/byte/parameter bounds, result closing and errors.
- `postgres/batch_integration_test.go`: empty/large blocks, all-field equivalence,
  idempotent replay and late-batch rollback followed by recovery.
- `postgres/observations_integration_test.go`, `admission_integration_test.go`:
  actual chain-lock and admission behavior with owning runtime privileges.

```sh
bash scripts/ci-integration.sh loader-postgres
```

Authentic offline tests compare records against captured source; their three
heights use separate databases. [Phase 1 evidence](../architecture/validation-offline-stage14.md)
and [Phase 2 measurements](../architecture/validation-performance-stage14.md) document
what passed and what was not measured. They do not certify consensus truth or
unbounded production capacity.
