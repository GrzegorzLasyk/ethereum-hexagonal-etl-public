# Transformer: durable raw input to a validated normalized document

Transformer separates source acquisition from a queryable representation. It
accepts immutable raw events even when Loader is down, subject to durable capacity,
and processes them asynchronously. A successful gRPC ACK means the Inbox owns the
input; it does not mean transformation or Loader persistence has finished.

## Source map, ports and representation

Within [the service](../../services/transformer-service), start at:

- [`cmd/transformer/main.go`](../../services/transformer-service/cmd/transformer/main.go):
  explicit wiring of pool, store, decoder, encoder, publisher, workers and health.
- [`application/ports/inbound`](../../services/transformer-service/internal/application/ports/inbound):
  `RawBlockAcceptor`, `Processor`, `Dispatcher` and plain request/progress values.
- [`application/ports/outbound`](../../services/transformer-service/internal/application/ports/outbound):
  `Inbox`, `Queue`, `Decoder`, `Encoder`, `DeliveryQueue`, `Publisher`, `RepairStore`,
  `StatusReader` and `ReadinessChecker`.
- [`domain`](../../services/transformer-service/internal/domain): `Snapshot`, `Block`,
  `Transaction`, `Receipt`, `Log`, `Access`, `Authorization`, exact `Quantity`,
  fixed-width identities, `Optional[T]` and source `Provenance`.
- [`adapters/outbound/ethereumjson`](../../services/transformer-service/internal/adapters/outbound/ethereumjson):
  Ethereum JSON boundary parsing into Transformer types.
- [`adapters/outbound/jsondocument`](../../services/transformer-service/internal/adapters/outbound/jsondocument):
  deterministic version-1 JSON encoding, including original source provenance.
- [`adapters/outbound/postgres`](../../services/transformer-service/internal/adapters/outbound/postgres):
  durable acceptance/claims/completion, Outbox leases and audited repair.
- Inbound `grpc`, `worker`, `dispatcher`, `metrics` adapters translate transport,
  scheduling and observation needs; outbound `grpc` sends to Loader.

Large quantities are decimal integers without floating point. `Optional[T]`
distinguishes absent, null and value. Raw unknown fields remain in the retained
input; the normalized document intentionally contains the modeled fields, not an
assertion that every future Ethereum field has a normalized column.

## Trace acceptance, processing and delivery

1. The gRPC admission wrapper reserves connection/request/message resources before
   decoding. The receiver validates metadata and JSON boundaries and maps protobuf
   into `AcceptRawBlockRequest` without copying/rebuilding raw payloads.
2. `Accept.AcceptRawBlock` validates the application envelope and calls `Inbox.Accept`.
3. `Store.Accept` serializes new admissions. Existing IDs are compared for exact
   content before capacity. New rows commit before ACK; conflicting IDs fail.
4. `Process.ProcessOne` claims one row in a short transaction, then decodes and
   validates outside that transaction. It checks envelope/snapshot identity and
   encodes a stable `transformed-block/v1/...` document carrying `SourceEventID`.
5. `Store.Complete` fences attempt/lease and atomically completes the Inbox, inserts
   `transformed_blocks` and creates the Outbox intent. Any statement failure rolls
   the whole transaction back.
6. The dispatcher claims a committed Outbox item, calls the publisher, then fences
   a delivered/retry update. The publisher makes one explicit RPC per invocation;
   durable retry/backoff belongs to the dispatch use case, not an invisible gRPC loop.

See [`ProcessOne`](../../services/transformer-service/internal/application/usecases/process.go)
and [`Store.Complete`](../../services/transformer-service/internal/adapters/outbound/postgres/store.go).
`ErrCommitUnknown` or `ErrLeaseLost` after completion prevents a second failure
mutation. A later claim/read resolves durable state. Cancellation leaves lease
recovery available rather than pretending that acknowledged work vanished.

## Owning tables and admission algorithm

`transformer.inbox_events` retains raw bytes, metadata, attempts and processing state.
`transformed_blocks` retains the immutable encoded result and source identity.
`outbox_events` retains delivery intent/status. `recovery_operations` records audited
finite retry allowances. Migration/readiness metadata stay within the schema.

[Migrations](../../migrations/transformer) 0001–0006 add privileges, Inbox/result/Outbox,
delivery state, readiness, recovery audit and partial outstanding-work indexes.
The runtime has narrow update permissions; it never queries Extractor or Loader SQL.

Admission counts incomplete Inbox plus undelivered Outbox using bounded partial-index
probes in one MVCC snapshot. Completion transfers a slot between these categories
atomically. Delivered Outbox frees capacity; failed rows retain it. This allows
pressure to reach Extractor during a Loader outage without losing accepted work.
Exact duplicates remain acknowledgeable at capacity. Restart does not reset attempts.

## Configuration and lifecycle

[`config.Load`](../../services/transformer-service/internal/config/config.go) validates:

| Setting | Default / constraint |
|---|---|
| `TRANSFORMER_DATABASE_URL`, `TRANSFORMER_CHAIN_ID` | Required, owning runtime role and canonical chain ID |
| `TRANSFORMER_GRPC_ADDRESS` | `127.0.0.1:50051`; loopback IP required |
| `TRANSFORMER_WORKERS`, `TRANSFORMER_MAX_ATTEMPTS` | 1 worker (1–8), 5 initial processing attempts (1–1000) |
| `TRANSFORMER_PROCESS_TIMEOUT`, `TRANSFORMER_LEASE` | 30s / 1m; lease must exceed process timeout |
| `TRANSFORMER_POLL_INTERVAL` | 1s |
| `TRANSFORMER_INITIAL_BACKOFF`, `TRANSFORMER_MAX_BACKOFF` | 1s / 1m; capped jittered exponential delay |
| `TRANSFORMER_MAX_OUTSTANDING_EVENTS` | 128 (1–10000), including failed outstanding work |
| `TRANSFORMER_MAX_PAYLOAD_BYTES`, `TRANSFORMER_MAX_MESSAGE_BYTES` | 64 MiB / 65 MiB maxima; message must exceed payload |
| `TRANSFORMER_DB_MAX_CONNECTIONS` | 12; validated shared resource settings |
| `TRANSFORMER_SHUTDOWN_TIMEOUT` | 10s |

Separate `TRANSFORMER_DISPATCH_*` controls publication deadlines/lease/backoff/workers.
The [runtime reference](../reference/runtime-reference.md#transformer-operation-stage-7)
and [backpressure table](../architecture/backpressure-stage14.md) include the remaining
request/byte/connection settings. Fixture Compose overrides polling and retries for
fault tests; those are not Mainnet recommendations.

Background pools claim independent work and stop through context cancellation.
Readiness uses only owning schema/migration/privilege checks, so Loader downtime
must not make an otherwise healthy durable receiver unready. Shutdown bounds gRPC
and worker lifetime; unfinished leased input can be reclaimed after restart.

## Error handling, metrics and recovery

Invalid/incomplete/unsupported data and identity conflicts become safe permanent
processing codes. Transient failures receive bounded retries; output exceeding the
contract is terminal without truncation. Overload maps to retryable `Unavailable`;
size violations to `ResourceExhausted`. Logs correlate raw and transformed event
IDs and never include raw documents, credentials or SQL details. Metrics expose
Inbox/Outbox backlog/age, transformation/dispatch outcomes, durations and admission.

Repair is an explicit owning-schema operation with a stable operation ID, bounded
new allowance and old-claim fencing. Follow [Stage 13 recovery](../architecture/operations-stage13.md)
from downstream toward upstream. Do not reset the Inbox, invent new event IDs or
retry malformed content forever. The immutable first result remains the source
for later handoffs; a new normalization version would need an explicit contract decision.

## Relevant tests

Domain validation and decoder tests cover transaction-type fields, quantities,
receipt/log membership and missing/null distinctions. Use-case fakes cover commit
uncertainty and retry policy. PostgreSQL tests cover exact duplicates, capacity
races, claim fencing, atomic output and repair. Publisher tests cover bounded caller
budgets and error classification. The owning integration runner is:

```sh
bash scripts/ci-integration.sh transformer-postgres
```

The [handoff runner](../../test/integration/handoff) additionally tests lost ACKs and
correlated durable completion across both real gRPC hops. These tests establish
specific exercised cases, not an exactly-once transport guarantee.
