# Transformer Inbox, transformation and Outbox (Stage 7)

This document records its stage's persistence boundary. [Stage 9](handoff-stage9.md)
now adds Outbox transmission and executable dispatch wiring; earlier statements
about deferred dispatch describe the original stage scope.

This stage implements the Stage 7 acceptance criterion: accept durably,
transform and queue output without Loader availability. No outgoing RPC, delivery
dispatcher, Loader persistence, canonical-chain reconciliation or broker is added.
The implementation refines ADR 0001/0002 and the approved JSON v1 contract.

## Domain and JSON v1

The Transformer owns typed block, transaction, receipt, log, access-list and
authorization entities. There are no JSON tags, database clients or generated
types in domain/application. The Ethereum JSON adapter parses syntax and
representations; domain validation enforces membership, unique hashes, contiguous
transaction/log indexes, receipt completeness, chain identity, gas totals,
transaction types 0–4, signature parity/chain metadata and required fee fields.
It does not verify signatures, trie roots, consensus or cryptographic hashes.

The normalized document has integer `schema_version: 1`, `chain_id`,
`source_event_id`, `provider`, `extracted_at`, a `block` object and separate
ordered `transactions`, `receipts` and `logs` arrays. Every quantity, including
block heights, timestamps in seconds, indexes and wei values, is a canonical
decimal **string**, never a float or machine-limited JSON number. Hashes, addresses,
byte data, nonce and bloom are lowercase, `0x`-prefixed hex of the appropriate
width. Field names use snake_case equivalents of ADR 0002's source names.

Named block/header fields, fork-dependent header fields, transaction amounts,
fees and signatures, access lists, blob hashes, authorization lists, receipt
outcomes/gas/addresses and log fields are explicit. Missing optional fields remain
absent; explicit null remains null where allowed. Contract creation is a null
recipient, never a zero address. Historical legacy receipts retain `root`
instead of inventing `status`; status 0 is valid failed execution. Empty arrays
are `[]`, not null. Receipts refer to transactions by hash/index; logs likewise
retain their block/transaction identities.

Unknown source fields and withdrawals remain retrievable in exact Inbox bytes;
they are not guessed into a normalized schema. Uncle hashes are retained; no
uncle-body or withdrawal records, traces, internal calls, ABI data or blob
sidecars are collected. A future schema expansion requires an explicit version
review. Optional fields do not constitute a full fork-activation registry.

JSON DTO maps are adapter-owned and their keys are sorted by the encoder.
Collection order is validated against source order. The payload has no processing
clock, attempt count or random ID. Identical event content always produces the
same bytes. The Outbox identity is `transformed-block/v1/<chain>/<number>/<hash>`.
The result's first commit time is stored separately from the deterministic JSON.

## Durability and recovery

`transformer.inbox_events` retains all accepted envelope fields, exact raw block
bytes and ordered receipt bytes. Original extraction time is RFC3339Nano text;
database scheduling times use timestamptz. Event ID is the primary deduplication
key. On a duplicate, compare every immutable field and every byte: identical
content returns already accepted, different content returns a conflict. A new
acceptance uses a local synchronous commit before ACK. Ambiguous commit results
remain errors and are resolved by retrying the same event.

Semantic poison input is durably accepted at the envelope boundary, then marked
failed by the worker. It is never silently dropped or acknowledged as processed.
Attempts, scheduling time and a safe failure code remain visible in the Inbox.
Re-delivery does not reset a terminal failure or overwrite its source content.

The queue claims one eligible event using `FOR UPDATE SKIP LOCKED`, incrementing
its persisted attempt number and setting a bounded lease. The attempt number is
a fencing token. Completion/failure updates require the same token, processing
state and an unexpired lease. Crashed work is reclaimed after lease expiry.
Crashes on the last permitted attempt become an explicit failed state during
bounded cleanup. A stale worker cannot commit a result after another claim.
The locking primitive follows the
[PostgreSQL queue-consumer guidance](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE).

The `Complete` transaction updates Inbox completion, inserts one immutable
`transformed_blocks` JSON document and one `outbox_events` intent together.
Any failure rolls all three back. The Outbox references a result in the same
schema; it does not duplicate its payload or read another service's tables.
All Outbox rows remain pending; there is no delivery status or dispatcher.

Permanent validation/unsupported/identity/incomplete/oversize errors fail
immediately. Other errors retry with bounded exponential backoff and equal
jitter, up to the persisted maximum attempt count. A cancelled operation leaves
its lease for recovery. An ambiguous completion commit is never followed by a
second state update; subsequent recovery observes completion or reclaims work.
No exactly-once transport claim or cross-service transaction is made.

The runtime role receives only SELECT/INSERT on owned tables and UPDATE on
Inbox processing columns. It cannot alter accepted bytes, delete events, change
results or access other service schemas. Apply the new migration as
`transformer_migrator`, never automatically from the service.

## Memory and boundaries

The approved raw/transformed payload ceiling remains 64 MiB and the gRPC message
ceiling remains 65 MiB. The asynchronous JSON parser necessarily allocates typed
representations; JSON encoding also allocates its result before the final byte
check. These are byte limits, not process-memory limits. The worker has bounded
concurrency and processing deadlines. Budget memory for codecs, parsed values,
encoded output and database buffers. No raw-byte clone is added at the inbound
port mapping. Payloads remain retained durably after processing.

Finality/reorg reconciliation, replay tooling, retention/deletion, outbox dispatch
and complete pipeline deployment remain later-stage work. Distinct block hashes
at one height remain distinct identities; Transformer does not select a canonical
fork or assume delivery ordering.

## Completion verification (2026-10-22)

At resumption, the domain, application ports/use cases, PostgreSQL migration and
adapter, JSON codecs, processing worker, executable wiring and integration runner
were already present as uncommitted work. Their unit and PostgreSQL tests passed.
The continuation preserved that implementation and completed missing worker and
configuration tests, extended normalization coverage for transaction types 1–4,
historical/null output and empty collections, formatted Stage 7 Go files and
updated operational documentation and plan status.

All Stage 7 requirements are complete: durable idempotent acceptance, safe leased
claims with bounded retry/error state, validated versioned transformation, atomic
result/Outbox/completion and restart/poison/duplicate/rollback coverage. Production
domain/application imports remain independent of adapters, SQL and protobuf;
only service-local transactions and schema access are used.

| Command | Result |
| --- | --- |
| `go test ./...` | PASS, including architecture checks and runner unit tests |
| `go test -race ./services/transformer-service/...` | PASS |
| `go run ./test/integration/cmd/transformer-postgres` | PASS; real PostgreSQL, tagged adapter tests with `-race`, migrations/permissions, cleanup |
| `go run ./test/integration/cmd/extractor-postgres` | PASS; regression suite with `-race`, migrations/permissions, cleanup |
| `go vet ./...` | PASS |
| `gofmt -w services/transformer-service test/integration/cmd/transformer-postgres` | Applied |
| `rg --files -g '*.go' -0 | xargs -0 gofmt -l` | PASS; no unformatted Go files |
| `git diff --check` | PASS |

Initial sandbox attempts could not access the Go build cache or bind a loopback
port. The affected checks were rerun with approved access and passed. No live
Ethereum provider was used. PostgreSQL runners removed their disposable resources.
No commit, branch creation or push was performed.

No unresolved architectural decision blocks Stage 7. Outgoing dispatch (Stage 9),
Loader persistence (Stage 8), finality/reorg reconciliation (Stage 10), retention,
failed-event replay tooling and production transport/deployment remain outside
this stage. Commit-loss tests inject errors around a real database COMMIT; they
do not simulate host power loss. Payload byte limits are not process-memory limits.

## Stage 7 file inventory

This inventory includes work retained from the interrupted execution. “New” means
untracked relative to HEAD, not necessarily first created during continuation.
Existing gRPC receiver and protobuf files were reused without code changes.

Modified tracked files:

- `PLAN.md`
- `README.md`
- `docs/architecture/protobuf-contracts.md`
- `services/transformer-service/cmd/transformer/main.go`
- `services/transformer-service/internal/config/config.go`
- `services/transformer-service/internal/config/config_test.go`
- `test/integration/postgres.sh`

New files:

- `docs/architecture/transformer-stage7.md`
- `migrations/transformer/0002_inbox_transformation_outbox.sql`
- `services/transformer-service/internal/adapters/inbound/worker/worker.go`
- `services/transformer-service/internal/adapters/inbound/worker/worker_test.go`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/decode.go`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/decode_test.go`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/fields.go`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/testdata/block.json`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/testdata/receipt_0.json`
- `services/transformer-service/internal/adapters/outbound/ethereumjson/testdata/receipt_1.json`
- `services/transformer-service/internal/adapters/outbound/jsondocument/encode.go`
- `services/transformer-service/internal/adapters/outbound/postgres/integration_test.go`
- `services/transformer-service/internal/adapters/outbound/postgres/store.go`
- `services/transformer-service/internal/application/ports/inbound/process.go`
- `services/transformer-service/internal/application/ports/inbound/validation.go`
- `services/transformer-service/internal/application/ports/outbound/processing.go`
- `services/transformer-service/internal/application/usecases/accept.go`
- `services/transformer-service/internal/application/usecases/process.go`
- `services/transformer-service/internal/application/usecases/usecases_test.go`
- `services/transformer-service/internal/domain/model.go`
- `services/transformer-service/internal/domain/provenance.go`
- `services/transformer-service/internal/domain/validation.go`
- `services/transformer-service/internal/domain/validation_test.go`
- `services/transformer-service/internal/domain/values.go`
- `test/integration/cmd/transformer-postgres/main.go`
- `test/integration/cmd/transformer-postgres/main_test.go`
