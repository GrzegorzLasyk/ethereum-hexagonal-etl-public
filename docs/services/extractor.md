# Extractor: retain a complete source observation before advancing

Extractor is the only service that acquires Ethereum data. It turns a provider's
responses into a retrievable, identity-checked observation and a durable delivery
intent. It does not normalize the entire dataset for queries or declare consensus
finality. Read [ADR 0002](../adr/0002-ethereum-source-data.md) and its
[ADR 0004 amendment](../adr/0004-ordered-extraction-reorg-recovery.md) together.

## Source map and ports

Paths below are relative to [the service](../../services/extractor-service).

| Layer / source | Responsibility |
|---|---|
| `internal/domain/block.go`, `checkpoint.go`, `indexing.go` | Chain/block identities, complete membership, continuity, capacity and eligible height |
| `application/ports/inbound/{extract_block,indexing,dispatch}.go` | Single-block, indexing-cycle and dispatch entry contracts |
| `application/ports/outbound/extraction.go` | `BlockSource` acquisition and atomic `ExtractionStore` persistence |
| `application/ports/outbound/{chain,indexing,drain}.go` | `ChainObserver`, `BranchStore`, `ReindexStore`, `ReplayStore`, `RangeDeliveryReader` |
| `application/ports/outbound/{delivery,status,readiness}.go` | Delivery claims/publication and owning-schema diagnostics |
| `application/usecases/` | Extraction, ordered cycle/reconciliation, separate reindex cursor, audited replay, dispatch and drain |
| `adapters/outbound/jsonrpc/` | HTTP/JSON-RPC validation, bounded response/attempt/pacing policies and receipt workers |
| `adapters/outbound/postgres/` | Raw storage, revision fences, active branch and leased Outbox |
| `adapters/outbound/grpc/` | Raw event → protobuf → Transformer; classify ACK/errors |
| `adapters/inbound/{indexer,dispatcher,metrics}/` | Polling/lifecycle loops and status-to-metrics translation |
| `cmd/extractor/` | Configuration, explicit dependency injection and shutdown ownership |
| `cmd/fixture-capture/`, `internal/adapters/fixturecapture/` | Opt-in acquisition artifacts and manifest-verified offline replay |

The abbreviated layer paths in the table all live under `internal/`. The
[source index](../learning/source-index.md) links individual files. Application
ports contain plain Go/domain values, not HTTP, SQL or protobuf DTOs.

## Domain and one cycle

`BlockID` includes chain, height and hash; `EventID` derives a stable raw-event key.
`Extraction` stores raw block/ordered receipt bytes with typed transaction, receipt
and log membership. `Validate` rejects missing receipts, duplicate/noncontiguous
positions, removed logs, mismatched identities or missing provenance. It cannot
cryptographically prove that provider bytes are an Ethereum-valid block.

`ExtractBlock` is the original single-block use case. Current scheduling uses
[`Index.Cycle`](../../services/extractor-service/internal/application/usecases/index.go):
read checkpoint → observe head/tip → reconcile if necessary → test inclusive range
and confirmations → test capacity → acquire one complete block → recheck anchor
and identity → atomic commit. `Batch` bounds sequential work per cycle, not parallel
height acquisition. Results retain completed block identities even if later work fails.

`Reindex` has a separate persisted operation/range cursor and does not advance the
main checkpoint. `Replay` requeues an existing exact event with audited finite
allowance; it does not fetch RPC data or erase a receiver's accepted state.

## RPC acquisition and cancellation

[`Client.Block`](../../services/extractor-service/internal/adapters/outbound/jsonrpc/client.go)
checks `eth_chainId`, requests a numbered block with full transaction bodies,
fetches every receipt, then checks chain identity again. `Head`/`Header` support
scheduling and recovery. Success preserves exact JSON result bytes; partial
acquisition returns an empty extraction plus an error.

The receipt pool defaults to 1 worker, allows 1–8, assigns unique slice indexes,
shares request pacing/cooldown and atomic attempt accounting, and joins before
cross-receipt validation. HTTP retries are bounded; 401/402/403 are permanent,
429 honors Retry-After, and transient failures retain classification across cycles.
Budget exhaustion and permanent source errors stop automatic acquisition. There
are separate request, cycle and whole-run deadlines. Redirects are disabled and
errors omit endpoints/provider response bodies. See the
[RPC policy](../architecture/rpc-reliability-stage13.md) and
[receipt implementation notes](../architecture/receipts-stage14.md).

## Durable objects and transaction boundaries

| Owned object | Purpose |
|---|---|
| `extractor.raw_blocks` | First immutable snapshot by full block identity |
| `extractor.outbox_events` | Stable intent; pending/sending/delivered/failed and attempt/lease state |
| `extractor.extraction_checkpoints` | Active last commit, original start, revision |
| `extractor.active_blocks` | Current provider-relative active membership |
| `extractor.rewinds` | Immutable revision/ancestor rewind audit |
| `extractor.reindex_jobs`, `extractor.replays` | Restartable range cursor and replay audit |
| `schema_migrations`, `schema_readiness` | Owner migration ledger and runtime readiness projection |

[Migrations](../../migrations/extractor) 0001–0005 cover privileges, source storage,
Outbox delivery, branch recovery and readiness. Runtime roles never migrate.

`Store.Commit` serializes per chain, compares the expected checkpoint including
revision, rechecks capacity, and commits snapshot + Outbox + active membership +
checkpoint together. An already retained identity keeps the first snapshot and
provenance; it is not an exact-byte equality certification for a new acquisition.
`Rewind` changes only active membership/cursor and its audit; historical observations
and events remain. Ambiguous commit has an explicit error and an idempotent durable
resolution path. Outbox claim and delivered/retry updates are separate fenced
transactions; the network call never shares a cross-service SQL transaction.

## Modes and configuration

Use [`config`](../../services/extractor-service/internal/config) and the complete
[runtime tables](../architecture/extraction-stage10.md).
The generic binary defaults to `dispatch`; the fixture overlay explicitly uses
`index`. Modes include `index`, `dispatch`, `drain`, `reindex` and `replay`.

- `EXTRACTOR_DATABASE_URL` and canonical `EXTRACTOR_CHAIN_ID` are required.
- Index/reindex require explicit start, confirmation/recovery depth and positive
  event/byte/database admission watermarks. No end means follow-head in generic
  operation; the private Mainnet wrapper requires an explicit bounded range.
- RPC defaults in the service configuration: 10s request timeout, 200ms pacing,
  3 attempts per logical call, 1s initial/30s maximum backoff, receipt concurrency 1.
- `EXTRACTOR_DB_MAX_CONNECTIONS` defaults to 12 (2–32); dispatch workers default to
  1 (1–8). Keep payload/message sizes compatible with Transformer.
- Non-loopback live acquisition requires explicit Mainnet opt-in and finite
  request/run budgets. The controlled final workflow further restricts block
  1000000, at most 100 attempts and two-minute acquisition.
- `EXTRACTOR_HEALTH_ADDRESS` enables the optional loopback HTTP listener; the
  Compose profiles supply it. Never log `EXTRACTOR_RPC_URL` or database URLs.

The [Mainnet operator](../../tools/mainnet) checks private permissions/settings,
filters inherited overrides and keeps its retained project separate from fixtures.
Instructions do not authorize another live acquisition.

## Lifecycle, failure and observations

The indexer runs sequential bounded cycles while dispatch can drain independently.
Finite acquisition uses `runAcquisition` to leave dispatch alive until bounded
range delivery completes. Cancellation joins workers; committed work remains
recoverable. A successful drain proves Extractor durable delivery, not necessarily
Loader completion; the separate verifier checks the whole pipeline.

Structured events retain block/event identity with safe codes. Metrics expose
head/checkpoint/revision/lag, snapshot freshness, Outbox count/age, backpressure,
RPC attempts/retries/budget and operation durations. A DB failure is not a zero
queue. No metric label contains event IDs or provider credentials. `/ready` checks
only the owning database/schema/privileges, never Transformer availability.

## Reading and testing

Start with `domain/block_test.go`, `usecases/index_test.go`, then JSON-RPC
`receipts_test.go` and `reliability_test.go`. PostgreSQL integration tests exercise
checkpoint atomicity, competing revisions, rewind, retained history, replay and
RPC failure preservation. The isolated runner is:

```sh
bash scripts/ci-integration.sh extractor-postgres
```

For a local learning run use the synthetic [developer setup](../../README.md#local-development-without-live-ethereum-rpc).
The [operations runbook](../architecture/operations-stage13.md) owns exact repair,
reindex and deep-reorg procedures. Provider trust, finite retry stops and growing
immutable history are deliberate limits, not problems to solve by deleting rows.
