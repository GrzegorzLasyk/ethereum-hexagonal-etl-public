# Stage 14.3 — Bounded concurrent receipt acquisition

Receipt fetching now uses a bounded worker pool inside the Extractor JSON-RPC
adapter. Block heights remain sequential under ADR 0004. No application/domain
concurrency, database schema, checkpoint, protobuf or canonicality change is made.
Stages 14.1/14.2 infrastructure and limits remain in place.

## Configuration and scope

`EXTRACTOR_RECEIPT_CONCURRENCY` is an integer from **1 to 8**, default **1**.
It configures indexing and bounded reindex acquisition and is passed through both
the fixture and isolated Mainnet Compose environments. Invalid explicit values,
including zero, are rejected at startup. `jsonrpc.Config.ReceiptConcurrency` uses
zero as the backwards-compatible default of one; negative values and values above
eight are rejected by the adapter constructor.

The conservative default preserves existing provider load and serial fixture
replay. Set 2 or 4 explicitly for a controlled offline comparison before choosing a
higher value. Eight is a safety ceiling, not a recommended throughput target.
The Stage 13 capture, smoke and artifact-verification callers retain their default
single-worker behavior; this change does not enable or execute live requests.

For the existing offline fixture environment, set the variable in its explicit
Compose configuration environment and follow the existing fixture runbook:

```sh
export EXTRACTOR_RECEIPT_CONCURRENCY=4
# Use the existing deployments/compose/fixture.yaml workflow, not Mainnet.
```

Do not infer block concurrency from this setting. Dispatcher and Inbox worker
settings have separate meanings. No block-range worker pool is introduced.

## Ordering, memory and cancellation

After validating the block, at most `min(concurrency, transaction_count)` goroutines
claim transaction indexes from one atomic cursor. There is no per-transaction
goroutine, additional producer goroutine, or unbounded channel/task queue. Each
worker owns exactly one result position at a time. Raw receipt JSON is retained
unchanged in that transaction's index position; completion order cannot reorder it.

Every receipt passes the existing production decoder: transaction hash/index,
block number/hash, type, addresses and logs must match the selected transaction.
Workers join before ordered cumulative-gas validation, final chain-ID recheck and
complete extraction validation. The public Block method returns an empty extraction
on any error, never a partial successful snapshot. The application still commits
raw source, checkpoint and Outbox only after a complete valid block is returned.

The first observed failure cancels the shared batch context with its original
cause. This includes permanent RPC errors, exhausted retry policy, budget
exhaustion, null/malformed receipts, identity errors and size overflow. Waiting
workers stop; active HTTP requests receive cancellation. All workers are joined
before returning. If failures race, the first observed cause wins; the error's
receipt index is not promised to be the lowest transaction index. Parent deadline
or cancellation remains distinguishable via errors.Is.

A mutex accounts for aggregate retained raw bytes without exceeding MaxSnapshotBytes.
Concurrent responses can temporarily occupy up to `concurrency * MaxResponseBytes`
in addition to retained raw data, decoded DTOs and HTTP/JSON overhead. Receipt
result arrays are proportional to the already validated block's transaction count.
This is not a strict heap bound; retain the Stage 14.2 process/container limits and
lower concurrency for large responses or small hosts. More workers may increase
memory use without improving throughput.

## Shared RPC admission and retries

All workers call the same Client.call path. Chain checks, headers/recovery requests,
block acquisition, receipt requests and retries share one client lifetime budget,
request ID generator, pacing gate and cooldown. The service still uses one client
for its bounded run. Separate client instances have separate budgets, as before.

- Each underlying HTTP RoundTrip invocation consumes one atomic budget slot,
  including retries and failed transport attempts. HTTP rejecting a canceled
  request before RoundTrip consumes no slot. A budget cannot be reset by launching
  a worker or retrying a block. No additional transport attempt is admitted once
  cancellation is observed or all slots are consumed.
- The budget reservation immediately before transport dispatch is the admission
  boundary. Attempts already admitted can race with cancellation or exhaustion;
  they remain counted and are canceled/joined, never refunded. This is an HTTP
  attempt counter, not proof of delivery or a provider billing counter.
- The global pacing gate admits starts at MinRequestInterval spacing. It is never
  held during timers or HTTP exchanges. Waiters recheck the current deadline after
  waking, so responses can extend a cooldown without being blocked by a waiter.
  Scheduler/network delays can affect wire arrival times; this is client admission
  pacing, not a guarantee about provider-observed packet spacing.
- A retryable HTTP/JSON-RPC rate-limit response with Retry-After extends the
  client-wide next-admission time. All workers and future retries honor it, not only
  the rejected transaction. Already admitted exchanges cannot be recalled before
  the rate-limit response becomes known. Concurrent cooldowns only extend the pause.
- Existing per-call MaxAttempts, exponential equal-jitter backoff, permanent HTTP
  classification, retryable 429/5xx/transport handling and ErrRetryDeferred remain.
  A Retry-After beyond permitted local backoff still halts acquisition for operator
  handling. There is no outer receipt retry loop or unbounded retry queue.
- Per-request timeout begins after pacing; whole-run/cycle/batch cancellation covers
  pacing, local backoff, HTTP and worker join. No new background lifetime is added.

Only sanitized numeric RPC/HTTP diagnostics and existing fixed error classes are
returned. Credentials, endpoint URLs and raw provider error bodies are not logged.

## Observability

The existing low-cardinality operation metrics add `operation="fetch_receipts"`:
count, duration histogram in seconds, and completed/failed/interrupted outcomes.
One observation covers the entire receipt batch, including validation and worker
join. A completed receipt batch is **not** proof of final chain recheck, durable
checkpoint advancement or downstream ETL completion. Existing extraction logs,
checkpoint/backpressure metrics and Client.Requests accounting remain available.
No transaction hashes, block heights or endpoint URLs are metric labels.

## Offline test methodology and validation

New synthetic workloads are created in memory, explicitly separate from authentic
Mainnet fixtures. No original artifact is changed. A local in-memory RoundTripper
has no network fallback. Go testing/synctest provides a virtual clock for simulated
server latency, global pacing, Retry-After, timeouts and cancellation. Channel
barriers synchronize permanent failure with an active sibling. Tests check:

- out-of-order completion with exactly bounded overlap, original byte equality and
  index-ordered results;
- shared attempt budgets across concurrent workers and retries;
- global pacing/cooldown and cancellation while workers wait for pacing/backoff;
- permanent errors, per-request timeout, whole-operation deadline, missing receipts
  and joined workers (synctest rejects surviving blocked goroutines);
- incorrect receipt identity/index/block, cumulative gas and aggregate size;
- configuration bounds and zero/default compatibility;
- the actual ExtractBlock use case never calling its atomic commit port after a
  failed concurrent batch; the fake checkpoint remains unchanged. This test is
  not PostgreSQL durability evidence.

Targeted commands (no full integration/race/E2E):

```sh
go test -count=1 \
  ./services/extractor-service/internal/adapters/outbound/jsonrpc \
  ./services/extractor-service/internal/config \
  ./services/extractor-service/internal/adapters/fixturecapture \
  ./internal/adapters/metrics ./test/architecture

go test -race -count=20 \
  -run 'TestConcurrentReceipt|TestReceiptConcurrencyBounds' \
  ./services/extractor-service/internal/adapters/outbound/jsonrpc

go build ./...
go vet ./...
git diff --check
```

The ordinary adapter package's existing live smoke remains explicitly opt-in and
was not executed. Full PostgreSQL, Compose fixture E2E, authentic Mainnet offline
ETL, complete race suite and throughput comparison are deferred to final Stage 14
validation. No live Ethereum provider was contacted and no throughput improvement
is claimed without measurements.

## Actual validation record

- PASS: targeted JSON-RPC, Extractor config, fixturecapture and metrics unit tests.
- PASS: targeted receipt-pool race tests, `-race -count=20`, after the accounting fix.
- PASS: architecture tests on a separate retry, `-count=1 -timeout=20s` (0.595s).
- PASS: `go build ./...`, `go vet ./...`, gofmt checks, `git diff --check`.
- NOT RUN: PostgreSQL integration, full Compose E2E, complete race suite, live
  extraction and performance benchmarks, intentionally deferred.

The initial new snapshot-size test incorrectly omitted JSON-RPC envelope overhead
from its response allowance; its setup was corrected. An initial race run then
exposed counted reservations that could be canceled before HTTP dispatch. Moving
accounting to the underlying RoundTrip boundary fixed this; the assertion still
requires matching counted/observed attempts in the concurrent budget test.

One architecture test process stalled and was interrupted after about 333 seconds;
it did not produce a diagnostic stack. The separate bounded retry passed. The
cause of that isolated stall is unresolved; it is not reported as a successful
first run. Go cache restrictions required approved retries outside the sandbox.
No live provider or database command was executed.

## Stage 14.3 file inventory

New:

- `services/extractor-service/internal/adapters/outbound/jsonrpc/receipts.go`
- `services/extractor-service/internal/adapters/outbound/jsonrpc/receipts_test.go`
- `docs/architecture/receipts-stage14.md`

Updated while preserving earlier changes:

- `services/extractor-service/internal/adapters/outbound/jsonrpc/client.go`
- `services/extractor-service/internal/config/indexing.go`
- `services/extractor-service/internal/config/indexing_test.go`
- `services/extractor-service/cmd/extractor/main.go`
- `internal/adapters/metrics/metrics.go`
- `deployments/compose/fixture.yaml`, `deployments/compose/mainnet.yaml`
- `.env.example`, `PLAN.md`, `README.md`
- `docs/architecture/performance-stage14.md`

No accepted ADR, migration, generated protobuf, original fixture or Loader
canonicality code was changed by Stage 14.3.
