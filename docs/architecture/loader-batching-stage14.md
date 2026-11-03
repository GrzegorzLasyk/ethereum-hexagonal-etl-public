# Stage 14.4 — Bounded PostgreSQL writes in Loader

Status: implementation complete. [Phase 1](validation-offline-stage14.md) records
PostgreSQL and authentic fixture validation. [Phase 2](validation-performance-stage14.md)
compares batches 1/64/256 at one worker and batches 1/64 at workers 1/2/4/8.
Measured results support retaining batch64 for the educational workload; the full
batch256 worker matrix remains incomplete. No migration, contract, canonicality
or application policy changes.

## Algorithm and configuration

`LOADER_SQL_BATCH_SIZE` defaults to **64 statements**, accepts **1..256**, and is
validated at startup. The fixture and isolated Mainnet Compose files pass it to
Loader. `1` uses direct `tx.Exec` for every record, providing an unbatched reference
on the same build. Existing callers of `postgres.New` use 64; the composition root
uses `NewWithBatchSize` to supply explicit tuning. No worker or pool limits change.

The PostgreSQL adapter queues the existing parameterized INSERT statements in a
`pgx.Batch`, in this order: block, transaction, its receipt, its logs, next
transaction. It flushes at the configured statement count, before exceeding a
fixed **1 MiB estimated coalescing budget**, and at the end of the block. SQL text
and parameter mappings are unchanged. Empty blocks still insert their block row;
transactions without logs still insert their receipts. There is no ORM, COPY,
expanded multi-row SQL, dynamic table name or additional goroutine.

The estimate includes SQL/framing allowance, twice string/byte lengths and array
framing. It is a conservative batching heuristic for these mappings, **not a hard
heap or wire-buffer limit**. An individual row exceeding the budget first flushes
previous work and is sent alone by `tx.Exec`. It remains intact; batching never
truncates or rejects a valid large source row. Existing 64 MiB payload admission,
worker and container limits remain relevant. The full decoded document, per-row
JSON encoding, pgx encoding buffers and PostgreSQL transaction state still consume
memory independently of the batch queue.

PostgreSQL's 65535 bind-parameter limit applies to each statement, not to the whole
pipeline. The four fixed INSERTs use 28/25/18/12 parameters; no SQL expansion occurs.
The batching helper additionally rejects more than 65535 parameters in one statement
and fails closed on unsupported internal parameter types. At maximum configuration,
there are at most 256 queued statements (at most 7168 parameter slots for the current
mappings), subject to the byte estimate. Values are retained by reference until
results are consumed; no source values are mutated.

## Atomicity and failure behavior

All batches use the **same existing `pgx.Tx`**. There are no per-batch commits:

1. Validate the envelope/document, begin READ COMMITTED with synchronous commit.
2. Acquire the existing per-chain transaction advisory lock and lock/fence the
   Inbox claim. A completed matching attempt returns idempotently without inserts.
3. Insert all records through bounded batches. Consume results and always close
   each batch before further SQL or rollback; retain the first SQL error.
4. Only after the final successful flush, update ADR-0003 observations and perform
   the existing lease/attempt-fenced Inbox completion update.
5. Commit once. Any SQL/close/context/lease failure returns through the existing
   deferred rollback, including when earlier batches have executed successfully.

The existing error mapper remains unchanged: unique violations (`23505`) are
conflicts, canceled/deadline operations retain their context errors, and other SQL
failures are sanitized as unavailable. Existing ambiguous-commit handling, retry
limits and failure states apply. A batch error never implies durable partial success.
Database errors and parameter values are not logged or added to probe reports.

Full block identity, constraints and foreign keys are unchanged. ADR-0003's
per-chain lock still serializes observation updates. Immutable prior observations
remain intact; every Loader candidate remains `canonical_status=unknown`. Extractor
checkpoints and both service-owned handoffs are untouched.

## Expected reduction and measurement

For the 1167 normalized INSERTs needed by block 12965000, the statement count stays
**1167**. With 64 statements per batch, the count-bound expectation is **19 batch
submissions instead of 1167 synchronous Exec calls**. The byte bound or an oversized
row can increase submissions. Preparation/description/cache behavior can add wire
exchanges; a batch submission is not an exact TCP packet or network-round-trip count.
Transaction control, locks, observations and Inbox completion queries are additional
and unchanged. This is an algorithmic expectation, **not measured throughput**.

The Stage 14.1 `perfbaseline` probe now implements `pgx.BatchTracer` as well as its
existing query tracer. Secret-free report additions are:

- `direct_query_calls`, `batch_calls`, `queued_batch_statements`, `batch_errors`;
- `client_batch_latency`: complete SendBatch-through-Close seconds with p50/p95;
- existing successful SQL-write/error counters now include batch query callbacks.

Direct query latency excludes batch queries; batch statement latencies are not
fabricated from pipelined result timing. Successful SQL command counts do not prove
commit: a later rollback can undo those writes. Whole-transaction latency and final
integrity gates remain essential. Probe scope still includes startup/observation.
Ordinary builds do not enable baseline instrumentation.

After separately authorizing final performance validation, run from the repository
root with Docker available. These commands are **offline**; they verify original
artifacts before use and create isolated disposable databases and new report paths:

```sh
go run ./test/integration/cmd/handoff-postgres --baseline --opt-in \
  --dataset authentic --fixtures .mainnet/fixtures/stage13-3b \
  --workers 1 --warmup 1 --repeat 3 --loader-batch-size 1 \
  --out .mainnet/baseline-stage14-4-direct

go run ./test/integration/cmd/handoff-postgres --baseline --opt-in \
  --dataset authentic --fixtures .mainnet/fixtures/stage13-3b \
  --workers 1 --warmup 1 --repeat 3 --loader-batch-size 64 \
  --out .mainnet/baseline-stage14-4-batched
```

The runner records `loader_sql_batch_size` in each scenario/report and explicitly
passes it to the Loader process; inherited `LOADER_*` variables are still filtered.
Use the same build, hardware, polling and resources; compare individual successful
repetitions and exclude warmup. Alternate execution order when investigating cache
bias. To test larger deterministic synthetic ranges, use `--dataset synthetic
--blocks 100` instead, clearly retaining the synthetic label. Neither mode changes
captured source files. See [Stage 14.1 methodology](performance-stage14.md).

## Verification and limitations

Targeted unit tests exercise empty blocks, 259 transactions with 648 synthetic logs,
transactions without logs, 1/2/64/256-statement boundaries, byte splitting, oversized
single rows, parameter bounds, SQL argument equality/order, cancellation, SQL and
close errors, and Complete's rollback path after an earlier successful batch.
The synthetic counts reproduce workload size, not authentic Mainnet field values.

The existing Loader PostgreSQL runner now includes a prepared batch subtest:
all normalized PostgreSQL field values (excluding per-run `loaded_at`) are compared
between direct writes and batch sizes 2/64/256 for empty and large synthetic blocks;
replay is checked, canonicality must stay unknown, and an injected unique violation
at log 647 must roll back every normalized row while preserving the claimed Inbox.
Removing the trigger permits retry of the same claim. Existing tests continue to
cover competing fork observations, lease fencing, every persistence boundary and
ambiguous commit under the new default. The authentic Stage 13.3C/14.1 full-field
oracle remains available without becoming an ordinary unit-test dependency.

These PostgreSQL assertions are **prepared, not executed in Stage 14.4**. Tests were
compiled with the integration tags and `-run '^$'`, which is compilation evidence
only. Full PostgreSQL, E2E, authentic replay and benchmark runs are deferred; no
throughput, WAL, memory or production-capacity improvement has been measured.
Batches still execute individual INSERTs and update the same indexes/WAL. Per-chain
serialization and whole-document memory remain potential bottlenecks.

## Stage 14.4 file inventory

New:

- `services/loader-service/internal/adapters/outbound/postgres/batch.go`
- `services/loader-service/internal/adapters/outbound/postgres/batch_test.go`
- `services/loader-service/internal/adapters/outbound/postgres/batch_integration_test.go`
- `services/loader-service/internal/config/batch_test.go`
- `docs/architecture/loader-batching-stage14.md`

Updated (preserving prior-stage changes):

- Loader PostgreSQL `complete.go`, `records.go`, `store.go`, `integration_test.go`
- Loader `internal/config/config.go` and `cmd/loader/main.go`
- `internal/adapters/baseline/enabled.go`, `enabled_test.go`
- `test/integration/cmd/handoff-postgres/baseline.go`, `baseline_test.go`
- `test/integration/handoff/performance_test.go`, `test/performance/report.go`
- `deployments/compose/fixture.yaml`, `deployments/compose/mainnet.yaml`, `.env.example`
- `PLAN.md`, `README.md`, `docs/architecture/performance-stage14.md`

## Validation record — 2026-10-30

- PASS: `go build ./...` (exit 0; sandbox emitted a module-cache write warning).
- PASS: `go vet ./...` (exit 0).
- PASS: `go test -count=1 -timeout 60s -tags perfbaseline ./services/loader-service/internal/adapters/outbound/postgres ./services/loader-service/internal/config ./internal/adapters/baseline ./test/performance ./test/integration/cmd/handoff-postgres ./test/architecture` (all six packages).
- PASS: `go test -race -count=1 -tags perfbaseline ./internal/adapters/baseline`
  (only the concurrent measurement counters, not a full race suite).
- PASS, compilation only: `go test -tags postgresintegration,perfbaseline -run '^$' ./services/loader-service/internal/adapters/outbound/postgres ./test/integration/handoff`
  (both packages report `[no tests to run]`; no PostgreSQL connections).
- PASS: `gofmt -l` on all Stage 14.4 Go files produced no output; `git diff --check`.
- NOT RUN by task scope: PostgreSQL execution, Compose E2E, authentic Mainnet offline
  regression, benchmarks, full regression and full race suite. No live RPC calls.

Earlier attempts to run some targeted Go checks were blocked by sandbox cache
permissions; authorized retries passed. No database performance result is inferred
from those compilation or unit checks. Stage 14.4 is ready for review and subsequent
Stage 14.5 planning, with final database and performance validation still outstanding.
