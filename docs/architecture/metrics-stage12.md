# Stage 12.3: local Prometheus metrics

## Access and architecture

All three services expose `GET /metrics` on the existing loopback HTTP listener:
`EXTRACTOR_HEALTH_ADDRESS`, `TRANSFORMER_HEALTH_ADDRESS`, and
`LOADER_HEALTH_ADDRESS`. The Stage 11 fixture configures `127.0.0.1:8091`, `:8092`
and `:8093` inside its shared network namespace. An unset address continues to
disable the optional HTTP listener for standalone commands. `/live` and `/ready`
retain their existing behavior, deadlines and dependency checks.

For standalone services, set the corresponding variable before starting and use
`curl http://127.0.0.1:8091/metrics` (adjust the port). In the persistent fixture
environment from the [Stage 11 runbook](local-stage11.md):

```sh
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8091/metrics
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8092/metrics
dc_fixture exec -T fixture /app/probe --body http://127.0.0.1:8093/metrics
```

HTTP returns Prometheus [text exposition 0.0.4](https://prometheus.io/docs/instrumenting/exposition_formats/),
with HELP/TYPE declarations, cumulative histogram buckets and seconds as the time
unit. The small formatter uses Go's standard library. There is no global registry,
new module dependency, Prometheus/Grafana deployment, host port publication or
external monitoring service. The endpoint is local only; a remote scraper cannot
reach container loopback without explicitly arranged local access.

Each service owns a `StatusReader` port and its snapshot DTOs. Its PostgreSQL
adapter reads only its schema, authenticated with its existing runtime role.
The inbound metrics adapter maps these views to technical exposition types.
Workers record returned operation outcomes in an explicitly injected, process-local
registry; the core imports no HTTP, Prometheus or shared metric package. Loader
also decorates its persistence port to time the actual `Complete` call without
changing its arguments, errors, retry or transaction behavior. Extractor's existing
index result carries a valid head observation and its timestamp, so scrapes never
make RPC calls. Accepted ADRs and immutable historical records are unchanged.

## Database availability and freshness

Every scrape attempts one read-only, repeatable-read local database transaction,
with a one-second deadline. Queue counts, ages and checkpoint/revision are read in
the same transaction. No source payload or credential is selected. Each process
permits at most one concurrent metrics read; an overlapping scrape reports an
unsuccessful snapshot rather than starting another database read.

`etl_database_snapshot_success` is **1 only for this scrape's complete successful
read**, otherwise **0**. On any query error, timeout, cancellation or busy collector,
**all database-derived queue/checkpoint/revision/lag samples are omitted**. No
partial snapshots, cached database values or synthetic healthy zeros are emitted.
HTTP remains 200 so process counters and the explicit failure indicator remain
scrapable. Prometheus `up=1` therefore does not imply database availability.

`etl_database_snapshot_last_success_timestamp_seconds` is the last complete read's
Unix timestamp, or 0 before any success. It survives failed scrapes within the
process, but resets at restart. Check this and snapshot success when interpreting
operational state. A successful empty queue really has backlog/age zero. An absent
checkpoint is represented by initialized=0 and omitted height/revision/lag, not
an invented genesis checkpoint.

Head is the last successful observation made by normal indexing/reindexing, with
`etl_extractor_head_observed_timestamp_seconds`. It is absent before the first
observation and can become stale during RPC failures. Scraping never refreshes
head. Lag is emitted only with a successful database snapshot, an initialized
checkpoint and a known head; it compares those two differently timed observations.
A negative lag explicitly means the last observed head is below the current
checkpoint. Neither head nor lag proves canonicality or consensus finality.

Backpressure describes the last successful indexing cycle, with
`etl_extractor_cycle_timestamp_seconds` recording that cycle's return time. Failed
cycles do not overwrite it with a healthy-looking zero. It is absent before the
first successful cycle. Use timestamp freshness and cycle failure counters to
detect a stopped/stalled indexer. Dispatch-only mode has no head/cycle series.

## Metric reference

| Metric | Type / units | Meaning |
| --- | --- | --- |
| `etl_process_start_time_seconds` | Gauge / Unix seconds | Current process registry start |
| `etl_database_snapshot_success` | Gauge / 0 or 1 | Complete database read for this scrape |
| `etl_database_snapshot_last_success_timestamp_seconds` | Gauge / Unix seconds | Last complete database read, 0 before any |
| `etl_queue_events{queue,state}` | Gauge / events | Current durable rows; states `pending`, `in_flight`, `failed`, `done` |
| `etl_queue_backlog{queue}` | Gauge / events | Pending + in-flight + failed retained work |
| `etl_queue_oldest_pending_age_seconds{queue}` | Gauge / seconds | Age since first local creation/acceptance of oldest unfinished row |
| `etl_extractor_checkpoint_initialized` | Gauge / 0 or 1 | Durable checkpoint exists |
| `etl_extractor_checkpoint` | Gauge / block height | Last durable Extractor checkpoint |
| `etl_extractor_revision` | Gauge / revision | Durable rewind revision |
| `etl_extractor_head` | Gauge / block height | Last valid observed provider head |
| `etl_extractor_head_observed_timestamp_seconds` | Gauge / Unix seconds | Last head observation time |
| `etl_extractor_lag_blocks` | Gauge / blocks | Observed head minus durable checkpoint (not confirmation-adjusted) |
| `etl_extractor_backpressure` | Gauge / 0 or 1 | Last successful cycle paused for capacity |
| `etl_extractor_cycle_timestamp_seconds` | Gauge / Unix seconds | Last successful cycle returned |
| `etl_extractor_backpressure_total` | Counter / cycles | Cycles paused for capacity since restart |
| `etl_operations_total{operation,outcome}` | Counter / operations | Returned operations by local outcome |
| `etl_operation_retries_total{operation}` | Counter / retries | Attempts returning a durably scheduled retry |
| `etl_operation_failures_total{operation}` | Counter / operations | Retry, terminal failure, interruption or unknown result |
| `etl_operation_duration_seconds{operation}` | Histogram / seconds | Local duration, including unsuccessful operations |

Extractor exposes Outbox; Transformer exposes Inbox and Outbox; Loader exposes
Inbox. Inbox `done` means completed processing, Outbox `done` means acknowledged
and marked delivered. Pending includes delayed retries. In-flight includes leased
sending/processing. **Failed rows remain in backlog and oldest age**, because they
are retained and require investigation/replay. Ages use PostgreSQL transaction
time and first local `created_at` / `accepted_at`, not source-chain time, last retry,
or a moving eligibility timestamp. Clock skew into the future is clamped to zero.
Transformer's Outbox joins only its own transformation and Inbox tables to select
the configured chain. No metric aggregates another service's data.

Operation labels are a fixed allowlist:

- `process_inbox`: Transformer transformation or Loader processing, including claim,
  decode, validation and durable completion/failure update. Idle polls are excluded.
- `dispatch_outbox`: one claimed delivery, including RPC and local state update.
- `claim_inbox`, `claim_outbox`: claim failures without a returned event; these do
  not inflate transformation/delivery attempts.
- `persist_block`: Loader's actual completion/persistence port call, excluding
  claim and decode. Outcomes distinguish success, interruption and commit ambiguity.
- `index_cycle`, `reindex_cycle`: each scheduler cycle, including idle waiting,
  head verification, failures and reorg search/recovery.
- `index_block`, `reindex_block`: individually confirmed block commits, even when
  a later block fails in the same batch. Failure counts belong to the cycle.

Outcomes are `completed`, `delivered`, `retry`, `failed`, `interrupted`, `unknown`,
`waiting`, `backpressure`, `done`, `rewound`, `progress`. Unused registered
operation/outcome combinations start at zero. Labels never contain event IDs,
source IDs, block hashes/heights, addresses, chain IDs, provider names, errors or
DSNs. Histogram buckets are 0.001, 0.01, 0.1, 0.5, 1, 5, 30, 120 and +Inf seconds;
`_count` and `_sum` follow standard histogram semantics. No dynamic buckets/labels.

## Restarts, failures and interpretation

Counters/histograms reset on service restart. They count observed returned calls,
not unique events or historical database totals. Retries and idempotent duplicate
operations may increase them again. A crash before observation can lose a process
counter update; they are not a durable audit. Unknown commit outcomes count as
unsuccessful observations, never as proven rollback. Shutdown cancellation may
increase interruption counters. Logs from Stage 12.2 retain detailed identity
correlation; metrics intentionally do not.

Durable queue gauges and checkpoint/revision are reconstructed on every successful
scrape and survive service restarts. Loader `etl_queue_events{queue="inbox",state="done"}`
is its current durable completed-work count, while process completion counters
cover only this process lifetime. Database restart may temporarily remove gauges;
subsequent successful scrapes restore them without resetting process counters.

Use `rate(etl_operation_retries_total[5m])` for retry frequency and
`rate(etl_operation_duration_seconds_sum[5m]) / rate(etl_operation_duration_seconds_count[5m])`
for average operation duration. Inspect `etl_database_snapshot_success == 0`
separately from `up == 0`, and alert on stale head/cycle timestamps as appropriate.
These are example expressions, not deployed alert rules or a monitoring stack.

Counts scan retained status rows (including completed history); no indexes,
retention, privileges or migrations are changed in 12.3. Scrape cost grows with
retained data; the timeout can cause explicit unavailable snapshots at scale.
Consider a modest scrape interval, e.g. 15 seconds. Index/retention work belongs
to later stages. Prometheus samples are float64, so integers above 2^53 cannot be
represented exactly. Current Ethereum heights are well below that bound.

## Verification

Unit tests cover bounded labels, concurrent collection, histogram buckets,
restart counters, deadlines, unavailable snapshots without healthy zeros, health
route preservation, log-level-independent observations and persistence ambiguity.
Existing PostgreSQL suites include runtime-role metrics reads, empty queues,
aged/failed/in-flight work, chain isolation and checkpoint rewind revision.
The fixture E2E checks actual endpoints through outages/recovery, retries,
service restart, final durable counts, Loader persistence and simulated reorg.
All tests use local deterministic fixtures, with no real Infura access.

## Stage 14.5 RPC budget visibility

Acquisition processes now expose `etl_rpc_attempts_total`, `etl_rpc_retries_total`
and `etl_rpc_budget_enabled`. Finite clients also expose `etl_rpc_budget_limit`
and `etl_rpc_budget_remaining`. These fixed series contain no labels/identities,
URLs or error text. They increment at the HTTP attempt-reservation boundary,
including failed dispatches; cancellation before reservation or exhausted budgets
add no attempt. Retries mean subsequent attempts within one RPC call, not worker
cycles. Values reset on process restart and remain available on database scrape
failure. A disabled budget omits limit/remaining instead of reporting healthy zero
headroom. Capture observations while finite processes run; do not invent a final
count if shutdown precedes scraping. See [final evidence requirements](release-stage14.md).
