# Stage 14.1 — Offline performance baseline

Status: instrumentation and opt-in runner implemented. This is measurement tooling,
not a concurrency optimization or a production capacity claim. Stage 14.2 resource limits are documented [separately](backpressure-stage14.md);
Stage 14.3 receipt acquisition is documented [separately](receipts-stage14.md);
Stage 14.4 Loader batching is documented [separately](loader-batching-stage14.md);
Stage 14.5 final configuration and validation are documented [separately](release-stage14.md).
See the execution record below for checks actually run.

## Execution and isolation

Run from the repository root with the Go version in `go.mod`, Docker Desktop or
Docker Engine/Compose, and the existing PostgreSQL image available. Go service
processes run on the **host**, PostgreSQL in the existing Compose configuration.
This reuses the Stage 9 handoff runner; it does not measure containerized Go CPU
limits. Native Go and the multiarchitecture PostgreSQL image support Apple Silicon.

Short infrastructure smoke (output directory must be new; its parent must exist):

```sh
go run ./test/integration/cmd/handoff-postgres --baseline --opt-in \
  --dataset synthetic --blocks 3 --workers 1 --repeat 1 --warmup 0 \
  --out .mainnet/baseline-smoke
```

Baseline matrix for subsequent comparison:

```sh
go run ./test/integration/cmd/handoff-postgres --baseline --opt-in \
  --dataset all --fixtures .mainnet/fixtures/stage13-3b \
  --blocks 100 --workers 1,2,4,8 --repeat 3 --warmup 1 \
  --out .mainnet/baseline-stage14-matrix
```

`--dataset authentic` selects only authentic inputs; `synthetic` needs no private
files. Authentic inputs must ALL pass the existing manifest verifier before any
benchmark database starts. Missing files, invalid hashes, missing Docker,
nonempty databases, failed events and incomplete processing fail explicitly.
No capture or live Ethereum command is invoked. JSON-RPC production decoding runs
through an in-memory transport with no socket or external-network fallback.

Each dataset/worker/repetition uses a new random `ethereum-stage14-*` project,
loopback database port, private generated credentials, own-schema runtime roles,
and the existing service migrations. Only this newly created project's volumes
are removed by the existing runner's cleanup. Existing Mainnet/E2E projects,
volumes, configuration and fixtures are untouched. `ETL_TEST_RETAIN=1` instead
stops and retains the newly created project and prints its private configuration
path; use this deliberately because retained networks consume Docker address pools.
Never prune unrelated projects to make the benchmark run.

Reports and binaries are retained in the requested directory (0700; JSON files
0600). Existing output directories/files cannot be overwritten. `.mainnet/` is
Git-ignored. Configuration contains local paths, not provider credentials. Probe
reports contain no SQL statements, parameters, DSNs, raw payloads or secrets.
CI/GITHUB_ACTIONS are refused; normal unit tests and CI do not run benchmarks.
Build tags `perfbaseline` and `postgresintegration` enable the harness; ordinary
service builds use a no-op instrumentation adapter.

## Datasets and workers

| Dataset | Blocks / transactions / logs | Intended use |
|---|---|---|
| authentic-mainnet 1000000 | 1 / 2 / 1 | Historical root receipts, legacy transactions |
| authentic-mainnet 12965000 | 1 / 259 / 648 | Types 0/1, 32 failed receipts, log-heavy block |
| authentic-mainnet 22431084 | 1 / 95 / 233 | Types 0/2/3, 3 failed receipts |
| synthetic-stage11 | N / 2N / N | Contiguous heights 100..100+N-1, deterministic bounded workload |

Authentic files retain their original raw bytes, provenance and SHA-256. Each
noncontiguous authentic height has a **separate checkpoint and database**. These
fixtures contain neither verified type 4 transactions nor contract creations.
Synthetic data is explicitly labelled in every scenario/report; it is not an
Ethereum acquisition and does not reproduce realistic large-block complexity.
N is bounded to 1..1000. No authentic block is cloned, renumbered or synthesized.

For each selected worker count (unique values from 1,2,4,8):

- Extractor acquisition stays **sequential (1)** through production ExtractBlock;
  its independent Outbox dispatcher has W workers.
- Transformer has W Inbox workers and W Outbox dispatchers.
- Loader has W Inbox workers; its existing per-chain completion lock still applies.
- Current production pool sizing, admission limits and message limits remain in force. Poll periods
  are explicitly 20ms for all these workers; defaults and retry policies otherwise
  remain unchanged. Source pool max is 5. After Stage 14.2 service pools default to 12 and receiver
admission limits also apply; record this configuration change when comparing runs. All configurations are comparable only
  with the same polling, build, hardware, fixture and observer settings.

More workers are not presumed faster. A one-block run cannot demonstrate parallel
block throughput. Use synthetic multi-block runs for contention comparisons.
The complete production indexing/head/reorg scheduler is not timed; sequential
acquisition uses its underlying existing use case. Recovery semantics are unchanged.

## Measurements and integrity gate

The timed window starts immediately before the first production extraction call
and ends when the source has finished and all four durable queues are observed in
their successful terminal state. Compilation, startup, migrations, readiness and
final correctness/duplicate checks are outside that window. Source report writing
and drain observation contribute to wall time. Failed/timed-out cases have
`status=failed` and null throughput; a checkpoint alone is never sufficient.

Before reporting success, the runner verifies exact raw block/receipt bytes and
provenance in Extractor and Transformer, active membership, ordered final checkpoint
and revision, stable source/result event IDs, both complete Outbox/Inbox handoffs,
unchanged normalized handoff payload, field-by-field Loader rows against the source,
counts, `canonical_status=unknown`, and retained raw/history rows. It repeats the
last extraction and both gRPC submissions to check source/inbox deduplication.
This baseline does not inject reorgs or measure fault recovery; Stage 10–13 tests
remain the evidence for those paths.

| Evidence | Units / interpretation |
|---|---|
| Verified throughput | blocks/s and transactions/s over successful full drain window |
| Per-block completion latency | seconds from extraction start to first observed Loader completion; 100ms polling plus query-time uncertainty |
| Extraction commit latency | seconds around production ExtractBlock, including in-memory RPC decoding and local transaction |
| Operation latency | p50/p95 seconds from existing structured log duration_ms; millisecond quantization, whole service lifetime including duplicate checks |
| gRPC | forwarded call-to-durable-ACK seconds, attempts/errors, protobuf message bytes including metadata; extra observing proxy hop; not TCP bytes |
| Runtime probes | per-process CPU user/system seconds, lifetime high-water RSS bytes, sampled heap peak bytes, allocated bytes/count, GC cycles/pause seconds |
| PostgreSQL tracing | client-observed query and transaction seconds; SQL verb groups only, error count, successful INSERT/UPDATE/DELETE command count including CTE writes |
| PostgreSQL snapshots | own-schema pg_stat_user_tables inserts/updates/deletes (eventually reported), database size bytes, cluster WAL insertion delta bytes |
| Pool probes | configured max, 200ms sampled acquired/total peaks, acquire count/duration, empty-pool wait count/duration, cancellations |
| Prometheus snapshots | existing queue backlog/oldest pending age, outcomes/retries and checkpoint observations; every >=250ms plus beginning/end |

Pool/runtime/SQL probes cover startup through graceful shutdown, including observers
and duplicate checks, **not solely the throughput window**. Source fixture lookup
preparation precedes its probe. CPU is CPU time, not a machine-wide utilization
percentage. PostgreSQL backend CPU/RSS is not sampled. RSS is a process lifetime
high-water mark; heap/pool peaks are sampled and may miss brief spikes. PostgreSQL
write stats may lag; do not interpret a zero delta as proof of zero writes. WAL is
cluster-wide (including checkpoints/observer activity) on the isolated instance;
it is not a precise per-service attribution. Query times include client/network
cost and are not EXPLAIN server timings. Existing Stage 12 EXPLAIN tooling remains
available for subsequent measured SQL investigations.

A failed database metrics snapshot aborts the run rather than supplying healthy
zeros. Metrics/tracing/proxies impose overhead. Tiny runs are dominated by polling,
startup scheduling and observer work. Exact nearest-rank p50/p95 are reported only
when all samples fit the bounded 100000-sample storage per distribution; otherwise
quantiles are null while count/sum remain available. A single sample has no useful
tail distribution. Do not average percentile values across runs.

## Repeatability and report format

Each sample starts fresh processes and a fresh database. `--warmup 1` creates one
separate excluded run per dataset/worker configuration; this warms host/tool/image
caches, **not the next database's cache**. `--repeat 3` records three independent
samples (range 1..10; warmup 0..2). Per-case test timeout is 6m, source timeout 4m,
drain observation 5m; the full runner has a 2h bound. Timeout means failure, not
partial throughput. Larger matrices may require several explicit bounded runs.

`environment.json` records host OS/architecture, Go version, CPU count, macOS CPU
model/RAM when available, Docker Engine resources/version, Git revision/dirty flag
and a digest of nonignored working-tree sources. PostgreSQL version/shared_buffers/
work_mem and per-service configuration are recorded alongside each sample. Record
power mode, competing workload, Docker Desktop resource settings, disk type/free
space and thermal conditions manually in the comparison notes; they are not inferred.

Versioned `stage14.1/v1` reports are defined in `test/performance/report.go`:

- `scenario.json`: exact dataset, worker count, start, repeat/warmup and local paths.
- `source.json`: extraction event identities, timestamps, durations and source counts.
- `*-probe.json`: process/runtime/pool/SQL evidence, no SQL text.
- `report.json`: status, scenario, throughput, distributions, Prometheus time series,
  PostgreSQL before/after/WAL, process probes, integrity gates and limitations.
- `completed.json`: written only when every requested sample and cleanup succeeded.

A case report can pass while a later case or cleanup fails; require both case reports
and `completed.json`. Warmup reports stay available but must be excluded from any
comparison. Do not compare race-instrumented binaries with this baseline.

Use this comparison record for Stage 14.2–14.5 (leave unmeasured values null):

```json
{
  "baseline_report_version": "stage14.1/v1",
  "baseline_directory": "local output directory",
  "candidate_directory": null,
  "hardware_and_configuration_equal": null,
  "dataset": "synthetic-stage11",
  "workers": 1,
  "successful_measured_runs": 0,
  "median_blocks_per_second": null,
  "median_transactions_per_second": null,
  "per_run_p50_p95_seconds": [],
  "cpu_rss_allocations_gc_pool_wal_comparison": null,
  "integrity_all_passed": null,
  "interpretation_and_limitations": "No results entered yet"
}
```

Report individual runs plus median/range across successful repetitions; retain
failed samples separately. No optimization is justified by a smoke run alone.

## Execution record

2026-10-29, macOS host: ordinary build/vet and targeted report/probe/runner
checks passed, including architecture boundaries and deterministic in-memory source
decoding. All three original authentic artifacts passed the offline manifest verifier;
their SHA-256 digests remained unchanged. The tagged PostgreSQL harness compiled without running database
checks. A short synthetic run (`3` blocks, `1` worker, no warmup, one repetition)
was attempted. The sandbox first denied the loopback port; an authorized retry
then failed because Docker Desktop was not running:

```
dial unix /Users/<username>/.docker/run/docker.sock: connect: no such file or directory
```

Startup and cleanup both failed to connect to Docker. No new database was started,
no benchmark throughput was produced, and no existing volume was touched. Local
attempt directories `.mainnet/baseline-stage14-smoke-01` and `-02` retain build/
scenario metadata, without a successful `completed.json`. Do not interpret those
as baseline results. PostgreSQL assertions, authentic pipeline measurements and
the 1/2/4/8 matrix remain **NOT RUN** until Docker is available. Full regression,
race suite and E2E were deliberately not run under the task's validation strategy.

## Stage 14.1 file inventory

Modified (earlier Stage 13 changes in these files are preserved):

- `PLAN.md`, `README.md`
- `services/extractor-service/cmd/extractor/main.go`
- `services/transformer-service/cmd/transformer/main.go`
- `services/loader-service/cmd/loader/main.go`
- `test/integration/cmd/handoff-postgres/main.go`

New:

- `docs/architecture/performance-stage14.md`
- `internal/adapters/baseline/disabled.go`
- `internal/adapters/baseline/enabled.go`
- `internal/adapters/baseline/enabled_test.go`
- `internal/adapters/baseline/stats.go`
- `internal/adapters/baseline/stats_test.go`
- `test/performance/report.go`
- `test/performance/report_test.go`
- `test/integration/cmd/handoff-postgres/baseline.go`
- `test/integration/cmd/handoff-postgres/baseline_test.go`
- `test/integration/handoff/performance_test.go`
- `test/integration/handoff/performance_proxy_test.go`
- `test/integration/handoff/performance_integrity_test.go`
- `services/extractor-service/internal/adapters/outbound/postgres/performance_seed_test.go`

Other dirty/untracked Stage 13 files were present before this task and are not
Stage 14.1 modifications. Generated local attempt metadata/binaries are retained
under the ignored `.mainnet/baseline-stage14-smoke-01` and `-02` directories.

## Stage 14.4 comparison support

`--loader-batch-size` defaults to 64 and accepts 1..256; 1 uses direct INSERTs.
Each scenario records the setting. The build-tagged probe includes batch calls,
queued statement counts, errors and whole-batch latency in addition to direct
query calls. SQL write counts include batch callbacks, including successful
statements later rolled back; always require final integrity/commit evidence.
Per-statement latency is unavailable for pipelined writes, and submission counts
are not exact wire round trips. See [the comparison procedure](loader-batching-stage14.md).
No Stage 14.4 benchmark results have been recorded.
