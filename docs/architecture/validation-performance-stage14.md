# Final validation — Phase 2 performance execution record

## Final consolidation — 2026-11-03

**PASS for the agreed educational/default-profile performance scope; sufficient
to proceed to separately authorized Phase 3.** This is not a PASS for the entire
original twelve-configuration matrix or a production-capacity certification.
The user explicitly accepted the existing measurements as sufficient to assess
batch64 and requested no automatic completion of the remaining batch256 cases.
No benchmark, regression or live RPC was run during this consolidation.

Docker Engine 29.9.2 is available again (12 CPUs, 8321810432 bytes daemon memory).
The initial sandbox denial was resolved by a read-only check outside the sandbox.
No Docker settings or resources were modified. No application changes were made.
The earlier partial/BLOCKED record is retained below as historical evidence.

### Completed evidence

| Loader batch | Complete worker configurations | Status |
|---|---|---|
| 1 | 1, 2, 4, 8 | PASS |
| 64 | 1, 2, 4, 8 | PASS |
| 256 | 1 | PASS |
| 256 | 2 | Interrupted warmup: FAIL; excluded from performance results |
| 256 | 4, 8 | NOT RUN; no longer required for the agreed default-profile decision |

Nine complete configurations contain **108 measured runs and 36 excluded warmups**
(four datasets, three measurements plus one warmup per configuration). All 144
reports have status passed and all seven integrity flags true: exact raw bytes,
ordered checkpoint, durable handoffs, normalized fields, unknown canonicality,
deduplication and preserved history. Each directory also has completed.json with
zero live RPC requests. The seven resumed successful configuration logs each
contain 16 TestPerformanceBaseline PASS markers and no FAIL marker.

The original completed directories remain `phase2-final-b1-w1-20261102` and
`phase2-final-b1-w2-20261102`. Additional complete directories under `.mainnet/`
are `phase2-resume-b1-w4-20261102`, `phase2-resume-b1-w8-20261102`,
`phase2-resume-b64-w{1,2,4,8}-20261102`, and `phase2-resume-b256-w1-20261102`.
Original reports, fixtures, summaries and retained Docker resources are unchanged.

Source-tree fingerprints differ because documentation changed between sessions,
but SHA-256 comparison confirms **identical binaries across all nine groups** for
Extractor, Transformer, Loader, source.test and pipeline.test. Fixture SHA-256
values still match the earlier verification. Host/Go/PostgreSQL configuration
and dataset definitions are recorded in the historical section below. Resumed
preflight observed 319 GiB host disk free, no active containers, no swap in use and
no reported thermal throttling; these are snapshots, not continuous resource proof.

Read-only consolidation created `.mainnet/phase2-consolidated-20261102/runs.csv`,
`groups.json` and `details.json`, using `.mainnet/phase2-analyze-resume.py`. They
contain all 108 per-run measurements, including p50/p95, CPU/RSS, allocations/GC,
pool waits, WAL, queue age/backlog, gRPC and SQL tracing. The earlier
`phase2-summary-20261102` and all raw evidence were preserved.

### SQLSTATE 57P01: infrastructure interruption

The failing project is `ethereum-stage14-066dcc852b66fae57da00e57`; only its
existing PostgreSQL container state and logs were inspected. No database restart
or query was needed. The evidence is:

1. At **2026-11-02 22:08:38.087 UTC**, PostgreSQL PID 1 logged
   `received fast shutdown request`, then aborted active transactions and
   terminated numerous connections with the administrator-command message.
2. It completed its shutdown checkpoint and logged `database system is shut down`
   at **22:08:38.421 UTC**. Container FinishedAt is 22:08:38.428357835Z,
   ExitCode=0, OOMKilled=false, Error empty. Health probes had succeeded immediately
   before shutdown; the current stopped-container health label is not evidence
   that readiness failed during acquisition.
3. The test failed at performance_test.go:282, in the final assertBaselineData
   integrity verification, with SQLSTATE 57P01. Its report is failed with no
   accepted throughput; an advanced checkpoint or earlier drain is not a PASS.
4. Only **after the test failure**, the runner attempted cleanup. That command
   could not connect because the Docker daemon socket had disappeared. Therefore
   this shutdown was not the runner's successful normal post-test cleanup.

**Classification: confirmed PostgreSQL shutdown / infrastructure interruption,
not demonstrated application or batching failure.** The missing daemon socket
also supports interruption of the Docker environment. The logs do not identify
who initiated the shutdown or why Docker became unavailable. There is no container
OOM evidence; broader host pressure cannot be ruled out from OOMKilled alone.
No arbitrary root cause, application patch or repeat benchmark is justified.

This incident is distinct from the older `baseline metric snapshot unavailable`
failure. That older failure did not recur in the seven successful resumed groups;
its precise cause remains unresolved. Historical failed reports remain retained.

Read-only diagnostic commands executed:

```sh
docker info --format '{{.ServerVersion}} CPUs={{.NCPU}} Memory={{.MemTotal}}'
docker inspect --format '{{json .State}}' ethereum-stage14-066dcc852b66fae57da00e57-postgres-1
docker logs --timestamps --tail 100 ethereum-stage14-066dcc852b66fae57da00e57-postgres-1
python3 .mainnet/phase2-analyze-resume.py
```

The state projection deliberately excludes container environment/credentials.
No live endpoints, private configuration contents or fixture payloads are in this
report. Additional local checks verified report counts/integrity, binary hashes,
fixture hashes, PASS/FAIL log markers and `git diff --check`.

### Throughput comparison

Values are medians of three successful measured runs. Synthetic data has 100 blocks,
200 transactions, 200 receipts and 100 logs. Authentic fixtures are one block per
isolated database: 1000000 (2/2/1), 12965000 (259/259/648), 22431084 (95/95/233).
Synthetic block throughput must not be extrapolated to historical Mainnet ranges.

| Batch | Workers | Synthetic blocks/s | 1000000 tx/s | 12965000 tx/s | 22431084 tx/s |
|---|---:|---:|---:|---:|---:|
| 1 | 1 | 19.719 | 5.862 | 71.692 | 47.959 |
| 1 | 2 | 23.810 | 4.938 | 60.695 | 59.500 |
| 1 | 4 | 25.175 | 5.261 | 61.834 | 46.201 |
| 1 | 8 | 18.586 | 4.439 | 52.761 | 41.865 |
| 64 | 1 | 19.728 | 5.437 | 147.670 | 107.511 |
| 64 | 2 | 23.619 | 5.667 | 120.193 | 86.213 |
| 64 | 4 | 20.786 | 4.118 | 103.622 | 66.970 |
| 64 | 8 | 17.440 | 4.232 | 79.389 | 68.478 |
| 256 | 1 | 18.481 | 5.237 | 102.154 | 56.695 |

For block 12965000 at W1, median full-drain duration changed from 3.613 s (batch1)
to 1.754 s (batch64) and 2.535 s (batch256). Observed per-run block-latency p95
ranges were 3.560–3.729 s, 1.718–1.750 s and 1.961–3.030 s respectively.
With one block per run p50=p95; these are not robust population tail percentiles.
For synthetic batch64, W1 p95 ranges 1.756–1.823 s versus W2 0.366–0.537 s.
Per-run quantiles are retained, never averaged into a claimed pooled percentile.

### Resource and database observations

Median probes for block 12965000 at one worker:

| Batch | CPU seconds | Max individual RSS MiB | Allocations | Allocated MiB | GC cycles | Pool wait seconds | WAL bytes | Batch calls / queued statements |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1.533 | 36.91 | 692415 | 121.43 | 27 | 0.158 | 2712904 | 0 / 0 |
| 64 | 1.072 | 37.73 | 670880 | 122.30 | 26 | 0.181 | 2712904 | 19 / 1167 |
| 256 | 1.231 | 39.07 | 674972 | 122.72 | 27 | 0.190 | 2712904 | 5 / 1167 |

Batch1's 0/0 denotes direct Exec, not zero writes. All three modes persist the same
1167 normalized rows/statements. Batch64 records 19 batches and batch256 5; neither
number equals SQL statement count or measured network round trips. The write
counter includes other queue statements, including zero-row updates and statements
that can later roll back. Cluster WAL is not attributable exactly to one service.

CPU is summed user+system time across four Go processes over their probe lifetime,
which includes more than the timed throughput window. RSS is the largest individual
process high-water mark, not simultaneous total RAM or PostgreSQL memory. GC pause
medians above are 3.40 / 3.60 / 4.72 ms for batch1/64/256. PostgreSQL statistics are
eventually reported; query/batch timings are client-side observations.

For synthetic batch64, increasing workers 1/2/4/8 produced CPU medians
3.035/3.984/5.559/9.141 s, allocated memory 65.88/71.76/87.90/122.33 MiB and
pool-wait medians 0.165/0.362/0.608/5.583 s. Sampled Loader acquired-connection
peaks were at most 3/4/6/9 of 12. This shows extra contention/cost, not proof of
which lock or subsystem caused the slowdown. There is no evidence justifying a
larger pool. Four canceled pool acquisitions occur across the complete reports;
probe scope includes shutdown, so their cause cannot be attributed from totals.

Synthetic batch64 observed maximum Extractor Outbox backlogs at W1/W2/W4/W8 were
38/3/3/2; Loader Inbox maxima 2/2/5/6. These are sampled peaks, not hard maxima.
All complete runs passed the durable-drain gate; both gRPC hops recorded zero
errors in the 108 measurements. Exact payload bytes, ACK distributions and oldest
pending ages remain in the consolidated reports. Observation polling (100 ms),
metrics scrapes and the extra gRPC observer affect measured times.

### Practical decision and remaining limits

- **Keep LOADER_SQL_BATCH_SIZE=64.** At W1 it measured about 2.06x and 2.24x the
  direct-write throughput on the two larger authentic blocks. Batch256 was slower
  than batch64 and used more RSS on 12965000. No universal speedup is claimed:
  synthetic W1 throughput is effectively unchanged and the tiny block is noisy.
- **Start Inbox workers and Outbox dispatchers at 1 each.** This is the best measured
  configuration for both larger authentic blocks with batch64. W2 is an optional
  measured alternative for many tiny synthetic blocks; W4/W8 are not general
  recommendations. Keep sequential height acquisition and receipt concurrency 1;
  receipt-concurrency performance was not exercised by this runner.
- **Keep current resource limits**, including pools 12 per service, incoming slots 2,
  durable capacity 128 and the existing Compose/GOMEMLIMIT profile. The results do
  not justify raising them. Host benchmark binaries do not validate service cgroup
  sizing or peak memory for maximum-size messages. Keep Mainnet pacing/budgets and
  explicit finite range unchanged; they were not tuned by these offline runs.
- Batch256/W2 remains an interrupted failed warmup; W4/W8 were not run. These are
  explicit gaps in the original matrix, not silently passed checks. No additional
  repetitions are needed for the agreed educational/default-batch64 decision.
- The shared host, nonrandomized configuration order, session interruption and
  three repetitions limit causal claims, particularly batch64 versus batch256.
  Long-duration disk growth, maximum-size blocks, container resource pressure,
  provider/network latency, receipt concurrency and sustained Mainnet capacity
  remain unmeasured. Type 4 and contract creation remain absent from authentic data.
- Phase 1 remains prior offline-correctness evidence; this session did not rerun it.
  Hosted CI and an exact clean release revision are still unvalidated. Phase 3 must
  be explicitly authorized and bounded; no live invocation was made here.

**Recommendation: proceed to Phase 3 with existing conservative defaults, without
additional performance optimization.** The confirmed infrastructure interruption
is retained as a failed attempt; accepting the documented reduced performance scope
does not erase that failure or claim all twelve configurations passed.

## Historical partial record — 2026-11-02

The following section preserves the earlier blocked assessment and its commands.
Its remaining-work list and blocked status are superseded by the decision above;
do not automatically execute that historical resume loop.


Status on resumption, 2026-11-02: **PARTIAL / BLOCKED**, not a completed performance
acceptance. Two of twelve worker/batch configurations completed before interruption.
The resumed Docker preflight failed: the `desktop-linux` daemon socket
`~/.docker/run/docker.sock` does not exist. No remaining benchmark was started.
Do not repeat the completed configurations or discard failed attempts.

## Evidence and environment

Retained local evidence is Git-ignored under `.mainnet/`. The two complete groups
are `phase2-final-b1-w1-20261102` and `phase2-final-b1-w2-20261102`.
Each has a successful `completed.json` and 16 successful reports: four datasets,
one excluded warmup and three measured repetitions per dataset. On resumption all
32 reports were inspected; all seven integrity flags are true. The comparison
below uses **24 measured runs**, not the eight warmups. No new benchmark results
were produced during this resumption.

Both environments record HEAD `c35b0bb09f597660f3483514725fcdab3789b3ea`, a dirty
working tree and the same source digest:
`382f2cbd5b8f2ea4ef1190780af99470515a65d9a233b12e2d2afefdc743cc1c`.
The benchmark observation fix described below was included. Reports represent
that source snapshot, not a future clean release commit.

- Intel i7-9750H, 12 logical CPUs, 16 GiB host RAM; macOS amd64; Go 1.27.3.
- Docker Engine 29.9.2 / Docker Desktop; recorded daemon capacity 12 CPUs and
  approximately 7.75 GiB memory. PostgreSQL 17.11; Compose ceiling 2 CPUs / 2 GiB.
- Host Go processes, without race instrumentation or container CPU/memory limits;
  GOMEMLIMIT/GOMAXPROCS were unset. Pools default to 12, source pool to 5,
  receiver admission to 2; workers poll every 20 ms. Acquisition stays sequential.
- Earlier host diagnostics showed swapping and variable load. They do not prove
  the cause of any individual slow sample. On resumption host disk had 319 GiB
  available. Docker/VM memory and disk headroom cannot currently be rechecked.
- Each sample uses a fresh isolated PostgreSQL project and fresh processes.
  The final-validation wrapper sets `ETL_TEST_RETAIN=1`; cleanup stops only its
  generated project and retains its volumes/configuration. No pruning was run.
- RPC replay uses an in-memory transport without a network fallback. No live
  RPC calls or receipt-concurrency comparison were performed.

## Datasets and verified completion

| Dataset | Blocks | Transactions / receipts / logs | Verified checkpoint |
|---|---:|---:|---:|
| Synthetic Stage 11, heights 100–199 | 100 | 200 / 200 / 100 | 199 |
| Authentic Mainnet 1000000 | 1 | 2 / 2 / 1 | 1000000 |
| Authentic Mainnet 12965000 | 1 | 259 / 259 / 648 | 12965000 |
| Authentic Mainnet 22431084 | 1 | 95 / 95 / 233 | 22431084 |

Authentic heights have separate databases/checkpoints. The existing capture
verifier passed again for all three artifacts on resumption; their SHA-256 hashes
are unchanged. Coverage remains types 0/1/2/3, failed receipts and logs; type 4
and contract creation are absent. Reports verify exact raw bytes, normalized
fields, both durable handoffs, deduplication, ordered checkpoint, retained history
and `canonical_status=unknown`. No incomplete case contributes throughput.

## Completed comparisons: Loader batch size 1 only

Each row represents three measured repetitions. Throughput, CPU, RSS and WAL are
medians. Latency columns are **ranges of individual-run p50/p95**, not pooled
percentiles. For a single-block dataset p50 equals p95 and is not tail evidence.
CPU is summed user+system seconds across the four measured Go processes; RSS is
the largest individual-process lifetime high-water mark, not simultaneous total
memory. CPU/runtime probe windows are broader than the E2E throughput window.

| Dataset | Workers | Blocks/s | Tx/s | E2E p50 range, s | E2E p95 range, s | CPU, s | RSS, MiB | WAL, bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Synthetic 100 blocks | 1 | 19.719 | 39.438 | 1.146–1.449 | 1.698–1.928 | 2.986 | 19.78 | 2439688 |
| Synthetic 100 blocks | 2 | 23.810 | 47.620 | 0.391–0.482 | 0.517–1.088 | 3.823 | 20.50 | 2430016 |
| Mainnet 1000000 | 1 | 2.931 | 5.862 | 0.279–0.351 | 0.279–0.351 | 0.306 | 15.82 | 30672 |
| Mainnet 1000000 | 2 | 2.469 | 4.938 | 0.286–0.380 | 0.286–0.380 | 0.516 | 17.78 | 30672 |
| Mainnet 12965000 | 1 | 0.277 | 71.692 | 3.560–3.729 | 3.560–3.729 | 1.533 | 36.91 | 2712904 |
| Mainnet 12965000 | 2 | 0.234 | 60.695 | 3.577–4.494 | 3.577–4.494 | 2.284 | 39.07 | 2712960 |
| Mainnet 22431084 | 1 | 0.505 | 47.959 | 1.556–2.103 | 1.556–2.103 | 0.910 | 26.30 | 979552 |
| Mainnet 22431084 | 2 | 0.626 | 59.500 | 1.539–1.873 | 1.539–1.873 | 1.099 | 26.42 | 979552 |

Additional median measurements (allocations/GC/pool waits summed across Go
processes, SQL writes from Loader tracing over its lifetime):

| Dataset | W | Allocations | Allocated MiB | GC cycles | Pool wait, s | Loader SQL writes |
|---|---:|---:|---:|---:|---:|---:|
| Synthetic | 1 | 667458 | 64.37 | 41 | 0.155 | 1352 |
| Synthetic | 2 | 729478 | 71.31 | 52 | 0.281 | 1778 |
| Mainnet 1000000 | 1 | 33261 | 4.65 | 0 | 0.136 | 91 |
| Mainnet 1000000 | 2 | 47367 | 6.07 | 1 | 0.266 | 203 |
| Mainnet 12965000 | 1 | 692415 | 121.43 | 27 | 0.158 | 1366 |
| Mainnet 12965000 | 2 | 754522 | 126.67 | 29 | 0.238 | 1766 |
| Mainnet 22431084 | 1 | 285573 | 51.61 | 20 | 0.208 | 575 |
| Mainnet 22431084 | 2 | 312340 | 53.95 | 19 | 0.267 | 771 |

SQL writes include queue polling/claim statements, even zero-row updates; these
counts are not normalized row counts or network round trips. WAL is an isolated
cluster delta, not a service-specific attribution. PostgreSQL row statistics are
eventually reported. All completed comparison runs use direct writes (zero batch
calls); **there is no measured batching benefit yet**.

For synthetic W1 versus W2, observed peak Extractor Outbox backlog ranges were
36–38 versus 3; oldest pending age 1.406–1.565 versus 0.085–0.163 seconds.
Loader Inbox peaks were 5–6 versus 8–10, ages 0.173–0.315 versus 0.281–0.720 seconds.
These are sampled peaks, not guaranteed maxima. Loader sampled acquired pool
peaks stayed at or below 4/12 in all comparison runs. This is no proof that pool
waits never constrained progress. GC pause medians range from zero to 6.1 ms.

Both gRPC hops recorded zero errors: 100 calls each for synthetic and one each
for authentic runs. Synthetic aggregate message bytes were 489616 on the raw hop
and 508137–508150 on the transformed hop. Block 12965000 used 1077956 / 1074538
bytes; its per-run gRPC p95 ranges were 72–115.8 ms / 69.8–134 ms across W1/W2.
These are protobuf bytes and proxy-observed durable-ACK latency, not TCP traffic.
Loader transaction p95 across synthetic runs ranged 36–67 ms; these samples mix
transaction purposes and must not be read as the completed-block commit latency.

Local derived files `.mainnet/phase2-summary-20261102/runs.csv`, `groups.json` and
`details.json` retain per-run quantiles, extraction/operation/SQL distributions,
all process probes, queue samples, pool waits and integrity evidence. They were
regenerated solely from the two complete groups; original reports were untouched.

## Failed and diagnostic attempts retained

- `phase2-smoke-20261102`: synthetic 3 blocks, W1, batch64, no warmup, one run;
  PASS, 0.393926 s, 7.616 blocks/s. Too small for a batching comparison.
- `phase2-matrix-b1-20261102`: four passing reports and one failed W2 warmup.
  Prior execution diagnosed `invalid completion latency`: the completed-ID query
  could see fewer commits than subsequent queue-count queries. The existing
  correction in `test/integration/handoff/performance_test.go` also requires all
  completion timestamps before exiting the observation loop. It changes no
  production behavior or integrity assertion.
- `phase2-w2-check-20261102`: targeted post-fix synthetic100 W2/batch1 PASS,
  4.054539 s, 24.664 blocks/s.
- `phase2-matrix-b1-20261102-v2`: eight passing reports, then failed W4 warmup
  (`baseline metric snapshot unavailable`, recorded by the prior execution).
  W2 successful durations varied from 2.858 to 29.481 seconds. Root cause of the
  snapshot failure is unresolved; host/resource variance is only a hypothesis.
- `phase2-w4-diagnostic-20261102`: unchanged configuration, synthetic100 W4/batch1
  PASS, 3.986224 s, 25.086 blocks/s. A single diagnostic run does not replace the
  full four-dataset W4 group or establish that the transient failure is fixed.
- Final b1/W4 and b1/W8 controller records have exit 1; neither has a report.
  Final b64/W1 has environment/scenario/binaries only, no terminal controller
  record or report. Their precise startup failure cannot be reconstructed:
  earlier `/tmp/phase2-final-*.log` files are no longer present. Today's missing
  Docker socket is proven; attributing every earlier failure to it is not.

Failed reports retain null throughput. Preliminary partial-group results are
excluded from the comparison table but remain evidence, including slow successes.
No production defect or reason for speculative optimization was established.

## Commands and remaining work

Completed comparison commands before interruption (W=1 and W=2):

```sh
bash scripts/validate-final.sh performance --opt-in .mainnet/phase2-final-b1-w1-20261102 --dataset all --fixtures .mainnet/fixtures/stage13-3b --blocks 100 --workers 1 --warmup 1 --repeat 3 --loader-batch-size 1
bash scripts/validate-final.sh performance --opt-in .mainnet/phase2-final-b1-w2-20261102 --dataset all --fixtures .mainnet/fixtures/stage13-3b --blocks 100 --workers 2 --warmup 1 --repeat 3 --loader-batch-size 1
```

Resume only after Docker is available and retained-project disk/network capacity
has been inspected. Ten groups remain: **120 measured runs plus 40 warmups**.
The following commands use new paths and preserve existing incomplete directories;
run serially, stop on failure and record each completed group before proceeding:

```sh
for pair in 1:4 1:8 64:1 64:2 64:4 64:8 256:1 256:2 256:4 256:8; do
  batch=${pair%:*}
  workers=${pair#*:}
  bash scripts/validate-final.sh performance --opt-in \
    ".mainnet/phase2-resume-b${batch}-w${workers}-20261102" \
    --dataset all --fixtures .mainnet/fixtures/stage13-3b --blocks 100 \
    --workers "$workers" --warmup 1 --repeat 3 --loader-batch-size "$batch" || break
done
```

Do not rerun this loop blindly after another interruption: remove completed pairs
from the invocation list and use new output names for failed groups. Existing
runner bounds remain: two hours per invocation, six-minute test timeout,
four-minute source and five-minute drain bounds. No performance phase invokes a
live provider. Receipt-concurrency performance is NOT RUN: this runner does not
expose that dimension; no new harness was added.

Checks actually executed during this resumption:

- PASS: `go vet -tags postgresintegration,perfbaseline ./test/integration/handoff`.
- PASS: `gofmt -l test/integration/handoff/performance_test.go` (no output),
  `git diff --check`, all 32 completed-group report/integrity checks.
- PASS: `go run ./services/extractor-service/cmd/fixture-capture verify
  test/fixtures/mainnet/v1/manifest.json CASE ARTIFACT` for historical-smoke,
  london-access-list and pectra-blob-dynamic-fee; SHA-256 comparison unchanged.
- BLOCKED: `docker info --format '{{.ServerVersion}} CPUs={{.NCPU}} Memory={{.MemTotal}}'`
  outside the sandbox, missing daemon socket. No benchmark process was found.
- NOT RUN: remaining benchmark groups, full offline regression, live Mainnet,
  receipt-concurrency comparison or hosted CI.

## Interpretation and release decision

Within the two complete configurations, W2 has the higher synthetic median
(about 21% over W1), at higher CPU/allocations; W1 is faster for blocks 1000000
and 12965000, W2 for 22431084. Three repetitions on a shared host and the earlier
large variance do not establish a universal best configuration. Single-block
authentic runs cannot measure parallel block throughput. The current default
batch64 was not measured at comparable workload/repetition settings.

The successful runs demonstrate bounded educational ETL processing; they do not
justify additional optimization or a production capacity/SLO claim. Preserve
conservative settings while completing measurement. **Phase 2 is not complete;
Phase 3 is prepared operationally but has not passed the performance acceptance
gate.** Live execution remains a separate explicitly authorized action. CI and
the exact clean release revision still require validation.
