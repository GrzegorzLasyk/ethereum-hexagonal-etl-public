# Stage 14.5 — Final configuration and release readiness

Implementation scope is frozen: Extractor, Transformer, Loader, Ethereum Mainnet,
PostgreSQL, gRPC, transactional Inbox/Outbox and Hexagonal Architecture. No KYT,
new service, broker, Kubernetes, canonicality feature or further implementation
stage is planned. The next phase is comprehensive validation and review.
**Implementation complete does not mean production validated.** Earlier execution
records describe earlier trees and do not certify the final Stage 14 tree.

## Configuration audit and conservative profiles

| Setting | Current default / rule | Operational interpretation |
|---|---|---|
| RPC pacing / timeout | 200ms global minimum / 10s per request | All receipt workers share pacing, cooldown and attempt accounting. Rate is a ceiling, not achieved throughput. |
| RPC retry | 3 attempts per call; 1s initial, 30s maximum backoff; Retry-After honored or deferred | A new indexing cycle can reacquire work; only the whole-run request budget caps all cycles together. |
| Receipts / block scheduling | Receipt concurrency 1 (1..8); sequential heights; batch size 1 (1..1000), 3s cycle poll, 5m cycle timeout, 5 consecutive errors | Batch size is sequential work per cycle, not parallel heights. Whole-run cancellation dominates cycle/request timeouts. |
| Controlled Mainnet acceptance | chain 1, start=end=1000000, <=100 actual attempts, <=2m acquisition, 30s drain, <=2m completion observation | Explicit private opt-in; no follow-head or automatic rerun. Drain shares acquisition deadline. RPC budget renews on process restart, not account-wide. |
| Inbox workers / dispatchers | 1 each, configurable 1..8 | Keep 1 before measurement. Higher Loader counts still serialize completion per chain. |
| Inbox processing | 30s operation, 1m lease, 5 attempts, 1s..1m backoff | Lease must exceed process timeout; retries exhaust into retained failed state. |
| Dispatch | 5s RPC, 30s operation, 1m lease, 20 attempts, 1s..1m backoff with jitter | Enforced lease > operation > RPC timeout; one RPC per durable attempt, no inner gRPC retry loop. |
| Durable capacity | Transformer 128 combined unfinished Inbox+Outbox; Loader 128 Inbox; range 1..10000 | Includes failed work. All instances must use the same schema-global limit. Lowering does not delete excess rows. |
| Extractor capacity | Explicit event/byte/database ceilings; isolated Mainnet defaults 100 / 64 MiB / 1 GiB | Pauses acquisition, not dispatch. Database ceiling includes retained history and requires operator disk planning. |
| gRPC payload / message | 64 MiB / 65 MiB maximum | Align both peers if lowered in standalone configuration; mismatches can permanently fail large events as oversize. |
| Incoming slots / reservations | 2 / 130 MiB; effective slots=min(request slots, bytes/message maximum) | Serialized reservation only, not heap/RSS. Headers/streams/connections also bounded. |
| gRPC connections | 16 (1..64) per receiver | Excess connections close; senders retry transiently. Slow trusted clients can occupy slots. |
| PostgreSQL pools | 12 (2..32) per service process | Default maximum 36 across services. Budget replicas, migration/diagnostic connections and PostgreSQL reserved slots; 32 per service is not a safe universal profile. |
| Loader batching | 64 statements (1..256), 1 MiB estimated coalescing bound | One complete-block transaction. Size 1 is the direct-write comparison. Large individual rows remain intact and are sent alone. |
| Compose | PostgreSQL 2 CPU/2g; Go services 1 CPU/2g each; fixture 0.5 CPU/256m | Limits are ceilings, not reservations. Docker Desktop needs headroom beyond these processes. |
| Go memory | Container GOMEMLIMIT=1536MiB | Soft GC target, not a hard allocation bound. Keep below the 2g cgroup ceiling; lowering only Docker memory can cause OOM. Host benchmark binaries are not cgroup-limited. |

Recommended starting profiles (not measured sizing or performance promises):

- **Local development:** existing fixture Compose defaults; receipt, worker and
  dispatcher counts 1, pools 12, incoming 2, capacities 128, Loader batch 64.
  Use the deterministic fixture endpoint. Keep the fixture runner's explicit
  short polling/backoff and larger fault-injection retry allowances confined to tests.
- **Offline comparison:** use the existing Stage 14.1 scenarios at 1/2/4/8 workers,
  identical datasets and polling (20ms), warmup/repetitions and unchanged resource
  settings. Compare Loader batches 1 versus 64 on the same build. Host processes
  do not inherit Compose CPU/memory limits; explicitly record GOMEMLIMIT, host
  memory/CPU and Docker resources. Synthetic ranges are not Mainnet evidence.
- **Controlled Mainnet:** use the one-block bounds above and conservative production
  defaults, not fixture retry settings. Start receipt concurrency at 1; higher
  settings need measurement and provider quota review. A budget/deadline failure
  requires diagnosis, not repeated invocations with newly replenished budgets.

### Concrete configuration correction

The private Mainnet parser previously rejected all Stage 14 resource keys while
filtering inherited shell overrides; Compose's apparent tuning options therefore
could not be used through the approved wrapper. It now explicitly accepts and
validates optional receipt concurrency, Loader batch size, workers/dispatchers,
pool sizes, receiver event/request/byte/connection limits, RPC interval and timeout.
Mainnet Compose now forwards RPC interval/timeout as well. Existing private files
remain valid; absent keys retain defaults. No file is rewritten automatically.

Optional literal lines in the existing **0600 private file** include:

```text
EXTRACTOR_RECEIPT_CONCURRENCY=1
EXTRACTOR_RPC_INTERVAL=200ms
EXTRACTOR_RPC_TIMEOUT=10s
LOADER_SQL_BATCH_SIZE=64
```

Allowed worker/pool/admission key names match the Stage 14.2 configuration table.
Payload/message and CPU/cgroup/Go memory limits remain fixed at reviewed Mainnet
Compose defaults through this wrapper; arbitrary shell overrides are still refused.
Use `go run ./tools/mainnet check PRIVATE_CONFIG` for non-RPC preflight. Native
service startup still validates its own configuration. No recommendation to set
all configurable maxima is implied.

## Backpressure, recovery and PostgreSQL review

Admission checks use owning-schema transaction locks, exact duplicate lookup before
capacity, and bounded partial-index probes. Transformer completion atomically
transfers one capacity slot from Inbox to Outbox. No downstream call occurs inside
that transaction. Loader releases capacity only on complete-block commit. Delivery
only reduces outstanding work; there is no cross-service database lock cycle.
Small pools can increase wait/deadline failures; they are not proof of a deadlock.

Temporary overload remains `Unavailable/RECEIVER_OVERLOADED`; oversize remains
permanent `ResourceExhausted`. Durable attempts/backoff are finite. If Loader is
down, pressure propagates through Transformer to Extractor and acquisition pauses.
Accepted duplicates remain durable; no payload is discarded to regain capacity.
Exhausted failures intentionally retain their slots and can stop progress until
an operator repairs the cause and grants an audited finite allowance. Restart and
upstream duplicate delivery do **not** reset those budgets. This is an operational
stop requiring repair, not evidence of data loss. Process-local reservations are
released after handlers finish, even on cancellation/errors.

Use the [Stage 13 recovery runbook](operations-stage13.md) for owning-queue repair,
original-ID replay, bounded historical reindex, restart/drain and deep-reorg stops.
Restore dependencies first, then repair **Loader Inbox → Transformer Outbox/Inbox
→ Extractor Outbox** as needed, verifying each queue before reopening acquisition.
Never delete a failed row or reset attempts directly. Preserve audited operation
IDs across ambiguous repair retries. No code change to these policies was justified
by this review; final fault-injection evidence is still required.

Loader batches keep the existing chain lock, fence, constraints, error mapping,
Inbox completion and one transaction per block. Earlier successful batches roll
back if a later batch fails. No indexes, claim batching, schemas or privileges
were changed. Claim/admission partial indexes remain; full-history metric aggregates
and immutable history still grow. Existing Stage 12 measurements are small synthetic
samples, not current Mainnet capacity evidence. Event limits bound outstanding
work, not total disk or decoded memory. Apply all owning migrations before starting
matching binaries; mixed-version/mixed-limit replicas are not supported upgrades.

## Final validation visibility

| Evidence | Existing source / limitation |
|---|---|
| Successful throughput and end-to-end latency | Stage 14.1 reports gated on all queues, counts and field-level integrity; checkpoint alone never passes |
| Operation duration, backlog and age | `/metrics` operation histograms and owning-schema snapshots; require database_snapshot_success=1 and freshness |
| RPC attempts/retries/budget | New fixed-label-free `etl_rpc_attempts_total`, `etl_rpc_retries_total`, `etl_rpc_budget_enabled`, finite `etl_rpc_budget_limit/remaining` |
| Admission and overload | Existing inflight/reserved/capacity/rejection/backpressure and durable overload-retry metrics |
| Loader batching / pool waits | Existing opt-in perfbaseline pgx query/batch tracers and pool sampling; ordinary runtime does not expose these probes |
| CPU, RSS, heap, allocations and GC | Existing opt-in baseline runtime probes; host processes differ from container limits |

RPC counters increment at the same actual HTTP attempt-reservation boundary as
budget enforcement; canceled-before-dispatch and refused-over-budget work do not
increment. Retries count subsequent HTTP attempts **within a logical call**, not
new indexing cycles. No method, URL, event ID, error detail or secret is a label.
Counters reset with the process and remain available during DB snapshot failure.
Unlimited local clients expose budget_enabled=0 and omit limit/remaining. A final
scrape may be missed when a finite process exits; retain observations and mark final
actual counts unmeasured if unavailable. Budget enforcement is not provider billing.
No new observability infrastructure was added.

## One final-validation entry point

The entry point is `bash scripts/validate-final.sh`. Every phase requires `--opt-in`;
there is **no all-mode** and no implicit live execution. Do not run these during
Stage 14.5 implementation. Missing Docker/artifacts, failed checks and timeouts are
failures, never silent skips. Commands retain owned volumes/configurations and
never delete original fixtures or unrelated storage.

1. **Offline correctness**, including storage/backup restore on disposable PostgreSQL:

   ```sh
   bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
   ```

   Reuses `validate-stage13.sh`: formatting, build, all unit tests (10m per package),
   targeted race checks, vet, protobuf, architecture, all three PostgreSQL runners,
   handoff, full fixture E2E plus storage-release, and authentic offline regression.
   Gates report PASS/FAIL separately; their private log directory is printed.
   Existing integration runners reject skips and use bounded test/Compose contexts.
   Unit/integration compilation and toolchain setup are separate overhead; there
   is no claim of a strict overall wall-clock SLA. Signals may leave retained test
   containers to inspect and stop explicitly; never prune as recovery.

   Review subtests for admission concurrency/dedup, concurrent receipts/shared
   limits, Loader late-batch rollback/replay, unknown canonicality/fork observations,
   checkpoint rewind, both durable handoffs, lost ACK/ambiguous commit, outages,
   restart and audited repair. Authentic fixtures verify 0/1/2/3 transaction types,
   failed receipts and logs; type 4 and contract creation remain uncovered.

2. **Offline measurements**, separately, on the same hardware/build/configuration:

   ```sh
   bash scripts/validate-final.sh performance --opt-in .mainnet/final-direct \
     --dataset all --fixtures .mainnet/fixtures/stage13-3b --blocks 100 \
     --workers 1,2,4,8 --warmup 1 --repeat 3 --loader-batch-size 1
   bash scripts/validate-final.sh performance --opt-in .mainnet/final-batched \
     --dataset all --fixtures .mainnet/fixtures/stage13-3b --blocks 100 \
     --workers 1,2,4,8 --warmup 1 --repeat 3 --loader-batch-size 64
   ```

   Output directories must not exist. Existing runner limits apply (2h overall,
   bounded repetitions, per-case/source/drain deadlines); split the matrix into
   explicit smaller scenarios if it cannot finish. Require each integrity report
   and completed.json, exclude warmup, and retain failures. No speedup threshold
   is invented; evaluate latency, writes, allocations, pool waits and WAL together.

3. **Controlled live acceptance**, manual user execution only, separate from CI:

   ```sh
   go run ./tools/mainnet check .mainnet/stage13.env
   go run ./tools/mainnet up .mainnet/stage13.env
   bash scripts/validate-final.sh live --opt-in .mainnet/stage13.env
   ```

   The existing private config must additionally opt in, select chain 1 and exact
   block1000000, <=100 attempts and <=2m acquisition; the verifier has <=2m timeout.
   No live capture/discovery/reindex is implied. Exit success requires completion
   verification after E2E, not merely an advanced checkpoint. Keep endpoint/keys
   only in the private file. Live failures stop this phase without automatic retry.

   Once acquisition has exited, manually verify restart without more Ethereum calls:

   ```sh
   go run ./tools/mainnet restart .mainnet/stage13.env
   go run ./tools/mainnet drain .mainnet/stage13.env
   go run ./tools/mainnet verify .mainnet/stage13.env
   go run ./tools/mainnet stop .mainnet/stage13.env
   ```

   Record this recovery drill separately. No failed state may be manufactured in
   retained Mainnet storage; fault injection belongs to disposable offline suites.

## Final release checklist

- [x] Implemented frozen ETL scope, bounded receipts/resources, batch writes,
  service-owned repair, offline fixtures, metrics and explicit validation tooling.
- [ ] Final exact-tree offline gates PASS; inspect failures/reruns and all expected
  subtest PASS markers, not just compilation or a wrapper exit code.
- [ ] Authentic fixture digests unchanged; record exact counts/coverage and missing cases.
- [ ] Compare successful baseline repetitions; record hardware, Go/PostgreSQL/Docker
  versions, source digest, configs, CPU/memory/pool/WAL and bottlenecks without extrapolation.
- [ ] Manual live E2E and no-RPC restart/drain/verify PASS; preserve redacted evidence.
- [ ] Review backup/restore, retention/disk headroom, migration compatibility,
  failed-message repair, timeout/lease and shutdown evidence.
- [ ] Review remaining limits: trusted plaintext local network; privileged Docker
  access; no HA/PITR/scheduled encrypted backup; provider trust; unknown canonicality;
  history grows indefinitely; no proven Mainnet capacity/SLO or finality.
- [ ] Review full Git diff/untracked files and secret exclusions. No private captures,
  endpoint URLs, environment files, backup archives or credentials in release artifacts.
- [ ] After explicit human authorization, prepare reviewed commits/PR on the intended
  branch; require hosted CI and branch protections, then validate the exact clean
  release revision. No commit/branch/PR/tag/push/publish was performed by this stage.
- [ ] A human owns release approval, artifact versions and deployment restrictions.
  A local implementation or test PASS is not approval for exposed production use.

The Stage 12 security/backup runbooks remain applicable with Stage 13 audited repair
and Stage 14 resource limits. No further feature development is part of this plan.

## Stage 14.5 implementation evidence (2026-10-30)

| Executed check | Result |
|---|---|
| `go build ./...` | PASS; approved retry after sandbox denied Go cache writes |
| `go vet ./...` | PASS; approved retry after the same cache restriction |
| `go test -count=1 -timeout=60s ./tools/mainnet ./internal/adapters/metrics ./test/validation` | PASS (packages from the combined targeted invocation) |
| `go test -count=1 -timeout=60s ./services/extractor-service/internal/adapters/outbound/jsonrpc` | PASS, 0.992s; approved retry after sandbox blocked loopback mock listeners |
| `go test -count=1 -timeout=60s ./test/architecture` | PASS, 0.776s |
| `go test -race -count=1 -timeout=60s -run 'TestRPCBudgetExposition\|TestRPCMetricsCountActualRetriesAndBudget' ./internal/adapters/metrics ./services/extractor-service/internal/adapters/outbound/jsonrpc` | PASS, both targeted tests; not a full race suite |
| `bash -n scripts/validate-final.sh scripts/validate-stage13.sh` | PASS |
| gofmt on changed Go files; `git diff --check` | PASS |
| Comprehensive offline, PostgreSQL, E2E, authentic pipeline regression, benchmark matrix, live RPC | NOT RUN, explicitly deferred |

The shell phase tests use fake executables and a fake legacy script: they establish
argument/opt-in/CI isolation and fail-fast behavior, not end-to-end orchestration
success. The Mainnet tests use fake Docker invocations and private temporary files.
No real configuration, fixture, volume or branch was changed by these tests.

Files added in this stage:

- `tools/mainnet/performance.go`, `performance_test.go`
- `internal/adapters/metrics/rpc.go`, `rpc_test.go`
- `services/extractor-service/internal/adapters/outbound/jsonrpc/metrics_test.go`
- `scripts/validate-final.sh`, `test/validation/final_test.go`
- `docs/architecture/release-stage14.md`

Existing files updated, with prior-stage changes retained:

- `tools/mainnet/main.go`, `deployments/compose/mainnet.yaml`
- `internal/adapters/metrics/metrics.go`
- `services/extractor-service/internal/adapters/outbound/jsonrpc/client.go`
- `scripts/validate-stage13.sh`, `PLAN.md`, `README.md`
- `docs/architecture/operations-stage12.md`, `operations-stage13.md`,
  `release-stage12.md`, `metrics-stage12.md`, `performance-stage14.md`
