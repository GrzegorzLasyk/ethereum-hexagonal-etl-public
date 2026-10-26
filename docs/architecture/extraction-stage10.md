# Ordered extraction, recovery and operator commands (Stage 10)

Current operations, storage preservation, failed-work limitations and release
checks are consolidated in the [Stage 12.5 runbook](operations-stage12.md).
Earlier exclusions below describe the original stage's scope, not missing current
health, metrics, dispatch or local deployment.

Stage 10 implements sequential inclusive ranges, follow-head, required confirmation
depth, bounded reorg recovery, backpressure, replay and restartable reindex.
[ADR 0004](../adr/0004-ordered-extraction-reorg-recovery.md) is accepted and amends
ADR 0002. Loader remains an observation store with `canonical_status=unknown`
under ADR 0003. Extractor membership is provider-relative, not Ethereum consensus
finality and not a canonicality instruction to downstream services.

## Boundaries and persistence

The application owns `Index`, `Reindex`, the replay use case and consumer-oriented
ports. Domain owns confirmation eligibility, capacity rules, identities and
errors. The inbound `indexer` adapter only schedules bounded cycles, logs progress
and controls retry/shutdown. JSON-RPC, PostgreSQL and gRPC stay in outbound
adapters; `cmd/extractor` composes them explicitly. No shared service domain,
broker, cross-schema access or new external dependency is introduced.

Apply `migrations/extractor/0004_active_branch.sql` using the existing migrator.
Stop old Extractor binaries before migrating; they do not maintain active branch
membership. Existing migrations remain immutable. Migration 0004 backfills the
old branch and pins its original starting height without changing source bytes,
provenance, event IDs or delivery state. The runtime gets no raw-data UPDATE or
DELETE privileges and cannot change the stored starting height.

| Extractor table | Meaning |
| --- | --- |
| `raw_blocks` | Every immutable observation keyed by chain, number and hash; multiple hashes at a height are allowed. |
| `outbox_events` | Original stable events and their mutable delivery state; historical forks remain eligible for delivery. |
| `active_blocks` | One provider-relative active hash per indexed height. |
| `extraction_checkpoints` | Last active durable identity, fixed starting height, monotonic rewind revision. |
| `rewinds` | Immutable audit keyed by chain and previous revision, including old tip and ancestor. |
| `reindex_jobs` | Operation ID, fixed inclusive range and separate last durable identity. |
| `replays` | Operation ID, exact event ID, additional retry allowance and invalidated claim token. |

Normal extraction commits source + Outbox intent + active membership + checkpoint
in one local transaction, under the existing per-chain advisory lock. Reindex and
rewind use the same lock and `synchronous_commit=on`. Retained exact identities
reuse original bytes/provenance/events. Historical reactivation must compare the
expected checkpoint and revision; only a no-op duplicate of an already active
identity may succeed with a stale expectation.

A rewind removes only active membership above the retained ancestor, moves the
checkpoint, increments its revision and writes the audit atomically. Previously
fetched work with an old revision cannot advance a replacement branch, including
when the same old tip reappears (an ABA race). Repeating an identical rewind finds
the durable audit and does not rewind again after later progress. An ambiguous
COMMIT remains an error; retrying the same operation or restarting and reading
current durable state resolves it. No cross-service transaction is involved.

## Configuration

`EXTRACTOR_MODE` selects `dispatch` (default, existing behavior), `index`, `reindex`
or `replay`. All modes use the existing private `EXTRACTOR_DATABASE_URL`, canonical
`EXTRACTOR_CHAIN_ID` and dispatch configuration. Credentials are process environment
only; the executable does not load `.env`. Development source remains explicitly
configured Infura Ethereum Mainnet, chain `1`, per ADR 0002.

For `index` and `reindex`:

| Variable | Default / requirement |
| --- | --- |
| `EXTRACTOR_START_BLOCK` | Required uint64 decimal; must equal the persisted original start for live indexing. |
| `EXTRACTOR_END_BLOCK` | Optional inclusive uint64 end; absent means follow-head. Required for reindex. |
| `EXTRACTOR_CONFIRMATION_DEPTH` | Required uint64 descendants; **no implied safe Mainnet default**. |
| `EXTRACTOR_RECOVERY_DEPTH` | Required, 1–10000; maximum distance from old tip to common ancestor. |
| `EXTRACTOR_MAX_OUTSTANDING_EVENTS` | Required positive int64 count of non-delivered local events. |
| `EXTRACTOR_MAX_OUTSTANDING_BYTES` | Required positive int64 aggregate raw payload bytes for those events. |
| `EXTRACTOR_MAX_DATABASE_BYTES` | Required positive int64 database admission watermark. |
| `EXTRACTOR_RPC_URL` | Required credential-bearing HTTPS endpoint; loopback HTTP allowed for controlled tests. |
| `EXTRACTOR_RPC_PROVIDER` | Required non-secret label, e.g. `infura-mainnet`. |
| `EXTRACTOR_BATCH_SIZE` | `1`; 1–1000 complete sequential blocks per cycle. |
| `EXTRACTOR_POLL_INTERVAL` | `3s`; wait between cycles, also while idle or backpressured. |
| `EXTRACTOR_CYCLE_TIMEOUT` | `5m`; bounds all I/O in a cycle, including recovery. |
| `EXTRACTOR_MAX_CYCLE_ERRORS` | `5`; 1–1000 consecutive failed cycles before exiting. |
| `EXTRACTOR_RPC_TIMEOUT` | `10s` per RPC attempt. |
| `EXTRACTOR_RPC_INTERVAL` | `200ms` between request starts on this client. |

Durations accept 1 ms–24 h. RPC uses three attempts, exponential backoff with
jitter from 1 s to 30 s, existing HTTP/RPC error classification and Retry-After
handling. An unconsumed positive Retry-After at retry exhaustion or above the
backoff bound halts indexing with the requested wait, so outer polling cannot
bypass the provider cooldown. Restart later after investigating the limit.
Response budget is min(16 MiB, configured payload limit); aggregate raw
snapshot budget equals the configured payload limit (maximum 64 MiB). Existing
gRPC message limits still apply. Configuration is validated before workers start.

For `reindex`, additionally set `EXTRACTOR_OPERATION_ID` (1–128 ASCII letters,
digits, dots, underscores or hyphens). For `replay`, set that ID,
`EXTRACTOR_REPLAY_EVENT_ID` and optionally `EXTRACTOR_REPLAY_ATTEMPTS` (default 20,
1–1000 additional attempts). Replay requires no RPC configuration.

## Ordered ranges, head following and confirmation policy

No checkpoint means the first block is exactly START_BLOCK. Restart reads the
last durable identity and resumes at its child; changing START_BLOCK for an
existing live chain fails instead of silently changing its retained anchor.
END_BLOCK is inclusive. A range is already complete if its end is behind the
verified checkpoint. A future end waits for sufficient confirmations. Follow-head
keeps polling after catching up. At uint64 maximum, indexing finishes without
wrapping to genesis.

For observed head H and required descendants D, eligibility is N <= H-D. H<D
means no eligible block; D=0 explicitly includes the current head. Shallow and
pending observations are not persisted or marked final. Previously accepted
observations are retained even if confirmations later decrease. A complete block
is fetched with full transactions and separate receipts/logs; an empty block is
valid, but a null block, missing receipt or partial snapshot never advances progress.

Each cycle verifies the durable checkpoint against the provider, including while
waiting at head or under pressure. Acquisition rechecks both the fetched header
and the sampled head anchor before committing. Header reads use
`eth_getBlockByNumber("latest", false)` or a numbered tag, bracketed by chain-ID
checks, with existing byte limits, cancellation, pacing and retry behavior.
These calls still return transaction hashes and are not zero-cost header APIs.

## Reorg recovery and failure behavior

A changed checkpoint hash triggers a backward walk through provider headers and
retained active headers. Both parent chains must remain contiguous. A shared
identity is accepted as the ancestor only within the configured depth and the
original retained range. The application rechecks the ancestor, sampled tip and
head before requesting a conditional transaction; database revision comparison
fences concurrent changes. Replacement blocks are then fetched sequentially and
subject to the same confirmations and admission limits.

| Condition | Behavior |
| --- | --- |
| Shallow fork with proven retained ancestor | Atomic audited rewind, then sequential replacement extraction. |
| No ancestor within depth or before original start | Halt with `ErrRecoveryRequired`; preserve all data and cursor. |
| Provider head below checkpoint | Halt; never interpret this as permission to rewind. |
| Missing/discontinuous retained ancestry | Halt for investigation. |
| Missing provider block, timeout, rate limit | No rewind; bounded RPC/cycle retries, then visible failure. |
| Moving/inconsistent sampled branch | Reject that cycle; retry within the consecutive-error budget. |
| Stale expected checkpoint/revision | No write; subsequent cycle rereads progress. |
| Commit response lost | Keep error explicit; reread durable cursor/audit or retry identical operation. |

RPC calls cannot provide an atomic multi-block view. Parent links and rechecks
reduce inconsistent-provider observations but are not cryptographic verification
or protection against a dishonest provider. A branch may change immediately after
a successful check or commit; the next cycle revalidates it. Deep recovery and a
fork below the retained start require investigation and a separately reviewed
repair strategy. There is deliberately no force-rewind, implicit deletion or
operator command that silently abandons the live lineage.

## Backpressure and lifecycle

The scheduler keeps one acquired block at a time, with a bounded number of blocks
per cycle. Before fetching and inside the serialized insert transaction, it
checks local Outbox count/bytes and `pg_database_size(current_database())`.
All non-delivered states count, including failed, leased, replayed and historical
branch events. Capacity becoming unavailable during acquisition prevents the
commit and leaves progress unchanged. A payload that cannot fit stays blocked;
inspect its size and raise an explicitly chosen budget if appropriate.

Count/raw-payload admission is serialized among same-chain acquisition/reindex
writers. Align their limits. Replay can requeue a retained event and thereby
raise the backlog; it does not allocate another source snapshot. Database size
is an admission watermark, **not a hard disk quota**: other services, indexes,
TOAST, audit records, WAL, database maintenance and existing transactions can grow
storage independently. The size query does not read another service's tables.
Outbox bytes measure raw JSON only, not transport/metadata/process memory.

Dispatch continues while acquisition waits. Failed events remain counted until
explicitly repaired/replayed and acknowledged; restarting does not reset their
budget. Retention is unlimited in this stage, so delivery does not reclaim raw
storage. No cleanup/retention automation is introduced.

SIGINT/SIGTERM cancel cycles and join dispatch workers before pool/client closure.
An interrupted transaction rolls back or has an explicitly unknown outcome;
acknowledged work remains durable. Range completion or an indexing halt ends this
process's dispatcher too. Run `dispatch` separately when it must drain remaining
intents independently of acquisition. A completed range means extraction intent
is durable, not that downstream processing or loading has completed.

## Operator commands

First stop old Extractor binaries and apply the owning migration with the existing
Compose `migrate-extractor` service. Keep private database/RPC URLs in the process
environment. The following values are examples of operator choices, not a claim
that 12 confirmations establish finality or that these capacities suit production:

```sh
export EXTRACTOR_CHAIN_ID=1
export EXTRACTOR_MODE=index
export EXTRACTOR_START_BLOCK=20000000
export EXTRACTOR_END_BLOCK=20000010
export EXTRACTOR_CONFIRMATION_DEPTH=12
export EXTRACTOR_RECOVERY_DEPTH=128
export EXTRACTOR_MAX_OUTSTANDING_EVENTS=100
export EXTRACTOR_MAX_OUTSTANDING_BYTES=536870912
export EXTRACTOR_MAX_DATABASE_BYTES=8589934592
export EXTRACTOR_RPC_PROVIDER=infura-mainnet
# EXTRACTOR_DATABASE_URL and EXTRACTOR_RPC_URL must already be set privately.
go run ./services/extractor-service/cmd/extractor

# Follow head with the same persisted START_BLOCK:
unset EXTRACTOR_END_BLOCK
go run ./services/extractor-service/cmd/extractor

# Independently drain retained events:
EXTRACTOR_MODE=dispatch go run ./services/extractor-service/cmd/extractor
```

For reindex, set a bounded historical range and a stable job ID. Reusing this ID
with the same range resumes after a crash; different range arguments are rejected.
Each observation plus any new Outbox event and the job's cursor commit together.
The live cursor/active branch is unchanged. Existing identities keep the first
snapshot and do not get a new delivery intent; competing hashes create distinct
stable events. If the provider changes the already-observed part of the job,
stop and investigate, then start a new job ID for a new observation pass.

```sh
EXTRACTOR_MODE=reindex EXTRACTOR_OPERATION_ID=history-pass-1 \
EXTRACTOR_START_BLOCK=20000000 EXTRACTOR_END_BLOCK=20000010 \
go run ./services/extractor-service/cmd/extractor
```

Replay is a bounded command selecting **one exact Extractor event**. Inspect the
failure and resolve configuration/receiver problems first. Supply the retained
`event_id` as `EXTRACTOR_REPLAY_EVENT_ID`, then:

```sh
EXTRACTOR_MODE=replay EXTRACTOR_OPERATION_ID=delivery-repair-1 \
EXTRACTOR_REPLAY_ATTEMPTS=20 go run ./services/extractor-service/cmd/extractor
EXTRACTOR_MODE=dispatch go run ./services/extractor-service/cmd/extractor
```

The same operation ID is safe to repeat after a lost reply; it does not grant
another allowance. A deliberately new operation ID grants another bounded
allowance. Monotonic attempts fence any former sender; integer exhaustion fails
without resetting the token. Source bytes, original provenance and event identity
remain unchanged. Successful replay command means locally requeued, not delivered.

Accepted downstream Inbox entries still deduplicate. Replay does not retransform
completed input, reset failed downstream Inbox processing or repair a failed
Transformer Outbox. Those service-owned repairs remain separate operations under
their existing contracts. Changed transformation semantics or destructive rebuilds
need their own versioning/rebuild decision.

Read-only inspection, authenticated only as `extractor_runtime`:

```sql
SELECT chain_id,start_number,block_number,encode(block_hash,'hex') AS hash,
       revision,updated_at FROM extractor.extraction_checkpoints;
SELECT status,count(*) FROM extractor.outbox_events GROUP BY status;
SELECT * FROM extractor.rewinds ORDER BY created_at DESC LIMIT 20;
SELECT * FROM extractor.reindex_jobs ORDER BY created_at DESC LIMIT 20;
SELECT * FROM extractor.replays ORDER BY created_at DESC LIMIT 20;
```

Do not manually delete observations, reset attempts, lower checkpoints or alter
active membership to bypass a halt. Increasing the recovery depth after checking
the provider may allow a deeper retained ancestor to be found (maximum 10000).
Reindex can retain more observations; it does not silently repair the live branch.

## Verification and limits

Deterministic tests cover ranges, inclusive boundaries, head waiting, confirmation
underflow, uint64 maximum, shallow/deep forks, missing ancestors, broken parents,
moving tips, source/commit failures, cancellation, restart, capacity and replay.
Real PostgreSQL tests cover populated migration, atomic rewind failure at every
write, unknown commit before/after server commit, concurrency/ABA fencing,
immutable reindex duplicates, separate job progress, replay claim fencing and
runtime privileges. Existing tests continue to verify at-least-once handoff and
Loader's unknown canonicality. No live Ethereum access is required.

```sh
go test ./...
go test -race ./...
go vet ./...
go run ./test/integration/cmd/extractor-postgres
go run ./test/integration/cmd/transformer-postgres
go run ./test/integration/cmd/loader-postgres
go run ./test/integration/cmd/handoff-postgres
git diff --check
```

Integration runners require Docker/Compose, use isolated temporary databases and
clean up their own resources. This stage adds no Stage 11 service deployment or
new full-system environment. Rate limits are per client, not coordinated across
processes/accounts; align provider budgets and cycle timeouts with large blocks
and ancestry depth. PostgreSQL advisory serialization favors correctness over
throughput. Source header hashes, consensus and provider honesty are not verified.
