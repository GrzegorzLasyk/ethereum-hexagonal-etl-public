# Final validation — Phase 3 controlled live execution record

Status: **PASS for the authorized one-block live pipeline and no-RPC recovery
scope**, with a final RPC-counter observation limitation described below. This is
not production certification, hosted CI evidence, or permission to repeat live
extraction. The single authorized acquisition has already been consumed.

## Recovered execution state

On resumption, existing evidence established case B: the earlier live command
had completed successfully. No second extraction was executed. Evidence is retained
privately under `.mainnet/phase3-live-20261103/`, including `live-started.json`,
`live-result.json`, `live.log`, sanitized Extractor events, 28 RPC metric samples,
database snapshots and recovery results. Original evidence was not overwritten.

The baseline snapshot had no raw blocks, checkpoint, events or normalized rows.
The live wrapper started at **2026-11-03 09:36:49.994 UTC**, returned exit code 0
after **13.330 seconds**, and its log contains both successful completion checks
plus `PASS controlled-live-e2e-and-verification`. This duration includes wrapper
startup and verification; it is not a pure RPC timing. The recorded index_block
operation took 2906 ms, followed by delivered Extractor Outbox attempt 1.

On resumption Docker was available, but PostgreSQL and both consumers were stopped.
An initial read-only `verify` consequently exhausted its two-minute deadline with
all services unavailable. This diagnostic attempt is **FAIL / database unavailable**,
not proof of missing durable data. Existing successful live evidence justified the
already authorized restart. No acquisition, replay, repair or reindex was used to
recover the stopped services. Verification after restart passed.

## Safety preflight

- Private regular configuration file, mode 0600, Git-ignored; endpoint validated
  as HTTPS `mainnet.infura.io` with credential present, no userinfo/query/fragment.
  No endpoint value, API key or database password was printed.
- Chain 1, explicit opt-in true, start=end=1000000, confirmation depth 0,
  recovery depth 1, maximum 100 actual RPC attempts, acquisition timeout 2m,
  drain timeout 30s, verification timeout 2m. No continuous extraction/follow-head.
- Docker Engine 29.9.2 and `mainnet check` passed outside the sandbox after the
  sandbox initially denied Docker access. Existing Mainnet volume ownership was
  checked by the operator tool. The original database and service-owned roles,
  schemas and migrations were retained; fixture/benchmark storage was untouched.
- The live phase's finite acquisition includes drain within its two-minute budget.
  The operator's e2e context is 4m40s plus bounded process-shutdown allowance;
  build/startup is separate. No budget, deadline, credential or application setting
  was changed during this live run or its resumed recovery.

## RPC observation and its limit

The last successful local metrics sample, received at **09:36:59.374829 UTC**, was:

```text
etl_rpc_attempts_total 20
etl_rpc_retries_total 0
etl_rpc_budget_enabled 1
etl_rpc_budget_limit 100
etl_rpc_budget_remaining 80
```

Thus **20 actual HTTP attempts were observed**, with zero retries observed at that
sample. The process exited shortly afterward and no final post-completion counter
was persisted. The last sample still had `index_cycle/outcome=done` equal to zero.
Do not present 20 as an independently proven final lifetime total, or 80 as a
guaranteed final remaining budget. The final exact total is **not determinable
from the retained telemetry**; the enforced upper bound remained 100. No extra
live run was made to improve telemetry. Provider billing/credit usage was not read.

**Additional Ethereum RPC during this resumption/recovery: 0.** `restart` starts
only PostgreSQL and consumers. `drain` explicitly uses mode drain, empty RPC URL
and opt-in false; the composition root constructs the RPC adapter only for index
or reindex. `verify` uses read-only PostgreSQL queries through owning runtime roles.

## Durable pipeline result

Pinned block identity, matched between the manifest and existing verifier:

```text
chain_id=1
block_number=1000000
block_hash=0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e
```

| Stage / assertion | Actual result |
|---|---|
| Extractor raw block / checkpoint | 1 raw block, 1 checkpoint at target, active membership verified |
| Extractor Outbox | 1 delivered |
| Transformer Inbox / Outbox | 1 completed / 1 delivered |
| Loader Inbox | 1 completed |
| Loader normalized records | 1 block, 2 transactions, 2 receipts, 1 log |
| Loader canonicality | 1 block with canonical_status=unknown; verifier rejects height conflict |
| Raw handoff | Original raw block/receipt SHA-256 digests match across Extractor and Transformer |
| Transformed handoff | Payload SHA-256 matches Transformer output and Loader Inbox |
| Correlation/provenance | Stable identity-derived event IDs/source ID, transaction membership and provider/extraction-time fingerprint agree |
| Pending/failed/stuck target work | None reported by final verifier |

The verifier checks receipt-to-transaction/block identities and ordering, exact
counts, both durable handoffs and provenance, not just readiness/checkpoint.
The raw payloads were not exported or rewritten. The persisted block hash matches
the pinned manifest; the fresh provider JSON was not required to be byte-identical
to an older fixture acquisition.

Read-only owning-schema snapshots after restart and after drain were identical,
including raw/normalized row fingerprints and all counts/queue states. This verifies
that the drill did not create duplicates or change retained records. It does **not**
constitute a newly injected duplicate-gRPC/lost-ACK test: explicit replay, repair,
reindex, reorg injection and fault injection were not run against retained live data.
Their prior offline coverage remains Phase 1 evidence. Likewise, the live verifier
does not perform every field-by-field fixture regression assertion from Phase 1.

## Commands and results

Already executed once before interruption:

```sh
go run ./tools/mainnet check .mainnet/stage13.env
go run ./tools/mainnet up .mainnet/stage13.env
bash scripts/validate-final.sh live --opt-in .mainnet/stage13.env
```

`up`: PASS, 50.118 s. Live wrapper: PASS, 13.330 s. The wrapper includes its
post-E2E `verify`; it was not invoked again on resumption.

Resumption used read-only configuration/manifest/log checks, Docker status,
`mainnet check`, the initial unavailable-database `verify`, then the authorized:

```sh
go run ./tools/mainnet restart .mainnet/stage13.env
python3 .mainnet/phase3-live-20261103/snapshot.py after
go run ./tools/mainnet drain .mainnet/stage13.env
go run ./tools/mainnet verify .mainnet/stage13.env
python3 .mainnet/phase3-live-20261103/snapshot.py recovery
go run ./tools/mainnet stop .mainnet/stage13.env
```

| Recovery step | Result | Wall duration |
|---|---|---:|
| Restart same volume, forward migrations and consumers | PASS | 22.849 s |
| Bounded no-RPC drain | PASS | 1.985 s |
| Complete pipeline verifier | PASS | 2.283 s |
| After-drain snapshot | PASS; equal to after-restart snapshot | 1.508 s |
| Stop; retain history/volume/credentials | PASS | 1.859 s |

Snapshot helpers are private diagnostic artifacts, not application code. They
authenticate separately as each owning runtime role, use read-only transactions
and two-second SQL statement timeouts, and emit counts/fingerprints only. Secret
scanning of private evidence found no configured full endpoint or database password.
No application, ADR or protobuf changes, no benchmarks/full regressions, and no
commit/push/merge were performed. Documentation only records the actual results.

## Remaining release gates

- Ready for final CI and review/PR preparation after explicit authorization.
  Hosted CI on the intended clean revision, Git diff/secret review and human
  release approval remain required; the current working tree contains prior fixes.
- Exact final live RPC count is an observability gap; do not infer it from an
  estimate or repeat extraction automatically. Enforcement and observed usage
  are separate evidence.
- This validates one small historical block and a clean restart/drain. It does
  not establish broad Mainnet throughput, provider quota availability, HA/PITR,
  consensus finality or safe public deployment. Loader canonicality stays unknown.
- Preserve existing offline limitations, including authentic type-4 and contract
  creation coverage gaps. Earlier phases and this live PASS are scoped evidence,
  not a claim of unrestricted production readiness.
