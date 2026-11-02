# Final static review and release preparation

Completed: 2026-11-02. Scope: the current Stages 0–14.5 implementation on
`feature/ethereum-mainnet-integration`, continuing the interrupted static review.
The working tree was clean before these corrections. No tests, builds, vet,
benchmarks, Docker/PostgreSQL commands, fixture verification runners, live RPC or
final-validation scripts were executed during this review. No commits or branch
changes were made. Earlier execution records remain historical evidence only.
`git diff --check` passed for the tracked changes; this is a whitespace check,
not execution validation of the script correction.

## Findings and disposition

| Priority | Finding | Disposition |
|---|---|---|
| Critical | No concrete critical defect identified in the inspected paths. | Static review cannot establish runtime correctness or absence of defects. |
| Important | The formatting gate could return PASS after a failed file-enumeration/gofmt pipeline with empty stdout. `run_gate` invokes the function as an `if` condition, so Bash `errexit` does not stop its body on the failed assignment. | Added an explicit pipeline-status check under existing `pipefail`. NUL-separated paths also avoid splitting filenames. Execution and regression verification are deferred. |
| Important | The final revision has no consolidated offline, performance, live E2E and hosted CI acceptance record. | Release remains gated on the checklist below. Earlier smoke or incremental PASS results do not close it. |
| Minor | README claimed captures and later Stage 13 work were pending; PLAN's headline omitted Stage 14, referred to future broker work and described Loader writes as upserts. | Updated current status, frozen scope and atomic insert/deduplication description. |
| Minor | Historical Stage 13 records could be mistaken for current implementation status. | Added historical scope/current-release pointers without changing past execution evidence. |

## Static conclusions and operational risks

- **Architecture and ownership:** inspected production imports respect inward
  dependencies and service boundaries. Adapter imports found in external use-case
  test packages are not production dependencies. A textual SQL review found no
  other-service schema references in production service query paths. Operator
  diagnostics remain distinct from runtime service permissions. These are static
  observations, not a substitute for architecture and privilege tests.
- **Durability:** receiver acceptance commits its own Inbox before ACK. Transformer
  completion and its outgoing intent share one local transaction. Loader batches
  flush before fenced Inbox completion in the same per-chain-locked transaction;
  batch failure follows the rollback path. No cross-service transaction or
  exactly-once delivery claim was introduced. Ambiguous commits, lease loss,
  idempotency and rollback still require fault-injection evidence.
- **Reorg/history:** Extractor rewind updates active membership and checkpoint
  revision after retained-ancestor verification; it does not delete raw source or
  Outbox history. Loader observations retain `canonical_status=unknown`. Provider
  branch selection does not prove consensus finality.
- **Backpressure:** duplicate lookup precedes durable capacity checks. Temporary
  overload is retryable; payload-size violations remain permanent. Failed work
  intentionally retains capacity and can stop the pipeline until audited repair;
  restart or upstream replay does not replenish exhausted attempts. Follow the
  [owning-queue recovery procedure](operations-stage13.md), not ad hoc SQL resets.
- **Resource/configuration limits:** existing bounds for receipt workers, leases,
  dispatch attempts, admission reservations, pools and SQL batches remain intact.
  Defaults and recommended profiles are in the [release runbook](release-stage14.md).
  Serialized message reservations are not decoded-heap bounds; `GOMEMLIMIT` is soft.
  Pools multiply across replicas, and Loader completion serializes per chain.
  No throughput claim or speculative tuning was added.
- **Storage and deployment:** immutable history and full-history metrics can grow
  independently of outstanding-event limits. Disk planning, backup/restore,
  trusted plaintext networking, Docker privileges and lack of HA/PITR remain
  operational limitations. Queue indexes and pool sizing need measured evidence
  before further changes.
- **Unfinished/dead code:** no actionable production TODO/FIXME or demonstrably
  unused production path was identified in the reviewed code. Defensive nil-port
  handling and idempotent shutdown cleanup were retained. Textual searches do not
  prove every branch reachable or every unfinished behavior marked.

## Tracked-content security review

The review examined 377 tracked paths and searched text for private-key headers,
common AWS/GitHub token formats, Infura credential URLs and credential-bearing
PostgreSQL URLs. No verified real credential or tracked private fixture/environment
archive was identified. Matches were configuration substitutions, generated test
connection strings or test literals used for validation/redaction. Matched values
were not included in this report. Git and Docker exclusions cover private Mainnet
data and environment files.

This was a limited current-tree inspection, not a Git-history scan, dependency
security audit or proof that all secret formats are absent. Ignored private data
was not inspected or modified. Recheck staged files and the final release artifact
before publication; do not print resolved secret-bearing Compose configuration.

## Files changed by this review

- `scripts/validate-stage13.sh`: explicit formatting-tool failure propagation and
  NUL-safe file enumeration.
- `PLAN.md`: current status, frozen messaging scope and Loader write description.
- `README.md`: current release state, historical stage wording and fixture status.
- `docs/architecture/rpc-reliability-stage13.md`: historical status clarification.
- `docs/architecture/validation-stage13-4a.md`: historical record/current-status pointer.
- `docs/architecture/release-stage14.md`: static-review link and execution deferral.
- `docs/architecture/static-review-stage14.md`: this report and release checklist.

No service implementation, migration, accepted ADR, protobuf contract, private
configuration or captured fixture was changed.

## Deferred validation and release checklist

All execution below is **NOT RUN in this review** and requires explicit user
instruction. Do not use static conclusions as PASS markers.

- [ ] Verify the formatting gate fails on missing/failing tools and malformed Go,
  passes formatted input, and handles filenames containing spaces. The gate fix
  has been reviewed statically only.
- [ ] Confirm prerequisites, isolated storage ownership and unchanged authentic
  fixture digests. Then run the single offline entry point:

  ```sh
  bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
  ```

  Inspect actual subtest results for unit/architecture/race, all three PostgreSQL
  services, migrations/privileges/readiness, handoffs, admission/deduplication,
  shared receipt limits, batch rollback, checkpoint/reorg, restart/replay/repair,
  backup/restore and synthetic/authentic E2E. Record FAIL/BLOCKED/NOT RUN honestly.
  Fix concrete failures and rerun affected gates rather than repeatedly restarting
  the whole suite without need.
- [ ] Separately authorize the bounded performance comparison documented in the
  [release runbook](release-stage14.md#one-final-validation-entry-point). Preserve
  successful integrity reports, hardware/configuration, warmups and repetitions;
  report latency, memory, pool waits and WAL without extrapolated claims.
- [ ] Separately authorize controlled live Mainnet E2E through the existing
  `live --opt-in` phase. Confirm chain 1, exact block 1000000, at most 100 actual
  attempts and two-minute acquisition bound. Verify downstream completion and
  no-RPC restart/drain/recovery. Never run it implicitly from offline validation.
- [ ] Retain coverage gaps: authentic fixtures do not establish transaction type 4
  or top-level contract-creation coverage. Reverify fixture contents offline during
  acceptance; this review did not execute their verifier.
- [ ] Review operational limits, backup recovery evidence, secret exclusions,
  migrations and deployment restrictions against the exact release revision.
- [ ] Only after separate human authorization: prepare reviewed Git changes/PR,
  obtain hosted CI results and branch-protection review, and validate the clean
  intended release revision. Commit, push, merge, tagging and release publication
  are not authorized by this review.

The static review is complete. Release acceptance and production readiness remain
unproven until the explicitly separated validation phases and human approval finish.
