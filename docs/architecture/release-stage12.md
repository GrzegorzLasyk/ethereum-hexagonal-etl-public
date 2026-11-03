# MVP release readiness (Stage 12.5)

The final project checklist and validation entry point are now
[Stage 14.5](release-stage14.md). This Stage 12 document retains the security and
storage review; historical execution evidence does not certify the final tree.

## Release boundary

The deliverable is a reproducible **local educational MVP** on a trusted host.
It is not approved for an exposed or unattended production deployment. Stage 12
adds quality gates, safe logs, metrics, readiness and storage/recovery evidence;
it does not supply production authentication, HA, a backup service or canonicality.
Accepted ADR 0001–0004 and strict Hexagonal Architecture remain the baseline.

Use the [operations index](operations-stage12.md) for incidents and maintenance,
[measurement report](storage-measurements-stage12.md) for measured limits and
[restore runbook](backup-restore-stage12.md) for recovery. Verification results
describe the working tree, not an immutable release artifact or GitHub run.

## Checklist for each local MVP release

- [ ] Identify the exact reviewed revision and preserve an inventory of changes;
  require a clean release checkout before tagging/building outside this task.
- [ ] Confirm AGENTS/PLAN/ADRs agree and architecture tests pass; no core imports
  infrastructure, no shared domain model, no cross-service schema access.
- [ ] Run `go test -count=1 ./...`, `go test -race -count=1 ./...`, `go vet ./...`,
  `sh scripts/generate-proto.sh --check`, gofmt verification and `git diff --check`.
- [ ] Run all three PostgreSQL suites, Stage 9 handoff and full fixture E2E via
  `scripts/ci-integration.sh`; require explicit PASS, no skipped tests.
- [ ] Run `ETL_STORAGE_VERIFY=1 ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e`;
  review complete archive/schema/row verification and EXPLAIN plans, not only exit status.
- [ ] Verify exact migrations with each owning migrator, runtime isolation,
  `/ready` failure/recovery, `/live`, `/metrics` outage semantics and shutdown.
- [ ] Exercise duplicates, lost ACK, ambiguous COMMIT, transient outages, leases,
  exhaustion, replay, simulated reorg and historical retention. ACK is acceptance.
- [ ] Review capacity, retry limits, timeouts, payload/concurrency memory budgets,
  confirmation/recovery depth and source chain/start before changing configuration.
- [ ] Confirm fixture defaults require no Infura credentials; local environment
  files, backups, DSNs and secrets are excluded from reviewed artifacts.
- [ ] Record actual platforms, PostgreSQL version, commands, failures and reruns.
  Re-run on another intended platform before claiming its validation.
- [ ] Have an operator review failed-work limitations and restore/cutover steps.
  Do not claim a failed Inbox can be repaired by upstream replay.

Checkboxes are an execution template, not permanent claims that future revisions
pass. This task does not tag, commit, push, deploy or publish an image.

## Security review and deployment limitations

| Area | Current protection / observed boundary | Remaining risk or decision |
| --- | --- | --- |
| Database roles | Separate runtime/migrator logins, SCRAM host/local authentication, own schema, restricted column updates, no runtime DDL/history deletion. Integration tests verify foreign schema/role escalation denial. | PostgreSQL admin and Docker daemon can bypass application isolation. Six-role bootstrap is not a general organizational IAM system. |
| Readiness metadata | Only owning-schema version/checksum view; bounded catalog checks; no error detail in HTTP. | Not a full adversarial schema audit. No automatic migration or repair. |
| Secret handling | Random 0600 test configs, private environment URLs, redacted application logs; no payloads/receipts/DSNs in structured events. | Environment secrets are visible to privileged host/container inspectors. No automatic rotation, KMS or encrypted local backup. Do not publish Compose config/env output. |
| Network | Health and gRPC restricted to loopback; fixture shares network namespace; published PostgreSQL/RPC ports bind host loopback. | gRPC and fixture controls are unauthenticated plaintext. PostgreSQL connections use `sslmode=disable`. Trusted local processes/containers are inside this boundary. Remote or multi-tenant exposure is not approved. |
| Container execution | Application UID 65532, read-only root, capabilities dropped and no-new-privileges. | Stage 14 Compose CPU/memory quotas and Go soft memory targets apply; they do not establish host isolation or measured sizing. PostgreSQL necessarily writes its volume. Fixture control/ACK proxy is test-only, never a production endpoint. |
| Supply chain | Go sums, pinned Go/protobuf tools, CI actions pinned by commit, explicit PostgreSQL tag. | Container tags are not immutable digests; base Debian tag changes. No signed release/SBOM or vulnerability-scan evidence in this task. Review and pin release artifacts separately. |
| CI | PR/main quality + mandatory integrations, read-only contents permission, no deployment/publish step. | Hosted execution, protected branches and required-check enforcement are GitHub configuration and were not verified locally. Fork secrets must never be needed. |
| Availability | Bounded retries, durable Inbox/Outbox, lease fencing, explicit unknown commits and local atomic transactions. | One PostgreSQL instance is a shared failure domain; no HA/PITR, remote backup scheduler or defined production RPO/RTO. |
| Storage | All source/candidate/event history retained; verified logical backup and measured queries. | Unlimited retention consumes disk; metrics aggregates scan growing history. Small synthetic measurements do not establish Mainnet capacity. |
| Data trust | Explicit chain, complete source membership checks, separate receipts, immutable identities and bounded provider-relative recovery. | Provider trust, cryptographic validation, consensus finality and downstream canonical reconciliation are not implemented. Confirmation depth does not establish finality. |
| Failed work | Safe codes, retained evidence, automatic bounded transient retries, audited Extractor replay. | Stage 13 provides audited finite repair for Transformer Inbox/Outbox and Loader Inbox. Fix the cause first; restart/upstream replay alone cannot replenish exhausted budgets. |

## Production blockers requiring a separate decision

Before any remote/unattended deployment, assign owners and approve transport
authentication/TLS and network isolation; secrets/rotation; resource sizing and
disk alerts; backup cadence/off-site encryption/PITR and measured RPO/RTO; reviewed use of existing audited
failed-work recovery; immutable builds, dependency review and hosted CI enforcement.
Define operational on-call/incident ownership and restore drills. None is silently
added by the local MVP checklist.

No policy here authorizes deleting blockchain history, shrinking deduplication
windows, promoting Loader candidates, disabling constraints, weakening tests,
resetting attempts or granting a service access to another schema.
