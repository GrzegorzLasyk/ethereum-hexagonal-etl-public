# Stage 13 final validation and resume record

Date: 2026-10-28. Host: macOS, Go 1.27.3 darwin/amd64; Docker Engine 29.9.2.
Stage 13.5 implementation is complete and ready for final review/validation.
Stage 13.4B live E2E is **NOT RUN**. No Infura or other live Ethereum RPC was
contacted. The earlier user-reported smoke is not an end-to-end result.
Accepted ADRs, protobuf contracts, canonicality and original artifacts are unchanged.

## Recovered work and corrections

The resumed tree already contained the Stage 13.4A bounded completion verifier,
finite-range dispatch drain, isolated Mainnet Compose workflow, Stage 13.5 audited
Transformer/Loader repairs, restart/replay/reindex commands, migrations/readiness
policies, runbook and final validation script. Earlier Stage 13 work was preserved.

The initial comprehensive pass exposed two specific defects:

1. New repair request validation imported `strconv`, which is not on the existing
   core import allowlist. Reused each service's existing `domain.Decimal(..., 64)`
   instead. No architecture test or import allowlist was weakened. This correction
   was already present at the latest resume and was verified again.
2. New Inbox stale-attempt integration assertions supplied `unavailable`, while
   the processing failure API accepts `transient`. Validation therefore rejected
   the test input before checking its fence. Corrected both tests to `transient`;
   the required `ErrLeaseLost` assertion is unchanged. No production retry or
   cancellation behavior was altered by this test correction.

## Initial comprehensive pass (before those corrections)

Executed once: `bash scripts/validate-stage13.sh .mainnet/fixtures/stage13-3b`.
The process exited **1**, correctly reporting failures; it was not a green run.
These results were observed in the preceding execution's tool output. Its temporary
raw logs were no longer present after the latest resume, so they are not offered
as currently accessible artifacts.

| Gate | Observed result |
| --- | --- |
| Formatting / build | PASS |
| `go test -count=1 ./...` | FAIL: architecture allowlist rejected the new `strconv` imports; other packages passed |
| `go test -race -count=1 ./...` | FAIL: same architecture guard |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS |
| Explicit architecture tests | FAIL: same new imports |
| Extractor PostgreSQL runner | PASS, including checkpoint/reorg/replay and drain regressions |
| Transformer PostgreSQL runner | FAIL: new Inbox stale-attempt test used the wrong failure code; Outbox repair cases passed |
| Loader PostgreSQL runner | FAIL: same new Inbox test error |
| Stage 9 gRPC handoff | PASS |
| Full Compose fixture E2E | PASS |
| Authentic Mainnet offline regression, all three blocks | PASS |
| Original fixture SHA-256 before/after / diff check | PASS |
| Live Mainnet E2E | NOT RUN |

No full race, handoff, Compose or authentic E2E suite was repeatedly executed
while fixing these findings. Existing fixture scenarios exercised outages,
restart, duplicate/lost-ACK delivery, replay and reorg history preservation.

## Checks executed after the latest resume

| Command / check | Result |
| --- | --- |
| `go build ./...` | PASS (exit 0 outside cache-restricted sandbox) |
| `go vet ./...` | PASS (exit 0) |
| Targeted unit tests below | PASS, all 9 packages |
| Targeted race tests below | PASS, all 4 packages |
| Transformer `TestPostgresIntegration`, `-race -count=1` | PASS, package 6.681s; audited recovery 0.56s |
| Loader `TestPostgresIntegration`, `-race -count=1` | PASS, package 6.747s; audited recovery 0.43s |
| `git diff --check`, formatting | PASS |
| `go run ./tools/mainnet check .mainnet/stage13.env` | PASS: read-only Compose and volume ownership preflight; no service/RPC started |
| Original authentic fixture SHA-256 | PASS, identical to the pre-resume values |
| Full unit/race/E2E rerun after correction | NOT RUN, deliberately restricted to affected tests |
| Live E2E / live reindex | NOT RUN |

Targeted commands:

```sh
go test -count=1 ./tools/mainnet ./internal/adapters/etlverify \
  ./services/extractor-service/cmd/extractor \
  ./services/extractor-service/internal/config \
  ./services/transformer-service/internal/application/usecases \
  ./services/transformer-service/internal/config \
  ./services/loader-service/internal/application/usecases \
  ./services/loader-service/internal/config ./test/architecture

go test -race -count=1 ./tools/mainnet \
  ./services/transformer-service/internal/application/usecases \
  ./services/loader-service/internal/application/usecases ./test/architecture

# Each against its existing, checked disposable PostgreSQL test project:
go test -tags postgresintegration -race -run '^TestPostgresIntegration$' \
  -count=1 -timeout=3m -v ./services/transformer-service/internal/adapters/outbound/postgres
go test -tags postgresintegration -race -run '^TestPostgresIntegration$' \
  -count=1 -timeout=3m -v ./services/loader-service/internal/adapters/outbound/postgres
```

The PostgreSQL commands were supplied owning runtime/migrator credentials privately
through environment variables and required `*_TEST_ISOLATED=1`. They did not skip.
They reused only the previous disposable projects
`ethereum-stage7-247c1eca4a9445f64d98f0ca` and
`ethereum-stage8-6117735696271b9a8bac6cdc`, verifying labels, stopped status and
project-owned volumes before starting them. Both were stopped after execution.
No new network, Mainnet operation or volume deletion was needed. Their test-owned
synthetic rows are reset by the existing regression suite; authentic fixtures and
Mainnet data are never used as mutable regression databases.

PostgreSQL assertions actually executed include failed-only transition, bounded
additional allowance, higher attempt fencing, same-ID concurrent repair retries,
commit-before/after ambiguity, changed operation rejection, processing completion,
refusal to reset pending/completed/delivered work, immutable audit privileges and
unchanged accepted inputs/deduplication. Standard readiness, least-privilege,
rollback, lease, normalized-data and retry tests also passed in both packages.

Latest temporary logs (may expire): `/tmp/stage135-resume-build.log`,
`/tmp/stage135-resume-vet.log`, `/tmp/stage135-resume-targeted.log`,
`/tmp/stage135-resume-targeted-race.log`,
`/tmp/stage135-resume-transformer-postgres.log`,
`/tmp/stage135-resume-loader-postgres.log`.
Docker was initially unavailable (`docker.sock: no such file or directory`),
including outside the sandbox. Docker Desktop was then explicitly started and
both PostgreSQL suites completed successfully. This is no longer a test blocker.

## Preserved authentic coverage

| Block | Transactions / types | Receipts | Failed receipts | Logs | Creation / type 4 |
| --- | --- | --- | --- | --- | --- |
| 1000000 | 2, type 0 | 2 historical-root receipts | Status unavailable in this historical format | 1 | Neither present |
| 22431084 | 95: type 0=10, type 2=83, type 3=2 | 95 | 3 | 233 | Neither present |
| 12965000 | 259: type 0=258, type 1=1 | 259 | 32 | 648 | Neither present |

Original artifact file digests checked again on resume:

```text
1000000  8ef602ed4635ccca386ddd226464d23dce8be424c2c8912c34279bf7946fc734
22431084 fa6e3b12876cc67946f67ed2c08a2f19de18e01e1272513c9b110807849e7b71
12965000 ab870cfbd26aaf80c39b0e6c4f8a05d1808e5cb8107a0ca2630d97cdf60d930e
```

## File inventory for Stage 13.5 (earlier Stage 13 changes retained)

- `tools/mainnet/recovery.go`, `recovery_test.go`: offline recovery command parsing,
  bounded orchestration and safety tests.
- `tools/mainnet/main.go`, `completion.go`, `completion_test.go`: commands, restart
  guard, all-service migration application and explicit read-only verification.
- Extractor `cmd/extractor/main.go`, `internal/config/indexing.go`,
  `execution.go`, `execution_test.go`: bounded no-RPC drain and explicit bounded reindex.
- For both `services/transformer-service` and `services/loader-service`:
  `cmd/<service>/main.go`; `internal/config/{config.go,repair.go,repair_test.go}`;
  `internal/application/ports/outbound/repair.go`;
  `internal/application/usecases/{repair.go,repair_test.go}`;
  `internal/adapters/outbound/postgres/{repair.go,repair_integration_test.go,integration_test.go,readiness_policy.go}`;
  `internal/adapters/logging/errors.go`.
- `migrations/transformer/0005_recovery_operations.sql`,
  `migrations/loader/0005_recovery_operations.sql`, `test/integration/postgres.sh`:
  immutable service-owned audit, narrow queue privileges and inventory verification.
- `scripts/validate-stage13.sh`: one explicit comprehensive offline acceptance pass.
- `PLAN.md`, `README.md`, `docs/architecture/operations-stage13.md`, this record,
  `operations-stage12.md`, `readiness-stage12.md`, `mainnet-stage13.md` and
  `mainnet-e2e-stage13.md`: status, safe commands, migration compatibility and limits.

The last resume changed only the two demonstrated test errors and final status
reporting. The repository still includes earlier uncommitted Stage 13 files;
this inventory does not claim those were reimplemented or discard them.

## Remaining acceptance and limitations

The workflow is prepared, but the existing `.mainnet/stage13.env` is not live-ready:
its private mode is 0600 and chain ID matches 1; the range/request/time settings
need operator review and the RPC URL is absent. No private values were printed
or modified. Set the exact reviewed values in the [runbook](operations-stage13.md)
and provide the endpoint privately before explicit user execution. Apply the new
service migrations with `up`; new binaries intentionally require them for readiness.

One final full validation command is prepared:

```sh
bash scripts/validate-stage13.sh .mainnet/fixtures/stage13-3b
```

Its result must be retained/reviewed on the chosen final tree; no all-green full
post-correction run is claimed here. The script never contacts Ethereum. Private
artifacts remain an explicit local prerequisite; ordinary CI remains synthetic.
Retained test networks consume Docker address pools; do not prune/delete volumes
as a workaround. Any release of specific stopped test networks needs review.

Then, **user execution only**, after configuration and live authorization:

```sh
go run ./tools/mainnet e2e .mainnet/stage13.env
go run ./tools/mainnet verify .mainnet/stage13.env
go run ./tools/mainnet stop .mainnet/stage13.env
```

Record actual live results, not the 100-attempt ceiling as a measured count.
The Mainnet wrapper's restart/reindex orchestration is tested with fake commands;
no live recovery/reindex or account-quota test is claimed. PostgreSQL repair and
existing synthetic restart/reorg drills are real offline tests. Type 4/creation,
consensus finality, downstream canonicality, hosted CI and production deployment
security remain explicit limitations. No commit or push was performed.
