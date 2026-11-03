# Final validation — Phase 1 offline execution record

Date: 2026-11-02. Initial revision:
`c35b0bb09f597660f3483514725fcdab3789b3ea`, branch
`feature/ethereum-mainnet-integration`, initially clean working tree.

**Overall result: PASS after targeted resumption on 2026-11-02.** Every required
offline gate now has a successful execution, including the corrected Loader
integration test. This combines the earlier successful Go gates with resumed
integration gates; it is not a claim that the original full-script invocation
exited successfully. Performance, live Mainnet and hosted CI remain unvalidated.
No commit, push, merge or release was performed.

## Resumption after Docker environment repair

The operator configured `default-address-pools` as `172.16.0.0/16` with `/24`
allocations. Docker Engine 29.9.2 was reachable. The newly created, retained
diagnostic network `etl-phase1-probe-052937d8fea4` received `172.16.2.0/24`.
Daemon-side registry metadata reads returned HTTP 200 for PostgreSQL
`17.11-bookworm`, Go `1.27.3-bookworm` and Debian `bookworm-slim`. Subsequent image
build and isolated database startup also succeeded.

Previously successful unit/build/vet/protobuf/architecture/race phases were not
repeated. The existing final wrapper's six blocked gates were invoked directly
through their existing scripts/runners, with its retention and offline environment:

```sh
export ETL_TEST_RETAIN=1 ETL_STORAGE_VERIFY=1 ETL_FINAL_VALIDATION=1
export ETHEREUM_LIVE_MAINNET_OPT_IN=false ETHEREUM_CAPTURE_OPT_IN=false
bash scripts/ci-integration.sh extractor-postgres
bash scripts/ci-integration.sh transformer-postgres
bash scripts/ci-integration.sh loader-postgres
bash scripts/ci-integration.sh handoff-postgres
bash scripts/ci-integration.sh storage-release
go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b
```

Logs: `$TMPDIR/etl-phase1-resume-a4_ut9wx/`. The initial Loader failure is retained
in `loader-postgres.log` and `results.json`; its successful correction is recorded
in `loader-postgres-rerun.log`. These original records were not overwritten.

| Resumed gate | Final result | Actual test evidence |
|---|---|---|
| Extractor PostgreSQL | PASS | `TestPostgresIntegration` 5.58s; 94 PASS markers including parent/subtests |
| Transformer PostgreSQL | PASS | `TestPostgresIntegration` 3.40s; 44 PASS markers |
| Loader PostgreSQL | PASS after test-query fix | `TestPostgresIntegration` 9.66s; 49 PASS markers; batch subtest 7.10s and late-batch rollback/recovery 1.11s |
| Stage 9 handoff | PASS | `TestSeedHandoff` 0.09s; `TestHandoff` 21.85s; duplicate ACK on each hop |
| Compose fixture E2E + storage release | PASS | `TestFixtureE2E` 152.71s, including `storage_release` 124.44s |
| Authentic offline ETL | PASS | Three separate seed/handoff pairs; handoff 18.31s / 21.27s / 20.72s for blocks 1000000 / 22431084 / 12965000 |
| Fixture immutability | PASS | SHA-256 before/after identical for all three original artifacts |
| Formatting / diff after correction | PASS | `gofmt -l` empty; `git diff --check` exit 0 |

All final integration logs were inspected: no FAIL or SKIP markers in successful
runs. The existing CI wrapper checks expected top-level PASS markers; the authentic
runner checks each seed/handoff result. PostgreSQL, handoff and Compose integration
runners execute their selected tests with `-race`; service binaries in the handoff
are also built with race instrumentation. This still does not claim a new full
repository race invocation.

Verified coverage now includes migration replay/concurrency and checksum failure
rollback; denied cross-schema/role-escalation operations; readiness failures and
recovery; checkpoint preservation under RPC faults; revision-fenced rewind,
retained fork history, reindex and replay; finite-range drain; durable ACK and
ambiguous commit recovery; admission concurrency/deduplication and capacity release;
atomic Inbox/Outbox completion; Loader observations with unknown canonicality.

The full fixture E2E exercised both consumer outages, lost ACKs on both hops,
PostgreSQL restart, service restart, original-ID replay, simulated reorg, metrics,
correlation and graceful shutdown. The completion verifier rejected an advanced
checkpoint with pending delivery, timed out within bounds, and detected failed
queues/expired leases before accepting complete processing.

Backup/restore used a separate fresh test cluster: all table rows, schema/ACL
metadata and runtime isolation matched; restoring over a populated destination was
rejected. Both volumes and private evidence were retained. The existing storage
release subtest also ran its bounded EXPLAIN checks on the restored test database;
the Stage 14 performance benchmark matrix was not invoked and no throughput
improvement is claimed.

### Authentic coverage observed in the resumed database tests

| Block | Transactions / receipts | Logs | Transaction types | Failed receipts | Contract creations |
|---|---|---|---|---|---|
| 1000000 | 2 / 2 | 1 | 0: 2 | 0; both receipts use historical roots | 0 |
| 22431084 | 95 / 95 | 233 | 0: 10, 2: 83, 3: 2 | 3 | 0 |
| 12965000 | 259 / 259 | 648 | 0: 258, 1: 1 | 32 | 0 |

Each noncontiguous height ran in its own isolated database project. Assertions
compared normalized fields with original data, exact raw bytes, receipt membership,
checkpoint, provenance, correlation IDs, queue completion and deduplication.
Loader `canonical_status=unknown` was verified. Type 4 and top-level contract
creation remain uncovered by these authentic captures.

### Defect found during resumption

`testBatches` used `to_jsonb(r)` with table alias `r`. `loader.transactions` also
has the scalar signature column `r`, so PostgreSQL resolved the expression to that
column. Subtracting `loaded_at` from the resulting scalar failed with SQLSTATE
22023 (`cannot delete from scalar`). This was a comparison-query defect in the
integration test, not a production batching failure.

Changed only the two expressions to `to_jsonb(r.*)` in
`services/loader-service/internal/adapters/outbound/postgres/batch_integration_test.go`.
This explicitly selects the whole row without weakening comparisons. The complete
Loader integration gate was rerun once with `ETL_TEST_RETAIN=1` and both live
opt-ins false. It passed empty and 259-transaction cases at batch sizes 1/2/64/256,
every-field equivalence, idempotent replay and late-batch SQL failure rollback
followed by successful recovery. No application code, schema, ADR or protobuf
changed. `gofmt -w`, `gofmt -l` and `git diff --check` completed.

The earlier `test/ci/integration_test.go` isolation fix remains preserved and was
not reimplemented. This resumption additionally updated this report and its pointer
in `release-stage14.md`. Existing containers, volumes and private fixtures were
preserved; newly owned test projects were stopped with their data retained.

**Phase 1 is complete for the documented offline scope. Phase 2 is ready for
separate authorization.** Performance sizing, live provider behavior, hosted CI,
full authentic type coverage and production deployment approval remain outside
this acceptance. The final clean release revision still needs human review and CI.

## Initial attempt — historical BLOCKED result

The requested full script initially exited 1. One test-isolation defect was fixed
and its unit phase rerun successfully. Infrastructure failures prevented database
and full ETL tests from starting. The following records describe that initial
attempt and are superseded by the successful resumption above.

## Preconditions and evidence

- `git status --short --branch` and `git rev-parse HEAD` recorded the initial tree.
- `docker info --format '{{.ServerVersion}}'` initially hit sandbox socket access
  denial; the approved execution outside the sandbox returned Docker **29.9.2**.
  An available daemon did not establish that it could allocate another network.
- The three `fixture-capture verify` commands below succeeded against the pinned
  manifest. The authentic runner repeated artifact verification successfully
  before failing to start its first database project.
- Reviewed the corrected formatting gate, final wrapper, CI gate and integration
  runner cleanup. Projects use random names, generated credentials and own Compose
  volumes. The final wrapper sets `ETL_TEST_RETAIN=1`: cleanup stops the owned
  project instead of removing its volumes. Restore targets a separate fresh
  test cluster. No private capture or existing unrelated volume was deleted.
- Raw per-gate logs and the original `results.txt` are retained under
  `$TMPDIR/ethereum-stage13-validation.HrdgKK/`; the unit rerun is
  `unit-rerun.log`. The original failure record was not rewritten as PASS.

Fixture SHA-256 values were identical before and after the suite:

| Block | SHA-256 of artifact |
|---|---|
| 1000000 | `8ef602ed4635ccca386ddd226464d23dce8be424c2c8912c34279bf7946fc734` |
| 12965000 | `ab870cfbd26aaf80c39b0e6c4f8a05d1808e5cb8107a0ca2630d97cdf60d930e` |
| 22431084 | `fa6e3b12876cc67946f67ed2c08a2f19de18e01e1272513c9b110807849e7b71` |

## Results verified from individual logs

| Category | Result | Evidence / limit |
|---|---|---|
| Manifest verification and unchanged artifacts | PASS | All three verifier exits 0; before/after digest diff exits 0 |
| Formatting / build | PASS | Actual formatting gate and `go build ./...` completed |
| Unit tests | FAIL initially; PASS after fix | `TestIntegrationGate/fixture` failed; full affected unit phase rerun: 47 passing packages, 18 without tests, exit 0 |
| Architecture | PASS | Explicit verbose boundary, import policy and schema policy tests; all three service boundaries covered |
| Targeted race | PASS | All eight packages selected by the final script returned `ok`; not a full-repository race run |
| Vet / protobuf | PASS | `go vet ./...`, Buf formatting/lint and generation comparison completed; targeted vet repeated after test fix |
| Extractor PostgreSQL / migrations | BLOCKED | Runner exited 1 creating the isolated network, before migrations/tests |
| Transformer PostgreSQL / migrations | BLOCKED | Same network-allocation failure |
| Loader PostgreSQL / migrations | BLOCKED | Same network-allocation failure |
| Stage 9 PostgreSQL/gRPC handoff | BLOCKED | Same network-allocation failure, before seed/handoff tests |
| Full Compose fixture E2E + storage release | BLOCKED | Fixture image build failed resolving Docker Hub; no E2E/backup-restore subtests ran |
| Authentic offline Mainnet ETL | BLOCKED | Artifacts verified; first isolated database failed to start; no block completed the pipeline |
| Diff whitespace check | PASS | Script gate and post-fix check |
| Performance matrix / live RPC / hosted CI | NOT RUN | Outside this phase; no performance or live wrapper invocation |

The race selection was `internal/adapters/{grpccontract,metrics,baseline}`,
Extractor JSON-RPC and gRPC adapters, Transformer gRPC adapter, Loader PostgreSQL
adapter and `tools/mainnet`, with `-tags perfbaseline`. That tag enables baseline
instrumentation unit checks; no benchmark matrix was run. PostgreSQL-backed tests
require separate integration tags/runners and are not implied by package PASS.

Receipt ordering, raw-byte preservation, shared budget/pacing, cancellation and
worker joining are ordinary tests in the JSON-RPC package, which passed unit and
race runs. Admission and Loader batch helper unit checks also passed. Their
database concurrency, overload recovery, late-batch rollback and replay guarantees
remain **unverified in this execution**. Likewise checkpoint/reorg durability,
Inbox/Outbox transactions, migration privileges, lost ACKs, restart recovery,
normalized authentic records and `canonical_status=unknown` persistence await the
blocked integration suites. No integration PASS marker was inferred from a unit
package result or the mere existence of a test.

## Defect and correction

The final wrapper exports `ETL_STORAGE_VERIFY=1`. `TestIntegrationGate` inherited
that value into its fake-runner shell process, although its basic `fixture` case
provided only `TestFixtureE2E` output. The real gate correctly demanded the
additional `TestFixtureE2E/storage_release` PASS and rejected it. The defect was
test environment isolation, not production persistence or the gate's enforcement.

`test/ci/integration_test.go` now filters the inherited storage flag and sets it
explicitly per case. Two regression cases check fixture mode with storage enabled:
complete output passes, missing storage output fails. All existing skip/failure
assertions remain. The targeted invocation under the final wrapper's environment
passed all 17 scenarios (package 5.766s), followed by a successful complete unit
phase rerun. No service, migration, ADR or protobuf change was necessary.

## Infrastructure blockers

1. Docker reports `all predefined address pools have been fully subnetted` when
   each PostgreSQL runner creates its new Compose network. A read-only
   `docker network ls --format '{{.Name}}'` showed numerous retained ETL projects
   alongside unrelated projects. It does not establish that any are safe to delete.
   No network, container or volume pruning was attempted.
2. The fixture image build reports `lookup registry-1.docker.io: no such host`
   while resolving `golang:1.27.3-bookworm` and `debian:bookworm-slim`. This occurred
   outside the sandbox, in Docker Desktop's build path. No live Ethereum endpoint
   was contacted. No Docker daemon/network settings were modified.

An operator must restore network allocation capacity and Docker image-resolution
availability while preserving retained data. Do not blindly prune or change
application Compose isolation to bypass these failures. The six failed integration
gates were not repeated under unchanged infrastructure.

## Commands executed

Preflight fixture verification:

```sh
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json historical-smoke .mainnet/fixtures/stage13-3b/1000000.json
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json london-access-list .mainnet/fixtures/stage13-3b/12965000.json
go run ./services/extractor-service/cmd/fixture-capture verify test/fixtures/mainnet/v1/manifest.json pectra-blob-dynamic-fee .mainnet/fixtures/stage13-3b/22431084.json
bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
```

The wrapper actually invoked these gates, including the blocked runners:

```sh
go build ./...
go test -count=1 -timeout=10m ./...
go test -race -count=1 -timeout=10m -tags perfbaseline \
  ./internal/adapters/grpccontract ./internal/adapters/metrics ./internal/adapters/baseline \
  ./services/extractor-service/internal/adapters/outbound/jsonrpc \
  ./services/extractor-service/internal/adapters/outbound/grpc \
  ./services/transformer-service/internal/adapters/outbound/grpc \
  ./services/loader-service/internal/adapters/outbound/postgres ./tools/mainnet
go vet ./...
sh scripts/generate-proto.sh --check
go test -count=1 -v ./test/architecture
bash scripts/ci-integration.sh extractor-postgres
bash scripts/ci-integration.sh transformer-postgres
bash scripts/ci-integration.sh loader-postgres
bash scripts/ci-integration.sh handoff-postgres
bash scripts/ci-integration.sh storage-release
go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b
git diff --check
```

Formatting used the corrected `rg --files -0 | xargs -0 gofmt -l` gate with its
existing Go-file/bin filters. The wrapper also ran `shasum -a 256` before/after and
`diff -u` on the resulting fixture digest files. Post-fix commands:

```sh
gofmt -w test/ci/integration_test.go
ETL_STORAGE_VERIFY=1 ETL_FINAL_VALIDATION=1 ETL_TEST_RETAIN=1 go test -count=1 -v ./test/ci
ETL_STORAGE_VERIFY=1 ETL_FINAL_VALIDATION=1 ETL_TEST_RETAIN=1 go test -count=1 -timeout=10m ./...
go vet ./test/ci
gofmt -l test/ci/integration_test.go
git diff --check
```

Files changed in the initial attempt: the CI test above, this execution record,
and a pointer in `release-stage14.md`. At that point the full offline script had
not passed and Phase 2 was blocked. The resumption section above records the later
successful gate executions without erasing this failure history.
