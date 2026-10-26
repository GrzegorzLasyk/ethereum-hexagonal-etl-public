# Stage 12.5 verification record — 2026-10-26

Scope: the current uncommitted working tree, preserving Stage 12.1–12.4. No
commit, push, image publication or production deployment was performed. Accepted
ADRs, protobuf contracts, dependencies and existing migrations are unchanged.

## Final results

| Command | Actual result |
| --- | --- |
| `go test -count=1 ./...` | PASS, including architecture, CI gate and runner tests |
| `go test -race -count=1 ./...` | PASS |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS; Buf format/lint and generated tree comparison |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres` | PASS; TestPostgresIntegration 2.90 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh transformer-postgres` | PASS; TestPostgresIntegration 1.54 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh loader-postgres` | PASS; TestPostgresIntegration 2.30 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh handoff-postgres` | PASS; seed 0.06 s, handoff 21.21 s |
| `bash scripts/ci-integration.sh storage-release` | PASS; full fixture E2E 125.64 s, nested backup/restore/measurement test 102.78 s; wrapper exit 0 |
| `git ls-files -z --cached --others --exclude-standard '*.go' \| xargs -0 gofmt -l` | PASS; no paths |
| `bash -n scripts/ci-integration.sh` | PASS |
| `git diff --check` | PASS |

The storage command runs the complete existing E2E, not a replacement smoke test.
All database/E2E tests run with race detection; the CI wrapper rejects skips and
missing expected PASS markers, including the nested storage test. Docker was
available and required tool permissions were granted. No real Infura calls ran.

Schema readiness, migration/privilege failures and recovery, queue fencing,
transaction rollback, ambiguous commit, duplicate/lost ACK, service/database
restarts, replay, reorg, exact historical retention, normalized data, metrics,
correlation and graceful shutdown passed their existing regression scenarios.
Backup/restore also verified complete row and schema/ACL equality, role attributes,
runtime authentication, cross-schema denial, archive checksum and refusal to
overwrite an existing target. See [raw plans and measurements](storage-measurements-stage12.md).

## Failures found and resolved during implementation

The first E2E failed at liveness during PostgreSQL outage. Extractor logged
`recovery_required`: any `ActiveHeader` error was wrapped as missing retained
ancestry, including a transient database disconnect. The application now adds
that sentinel only for `domain.ErrUnavailable`. Other errors preserve their
identity and follow the existing bounded retry policy; no failed read authorizes
a rewind or checkpoint mutation. A deterministic regression covers normal
checkpoint and ancestor-walk reads, transient/canceled/deadline/missing cases,
unchanged progress on failure and successful retry after restoration.

The new unit test initially wired its failing store into the observer slot;
correcting that test wiring made the intended regression execute and pass.
A subsequent E2E/storage subtest passed, but its still-running shell wrapper
encountered a syntax error after the wrapper file was edited during execution.
That intermediate invocation is not counted as a successful quality gate. The
final stable `storage-release` invocation above passed including the wrapper and
retained-resource shutdown. Test deadlines/assertions were not weakened.

## Retained resources and limitations

All actual integration invocations in this stage retained their database volumes
and private configuration rather than automatically deleting them. This includes
the initial failed E2E and both successful restore runs. Runners print their exact
project and mode-0600 configuration paths. Storage evidence lives in private
temporary directories and includes the archive, checksum, restored manifest,
environment and 33 EXPLAIN JSON plans. Only non-secret measured plans/environment
are copied into the repository; raw backups and credentials are excluded.

The measured PostgreSQL container reports x86_64 Linux, PostgreSQL 17.11. Native
Apple Silicon, Windows, hosted GitHub Actions, clean release checkout, production
load, off-host recovery, encrypted remote storage, PITR, power-loss durability,
live restore cutover and pending-work resumption from an archive were not verified.
The snapshot drill used the drained fixture; existing separate restart tests cover
live-database pending work. Production RPO/RTO/capacity remain unspecified.

No index was added and no query was optimized speculatively. Historical records,
accepted architecture and immutable migrations remain preserved. The
[release checklist](release-stage12.md) records unresolved deployment and recovery
risks; completion of this local stage is not production certification.
