# Stage 13.3C validation — 2026-10-28

Environment: Go 1.27.3, macOS darwin/amd64, Docker Desktop. This record covers
this working tree, including preserved Stage 13.1–13.3B changes. No Ethereum RPC,
new capture, fixture rewrite, commit or push was performed.

## Recovered state and changes

At resume, capture/verify tooling, pinned manifest, three private captures and
Stage 9/11 infrastructure were present. The interrupted attempt had inspected
and verified artifacts; there was no completed authentic-data ETL runner. The
following Stage 13.3C work was completed without replacing the previous stages.

Modified files (including files already untracked before this task):

- `services/extractor-service/internal/adapters/fixturecapture/artifact.go`:
  share verified artifact loading with replay, preserving verification semantics.
- `test/integration/cmd/handoff-postgres/main.go`: explicit local mode, environment
  filtering, isolated per-height projects, retained storage, mandatory PASS gate.
- `test/integration/cmd/handoff-postgres/main_test.go`: isolated retained lifecycle,
  seed failure ordering, pinned configuration and missing-input failure tests.
- `test/integration/handoff/handoff_test.go`: optional authentic input/assertions,
  expected counts and whole-protobuf retry equality; synthetic defaults preserved.
- `README.md`, `PLAN.md`: usage, stage status, scope and links.
- `docs/architecture/mainnet-fixtures-stage13.md`,
  `docs/architecture/mainnet-capture-stage13.md`: subsequent-status notes preserving
  their historical preflight/selection evidence.

New files:

- `services/extractor-service/internal/adapters/fixturecapture/source.go`
- `services/extractor-service/internal/adapters/fixturecapture/source_test.go`
- `services/extractor-service/internal/adapters/outbound/postgres/mainnet_seed_test.go`
- `test/integration/cmd/handoff-postgres/mainnet.go`
- `test/integration/handoff/mainnet_test.go`
- `test/integration/handoff/oracle_test.go`
- `docs/architecture/mainnet-offline-stage13.md`
- `docs/architecture/validation-stage13-3c.md`

Existing dirty Stage 13.1–13.3B files outside this list were preserved. There are
no protobuf, ADR, migration, dependency or production behavior changes.

## Final validation results

| Command/check | Result |
| --- | --- |
| Existing `fixture-capture verify` for all three pinned cases | PASS before database startup |
| `go test -count=1 ./...` | PASS, including architecture, replay, oracle and runner tests |
| `go test -race -count=1 ./...` | PASS |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres` | PASS; TestPostgresIntegration 3.26 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh transformer-postgres` | PASS; TestPostgresIntegration 1.54 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh loader-postgres` | PASS; TestPostgresIntegration 2.43 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh handoff-postgres` | PASS; seed 0.09 s, handoff 17.34 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e` | PASS; TestFixtureE2E 24.03 s |
| `go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b` | PASS; all three isolated seeds and handoffs with race detection |
| gofmt check of all repository Go sources | PASS; no paths reported |
| Original three artifact file SHA-256 after execution | PASS; unchanged |
| `git diff --check` | PASS |

Authentic per-block results:

| Height | Seed | Handoff | Transactions / receipts / logs | Duplicate raw / normalized ACKs |
| --- | --- | --- | --- | --- |
| 1000000 | PASS 0.15 s | PASS 17.32 s | 2 / 2 / 1 | 1 / 1 |
| 22431084 | PASS 1.48 s | PASS 18.85 s | 95 / 95 / 233 | 1 / 1 |
| 12965000 | PASS 3.94 s | PASS 18.71 s | 259 / 259 / 648 | 1 / 1 |

Executed assertions include the exact raw bytes and original provenance at both
raw stores/wire, isolated checkpoint and active membership, duplicate extraction,
committed Inbox before each lost ACK, delivered Outboxes, completed Inboxes,
stable event/source identity, full normalized payload preservation, and every
approved normalized SQL field against original JSON. Whole protobuf equality
is checked across retries. Role isolation and graceful final shutdown also ran.
See [coverage and field comparison scope](mainnet-offline-stage13.md).

The successful retained projects are `ethereum-stage13c-b928d642365bf50706a38e08`,
`ethereum-stage13c-01926c50e5a97086f5f343e2` and
`ethereum-stage13c-7720a66f702ae986a18243b3` in the order above. Their private
configuration paths were printed by the runner; no passwords are recorded here.

## Earlier unsuccessful attempts

- Initial sandbox unit run failed on restricted loopback listeners. Approved
  execution outside the sandbox passed. Tagged compilation also initially failed
  because sandbox access to Go's build cache was denied; approved retry passed.
- First authentic runner invocation verified all three files but failed before
  PostgreSQL startup because Docker Desktop was stopped (socket did not exist).
  Docker was started with approval; subsequent runs used its local daemon.
- First database-backed authentic run passed seed and pipeline processing but
  failed a new test assertion scanning Transformer `transformed_at` (timestamptz)
  into a string. The assertion now scans `time.Time` and compares the UTC RFC3339
  value to Loader's preserved text. No production code or data was changed to
  satisfy the assertion. The complete three-case rerun passed.

- First Stage 11 E2E attempt built images successfully but PostgreSQL startup
  failed with `all predefined address pools have been fully subnetted`. With
  approval, only the stopped failed-run project
  `ethereum-stage13c-f5310f54bcee514ece9cced3` had its container/network removed
  using Compose `down` **without** `--volumes`. Its database volume and private
  configuration were retained; this released one subnet for the E2E rerun, which
  passed all assertions including readiness recovery, service restart, lost ACKs,
  operator replay, simulated reorg, metrics and correlation.

Failed-run and successful-run volumes/configurations were retained. No existing
Mainnet or fixture E2E volume was deleted.

## Limits

No hosted Linux CI or native Apple Silicon execution was performed. The real
artifacts remain private/Git-ignored, so ordinary public CI verifies synthetic
replay and existing synthetic E2E only. Publishing immutable sanitized artifacts
is a separate prerequisite for a reproducible hosted real-data gate. No type 4,
top-level contract creation, internal traces, live acquisition, canonicality,
finality, consecutive Mainnet history or Stage 13.4/13.5 acceptance is asserted.
The source and normalized-field scopes remain ADR 0001–0004 compliant.

Stage 13.3C is ready for final review within the documented offline/local scope.
