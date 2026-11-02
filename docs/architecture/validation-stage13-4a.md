# Stage 13.4A validation — 2026-10-28

This is a historical execution record for Stage 13.4A, not the final tree.
Stage 13.5 recovery tooling was implemented subsequently; current acceptance
requirements are in the [Stage 14 release checklist](release-stage14.md).

Scope: the preserved working tree on Go 1.27.3, macOS darwin/amd64 and Docker
Desktop. Stage 13.1–13.3C changes and private captures were preserved. No Infura or
other live Ethereum RPC, Mainnet extraction, fixture capture, commit or push ran.

## Recovered and completed work

The prior stages supplied isolated Mainnet Compose/configuration, request/run
budgets, three verified captures and synthetic/authentic PostgreSQL/gRPC runners.
The Stage 13.4A audit found finite acquisition canceled dispatch immediately after
range completion. The implemented correction retains dispatch during a bounded
application drain, backed by an Extractor-only PostgreSQL port. The operator
verifier and `verify`/guarded `e2e` CLI commands were completed in the same working
tree. The resumed work preserved these pieces, finished failure-path handling,
reran remaining validation and recorded results here.

The implementation introduces no ADR, protobuf, migration, schema privilege,
checkpoint, canonicality or source-payload change. The verifier is explicitly
operator tooling with three separate own-role connections, not a service querying
another service's schema. Acquisition and dispatch retain their existing retry,
lease, ambiguous-commit and deduplication behavior.

## Final results

| Command / check | Result |
| --- | --- |
| `go test -count=1 ./...` | PASS |
| `go test -race -count=1 ./...` | PASS |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres` | PASS; TestPostgresIntegration 3.70s, finite-range drain subtest 0.07s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh transformer-postgres` | PASS; TestPostgresIntegration 1.80s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh loader-postgres` | PASS; TestPostgresIntegration 2.20s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh handoff-postgres` | PASS; seed 0.05s, handoff 19.14s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e` | PASS on corrected rerun; TestFixtureE2E 27.92s |
| `go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures .mainnet/fixtures/stage13-3b` | PASS on corrected rerun; all three isolated authentic datasets |
| Completion success, timeout, partial, terminal and expired-lease tests | PASS; unit and PostgreSQL assertions described below |
| gofmt verification across all repository Go files | PASS; no paths |
| Original private artifact SHA-256 | PASS; unchanged from Stage 13.3C |
| `git diff --check` | PASS |
| Live `mainnet e2e` / Infura execution | NOT RUN; requires separate authorization |
| Hosted GitHub Actions / native Apple Silicon | NOT RUN |

The four final Go/protobuf commands each returned exit status 0. PostgreSQL,
handoff and Compose runners enforce required PASS markers and reject skipped
integration suites. Integration harnesses use race detection.

Authentic regression results, including the new completion verifier:

| Block | Seed | Handoff | Transactions / receipts / logs |
| --- | --- | --- | --- |
| 1000000 | PASS 0.11s | PASS 23.75s | 2 / 2 / 1 |
| 22431084 | PASS 2.39s | PASS 25.33s | 95 / 95 / 233 |
| 12965000 | PASS 3.85s | PASS 21.77s | 259 / 259 / 648 |

Unit assertions: dispatch remains alive after the finite producer returns; drain
success joins the dispatcher; acquisition failure is not mistaken for completion;
finite cancellation fails while continuous follow-head retains graceful shutdown;
virtual-time drain/completion deadlines; invalid configuration; missing/incorrect
states and digest mismatches; unavailable DB never gives healthy zero; a known
failed event is retained even if a downstream observation is unavailable; explicit
live opt-in, CI refusal, pinned range, 100-attempt/2m guards, own-role command
construction, redaction and no automatic acquisition retry.

PostgreSQL assertions: missing raw range, advanced checkpoint with pending Outbox,
partial-height coverage, failed Outbox and delivered-range drain. Synthetic and
three authentic handoffs exercise actual verifier SQL with separate runtime roles:
partial completion times out, all handoffs/materialization complete successfully,
failed Extractor Outbox/Transformer Inbox/Transformer Outbox and expired Loader
lease are detected using local transactions rolled back after each probe. No
persistent fault mutation or historical deletion is used. The earlier exact raw
byte/provenance, full normalized-field comparison, lost-ACK/restart/deduplication,
role-isolation and `canonical_status=unknown` assertions remain enabled.

Full Compose fixture E2E additionally passed downstream outages, PostgreSQL
restart/readiness recovery, service restart, operator replay, lost ACKs,
simulated reorg with retained branches, metrics, correlation and graceful stop.

## Failures encountered and fixed

- Sandbox denied Go cache access; authorized validation outside the sandbox ran
  local mocks/loopback and Docker, without Ethereum RPC.
- Initial lifecycle refactoring treated follow-head SIGTERM as failed finite
  acquisition. Compose E2E caught exit status 1. The lifecycle now distinguishes
  finite and continuous operation; a dedicated unit regression and full E2E passed.
- First completion query repeatedly decoded the full raw block inside each receipt
  comparison. The 259-transaction case exceeded the 1s integration verification
  budget. A materialized CTE decodes the original document once per observation;
  the complete three-case rerun passed **without increasing timeouts**.
- Adding strict status-shape validation exposed an invalid new unit mock that gave
  Extractor an Inbox. The mock now uses each service's real queue shape; the
  failure assertion was preserved and final unit/race runs passed.

Docker's address pools had been exhausted by retained projects in the preceding
stage. With approval, stopped test containers/networks from an explicit allowlist
were released using Compose `down` without `--volumes`; all named volumes and
private configurations were preserved. The same procedure released four completed
first-attempt projects before the corrected reruns. No prune, volume deletion,
fixture replacement or Mainnet data cleanup occurred. Successful current test
projects were stopped and retained by their runners.

Local final logs: `/tmp/stage134a-unit-final.log`, `stage134a-race-final.log`,
`stage134a-vet-final.log`, `stage134a-proto-final.log`, `stage134a-extractor.log`,
`stage134a-transformer.log`, `stage134a-loader.log`, `stage134a-handoff.log`,
`stage134a-fixture-final.log` and `stage134a-real-final.log` (all under `/tmp`).
These temporary logs are not committed release artifacts and may expire.

## Incremental file inventory

Modified existing files (some already untracked from earlier stages):

- `.env.example`, `PLAN.md`, `README.md`
- `deployments/compose/mainnet.yaml`
- `docs/architecture/mainnet-stage13.md`
- `services/extractor-service/cmd/extractor/main.go`
- `services/extractor-service/internal/adapters/logging/errors.go`
- `services/extractor-service/internal/adapters/outbound/postgres/integration_test.go`
- `services/extractor-service/internal/config/indexing.go`
- `services/extractor-service/internal/config/indexing_test.go`
- `test/integration/handoff/handoff_test.go`
- `tools/mainnet/main.go`

New Stage 13.4A files:

- `internal/adapters/etlverify/verify.go`
- `internal/adapters/etlverify/sql.go`
- `internal/adapters/etlverify/verify_test.go`
- `services/extractor-service/cmd/extractor/lifecycle.go`
- `services/extractor-service/cmd/extractor/lifecycle_test.go`
- `services/extractor-service/internal/application/ports/outbound/drain.go`
- `services/extractor-service/internal/application/usecases/drain.go`
- `services/extractor-service/internal/application/usecases/drain_test.go`
- `services/extractor-service/internal/adapters/outbound/postgres/drain.go`
- `services/extractor-service/internal/adapters/outbound/postgres/drain_integration_test.go`
- `test/integration/handoff/completion_test.go`
- `tools/mainnet/completion.go`
- `tools/mainnet/completion_test.go`
- `docs/architecture/mainnet-e2e-stage13.md`
- `docs/architecture/validation-stage13-4a.md`

All other dirty/untracked Stage 13 files predate this increment and were preserved.

## Readiness and limits

Stage 13.4A is ready for review and separately authorized Stage 13.4B execution.
The [runbook](mainnet-e2e-stage13.md) gives configuration and exact safe commands.
No live end-to-end success or current credential/provider availability is claimed.
The indexed path is expected to use 20 attempts without retries on a fresh stable
run (code-derived estimate, not the earlier five-request smoke); the hard E2E
command ceiling is 100, including all header/chain/retry/recovery requests.

Private authentic fixtures are still excluded from Git/ordinary CI. Type 4 and
contract creation coverage remain absent. The verifier observes separate local
snapshots, not a cross-service atomic transaction or consensus finality. Runtime
schema isolation and trusted-local-host deployment assumptions are unchanged.
Stage 13.5 repair/recovery tooling is not implemented.
