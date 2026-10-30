# Stage 13.1 verification — 2026-10-27

Scope: the current working tree, macOS darwin/amd64 and local Docker Desktop.
No Infura call, live build tag, Mainnet acquisition, commit or push was performed.
Merged gRPC cancellation-test corrections, existing fixture Compose, migrations,
protobuf output/contracts, Go dependencies and canonicality behavior are unchanged.

## Results

| Command/check | Actual result |
| --- | --- |
| gofmt verification across tracked/untracked Go files | PASS; no paths |
| `go test -count=1 ./...` | PASS, including architecture and new safety tests |
| `go test -race -count=1 ./...` | PASS |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres` | PASS; PostgreSQL test package 6.097 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh transformer-postgres` | PASS; TestPostgresIntegration 2.82 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh loader-postgres` | PASS; TestPostgresIntegration 2.77 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh handoff-postgres` | PASS; seed 0.07 s, handoff 19.20 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e` | PASS; TestFixtureE2E 26.23 s |
| Mainnet tool `init`, `up`, `stop`, then `check` using `.mainnet/stage13.env` | PASS; database/migrations/consumer startup and retained volume ownership checked; acquisition disabled |
| Private configuration permissions / Git exclusion | PASS; 0600 and ignored |
| `git diff --check` | PASS |

Integration runners enforce expected PASS markers and reject skips. They include
race detection. Mainnet setup started only PostgreSQL, migrations and the two
consumers, whose health checks passed. Its private file has no RPC endpoint and
opt-in remains false. `stop` retained the database and configuration. All regression
projects also retained their volumes/private configuration, as requested by the
explicit test setting; no history or volume was automatically deleted.

## Failures and corrections during validation

Initial sandbox commands could not write Go's cache or bind test loopback ports.
Approved reruns outside the sandbox completed. Docker Desktop was initially not
running and was started with approval.

The first Mainnet `up` invocation reported failure because it included the
one-shot Extractor migrator among `up --wait` targets. Migration had exited zero
and both consumers were healthy, but an exited standalone target does not meet
that wait condition. Setup now starts PostgreSQL, runs that migration explicitly,
then waits for consumers; the full corrected setup and retained shutdown passed.
No tests or assertions were weakened.

## Change inventory

- `deployments/compose/mainnet.yaml`: standalone isolated database, unchanged
  migrations, consumers, and explicitly invoked bounded acquisition service.
- `tools/mainnet/main.go`, `main_test.go`: private initialization, config validation,
  fixed Compose inputs, volume ownership refusal, safe setup/extract/stop commands
  and deterministic offline isolation/security tests.
- Extractor `internal/config/execution.go`, `execution_test.go`, `indexing.go`,
  `indexing_test.go`: explicit Mainnet safety settings; local fixture compatibility.
- Extractor `internal/adapters/outbound/jsonrpc/client.go`, `budget_test.go`:
  atomic total HTTP-attempt allowance and tests for receipts, verification headers,
  retries, concurrency, partial snapshots and cancellation.
- Extractor `internal/application/ports/outbound/chain.go`: infrastructure-neutral
  budget errors; `internal/application/usecases/index_test.go`: no mutation of
  retained progress/Outbox on budget failure during acquisition or recovery.
- Extractor `internal/adapters/inbound/indexer/worker.go`, `worker_test.go`:
  immediate halt on request-budget exhaustion, preserving other retry behavior.
- Extractor `internal/adapters/logging/errors.go`: fixed safe budget error codes.
- Extractor `cmd/extractor/main.go`, `budget_test.go`: whole-run deadline including
  startup, cooperative shutdown and failure status on time-budget exhaustion.
- `.gitignore`, `.dockerignore`: private Mainnet directory excluded.
- `README.md`, `PLAN.md`, `docs/architecture/mainnet-stage13.md`, this record:
  setup, configuration, budget/retention semantics, audit findings and limitations.

## Limits of evidence

No Linux/GitHub Actions or native Apple Silicon run was performed here. Docker
image build and local Compose operation passed on the available host. This is
not a live provider compatibility, cost, throughput or real-data E2E result.
Provider failure classification, captured Mainnet datasets, dispatch/drain E2E
verification and subsequent operational acceptance remain Stage 13.2–13.5.
Budgets are per process, not a durable provider-account quota; explicit restart
replenishes them. Cancellation is cooperative, so cleanup can exceed the nominal
run deadline while preserving the existing local transaction/lease guarantees.
