# Stage 13.2A — RPC reliability and offline validation

Subsequent status: the user reports Stage 13.2B smoke PASS for block 1000000,
2 transactions, 2 receipts and 5 RPC requests. The preflight/verification records
below describe the earlier work and were not rerun live. No raw artifact was
supplied. [Stage 13.3A](mainnet-fixtures-stage13.md) prepares selection and bounded
capture; actual capture remains separately authorized and pending.

## Scope and ownership

This increment preserves Stage 13.1, accepted ADR 0001–0004 and the merged gRPC
test fixes. It adds no schema, protobuf, canonicality or downstream behavior.
The JSON-RPC adapter owns HTTP/RPC classification. Workers consume the
infrastructure-neutral `outbound.ErrSourcePermanent` error; domain/application
packages import no HTTP or adapter code. No Infura or other live provider was
contacted. The live smoke is only prepared for separately authorized Stage 13.2B.

The audit found two levels of retry: the client bounded each RPC call, but the
worker could repeat permanently rejected calls in subsequent cycles. The adapter
now preserves a permanent error category through wrapping, and the worker halts.
HTTP status is classified before reading error bodies, so an oversized, broken
or stalled authentication error body cannot become a retriable transport error.
Error bodies are not read or logged; connections may therefore need reopening
after non-200 responses. Numeric status/code diagnostics remain available through
`errors.As`, without URL, provider text or payload disclosure.

## Failure policy

| Condition | Client | Worker |
| --- | --- | --- |
| HTTP 401/402/403, other non-retriable statuses | One attempt; permanent source rejection | Halt immediately; operator correction required |
| HTTP 408/429/500/502/503/504 | Bounded attempts, backoff/jitter | Bounded consecutive-error cycles if no deferred cooldown |
| HTTP 501 and other unsupported status codes | Permanent; existing inner retry whitelist is unchanged | Halt |
| JSON-RPC 429 / -32005 | Bounded retries | Same cooldown/cycle rules |
| Other well-formed JSON-RPC errors, including 402 / invalid params | Permanent | Halt |
| Transport failure / per-attempt timeout | Bounded retries | Bounded cycles; whole-run deadline still takes precedence |
| Wrong chain ID (including mixed snapshot metadata) | Reject data | Halt immediately |
| Missing/null block or receipt, malformed/incomplete source data | Reject entire snapshot; no partial success | Existing bounded error cycles; never authorize a rewind |
| Shared request budget exhausted | Refuse further dispatch | Halt; preserve durable progress |
| Caller/run cancellation | Cancel requests, pacing and backoff | Join work; run-budget expiration remains failure |

`Retry-After` delta seconds and HTTP dates are respected within the configured
maximum backoff. A larger delay, or exhaustion of attempts with a positive
cooldown, returns `ErrRetryDeferred`; the outer worker must not bypass that delay
with another cycle. Missing, zero or malformed values use the existing bounded
jitter policy. Permanent responses remain permanent even with `Retry-After`.
Cooldown is not a durable scheduler: an operator must respect it before restarting.

Stage 13.1 time/request limits are unchanged and cover all RPC methods and retries.
They reset on explicit process restart, not on each cycle. Correcting credentials
or an endpoint does not reset the checkpoint, recreate historical events or modify
the Inbox/Outbox. The safe log categories include `source_permanent`,
`chain_mismatch`, `retry_deferred` and `rpc_budget_exhausted`.

## Deterministic evidence

- Existing local HTTP fixtures cover full block/receipt membership, missing
  resources, invalid logs, wrong/changing chain, payload limits and redaction.
- New tests combine the real RPC adapter and worker with an in-process
  RoundTripper and `testing/synctest`. They assert exact attempts/cycles, retry
  recovery, permanent halt, timeouts and the shared request cap across cycles.
- Virtual time verifies delta/date Retry-After timing and cancellation during
  backoff and pacing without wall-clock sleeps or external I/O.
- An unreadable error body proves 401/402/403 classification does not depend on
  reading provider error text, and that the body is closed.
- A new PostgreSQL subtest uses the real RPC adapter, worker and Index use case
  against local `httptest` fixtures. After seeding a committed block, each failure
  compares complete raw, Outbox, active-membership, checkpoint and rewind rows.
  It covers 401/402/403, 503, wrong chain, missing block/receipt, invalid receipt,
  budget exhaustion and cancellation during receipts. A fresh client/store then
  resumes at the next block with exactly one additional Outbox event.

## Prepared live gate — not executed

`live_test.go` remains excluded unless the `live` build tag is explicitly selected.
Its configuration is tested offline in `smoke_config_test.go`, before constructing
any network client. All of the following are required:

- `ETHEREUM_LIVE_MAINNET_OPT_IN=true` and no nonempty `CI`/`GITHUB_ACTIONS` marker;
- private `ETHEREUM_RPC_URL` on the exact HTTPS host `mainnet.infura.io`, with a
  nonempty `/v3/` path and no URL userinfo or fragment;
- explicit decimal `ETHEREUM_SMOKE_BLOCK`, `ETHEREUM_SMOKE_MAX_REQUESTS` in 1–100, and
  `ETHEREUM_SMOKE_TIMEOUT` between 1ms and 2m.

CI explicitly sets opt-in false and does not select the live tag. An explicitly
invoked smoke with missing configuration fails rather than skipping. Compile-only
validation with `-tags live -run '^$'` executes no tests and contacts no provider.
Stage 13.2B still needs separate authorization; this increment provides no evidence
about actual credentials, provider compatibility, billing or throughput.

## Stage 13.2B preflight — live execution pending

The smoke configuration now rejects budgets above 100 instead of silently
accepting them. Its existing client counts all actual HTTP attempts, including
retries, receipts and both chain checks, before dispatch. Cancellation racing
with dispatch may conservatively consume an allowance without sending a request;
this counter is not a provider billing/credit counter. No acquisition starts on
invalid configuration. The 1ms–2m operation deadline remains enforced by the
runner's context, including request pacing and backoff. Go compilation and setup
before that context are outside this deadline; `go test -timeout=2m` additionally
bounds test-binary execution, not the whole shell command.

The block must be an explicit decimal uint64. Empty values, `latest`, `pending`,
`safe`, `finalized` and hex input fail validation. The adapter sends that exact
number as a hexadecimal RPC quantity with full transactions requested and rejects
a response with a different number. Offline wire tests verify that selection,
both chain checks and every fixture receipt; guard tests cover 1/100 attempts,
over-limit/overflow values and both timeout boundaries.

Public explorer metadata was checked on 2026-10-27 using only a public REST GET,
without credentials or Ethereum JSON-RPC calls:
[Blockscout block 1000000 metadata](https://eth.blockscout.com/api/v2/blocks/1000000)
([explorer page](https://eth.blockscout.com/block/1000000)). It reports:

- height: `1000000`, timestamp: `2016-02-13T22:54:13.000000Z`;
- hash: `0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e`;
- `transactions_count: 2`, gas used: `50244`.

This is a suitable nonempty first smoke block: two chain checks + one block + two
receipts = **5 attempts without retries**, at most 15 when each of those calls
uses its three-attempt allowance. The overall request and time limits can still
terminate the operation before completion. Explorer metadata is independent
preflight evidence, not proof that Infura will serve complete historical receipts.
This pre-Byzantium block exercises historical receipt roots, not modern status or
typed-transaction coverage. The smoke validates receipt membership/completeness
and snapshot consistency but does not pin this public hash or prove consensus.

Proposed command, **only after separate authorization**, from the repository root
on a trusted local terminal (do not clear CI markers to bypass the gate):

```bash
bash -c '
  set +x
  IFS= read -r -s -p "Private Infura Mainnet URL: " ETHEREUM_RPC_URL || exit 2
  printf "\n"
  export ETHEREUM_RPC_URL
  ETHEREUM_LIVE_MAINNET_OPT_IN=true \
  ETHEREUM_SMOKE_BLOCK=1000000 \
  ETHEREUM_SMOKE_MAX_REQUESTS=100 \
  ETHEREUM_SMOKE_TIMEOUT=2m \
  go test -tags live -run "^TestLiveMainnetBlock$" -count=1 -timeout=2m -v \
    ./services/extractor-service/internal/adapters/outbound/jsonrpc
'
```

Enter the private `https://mainnet.infura.io/v3/<YOUR-API-KEY>` URL at the hidden
prompt, never as a literal shell command. The subshell limits environment lifetime;
trusted-host administrators can still inspect process environments. Output is
limited to block identity, transaction/receipt counts, attempt count, duration and
sanitized errors. Expected success is chain `1`, block `1000000`, two transactions,
two receipts and ordinarily five attempts. No database state is written.

Remaining live prerequisites: separate authorization, private valid credentials,
provider access/credits and availability of historical block/receipt data. None
has been tested live. No service, application/domain, ADR or protobuf changes are
part of this preflight increment.

Preflight verification on 2026-10-27 (all exit code 0):

- `go test -count=1 ./...` — PASS; JSON-RPC package 4.911s.
- `go test -race -count=1 ./...` — PASS; JSON-RPC package 6.910s.
- `go vet ./...` — PASS, no diagnostics.
- `gofmt -w services/extractor-service/internal/adapters/outbound/jsonrpc/smoke_config_test.go`
  followed by `gofmt -l` over all tracked/untracked nonignored Go files — clean.
- `git diff --check` — PASS.

No live smoke, live Ethereum RPC, PostgreSQL integration runner or Compose E2E
was executed for this configuration-only increment. Earlier integration results
below belong to Stage 13.2A. Public Blockscout REST metadata lookup is the only
block-data network access used for this preflight.

## Change inventory for this increment

- Extractor `internal/application/ports/outbound/chain.go`: permanent error category.
- Extractor `internal/adapters/outbound/jsonrpc/client.go`: early HTTP classification
  and permanent JSON-RPC error propagation.
- Extractor `internal/adapters/inbound/indexer/worker.go`, `worker_test.go`: permanent
  source and wrong-chain halt policy.
- Extractor `internal/adapters/logging/errors.go`: safe permanent-failure code.
- Extractor JSON-RPC `reliability_test.go`: deterministic cross-cycle policy tests.
- Extractor PostgreSQL `rpc_reliability_integration_test.go`, `integration_test.go`:
  durable-state preservation and recovery regression in the existing runner.
- Extractor JSON-RPC `live_test.go`, `smoke_config_test.go`: prepared live gate and
  offline validation only.
- `.env.example`, `.github/workflows/ci.yml`, `README.md`, `PLAN.md`,
  `mainnet-stage13.md` and this document: explicit gates, policy and instructions.

Earlier Stage 13.1 files remain in the same uncommitted working tree. This inventory
describes only the incremental Stage 13.2A work, not a replacement of Stage 13.1.

## Verification record — 2026-10-27

Verified on macOS darwin/amd64 with Docker Desktop. All commands below completed
against the preserved Stage 13.1 plus Stage 13.2A working tree.

| Command/check | Result |
| --- | --- |
| gofmt verification on tracked and untracked Go files | PASS, no paths |
| `go test -count=1 ./...` | PASS |
| `go test -race -count=1 ./...` | PASS |
| New RPC policy/Retry-After/body/gate tests, `-race -count=20` | PASS, package 2.361 s |
| `go vet ./...` | PASS |
| `sh scripts/generate-proto.sh --check` | PASS |
| `go test -tags live -run '^$' ./services/extractor-service/internal/adapters/outbound/jsonrpc` | Compiles; no tests run, no RPC calls |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh extractor-postgres` | PASS; TestPostgresIntegration 2.99 s; new preservation subtest 0.20 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh transformer-postgres` | PASS; TestPostgresIntegration 1.39 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh loader-postgres` | PASS; TestPostgresIntegration 2.09 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh handoff-postgres` | PASS; seed 0.05 s, handoff 21.05 s |
| `ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e` | PASS; TestFixtureE2E 56.42 s |
| `git diff --check` | PASS |

The repeated targeted command used `-run
'TestRPCPolicyAcrossWorkerCycles|TestRetryAfterTimingAndCycleHalt|TestPermanentHTTPClassificationDoesNotReadErrorBody|TestLiveSmokeGateOffline'`
in the JSON-RPC package. All database and fixture runners enforce expected PASS
markers, reject skips and run their integration tests with race detection.

During development, a local variable redeclaration caused one compilation
failure. A new test mock then consumed the same request body twice, producing an
incorrect response ID and a virtual-clock goroutine cleanup failure. Both were
fixed; no production assertions/timeouts were relaxed. Initial sandbox commands
could not access the Go cache/Docker socket. The first integration attempt also
failed because the Docker daemon was not available. After approved Docker startup,
all five integration suites passed. Infrastructure failures are not counted as PASS.

The integration projects were stopped with volumes and private configurations
retained. Logs are local `/tmp/stage132a-*-verified.log`; unit/race logs are
`/tmp/stage132a-unit.log` and `/tmp/stage132a-race.log`. Nothing was committed or pushed.
The earlier Stage 13.1 verification record remains historical evidence for that
increment; its files and completed implementation were not recreated.

Remaining limits: no live provider, hosted GitHub Actions or native Apple Silicon
validation; no provider-account quota or real-response compatibility claim.
Retries remain bounded per attempt/cycle and by Stage 13.1 process budgets.
At the end of this Stage 13.2A increment, Stages 13.2B–13.5 remained
unexecuted/unimplemented beyond preparation of the existing smoke gate. For the
current implementation and outstanding acceptance, see the [Stage 14 release
checklist](release-stage14.md). No migration, ADR, protobuf, dependency
or merged outbound gRPC publisher-test file changed in this increment.
