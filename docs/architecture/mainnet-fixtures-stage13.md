# Stage 13.3A — Mainnet fixture infrastructure and selection

Subsequent status: three captured artifacts are now available locally and verified
by the [Stage 13.3C offline ETL regression](mainnet-offline-stage13.md). The
preflight/selection status below records the earlier stage; it is not a claim
that capture is still pending. Type 4 and top-level creation coverage remain missing.

Subsequent [Stage 13.3B preflight](mainnet-capture-stage13.md) adds independent
manifest verification and offline secret checks. Use its ordered capture/verify
commands for controlled acquisition. No capture has been executed by that preflight.

## Scope and evidence

The user reports Stage 13.2B live smoke PASS: Mainnet block 1000000, two
transactions, two receipts, five RPC requests. This is user-supplied execution
evidence; no raw response artifact or terminal transcript was supplied. This
increment neither repeats that test nor captures any real fixtures. Earlier
13.1/13.2 verification records remain historical reports of those executions.

Selection research on 2026-10-27 used public Blockscout REST explorer metadata,
not Infura or Ethereum JSON-RPC. Full raw block/receipt responses were not saved.
The [v1 manifest](../../test/fixtures/mainnet/v1/manifest.json) pins block hashes,
transaction witnesses, source links, known counts and explicit unknown coverage.

| Case | Block | Transactions | Confirmed selection coverage | RPC attempts without retries / up to three attempts each | Proposed allowance |
| --- | ---: | ---: | --- | ---: | ---: |
| historical-smoke | 1000000 | 2 | User-reported full smoke; historical raw coverage still pending | 5 / 15 | 100 |
| london-access-list | 12965000 | 259 | Types 0 and 1, failed transaction, event logs | 262 / 786 | 800 |
| pectra-blob-dynamic-fee | 22431084 | 95 | Types 2 and 3 | 98 / 294 | 300 |

Sources: [historical block](https://eth.blockscout.com/api/v2/blocks/1000000),
[London block](https://eth.blockscout.com/api/v2/blocks/12965000) and
[Pectra block](https://eth.blockscout.com/api/v2/blocks/22431084).
All six London transaction pages and both Pectra pages were inspected. The
type-1 transaction `0xf6a0743de5a49d44a87e3f050824a9e20b9bbacaa6227595b66a3a5653f13b92`
has [eight explorer log entries](https://eth.blockscout.com/api/v2/transactions/0xf6a0743de5a49d44a87e3f050824a9e20b9bbacaa6227595b66a3a5653f13b92/logs),
with no next page. This does not establish a whole-block log count. Block
19426589 was also inspected (79 transactions; first page types 0/2); it is not
selected because the chosen Pectra block already covers type 2 and contains a
verified type 3 witness.

**Pending selection:** no type 4 appeared among the 95 Pectra block entries;
an active fork alone is not evidence of a transaction type. [EIP-7702](https://eips.ethereum.org/EIPS/eip-7702)
specifies type `0x04`, but a real transaction witness and full block identity still
need verification. No top-level contract creation appeared in the inspected
London pages; the attempted USDT contract-address metadata lookup returned HTTP
500, so no creation hash/block is asserted. A successful creation candidate must
have null transaction `to` and nonnull receipt `contractAddress`; internal CREATE
traces are outside ADR 0002. Both categories remain `pending` in the manifest.

Receipt totals are expected to equal transaction totals after complete capture,
but are recorded as unknown until evidenced (except the user-reported smoke).
Whole-block log totals, raw payload sizes, checksums and actual provider
compatibility remain unknown for every case. No checksum placeholders are used.

## Tool layout and architecture

- `services/extractor-service/cmd/fixture-capture`: composition root, CLI modes,
  signal cancellation, private endpoint loading and fixed safe output.
- `services/extractor-service/internal/adapters/fixturecapture`: technical
  selection/artifact DTOs, file I/O, offline validation and capture orchestration
  through a small source interface using Extractor's existing domain result.
- Existing outbound JSON-RPC adapter: unchanged acquisition, validation, request
  counter, retries, pacing and limits. Offline validation installs an in-memory
  transport with no network fallback. Synthetic tests use existing synthetic
  JSON-RPC fixtures, never label those bytes as Mainnet evidence.

There are no domain/application infrastructure imports, cross-service internal
dependencies, schema access, database writes, gRPC publication, migrations,
protobuf changes or canonicality changes. The command is not started by Compose.
ADR 0001–0004 remain unchanged. Loader canonical status stays `unknown`.

## Bounded capture — prepared, not executed

Capture selects exactly one manifest ID, with explicit chain 1, number and hash.
There is no range, latest, follow-head, discovery loop or automatic restart.

| Variable | Rule |
| --- | --- |
| `ETHEREUM_CAPTURE_OPT_IN` | Exactly `true`; default/example/CI is false |
| `ETHEREUM_RPC_URL` | Private HTTPS Infura Mainnet `/v3/` endpoint; no userinfo, query or fragment; never a command argument |
| `ETHEREUM_CAPTURE_MAX_REQUESTS` | Explicit integer, at least selected transaction count + 3, absolute maximum 1000 |
| `ETHEREUM_CAPTURE_TIMEOUT` | Explicit duration from 1ms through 2m |
| `CI`, `GITHUB_ACTIONS` | Any nonempty value rejects capture, even `false`; do not unset to bypass |

The capture ceiling is separate from the **unchanged 100-attempt smoke ceiling**:
the selected London block alone requires 262 attempts without retries. Allowances
are per process; restarting requires explicit action and replenishes them. Every
actual HTTP attempt is counted, including failed requests, retries, both chain
checks and each receipt. The tool has no recovery/head operations; if additional
methods are introduced later they must share this client/counter. A cancellation
race may conservatively consume an allowance before network delivery. Attempt
counts are not Infura credits, account quotas or a price estimate.

Per-request timeout is 15s, at most three attempts, 500ms initial/5s maximum
backoff and 200ms pacing. The response ceiling is 8 MiB and the aggregate raw
snapshot ceiling is 64 MiB. The operation context starts before client creation,
covering acquisition, replay validation and pre-write checks. It excludes Go
compilation/config loading. File write/sync and cancellation cleanup are
cooperative, not a hard real-time process deadline. The two-minute bound may
stop a heavily retried block before its request allowance is exhausted; no
partial snapshot is accepted and no bound is silently increased.

Only after separate authorization to capture, from the repository root:

```bash
mkdir -p .mainnet/fixtures
bash -c '
  set +x
  IFS= read -r -s -p "Private Infura Mainnet URL: " ETHEREUM_RPC_URL || exit 2
  printf "\n"
  export ETHEREUM_RPC_URL
  ETHEREUM_CAPTURE_OPT_IN=true \
  ETHEREUM_CAPTURE_MAX_REQUESTS=100 \
  ETHEREUM_CAPTURE_TIMEOUT=2m \
  go run ./services/extractor-service/cmd/fixture-capture capture \
    test/fixtures/mainnet/v1/manifest.json historical-smoke \
    .mainnet/fixtures/historical-smoke.json
'
```

This command was **not run** in Stage 13.3A. Selection/configuration is validated
before network access. Existing destinations and missing parent directories fail
before acquisition. Output is exclusively created with mode 0600 only after a
complete validated acquisition and secret-echo check. Both literal and JSON-escaped
configured endpoint/key echoes cause refusal, never source redaction. No endpoint
or key is included in artifact metadata or logs. Environments remain visible to
trusted-host administrators. Success/error output is fixed text, without raw
payloads, provider details or paths. The artifact itself records counts/attempts.

An interrupted file write may leave an incomplete file: retain it for inspection,
require offline validation and choose a new output path for a separately approved
retry. Existing output is never overwritten and nothing is automatically deleted.
No capture has a checkpoint, Inbox, Outbox or downstream side effect.

## Planned full ETL tests — not implemented here

After capture/review and separately scoped real-data regression work:

1. Validate every artifact offline with the production decoder, SHA-256, pinned
   identity, receipt completeness/order, total counts and exact feature witnesses.
   Preserve unknown fields byte-for-byte and original provenance. Missing type 4
   or creation fixtures must remain visible gaps, never synthetic substitutes.
2. Extend the existing local fixture server to serve reviewed immutable artifacts;
   block unsupported methods/identities. Keep all E2E networking local and reuse
   existing Compose/test runners with isolated disposable storage and own-schema
   test roles. Do not mix synthetic and Mainnet checkpoints merely because both
   use chain ID 1. Noncontiguous selected heights require isolated runs, not a
   relaxation of ordered extraction or checkpoint semantics.
3. Verify raw bytes/event ID/checkpoint/Outbox at Extractor's durable boundary,
   Transformer Inbox acceptance/completion and transformed Outbox, and Loader
   Inbox completion plus exact block/transaction/receipt/log counts and fields.
   Check type-specific fields, failed status, contract address and topics/data;
   preserve historical receipt roots without inventing status. Receivers use
   only their own schemas; inter-service transport remains gRPC.
4. Replay exact event IDs and lost ACKs; assert idempotency, retained history,
   original provenance and no duplicate normalized rows. Source/snapshot failure
   must leave durable progress unchanged. Assert `canonical_status=unknown`.

This is a test plan, not a Stage 13.4 implementation or E2E success claim.

## Remaining blockers

- No actual captured artifacts; full real-data validation/ETL remains pending.
- Type 4 and successful top-level contract creation still need verified witnesses.
- Separate authorization and private valid credentials are required for capture;
  provider historical availability, credits, payload size and runtime are not
  inferred from explorer metadata or the single user-reported smoke.
- Checksums and explorer evidence are not cryptographic Ethereum validation.
- Base64 adds storage/memory overhead; payload budgets are not process RSS limits.
- No Infura/other live Ethereum RPC, fixture capture, Stage 13.4/13.5 work,
  commit or push was performed in this increment.

## Verification — 2026-10-27

All commands below exited 0 on the current macOS working tree:

| Check | Result |
| --- | --- |
| Targeted fixture adapter + CLI tests, `go test -count=1` | PASS (0.684s / 1.212s before final refinements) |
| `go test -count=1 ./...` | PASS; final fixture adapter 3.894s, CLI 3.367s |
| `go test -race -count=1 ./...` | PASS; final fixture adapter 11.008s, CLI 10.364s |
| `go vet ./...` | PASS, no diagnostics |
| `gofmt` on new Go files; `gofmt -l` across tracked/untracked nonignored Go files | PASS, no unformatted paths |
| `git diff --check` | PASS |

Tests cover opt-in/CI/endpoint/budget/deadline guards, manifest versions and
selection completeness, exact bytes and permissions, no overwrite, missing or
reordered receipts, pinned identity, checksums, false counts/coverage, incomplete
files, literal/escaped secret refusal, cancellation and real adapter budget
exhaustion/retry accounting. Time-dependent acquisition tests use `synctest` and
an in-memory transport. Architecture boundary tests are included in both full
runs. No assertions were weakened and no arbitrary sleeps were introduced.

PostgreSQL/Compose integration runners were not executed in this tooling-only
increment. Ordinary package tests are not evidence of a real-data E2E run.
Public metadata research encountered HTTP rejection with Python's HTTP client;
curl successfully read the selected block metadata. The additional contract
metadata request returned HTTP 500 and remains an explicit selection gap.

New files: `cmd/fixture-capture/{main.go,main_test.go}` and
`internal/adapters/fixturecapture/{manifest.go,config.go,artifact.go,fixture_test.go}`
under Extractor; `test/fixtures/mainnet/v1/{manifest.json,README.md}`; this runbook.
Updated files: `.env.example`, `.github/workflows/ci.yml`, `PLAN.md`, `README.md`,
`docs/architecture/mainnet-stage13.md`, `docs/architecture/rpc-reliability-stage13.md`.
All prior uncommitted Stage 13 work and merged gRPC test fixes were preserved.
