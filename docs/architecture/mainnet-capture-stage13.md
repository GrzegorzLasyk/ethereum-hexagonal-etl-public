# Stage 13.3B — Controlled capture preflight

Subsequent status: three captured artifacts are now available locally and verified
by the [Stage 13.3C offline ETL regression](mainnet-offline-stage13.md). The
preflight/selection status below records the earlier stage; it is not a claim
that capture is still pending. Type 4 and top-level creation coverage remain missing.

Status: offline preflight only. **No live RPC or fixture capture was executed.**
Capture requires separate explicit user authorization. The user-reported Stage
13.2B smoke remains evidence for block 1000000 only, not these future artifacts.

## Audit and offline corrections

Reviewed AGENTS.md, PLAN.md, accepted ADR 0001–0004, Stage 13.1/13.2 documentation,
the [Stage 13.3A runbook](mainnet-fixtures-stage13.md), v1 selection manifest,
capture CLI, fixture tooling and production RPC decoder/retry implementation.
No production service, ADR, protobuf, database or canonicality behavior changes.

Two offline verification gaps were corrected before any acquisition:

- `validate` checked self-consistency against the candidate embedded in the
  artifact. New `verify MANIFEST CASE ARTIFACT` also requires exact agreement
  with the independently reviewed manifest entry. Use `verify` before advancing
  to the next capture; `validate` remains a structural inspection command.
- Secret checks previously ran only while capturing. Offline validation now
  rejects Infura endpoint references in metadata and decoded raw JSON; `verify`
  additionally checks the configured URL/key when `ETHEREUM_RPC_URL` is present.
  Literal and JSON-escaped strings are inspected, including unknown fields.
  Endpoint configuration now permits a single unescaped API-key path component,
  preventing ambiguous token extraction. No query, userinfo, fragment or redirect.

Capture still validates its complete snapshot through the existing RPC decoder
before writing, compares the pinned hash, counts and coverage witnesses, and
checks every raw result for configured endpoint/key echoes. It refuses rather
than redacts any such response. Logs are fixed generic text: no config, paths,
provider messages, response bodies or secrets. Files use exclusive creation and
0600 permissions. A raced destination or pre-existing symlink is not overwritten.
An interrupted disk write may leave a partial file, which fails offline validation
and must be retained for inspection; the sequence stops without automatic cleanup.

Byte preservation follows ADR 0001/0002: full block and every receipt JSON result,
including all transaction objects, logs, unknown fields and internal whitespace,
are base64-encoded without JSON reserialization. SHA-256 covers the original bytes
in block/receipt order. HTTP headers, request URLs and RPC envelope IDs are not
source payloads. Offline replay has an explicit in-memory transport with no live
fallback; a regression installs a failing default HTTP transport to check this.

Secret protection has a precise scope: configured URL/key echoes and recognizable
Infura endpoint references are rejected. An arbitrary unknown secret encoded in
an unrelated representation cannot be identified conclusively. Offline checking
without credentials still checks endpoint references; known-key checking requires
the same private endpoint in the environment. Inspect artifacts privately before
any future publication. No key value was read or printed during this preflight.

## Reviewed acquisition order and pinned identities

All entries use chain ID `1`. The manifest is unchanged; its existing public
explorer evidence was reviewed locally, without additional network research.

| Order / case | Block | Expected transactions / receipts | No-retry attempts | Three-attempt upper bound | Requested ceiling |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 / historical-smoke | 1000000 | 2 / 2 | 5 | 15 | 100 |
| 2 / pectra-blob-dynamic-fee | 22431084 | 95 / 95 | 98 | 294 | 300 |
| 3 / london-access-list | 12965000 | 259 / 259 | 262 | 786 | 800 |

```text
1000000  0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e
22431084 0x50c8cab760b2948349c590461b166773c45d8f4858cccf5a43025ab2960152e8
12965000 0x9b83c12c69edb74f6c8dd5d052765c1adf940e320bd1291696e6fa07829eee71
```

The latter two receipt counts are completeness requirements, not previously
captured measurements. Whole-block log counts remain unknown. No receipt is
omitted to fit a budget. Each run uses one fresh adapter, exactly one selected
block, two chain checks, and one receipt per transaction. Failed HTTP/transport
attempts and all retries consume the same atomic budget. No discovery/head calls
or outer retry loop occur. The absolute tool ceiling stays 1000; the commands
below use the user's stricter 100/300/800 limits. Restart would replenish a budget
and requires a new explicit decision, never an automatic retry of the whole run.

Each capture has a **two-minute operation context**, including RPC pacing and
backoff. Each HTTP attempt is bounded by 15s; each RPC has at most three attempts,
500ms initial/5s maximum backoff with jitter and Retry-After handling, and 200ms
pacing. 401/402/403 are permanent. Response/aggregate limits are 8 MiB/64 MiB.
Even the three-attempt theoretical bounds are not completion guarantees: the
deadline may stop acquisition first. Go compilation is outside this operation
deadline; local write/sync and cleanup are cooperative. These counts are HTTP
attempts, not provider credits or a monetary estimate.

## Exact commands — prepared, not executed

From the repository root, after separate authorization for the applicable live
captures. This sequence stops on the first failed capture **or verification**;
do not resume at the next block after a failure. For authorization of only the
first capture, execute only its capture/verify pair.

```bash
bash -euo pipefail <<'CAPTURE'
set +x
test -z "${CI:-}"
test -z "${GITHUB_ACTIONS:-}"
test -d .mainnet && test ! -L .mainnet
if [ ! -e .mainnet/fixtures ]; then
  mkdir -m 700 .mainnet/fixtures
fi
test -d .mainnet/fixtures && test ! -L .mainnet/fixtures
# Deliberately fail if this run directory already exists; never replace it.
mkdir -m 700 .mainnet/fixtures/stage13-3b
out=.mainnet/fixtures/stage13-3b
manifest=test/fixtures/mainnet/v1/manifest.json
tool=./services/extractor-service/cmd/fixture-capture
go run "$tool" manifest "$manifest"

# Read from the terminal, not the heredoc, without echo or shell-history exposure.
IFS= read -r -s -p 'Private existing Infura Mainnet URL: ' ETHEREUM_RPC_URL </dev/tty
printf '\n'
export ETHEREUM_RPC_URL
export ETHEREUM_CAPTURE_OPT_IN=true ETHEREUM_CAPTURE_TIMEOUT=2m

ETHEREUM_CAPTURE_MAX_REQUESTS=100 go run "$tool" capture "$manifest" historical-smoke "$out/1000000.json"
go run "$tool" verify "$manifest" historical-smoke "$out/1000000.json"

ETHEREUM_CAPTURE_MAX_REQUESTS=300 go run "$tool" capture "$manifest" pectra-blob-dynamic-fee "$out/22431084.json"
go run "$tool" verify "$manifest" pectra-blob-dynamic-fee "$out/22431084.json"

ETHEREUM_CAPTURE_MAX_REQUESTS=800 go run "$tool" capture "$manifest" london-access-list "$out/12965000.json"
go run "$tool" verify "$manifest" london-access-list "$out/12965000.json"
CAPTURE
```

No `.env` is sourced automatically. Use the existing private HTTPS endpoint;
never paste its literal value into a command or report. `verify` never contacts
the endpoint even when it reads it for known-key detection. Its operation is
allowed in CI and needs no opt-in. Capture is refused on any nonempty `CI` or
`GITHUB_ACTIONS` marker. The subshell limits secret environment lifetime; trusted
host administrators can still inspect process environments.

Read-only path inspection during preflight found `.mainnet` to be a real 0700
directory and `.mainnet/fixtures` and its `stage13-3b` subdirectory absent. Git
ignores these output paths. No Mainnet output directories/artifacts were created
by this audit; unit tests wrote only synthetic artifacts in temporary test directories.
Compose uses named PostgreSQL volumes (`mainnet_postgres_data` for Mainnet and
separate fixture volumes); these output paths are neither volumes nor bind mounts
for database data. The tool has no PostgreSQL, gRPC, Docker or checkpoint access.
The commands refuse symlink parents under `.mainnet` and existing run directories.

## Acceptance and remaining limits

The first capture is ready for separate authorization after the offline gates
pass. Provider credentials, credits, availability and actual response compatibility
remain live checks; no live authorization is implied by this readiness report.

Real type-4 and successful top-level contract-creation coverage remain pending.
Explorer evidence for types 0–3, failure and logs is not raw-artifact evidence.
No raw captures currently exist, so actual counts, hashes of captured bytes and
real-data decoder success cannot yet be reported. The production Extractor RPC
decoder is tested offline here; Transformer/Loader real-data and full ETL tests
remain subsequent work. Checksums are not signature/trie/consensus verification,
nor protection against coordinated malicious replacement of data and metadata.
ADR 0003/0004 canonicality/history rules remain unchanged. Stage 13.4/13.5,
database/E2E execution, commits and pushes are outside this increment.

## Offline verification results — 2026-10-27

All checks completed with exit code 0 on the current macOS working tree:

| Command/check | Result |
| --- | --- |
| `go test -count=1 ./services/extractor-service/internal/adapters/fixturecapture ./services/extractor-service/cmd/fixture-capture` | PASS, 0.764s / 1.134s before final guard/test refinements |
| `go test -count=1 ./...` | PASS; final fixture package 3.780s, capture CLI 3.161s |
| `go test -race -count=1 ./...` | PASS; final fixture package 11.440s, capture CLI 10.893s |
| `go vet ./...` | PASS, no diagnostics |
| `gofmt` on changed Go files; `gofmt -l` on all tracked/untracked nonignored Go files | PASS, no unformatted paths |
| `git diff --check` | PASS |
| `bash -n` on the documented capture sequence | PASS; syntax checked, sequence not executed |

Deterministic regressions cover reviewed manifest pins and all three budgets,
independent manifest comparison, byte preservation for every receipt, credential
echoes including JSON escapes, receipt identity/index/type, log ordering/removal
and checksum corruption, no default network transport during replay, and a raced
destination/existing symlink. Existing tests cover missing receipts, counts,
type coverage, deadline cancellation, retries and budget exhaustion. Architecture
tests passed as part of both full runs. PostgreSQL and Compose E2E runners were
not executed; ordinary Go package tests are not evidence of those integrations.

Incremental changes: `fixturecapture/{artifact.go,config.go,fixture_test.go}`,
new `fixturecapture/preflight_test.go`, capture CLI `main.go`, this runbook,
`mainnet-fixtures-stage13.md`, fixture README, root README and PLAN.md. No
selection-manifest content, production RPC adapter, ADR, protobuf, schema,
Compose fixture configuration or existing gRPC regression test was changed.
