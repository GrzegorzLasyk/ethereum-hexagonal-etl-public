# Documentation inventory and Stage 15 completion record

The [README](../../README.md) is the entry point; [service guides](../services/extractor.md)
follow the actual implementation and [learning chapters](README.md) explain why it
uses these boundaries. The [source index](source-index.md) locates every handwritten
Go file and its declarations. These pages complement the accepted ADRs and retain
older operational detail in the [runtime reference](../reference/runtime-reference.md).

## Which document is authoritative?

| Question | Read first |
|---|---|
| Approved design and subsequent amendments | ADR 0001–0004, read in order; later amendments qualify earlier stage constraints |
| Implemented runtime behavior | Current service source, migrations and validated configuration |
| Learning the implementation | README → service guide → learning chapter → linked source |
| Exact recovery command and operator safety | Stage 13 operations and Stage 14 release runbook |
| What validation actually ran | Phase 1 offline, Phase 2 performance and Phase 3 live execution records |
| What CI executes | `.github/workflows/ci.yml` and `scripts/ci-integration.sh` |

An earlier stage's design/validation is historical evidence, not a claim that later
features already existed then. Documentation does not grant permission to perform
live acquisition or destructive cleanup. Stage 15 adds neither production
certification nor new runtime guarantees.

## Architecture and operational library

The complete retained library is linked below. Stage-numbered documents keep their
original scope; accepted ADR text has not been rewritten.

### Accepted decisions

- [ADR 0001: MVP transport, storage, and reliable service handoff](../adr/0001-mvp-transport-and-storage.md).
- [ADR 0002: Ethereum source data and single-block extraction boundary](../adr/0002-ethereum-source-data.md).
- [ADR 0003: Loader conflict observation without canonical-chain selection](../adr/0003-loader-conflict-observation.md).
- [ADR 0004: Ordered extraction and provider-relative reorg recovery](../adr/0004-ordered-extraction-reorg-recovery.md).

### Design, runbooks and validation evidence

- [Stage 14.2 — Resource limits and end-to-end backpressure](../architecture/backpressure-stage14.md).
- [Safe backup and restore rehearsal (Stage 12.5)](../architecture/backup-restore-stage12.md).
- [Ordered extraction, recovery and operator commands (Stage 10)](../architecture/extraction-stage10.md).
- [Outbox dispatch and reliable gRPC handoff (Stage 9)](../architecture/handoff-stage9.md).
- [Stage 14.4 — Bounded PostgreSQL writes in Loader](../architecture/loader-batching-stage14.md).
- [Loader Inbox and normalized persistence (Stage 8)](../architecture/loader-stage8.md).
- [Local complete ETL with deterministic fixtures (Stage 11)](../architecture/local-stage11.md).
- [Stage 12.2: structured logging and event correlation](../architecture/logging-stage12.md).
- [Stage 13.3B — Controlled capture preflight](../architecture/mainnet-capture-stage13.md).
- [Stage 13.4A — Live Mainnet E2E preparation](../architecture/mainnet-e2e-stage13.md).
- [Stage 13.3A — Mainnet fixture infrastructure and selection](../architecture/mainnet-fixtures-stage13.md).
- [Stage 13.3C — Authentic Mainnet offline ETL regression](../architecture/mainnet-offline-stage13.md).
- [Stage 13.1 — Isolated Mainnet environment and execution safety](../architecture/mainnet-stage13.md).
- [Stage 12.3: local Prometheus metrics](../architecture/metrics-stage12.md).
- [MVP operations and storage (Stage 12.5)](../architecture/operations-stage12.md).
- [Stage 13.5 — Mainnet recovery and operational acceptance](../architecture/operations-stage13.md).
- [Stage 14.1 — Offline performance baseline](../architecture/performance-stage14.md).
- [Protobuf contracts and receiver boundary (Stage 6)](../architecture/protobuf-contracts.md).
- [Stage 12.4: readiness and service-owned database validation](../architecture/readiness-stage12.md).
- [Stage 14.3 — Bounded concurrent receipt acquisition](../architecture/receipts-stage14.md).
- [MVP release readiness (Stage 12.5)](../architecture/release-stage12.md).
- [Stage 14.5 — Final configuration and release readiness](../architecture/release-stage14.md).
- [Stage 13.2A — RPC reliability and offline validation](../architecture/rpc-reliability-stage13.md).
- [Final static review and release preparation](../architecture/static-review-stage14.md).
- [Storage measurements (Stage 12.5)](../architecture/storage-measurements-stage12.md).
- [Transformer Inbox, transformation and Outbox (Stage 7)](../architecture/transformer-stage7.md).
- [Final validation — Phase 3 controlled live execution record](../architecture/validation-live-stage14.md).
- [Final validation — Phase 1 offline execution record](../architecture/validation-offline-stage14.md).
- [Final validation — Phase 2 performance execution record](../architecture/validation-performance-stage14.md).
- [Stage 12.5 verification record — 2026-10-26](../architecture/validation-stage12.md).
- [Stage 13.3C validation — 2026-10-28](../architecture/validation-stage13-3c.md).
- [Stage 13.4A validation — 2026-10-28](../architecture/validation-stage13-4a.md).
- [Stage 13 final validation and resume record](../architecture/validation-stage13-final.md).
- [Stage 13.1 verification — 2026-10-27](../architecture/validation-stage13.md).

## Execution and storage sources

| Area | Source and responsibility |
|---|---|
| Repository constraints and staged history | [AGENTS](../../AGENTS.md), [PLAN](../../PLAN.md) |
| Single module and versions | [go.mod](../../go.mod); root Go commands cover all three services |
| Service composition and ports | [Extractor](../services/extractor.md), [Transformer](../services/transformer.md), [Loader](../services/loader.md) |
| Database base and migration jobs | [compose.yaml](../../deployments/compose/compose.yaml), [initialization](../../deployments/compose/init), [migration scripts](../../deployments/compose/scripts) |
| Synthetic local application | [fixture.yaml](../../deployments/compose/fixture.yaml), [Dockerfile](../../deployments/docker/Dockerfile) |
| Isolated Mainnet and restore topology | [mainnet.yaml](../../deployments/compose/mainnet.yaml), [storage.yaml](../../deployments/compose/storage.yaml) |
| Owning-role DDL and readiness metadata | [Extractor migrations](../../migrations/extractor), [Transformer migrations](../../migrations/transformer), [Loader migrations](../../migrations/loader) |
| Configuration examples | [.env.example](../../.env.example); fixture/private initializers create local credentials, never copy committed secrets |
| gRPC contract and generation | [proto](../../proto/etl/v1), [generation script](../../scripts/generate-proto.sh); generated Go remains unchanged |
| CI gates | [workflow](../../.github/workflows/ci.yml), [required integration PASS checks](../../scripts/ci-integration.sh) |
| Final bounded phases | [validate-final.sh](../../scripts/validate-final.sh), [Stage 13 gate](../../scripts/validate-stage13.sh) |
| Convenience commands | [Makefile](../../Makefile); no implicit live target and no volume deletion |
| Deterministic data and authentic pins | [synthetic fixture](../../test/fixture), [versioned manifest](../../test/fixtures/mainnet/v1/manifest.json) |
| Recorded storage measurements | [Stage 12 JSON](../measurements/stage12-storage.json); not new Stage 15 measurements |

Private `.mainnet/` artifacts, environment files and benchmark outputs are neither
copied into these guides nor required for ordinary unit tests. Authentic replay
requires the explicit local path and manifest validation. Synthetic fixtures are
always identified as synthetic.

## Stage 15 status

| Phase | Completion |
|---|---|
| 15.1 | Expanded README, safe developer setup, test command classification, retained detailed runtime reference |
| 15.2 | Three service guides and six source-linked learning chapters |
| 15.3 | Introductions for 283 handwritten Go files; documented package-level declarations, functions/methods and explicit interface methods |
| 15.4 | Root Makefile wrappers, source/document index, compatibility anchors for old README links and consistency review |

The resumed work preserved the already-written README, runtime reference and six
learning chapters. It completed service guides, source comments, indexes, Makefile
and PLAN updates. The application remains a three-service educational reference.

## Review and validation boundary

The comment inventory excludes generated files, local variables and anonymous
functions. Struct-field comments are selective: describe non-obvious presence,
identity and ownership rules rather than repeating every field name. Explicit
interface methods and package-level constant/variable declarations are included.

Stage 15 checks formatting, comment-free Go syntax equivalence, preserved build
directives, build/vet, local documentation links and safe Makefile command guards.
No PostgreSQL/E2E suite, benchmark, capture or live RPC is rerun. Consult the final
Stage 15 report for exact commands/results; historical Phase 1–3 results remain
in their original records. Hosted CI and review on the intended release commit
remain separate release requirements.

### Checks completed for this documentation change

- **PASS:** `go build ./...` and `go vet ./...` for the single root module.
  The sandbox initially denied the standard Go cache; the authorized runs outside
  that restriction completed successfully. No services or integration runners ran.
- **PASS:** gofmt for all 283 handwritten files and `git diff --check`.
- **PASS:** comment-free syntax comparison for all 283 files; Go build/compiler
  directives unchanged. Generated code, accepted ADRs, protobuf, migrations and
  module/dependency files remain unchanged.
- **PASS:** all 1608 inventoried package-level declarations and explicit interface
  methods have comments: 1074 functions/methods, 227 structs, 41 interfaces, 41
  other types, 167 constant/variable specifications and 58 interface methods.
  Every exported declaration comment begins with its declaration name. The one
  named function-local helper type is documented separately from these counts.
- **PASS:** 668 local Markdown paths/anchors checked; old README links retain
  compatibility anchors without changing accepted ADRs.
- **PASS:** `make help`, recipe dry-runs, and expected guard rejection for
  `make test-offline`, `make test-performance` and `make test-performance OPT_IN=1`
  without the required arguments. Guards fail before invoking a runner.
- **NOT RUN:** Go unit/race suites, PostgreSQL integration, Docker E2E, performance
  workloads, capture and live RPC. Documentation changes do not replace those
  release gates or create new execution evidence.
