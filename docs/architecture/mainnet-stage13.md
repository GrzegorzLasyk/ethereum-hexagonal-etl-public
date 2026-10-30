# Stage 13.1 — Isolated Mainnet environment and execution safety

## Boundary and audit findings

Stage 13.1 prepares a bounded local environment. It does not validate Infura
credentials, historical availability, provider quotas, real payload compatibility
or a live end-to-end pipeline. No live RPC is needed for its tests. Stage 13.2A
adds offline reliability validation. The user subsequently reports Stage 13.2B
smoke PASS (block 1000000, 2 transactions, 2 receipts, 5 requests).
[Stage 13.3A](mainnet-fixtures-stage13.md) prepares fixture selection and capture
tooling; three artifacts are now captured and verified offline in Stage 13.3C.
Stage 13.4A and [13.5 recovery](operations-stage13.md) are implemented; live E2E
acceptance remains an explicit user step. ADR 0001–0004 are unchanged.

The Stage 13 audit found an existing JSON-RPC adapter and complete local ETL,
but only a single-block, non-persisting optional live smoke test. Fixture data
also uses chain ID 1, so changing its RPC URL would not isolate Mainnet data.
Acquisition completion does not mean Outbox delivery or downstream completion.
Stage 13.2A now propagates permanent RPC rejection to the outer scheduler,
which halts rather than starting another cycle. Wrong chain also halts. The
Stage 13.1 global request/time bounds remain in force for all attempts. See the
[offline reliability policy](rpc-reliability-stage13.md).

## Safe local setup (no Ethereum requests)

Prerequisites: the repository's Go version, Docker Desktop/Engine and Compose v2.
Run from the repository root on a trusted host:

```sh
mkdir -m 700 .mainnet
go run ./tools/mainnet init .mainnet/local.env
go run ./tools/mainnet check .mainnet/local.env
go run ./tools/mainnet up .mainnet/local.env
```

`init` creates a mode-0600 file, seven independent random PostgreSQL passwords
and a random `ethereum-mainnet-<24 hex digits>` project identifier. It refuses
to overwrite an existing file. Opt-in defaults to false; range, RPC URL,
confirmation/recovery depth and execution budgets are deliberately blank.
`check` validates Compose and volume ownership without starting services.
`up` builds the existing service image, starts PostgreSQL, applies existing
service-owned migrations, and starts Transformer and Loader. It never starts
Extractor. Neither command requires an RPC endpoint or opt-in.

Only this standalone Compose file is used: `deployments/compose/mainnet.yaml`.
The runner pins `-p`, `-f`, and an empty dotenv file, and filters inherited
Compose/service/database overrides. Do not combine it with the fixture overlay.
Its project-scoped `mainnet_postgres_data` volume is distinct from fixture
`postgres_data`; a pre-existing volume must carry matching environment, project
and volume-key labels. Unknown ownership is an error, never a reason to reset
or relabel storage. New configurations create separate projects and volumes;
retain the original configuration to resume its database.

The database remains `ethereum_etl`, with unchanged `extractor`, `transformer`
and `loader` schemas and separate runtime/migration roles. No service receives
another service's credentials. App containers share the PostgreSQL network
namespace so gRPC and health listeners remain loopback-only; there is no fixture
RPC server, ACK proxy, or published host port. The project has its own Docker
network. Docker administration is trusted and can bypass this isolation.

## Configuration and future bounded execution

The private file contains literal `NAME=value` lines. Do not source it or add
shell quoting. Unknown/duplicate keys, symlinks, non-0600 permissions and files
larger than 16 KiB are rejected. Never print the file or resolved Compose config.
The `.mainnet/` directory and `.env*` files are excluded from Git and Docker builds.
Do not put an actual Infura URL/key in documentation, command arguments or issues.

| Setting | Requirement |
| --- | --- |
| `MAINNET_PROJECT` | Generated isolated project identity; do not edit/reuse a fixture identity |
| `EXTRACTOR_MAINNET_OPT_IN` | Exactly `true` for `extract`; generated value is `false` |
| `EXTRACTOR_CHAIN_ID` | Exactly `1`; Compose uses this value for all three services |
| `EXTRACTOR_START_BLOCK`, `EXTRACTOR_END_BLOCK` | Explicit decimal uint64, inclusive end >= start; no follow-head |
| `EXTRACTOR_CONFIRMATION_DEPTH` | Explicit decimal uint64; confirmations are not consensus finality |
| `EXTRACTOR_RECOVERY_DEPTH` | Explicit 1–10000, bounded retained ancestry per ADR 0004 |
| `EXTRACTOR_MAX_RPC_REQUESTS` | Explicit positive uint64; total HTTP attempt allowance for one Extractor process |
| `EXTRACTOR_RUN_TIMEOUT` | Explicit Go duration, 1 ms–24 h; total process work deadline |
| `EXTRACTOR_RPC_URL` | Private HTTPS endpoint without userinfo or fragment; no network validation in preflight |
| `EXTRACTOR_MAX_OUTSTANDING_EVENTS` | Positive admission watermark; initialized to 100 |
| `EXTRACTOR_MAX_OUTSTANDING_BYTES` | Positive bytes watermark; initialized to 67108864 |
| `EXTRACTOR_MAX_DATABASE_BYTES` | Positive database bytes watermark; initialized to 1073741824; not a disk quota |
| Seven `*_PASSWORD` settings | Generated random hex credentials; retain privately with the project |

Compose pins `EXTRACTOR_ENVIRONMENT=mainnet`, `EXTRACTOR_MODE=index` and the
non-secret provider label `infura-mainnet`. Ordinary service configuration also
rejects non-loopback acquisition unless Mainnet safety is explicitly selected.
Local loopback fixture range/follow-head behavior is unchanged. Existing adapter
per-request timeout (10 s), pacing (200 ms), three attempts, backoff and payload
limits remain in force; they do not replace the total budgets.

After separate authorization for live work and selection of a small reviewed
range and budget, the execution command will be:

```sh
go run ./tools/mainnet extract .mainnet/local.env
```

**Do not run this command during Stage 13.1 verification.** It validates opt-in
and every bound before Docker invocation, then starts exactly one foreground
Extractor with no automatic restart. A fixed per-project acquisition container
name prevents overlapping invocations. `--no-deps` requires prior `up` and does
not start or replace other services. The service repeats safety validation before
database/RPC I/O. The runner suppresses subprocess output, including failures,
to avoid leaking credentials; inspect only the services' redacted structured logs
through trusted Docker access when diagnosing an error.

## Request and time accounting

One concurrency-safe counter in the JSON-RPC client reserves a slot immediately
before each HTTP dispatch, after pacing and request construction. All methods
share it: `eth_chainId`, blocks, each receipt, head/ancestor verification and
recovery. Every retry reserves another slot. Transport failures count because
delivery may be ambiguous. Redirects remain disabled. `Client.Requests()` exposes
the process-local count for tests/diagnostics; it is not a Prometheus metric.

At most the configured number of HTTP attempts can be dispatched. Requests
canceled before reservation consume no slot. Cancellation racing dispatch may
consume a slot without reaching the provider; slots are never refunded. This is
a conservative attempt ceiling, not a count of successful responses, provider
credits, TCP reconnects or billable units. Limits are shared within one client/run,
not across independent projects or external clients using the same provider key.

Exhaustion returns an application-port sentinel, halts cycle retries, and emits
the safe code `rpc_budget_exhausted`. An incomplete snapshot is not committed.
A completed last allowed response can still participate in a valid local commit;
the next attempted request is refused. No special database mutation, deletion,
Inbox reset or checkpoint rewind is authorized by exhaustion.

The run deadline starts before database setup and flows to RPC, backoff, pacing,
database work and dispatcher shutdown. Even if the worker returns gracefully on
cancellation, expiration returns failure with `run_budget_exhausted`. Shutdown
joins in-flight work; cancellation is cooperative and cleanup can finish after
the nominal deadline. An ambiguous database COMMIT is handled by the existing
identity/revision fencing, never by resetting progress.

Budgets reset only on an explicit new process invocation. Original checkpoint
and pinned start survive; keep the same start when resuming. This is not a durable
lifetime account quota. Do not configure an external restart loop: it would
replenish per-run budgets. Pending Outbox/Inbox work is retained; consumers can
continue processing previously accepted work. Range completion or budget exit
does not certify a drained E2E pipeline. A reviewed dispatcher/drain workflow
and live data assertions are prepared in [Stage 13.4A](mainnet-e2e-stage13.md).
Finite index/reindex now drains its own Outbox before a successful exit; the
separate verifier is still required to establish Loader completion.

## Stop, retention and limitations

```sh
go run ./tools/mainnet stop .mainnet/local.env
```

Stop retains containers, volumes, historical observations and credentials. There
is deliberately no delete/reset command or automatic volume cleanup. Back up and
restore according to the Stage 12 runbook before any separately reviewed removal.
Do not manually repurpose a Mainnet volume for fixtures, or vice versa; Docker
labels prove tooling ownership, not cryptographic provenance of database contents.

The local trusted-host security boundary remains: plaintext loopback gRPC,
PostgreSQL `sslmode=disable`, privileged host access to environment secrets,
no production TLS/authentication, HA, PITR or automatic secret rotation.
Loader candidates remain `canonical_status=unknown`. No live compatibility,
provider billing, performance or production readiness is claimed.

## Offline verification

Unit tests cover invalid/missing configuration, inherited override filtering,
private file handling, foreign-volume refusal, no acquisition during setup,
attempt accounting across methods/retries/concurrency, incomplete receipts,
deadline cancellation, preserved checkpoint/history/Outbox and restart progress.
Existing architecture, PostgreSQL, handoff and full fixture E2E gates remain
mandatory. No test enables the `live` build tag or supplies Infura credentials.

See the [actual Stage 13.1 validation record](validation-stage13.md) for commands,
results, corrected setup failure, retained resources and platform limitations.
