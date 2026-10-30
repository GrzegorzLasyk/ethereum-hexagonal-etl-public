# Stage 13.4A — Live Mainnet E2E preparation

Status: implementation and offline verification only. No live Ethereum RPC call
is authorized or performed by this stage. The target is chain 1, block 1000000,
hash `8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e`,
with 2 transactions, 2 historical-root receipts and 1 log, confirmed by Stage 13.3C.

## Finite acquisition audit and correction

Previously, `acquisition.Run` returned when its range finished and the composition
root immediately canceled the dispatcher. Checkpoint/raw/Outbox commit was durable,
but Outbox delivery could still be pending. Exit zero was not E2E completion.

Finite index/reindex now keeps the existing dispatcher alive while the application
`Drain` use case observes an Extractor-owned port. PostgreSQL checks coverage of
every requested height and delivery of **all retained raw observations** in that
range, including historical branches. It never deletes observations/events, resets
attempts, marks an event delivered itself or queries downstream schemas.

`EXTRACTOR_DRAIN_TIMEOUT` defaults to 30s, accepts 1ms–2m and is also constrained by
the existing whole-run deadline. A missing/pending/sending event cannot complete
drain. Failed events, database errors or timeout return nonzero while retaining
state. Sender cancellation/join occurs after drain succeeds or fails. Acquisition
errors/cancellation do not start a fresh drain budget. Follow-head stays continuous;
dispatch-only and explicit replay retain their existing modes. Reindex still
preserves its separate progress/canonicality rules. SIGTERM is joined safely;
interrupted acquisition is not a successful finite completion.

Drain confirms the Extractor's durable handoff to Transformer, not Loader success.
The separate operator verifier is required for full E2E acceptance.

## Read-only bounded completion verifier

```sh
go run ./tools/mainnet verify .mainnet/stage13.env
```

Use the existing private Mainnet configuration path, adjusting the path above if
it was initialized elsewhere. `verify` requires explicit start=end=1000000 and
chain 1. It does not require RPC credentials or opt-in and never starts acquisition,
containers, migrations or retries of failed events. Existing Compose project and
volume ownership checks remain mandatory. PostgreSQL and consumers should already
be running after `up`.

`MAINNET_VERIFY_TIMEOUT` defaults to 2m and accepts 1ms–2m. It bounds the verifier
itself, including its Docker preflight. Each role observation has a 3s context and
2s PostgreSQL statement timeout. The command returns nonzero for incomplete,
unavailable, failed or stuck state. Cancellation/command cleanup is cooperative;
compilation and OS/Docker shutdown overhead are not a hard wall-clock SLA.

The technical operator adapter opens separate `psql` sessions as
`extractor_runtime`, `transformer_runtime` and `loader_runtime`; each statement
references only its owning schema, with explicit read-only transactions. Services
receive no new credentials or privileges. The trusted operator already holds the
private environment credentials. No cross-service transaction or shared domain
model is introduced. There is no schema/protobuf/ADR change.

The verifier checks:

- Exact pinned chain/height/hash; raw block identity, 2 full transactions, 2 receipts,
  receipt membership/index/block identity and total 1 log. Checkpoint start and tip
  must both equal the target and active membership must exist.
- Extractor Outbox `delivered`; Transformer Inbox `completed`, transformed snapshot
  and corresponding Outbox `delivered`; Loader Inbox `completed`. Missing rows fail.
- Stable raw/transformed/source event identities, raw-byte digests across the first
  handoff, transformed-payload digest across the second, and original provenance.
- Loader's exact block identity and event, 2 transactions, 2 receipts, 1 log,
  transaction hashes/order matching original raw data, `canonical_status=unknown`
  and no height conflict. Existing Loader foreign keys enforce receipt/log membership.
- Failed events immediately. An expired sending/processing lease is reported as
  stuck, even though a worker may subsequently recover it. Pending/retrying work
  waits within the deadline; an active but non-progressing lease cannot pass and
  eventually times out. No automatic repair is performed; [Stage 13.5](operations-stage13.md) provides explicit recovery commands.

DB/query failures are reported as unavailable, never healthy zero counts. Output
contains fixed status categories and counts; it does not print SQL errors, payloads,
provenance values, DSNs or credentials. Queries compare digests but do not export
source payloads. Separate service observations are not a global atomic snapshot;
do not run operator replay/repair concurrently with an acceptance check.

## Safe commands prepared for separate live authorization

Preserve the existing `.mainnet/stage13.env`, its generated project and passwords.
Do not run `init` over it or copy fixture project credentials. Edit the mode-0600
file privately (literal `NAME=value`, not shell syntax), setting:

```text
EXTRACTOR_CHAIN_ID=1
EXTRACTOR_MAINNET_OPT_IN=true
EXTRACTOR_START_BLOCK=1000000
EXTRACTOR_END_BLOCK=1000000
EXTRACTOR_CONFIRMATION_DEPTH=0
EXTRACTOR_RECOVERY_DEPTH=1
EXTRACTOR_MAX_RPC_REQUESTS=100
EXTRACTOR_RUN_TIMEOUT=2m
EXTRACTOR_DRAIN_TIMEOUT=30s
MAINNET_VERIFY_TIMEOUT=2m
```

Retain the private existing `EXTRACTOR_RPC_URL` in that file; never paste an actual
key/URL into a shell command, terminal output, issue or documentation. Do not source
this file or run shell tracing. Zero confirmation depth is an explicit historical
test choice, not a finality claim. A pre-existing incompatible checkpoint/start
must be investigated; never reset it to make this test pass.

Preparation without live acquisition:

```sh
go run ./tools/mainnet check .mainnet/stage13.env
go run ./tools/mainnet up .mainnet/stage13.env
```

Only **after separate authorization to call Infura**:

```sh
go run ./tools/mainnet e2e .mainnet/stage13.env
```

`e2e` independently rejects other ranges, more than 100 actual RPC attempts, run
timeouts over 2m, absent opt-in and any nonempty `CI`/`GITHUB_ACTIONS` marker before
Docker invocation. It runs one foreground finite Extractor in the existing isolated
Mainnet Compose project, with no automatic restart. It then runs the bounded
read-only completion verifier. No provider request is needed for verification.
Generic `extract` remains the previously approved Stage 13.1 range tool; use `e2e`
for these stricter Stage 13.4A guards.

On acquisition/drain failure the command stops nonzero; do not blindly repeat it,
as each new acquisition process replenishes its request budget. Diagnose safely:

```sh
go run ./tools/mainnet verify .mainnet/stage13.env
go run ./tools/mainnet stop .mainnet/stage13.env
```

These commands never contact Ethereum. Stop preserves all historical data,
Outbox/Inbox rows, volumes and private configuration. Verification can be repeated
without replenishing an Ethereum budget or changing any persistent state.

The existing indexed extraction path samples/rechecks headers, unlike the earlier
5-request single-block smoke. Code inspection predicts **20 attempts without
retries** for a fresh stable one-block run: head (3), complete block/receipts (5),
block/anchor header rechecks (6), then completion-cycle head/checkpoint checks (6).
Head movement, retries or recovery can increase that count; all share the enforced
100-attempt ceiling. No live count is claimed. Acquisition plus drain has a 2m
process budget; completion observation has a separate maximum 2m. The CLI bounds
its overall E2E orchestration by those budgets plus 40s shutdown allowance.

## Offline evidence and limits

Unit tests use virtual time for drain, delayed completion, failure and cancellation.
PostgreSQL tests cover pending checkpoint, missing heights, failed delivery and
successful drain. The existing synthetic and authentic-data handoff runners execute
the actual verifier queries using isolated runtime roles, reject partial completion,
exercise timeout, and inject failed Inbox/Outbox and expired leases in rolled-back
local transactions. Existing raw/normalized field-by-field assertions remain.

No claim of live provider availability, mainnet acquisition success, cryptographic
consensus validation, canonicality or type-4/creation coverage follows from these offline results. Ordinary CI needs no private
fixtures or live credentials. See [validation results](validation-stage13-4a.md).
