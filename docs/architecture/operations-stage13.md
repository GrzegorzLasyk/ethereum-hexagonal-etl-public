# Stage 13.5 — Mainnet recovery and operational acceptance

Stage 13.4B is prepared for explicit user execution; it has **not** been run by
this implementation. Stage 13.5 adds service-owned failed-event repair and bounded
operator commands. The latest targeted corrections pass; the final full
post-correction pass and user-run live acceptance remain outstanding. It preserves ADR 0001–0004, stable event identities, exact raw
bytes, all historical candidates and Loader `canonical_status=unknown`.

## Prepared live acceptance (user only)

Use the existing isolated Mainnet project and private mode-0600 configuration.
Do not create a new environment to hide an incompatible checkpoint. Follow the
[Stage 13.4A configuration](mainnet-e2e-stage13.md#safe-commands-prepared-for-separate-live-authorization):
chain 1, explicit start=end=1000000, confirmation depth 0, recovery depth 1,
maximum 100 actual RPC attempts, maximum 2m acquisition including dispatch drain,
30s drain and maximum 2m completion observation. Privately retain the existing
Infura URL in the configuration; never put it in arguments, logs or tickets.

From the repository root, substituting the existing private configuration path:

```sh
# Offline preparation; apply all three services' forward migrations.
go run ./tools/mainnet check .mainnet/stage13.env
go run ./tools/mainnet up .mainnet/stage13.env

# USER EXECUTION ONLY: this command contacts Infura.
go run ./tools/mainnet e2e .mainnet/stage13.env

# Read-only acceptance, no Ethereum requests.
go run ./tools/mainnet verify .mainnet/stage13.env
```

`e2e` refuses absent opt-in, other ranges, budget >100, acquisition >2m and CI.
Every actual JSON-RPC HTTP attempt shares the process budget, including chain ID,
headers, receipts, retries and recovery. No wrapper restarts acquisition. A fresh
stable run is estimated at 20 attempts without retries; this is not a measured
live result. The earlier user-reported 5-request smoke did not exercise ETL.
Exit zero requires raw/checkpoint and both durable handoffs plus Loader's pinned
block, 2 transactions, 2 receipts and 1 log. A checkpoint or durable ACK alone is
insufficient. Nonzero means investigate; do not repeatedly replenish live budgets.

## Diagnose before changing state

Run `verify` while PostgreSQL/consumers are available. It uses separate owning
runtime-role connections, read-only transactions and bounded statements. It
reports incomplete, failed, stuck or unavailable state without payloads/SQL errors.
Follow the [service-owned queue queries](operations-stage12.md#incident-sequence)
for detailed diagnosis, using a private PostgreSQL client and one own role per
connection. Do not expose host ports, copy runtime passwords into arguments or
print resolved Compose configuration. The operator holds credentials; services
still cannot inspect another service's schema.

For this target, the retained event identities are:

```text
raw-block/v1/1/1000000/8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e
transformed-block/v1/1/1000000/8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e
```

| Observed state | Action |
| --- | --- |
| DB unavailable/not ready | Restore PostgreSQL, disk space and required service-owned migrations/privileges; never interpret missing metrics as zero. |
| Pending/backoff | Restore dependency; normal workers resume within their persisted allowance. No repair required. |
| Processing/sending with live lease | Allow the bounded operation to finish. Do not overwrite the lease. |
| Expired lease | Healthy worker reclaims with a higher attempt fence, or records failed if exhausted. A verifier may report stuck before recovery finishes; verify again after progress. |
| Failed | Correct the cause, then explicitly repair only that owning queue with a reviewed finite allowance. |
| Lost ACK/ambiguous commit | Reuse original event bytes/ID. Retry the same repair operation ID after an ambiguous repair outcome. |
| Completed/delivered | Do not reset processing. Read-only verification or an explicitly audited Extractor resend can test deduplication. |
| Unsupported/invalid/conflicting content | Preserve evidence. Requeue cannot make invalid bytes valid. Any new contract/semantic interpretation needs separately approved code/versioning, not SQL edits. |

## Restart and finish pending delivery without Ethereum

Stop an active foreground acquisition with Ctrl-C and wait for it to exit first.
Do not run concurrent operator changes and acceptance checks. Then:

```sh
go run ./tools/mainnet restart .mainnet/stage13.env
go run ./tools/mainnet drain .mainnet/stage13.env
go run ./tools/mainnet verify .mainnet/stage13.env
```

`restart` refuses a running project acquisition/replay/drain container. It stops
consumers and PostgreSQL gracefully, rebuilds the existing service image, starts
the same database, reapplies all service migrations idempotently and starts only
Transformer/Loader. It retains the same volume, credentials, Inbox/Outbox and
checkpoints. No Ethereum acquisition is started. This is operator maintenance,
not an automatic restart policy or an atomic distributed restart; serialize
operator actions. Existing workers resolve expired leases and ambiguous commits.

`drain` starts one bounded Extractor dispatcher with `EXTRACTOR_MODE=drain`, an
empty RPC URL and opt-in false. It never constructs the RPC adapter or changes
the checkpoint. The Stage 13 command requires the reviewed range 1000000..1000000.
It waits for all retained Extractor observations in that range to be delivered;
missing raw data, terminal failure or timeout returns nonzero. The existing
chain-wide dispatcher may also deliver other retained pending events on chain 1.
It cannot acquire missing blocks. Whole-service drain is bounded by drain timeout
plus 10s; the operator command also has a 2m40s bound including shutdown allowance.
Downstream processing is checked by `verify`, not by drain alone.

## Explicit failed-event repair

Stage 13.5 adds `*_MODE=repair` to Transformer and Loader. The application validates
a single event, allowed queue, stable identity/chain, operation ID and allowance
1..1000 before invoking its own outbound port. PostgreSQL atomically changes only
`failed` to `pending`, clears scheduling/failure state, increments the attempt fence
and assigns exactly the requested additional attempts. Counts never reset to zero.
A service-owned `recovery_operations` row records chain, operation ID, queue, event,
allowance, fence and timestamp. Same-ID concurrent/repeated operations are
idempotent; changed arguments under the same ID are refused. Stale claimants are
fenced. No pending, processing, completed or delivered event is reset by repair.

New forward migrations `transformer/0005_recovery_operations.sql` and
`loader/0005_recovery_operations.sql` grant runtime SELECT/INSERT on the immutable
audit, plus Inbox UPDATE(max_attempts); existing queue grants remain narrow.
Cumulative max_attempts can exceed 1000 across explicitly audited repairs; each
additional allowance is capped at 1000 and integer overflow is rejected. `/ready`
requires the new migration checksums, audit objects and exact privileges. The
repair executable is bounded to 30s, uses only its own runtime role, opens no
listeners/workers and emits only safe event/outcome/error categories.

After correcting the cause, run **only the applicable command**, replacing the
example operation ID with an incident-specific ID. Keep that same ID on ambiguous
retry. These examples add three attempts and never contact Ethereum:

```sh
raw_event=raw-block/v1/1/1000000/8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e
transformed_event=transformed-block/v1/1/1000000/8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e

go run ./tools/mainnet repair .mainnet/stage13.env transformer inbox "$raw_event" incident-001-t-in 3
go run ./tools/mainnet repair .mainnet/stage13.env transformer outbox "$transformed_event" incident-001-t-out 3
go run ./tools/mainnet repair .mainnet/stage13.env loader inbox "$transformed_event" incident-001-l-in 3
```

Consumers must be running (use `up`) for repaired work to process. The repair
command itself only durably requeues. Verify the full pipeline afterwards. No
manual UPDATE, DELETE, TRUNCATE or payload rewrite is part of this procedure.

## Safe replay versus reindex

**Replay** uses retained bytes and requires no Ethereum connection. Existing
Extractor semantics deliberately allow resend of delivered events as well as
failed ones, fence old attempts and deduplicate an operation ID. It preserves
first-committed provenance and event ID. It does not reset a downstream failed
Inbox; use the owning repair command above for that.

```sh
go run ./tools/mainnet replay .mainnet/stage13.env "$raw_event" incident-001-replay 3
go run ./tools/mainnet drain .mainnet/stage13.env
go run ./tools/mainnet verify .mainnet/stage13.env
```

**Reindex** is a separate audited historical job; it preserves the live checkpoint,
active membership, existing raw observations and stable event identities. Existing
observations retain their original bytes/provenance. It is not a destructive
rebuild, downstream reprocessing or a method to make Loader canonical.

```sh
# USER EXECUTION ONLY: this reacquires from Infura; never run automatically.
go run ./tools/mainnet reindex .mainnet/stage13.env incident-001-reindex
go run ./tools/mainnet verify .mainnet/stage13.env
```

The Stage 13 operator wrapper limits reindex to the same explicit block1000000,
100-attempt ceiling, 2m process deadline and CI refusal as live E2E. Application
reindex still supports the existing explicitly bounded historical range API;
this wrapper intentionally exposes only the reviewed Mainnet target. Use the same
job ID to resume its progress; changing its range under that ID is refused. A new
process has a new request budget, so review each invocation. Reindex alone on an
empty DB creates no live checkpoint and cannot satisfy the live verifier.

## Reorg, backup and operational limits

ADR 0004 recovery remains provider-relative, confirmation-aware and revision-fenced.
Only a verified retained common ancestor permits an atomic local checkpoint rewind;
all raw blocks, events and audit history remain. Deep/unprovable reorgs stop
acquisition. Investigate provider consistency and retained ancestry; do not force
rewind, invent intermediate heights or delete conflicting candidates. Historical
reindex cannot authorize active-branch replacement. Loader stays unknown under
ADR 0003; there is no downstream canonical reconciliation or consensus finality.

Reuse [retention, backup/restore and security procedures](operations-stage12.md).
No automatic deletion/archival eviction is introduced. Restore only to a separate
reviewed project/database and verify all schemas, migrations and handoffs before
cutover. Preserve repair audits in the same consistent backup. Docker/network
administration is trusted; local plaintext DB/gRPC, no HA, external secret manager,
TLS deployment or scheduled backups are provided. Budget limits are per process,
not an Infura-account quota. Docker/OS shutdown overhead is not a hard wall-clock
SLA. Raw Ethereum inputs are structurally checked, not consensus-validated.

Shutdown without deletion:

```sh
go run ./tools/mainnet stop .mainnet/stage13.env
```

Interrupt and join any foreground one-shot command before shutdown. Never use
`down --volumes`, prune or delete `.mainnet` data as routine recovery.

## One final validation and acceptance checklist

Stage 14.5 now provides the canonical [final entry point and configuration review](release-stage14.md).
The legacy Stage 13 script remains reusable; final offline mode selects targeted
race checks and includes the existing storage/backup-restore gate. Performance and
live acceptance require separate explicit phases. No earlier result certifies the
final Stage 14 tree.


```sh
bash scripts/validate-final.sh offline --opt-in .mainnet/fixtures/stage13-3b
```

This runs build, formatting, all unit and targeted race tests, vet, protobuf check, architecture
boundaries, three PostgreSQL suites, gRPC handoff, Compose fixture E2E and all three
authentic offline regressions once. Required local artifacts must exist and pass
the pinned verification before use; the authentic runner retains a separate
project for each noncontiguous block. SHA-256 before/after must be unchanged.
Integration runners use `-race`, fail on missing expected PASS/skip and retain all
created volumes/configurations (`ETL_TEST_RETAIN=1`). There is no global cleanup.
It reports each gate independently and exits nonzero if any fails. Ordinary CI
remains synthetic and requires no private artifacts or Infura credentials.

Recovery coverage includes atomic failed-queue repair before/after ambiguous
commit, concurrent same-operation retries, new attempt fencing, completion,
immutable audit/inputs and deduplication in the real PostgreSQL suites. Existing
fixture/handoff scenarios cover DB/service outages, restarts, lost ACK, replay,
reindex, reorg preservation and bounded completion failures. Live E2E is always
reported separately as NOT RUN by this script; it never invokes the provider.

- [ ] All offline gates PASS on the final tree; retain exact logs and host/tool versions.
- [ ] Runtime isolation, migration/readiness and secret-redaction tests PASS.
- [ ] Recovery/replay/reindex and restart evidence reviewed; no history removed.
- [ ] Original artifact digests unchanged; verified types 0/1/2/3, failed receipts
      and logs documented. Type 4 and contract creation remain uncovered.
- [ ] Explicit user-run live E2E for block1000000 PASS, followed by read-only verify.
- [ ] Record actual attempts/duration, completion counts and safe failure categories;
      do not record credentials/provider response text. If attempt telemetry is
      unavailable, mark it unmeasured; enforced ceiling is not an observed count.
- [ ] Review limitations (canonicality, security, storage/backup operations) before
      declaring Stage 13 accepted. Offline success alone is not live acceptance.

See [final validation record](validation-stage13-final.md) for executed results.
