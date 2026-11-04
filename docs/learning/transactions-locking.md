# PostgreSQL transactions, locks and fencing

All consistency boundaries are **local** to one service-owned schema. The physical
instance is shared; database roles prevent a service from treating another schema
as an implementation shortcut. Migrations use distinct owning logins; runtime roles
receive narrow table/column permissions. There is no distributed transaction.

## The important transactions

| Adapter operation | Atomic effect |
|---|---|
| Extractor `Store.Commit` | Snapshot + Outbox + active membership + checkpoint |
| Extractor `Store.Rewind` | Rewind audit + active suffix removal + checkpoint revision increment |
| Receiver `Store.Accept` | New Inbox row, after exact-duplicate/capacity decision |
| Queue claim | Lease/attempt update, returned only after commit |
| Transformer `Store.Complete` | Result + Outbox + Inbox completion |
| Loader `Store.Complete` | Every normalized row + observations + Inbox completion |
| Repair/replay | Audited allowance and claim fencing; no history deletion |

The adapters explicitly enable synchronous commit. Error wrappers suppress SQL
payloads/DSNs; this does not mean an error was swallowed or that its commit outcome
is known. Rollback cleanup uses a separate bounded context after caller cancellation.

## Claims are coordination, not ownership forever

A claim selects due or expired work using `FOR UPDATE SKIP LOCKED`, updates status,
increments attempts and stores `lease_until`. The short claim transaction commits
before transformation or network work. Other workers can claim different rows.
A live lease does not survive forever just because the old process still runs.

Suppose worker A has attempt 3, pauses beyond its lease, and worker B reclaims
attempt 4. A must not finish B's work or reset its retry state. Completion predicates
compare the claimed attempt and require an unexpired lease. This is **fencing**.
`ErrLeaseLost` is a valid coordination outcome, not permission to overwrite state.

## Why more than row locks?

Extractor uses a per-chain advisory transaction lock so empty-checkpoint bootstrap,
forward commits and rewinds share one serialization rule. Its checkpoint revision
protects against the ABA case: a tip changes and later returns to the same hash,
but an old fetched request must not act as if nothing happened.

Transformer/Loader admission uses a service-specific advisory lock before checking
for duplicates and outstanding capacity. Completion does not need the same lock:
Transformer atomically transfers a slot from incomplete Inbox to undelivered Outbox;
Loader completion releases a slot. Delivery decreases outstanding work. Failed
rows still occupy capacity. Bounded partial-index probes avoid scanning all completed
history on new-event admission.

Loader takes its chain observation lock **before** Inbox/block row locks, then
computes neighboring conflicts after insert. Two concurrently arriving forks must
both see a serialized observation decision. The cost is serialized same-chain
completion; raising worker count does not remove it. Hash collisions in advisory
keys can add serialization, not merge data identities.

## Batching never divides the transaction

[recordBatch](../../services/loader-service/internal/adapters/outbound/postgres/batch.go)
queues INSERTs but does not commit. `Complete` flushes all batches, updates
observations, fences the final Inbox update and commits once. `BatchResults.Close`
runs before another transaction operation. A failure in the final batch rolls back
the earlier successful INSERTs too. Tests install a failure trigger to demonstrate
that behavior against real PostgreSQL.

## Migrations and schema readiness

[The migrator](../../deployments/compose/scripts/migrate.sh) serializes by service,
checks immutable migration checksums and runs pending files in one transaction.
Each schema has owner-only migration metadata and runtime-readable readiness
metadata. `/ready` checks the required objects and privileges through the runtime
role with bounded timeouts. It neither grants privileges nor runs migrations.

[ADR 0003](../adr/0003-loader-conflict-observation.md) explains the observation lock;
[ADR 0004](../adr/0004-ordered-extraction-reorg-recovery.md) defines revision-fenced
rewind. SQL locks alone cannot verify Ethereum consensus or make a remote ACK atomic.
