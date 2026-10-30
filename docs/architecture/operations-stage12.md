# MVP operations and storage (Stage 12.5)

This is the current operational entry point. Stages 5–11 describe the history of
individual boundaries; the current system includes all three services, dispatch,
provider-relative reorg recovery, local health, metrics and structured logs.
Nothing here authorizes history deletion, canonical-chain selection or a broker.

## Runbook map

| Operation | Authoritative procedure |
| --- | --- |
| Local setup, fixture, start and stop | [Stage 11](local-stage11.md), [README](../../README.md) |
| Runtime credentials and migrations | [README PostgreSQL](../../README.md#local-postgresql-stage-2) |
| Missing schema/privilege, upgrade, readiness | [Stage 12.4](readiness-stage12.md) |
| Queue lag, ages, retry counters, scrape failures | [Metrics](metrics-stage12.md) |
| Event identity, safe error codes, ambiguous outcomes | [Logging](logging-stage12.md) |
| Extractor replay/reindex, backpressure, deep reorg | [Stage 10](extraction-stage10.md) |
| Failed Inbox/Outbox | Recovery matrix below |
| Backup, restore and rehearsal | [Backup/restore](backup-restore-stage12.md) |
| Query measurements and capacity | [Measurement evidence](storage-measurements-stage12.md) |
| Actual Stage 12.5 validation results | [Verification record](validation-stage12.md) |
| Release decision and security limitations | [Release checklist](release-stage12.md) |

## Incident sequence

1. Record UTC time, service/version, chain, event/source IDs, checkpoint/revision,
   queue states, attempts and safe error code. Do not copy payloads, secrets, DSNs
   or raw SQL errors into tickets. Query each schema with its own runtime login.
2. Distinguish process liveness, schema readiness and progress. `/live=200` does
   not establish database availability. `/ready=200` does not mean drained queues.
   A failed database metric snapshot omits gauges; missing values are not zeros.
3. Check disk/WAL space, PostgreSQL connectivity, required migrations and privileges
   before restarting services. Stop acquisition when capacity is threatened;
   allow dispatch to drain if its dependencies are healthy. Never delete old rows
   to clear backpressure. Preserve a backup before reviewed maintenance.
4. Resolve the cause, then verify the owning queue progresses and Loader records
   exist. A gRPC ACK establishes durable acceptance only. Compare identities,
   receipts/log counts and conflict observations, not just HTTP status.

Read-only queue inspection (one connection per owning runtime role):

```sql
-- extractor_runtime
SELECT event_id,status,attempts,max_attempts,last_error_code,next_attempt_at,lease_until
FROM extractor.outbox_events WHERE status <> 'delivered' ORDER BY created_at LIMIT 50;
-- transformer_runtime (run separately)
SELECT event_id,status,attempts,max_attempts,last_error_code,next_attempt_at,lease_until
FROM transformer.inbox_events WHERE status <> 'completed' ORDER BY accepted_at LIMIT 50;
SELECT event_id,status,attempts,max_attempts,last_error_code,next_attempt_at,lease_until
FROM transformer.outbox_events WHERE status <> 'delivered' ORDER BY created_at LIMIT 50;
-- loader_runtime (run separately)
SELECT event_id,source_event_id,status,attempts,max_attempts,last_error_code,next_attempt_at,lease_until
FROM loader.inbox_events WHERE status <> 'completed' ORDER BY accepted_at LIMIT 50;
```

## Failed work and recovery

| State / failure | Supported action and durable boundary |
| --- | --- |
| Pending retry after DB/downstream outage | Restore dependency; existing backoff resumes within the persisted budget. Monitor both sender delivery and receiver processing. |
| Expired sending/processing lease | Restart worker; lease reclamation fences the old attempt. Final exhausted claims become failed. Do not lower attempts or edit lease tokens. |
| Lost ACK or ambiguous COMMIT | Preserve the same ID and bytes. Existing retry/claim paths observe committed state or safely retry. Do not declare rollback solely from a network error. |
| Failed Extractor Outbox | Fix receiver/configuration first. Use the bounded, audited `EXTRACTOR_MODE=replay` command in Stage 10, with exact retained event ID and a new repair operation ID. Repeating the same operation ID does not add another allowance. Then run dispatch and verify acceptance/processing. |
| Failed Transformer Inbox | Stage 13.5 adds an explicit audited own-service repair for a single failed event. Fix the cause, grant a finite allowance and verify processing. Duplicate upstream delivery alone still only deduplicates. See the [Stage 13 runbook](operations-stage13.md#explicit-failed-event-repair). |
| Failed Transformer Outbox | Use the Stage 13.5 audited Transformer Outbox repair after correcting the cause. Restart or Extractor replay does not replenish this queue’s exhausted budget. |
| Failed Loader Inbox | Use the Stage 13.5 audited Loader Inbox repair after correcting the cause. Exact accepted input and all normalized candidates are retained; upstream replay alone only deduplicates. |
| Invalid/unsupported/conflicting payload | Do not edit the retained bytes under the same event ID. Contract/semantic changes require an approved versioning decision and tests. |
| Deep or unprovable reorg | Halt acquisition and investigate provider and retained ancestry. No force rewind, deletion or downstream canonical promotion. See ADR 0004. |

The explicit Stage 13.5 repair commands are not permission to
run manual UPDATE/DELETE statements. Do not truncate an Inbox to defeat deduplication,
reset completed processing, erase failed rows or lower a checkpoint. Whole-system
restore is disaster recovery, not a routine poison-event retry mechanism.

## Retention and archiving policy

The MVP retains all blockchain observations and delivery/deduplication history
indefinitely. There is no TTL, partition drop, scheduled DELETE or automatic archive
eviction. This is a preservation policy, not a claim of unlimited storage capacity.

| Data | Retention rule and reason |
| --- | --- |
| Raw blocks/receipts, provenance, all forks | Keep exact bytes and full identity; required for retrieval, audit, reindex and reorg evidence. |
| Outbox, Inbox, transformed documents | Keep pending, failed and terminal rows. Terminal Inbox IDs preserve deduplication; delivered events remain replayable where supported. |
| Loader blocks/transactions/receipts/logs | Keep all candidates, including competing hashes; `canonical_status=unknown` remains unchanged. |
| Active membership and checkpoint | Maintain via existing fenced local transactions only. Removing active membership during an audited rewind is not deletion of historical observations. |
| Rewind/reindex/replay audit and migrations | Retain with the dataset; needed to interpret revision and operation identity and verify binary compatibility. |
| Logs/metric samples | Operational evidence, not authoritative blockchain history. Rotation/storage policy belongs to the operator; never substitute them for durable records. |

Archiving currently means an additional verified backup copy, not moving rows out
of the online database. Preserve every schema at one consistent database snapshot,
checksums, migration identities, role provisioning and binary/configuration version.
Store encrypted copies off-host with restricted access and independently managed
keys; periodically restore them. These facilities are operator responsibilities,
not implemented cloud storage or backup scheduling.

Any future online/offline retention boundary needs explicit approval covering
deduplication lifetime, exact replay, referential integrity, reorg ancestry, failed
work, retrieval and recovery tests. An acknowledged event is not safe to delete
merely because its receiver processed it.

## Capacity and routine PostgreSQL care

Track database/volume/WAL growth, each owning relation plus indexes/TOAST, queue age,
failed counts, replication/backup space where configured, and the Extractor watermark.
Choose capacity and warning thresholds from measured growth and available disk;
there is no universal Mainnet-sized default. Reserve room for backup, restore,
index creation, WAL and autovacuum. Backpressure does not impose a disk quota and
does not prevent Transformer/Loader or WAL from growing.

Keep autovacuum and autoanalyze enabled. Queue state updates create dead tuples;
ordinary VACUUM reclaims obsolete physical row versions, not live blockchain
history. Investigate long transactions and stale statistics before index changes.
Avoid unplanned VACUUM FULL, REINDEX or blocking DDL on running services. Measure
the actual workload with EXPLAIN on an isolated copy; ANALYZE executes writes in
DML plans, so use a transaction and ROLLBACK. Do not run the synthetic seed on a
developer's persistent or production database.

## Keeping verification evidence

Set `ETL_TEST_RETAIN=1` for existing Go integration runners to stop their isolated
Compose project without deleting its containers, network, database volume or
0600 credential file. The runner prints the project name and private file path,
never its contents. Inspect only that generated project. Each rerun creates a new
one. Stage 12.5 storage verification implies retention even without that setting.

The PostgreSQL regression tests still deliberately mutate/truncate their own test
fixtures and install fault-injection objects. They never accept arbitrary database
credentials through these runners. Retention changes post-test resource disposal,
not those regression semantics. Existing default CI cleanup remains limited to
fresh disposable projects; this Stage 12.5 verification uses retention mode.
No automated history or backup deletion is introduced. Retained resources consume
disk until an operator explicitly authorizes disposal of those specific fixtures.

## Final Stage 14 configuration and release review

Use the [Stage 14.5 audit/checklist](release-stage14.md) and its single validation
entry point for the final tree. Failed queues now have Stage 13 owning-service,
audited finite repair; upstream duplicate delivery still does not reset failed
processing. Under Stage 14 capacity limits, failed rows retain their slots:
restore Loader capacity and repair downstream failures before reopening upstream
acquisition. Restart alone does not clear failure states. Keep original payloads,
identities and history. No direct SQL reset or retention deletion is authorized.
