# Safe backup and restore rehearsal (Stage 12.5)

## Reproduce locally

Prerequisites: Go from `go.mod`, Docker Desktop/Engine with Compose, sufficient free
disk for two PostgreSQL volumes, the backup and measurement data. No real Ethereum
credentials are used. Run from the repository root:

```sh
ETL_STORAGE_VERIFY=1 ETL_TEST_RETAIN=1 bash scripts/ci-integration.sh fixture-e2e
```

This reuses the full fixture E2E runner and adds a storage subtest after graceful
service shutdown. `deployments/compose/storage.yaml` adds a second PostgreSQL 17.11
cluster with its own volume, no published port and initially no `ethereum_etl`
database. The database administrator is used only by the test observer. Application
roles and service boundaries are unchanged.

The harness:

1. Completes the real Extractor → Transformer → Loader fixture, including restarts,
   lost ACK, replay and reorg. Stops all writers; leaves source PostgreSQL running.
2. Hashes every row of every service table, including exact bytea payloads, all
   queue state, checkpoints, audit, normalized entities and migration metadata.
   Captures schema definitions, owners, column/table/default ACLs, indexes,
   constraints, views, DB grants and role/database settings.
3. Runs a whole-database `pg_dump -Fc`, writes a SHA-256 sidecar, and copies the
   archive to a private retained host directory. No per-schema snapshots are mixed.
4. Starts a fresh destination and rejects an existing application database before
   restore. Provisions only the six approved roles with generated credentials;
   passwords are read from the container environment, never dumped or printed.
5. Verifies the transferred archive checksum and runs
   `pg_restore --exit-on-error --create -d postgres`. Ownership and ACLs are restored;
   there is no `--clean`, `--no-owner`, `--no-acl`, overwrite or database drop.
6. Compares complete row hashes, schema/ACL definitions and restricted role
   attributes. Authenticates as all three runtime roles, checks migration-view
   reads and denies foreign-schema access. Repeating the restore is rejected.
7. Uses only the restored clone for SQL performance fixtures. Rechecks that the
   source snapshot is identical. Stops both clusters and retains their volumes,
   backup, evidence directory and private runner configuration, even on failure.

The synthetic measurement clone is deliberately **not** a runnable ETL dataset:
copied payloads do not describe its synthetic block identities. The verified
`snapshot.dump` and original fixture database remain unchanged and can be used
for another clean restore. Never restart ETL workers against the seeded clone.

## Operator backup procedure

The test is not a general production backup command. For a needed database:

1. Identify source cluster/database, approved server/client versions and exact
   binary/migration/configuration versions. Check disk and backup permissions.
   Use a dedicated administrator/backup session, never broaden runtime roles.
2. For a simple cutover rehearsal, quiesce acquisition and service writers and
   wait for graceful shutdown. Queue drainage is optional; retained pending work
   must be included. Never discard it to obtain a clean-looking snapshot.
3. Use authenticated local tooling or an approved secure transport with credentials
   in a protected environment/password file, not shell arguments/history. Create
   a new custom-format whole-database archive at a non-existing protected path:

   ```sh
   # PGHOST/PGPORT/PGUSER/PGDATABASE and private authentication already configured.
   # BACKUP_FILE is a new destination in a private directory; do not overwrite.
   set -eu
   umask 077
   test ! -e "$BACKUP_FILE"
   pg_dump --format=custom --file="$BACKUP_FILE"
   pg_restore --list "$BACKUP_FILE" > "$BACKUP_FILE.toc"
   shasum -a 256 "$BACKUP_FILE" > "$BACKUP_FILE.sha256"
   ```

   Abort on any command error or warning requiring investigation. In automation,
   use `set -eu` and create destinations exclusively to prevent races/overwrites.
4. Record role provisioning separately: `pg_dump` does not back up cluster-global
   roles. Preserve the reviewed six-role attributes and database settings and
   manage passwords in a secret store. The rehearsal recreates the repository's
   known roles; it is not a generic roles/tablespaces/extension backup utility.
5. Encrypt and copy the archive, manifest and version metadata off-host; verify the
   checksum after transfer and restrict restore access. The local rehearsal's
   archive is not encrypted and is not an off-site backup.

`pg_dump` provides a consistent snapshot of one database even with concurrent
writes; that does not include later commits, roles or WAL/PITR capability. See
[PostgreSQL 17 pg_dump](https://www.postgresql.org/docs/17/app-pgdump.html).
Quiescing simplifies whole-system cutover and post-restore comparison; it does
not replace the database snapshot guarantee.

## Restore and cutover

Restore only a trusted archive into a new isolated cluster. Confirm the target
independently, verify archive checksum, provision approved role identities with
new credentials, and check that `ethereum_etl` does not exist. Do not point tools
at the source. `pg_restore --create` uses the database name in the archive; `-d
postgres` is only its initial administrative connection. Never use `--clean` as
a convenience. See [PostgreSQL 17 pg_restore](https://www.postgresql.org/docs/17/app-pgrestore.html).

On partial failure, leave the destination isolated for investigation and retry in
another fresh destination. The restore does not wrap CREATE DATABASE in one
transaction. Never cut over to a partially restored database.

After restore, validate counts/digests, exact migration state, constraints, indexes,
owners, ACLs and foreign-schema denial. Run ANALYZE to rebuild planner statistics
(they are not in the logical dump). Use a binary compatible with the exact migration
manifest. Test `/ready` and `/metrics`, then start downstream receivers before
dispatch/acquisition. Verify expired leases reclaim correctly, pending work resumes,
duplicates deduplicate and normalized records match. Do not reset attempts to
make a restore appear successful. Confirm provider/chain/start/depth before live RPC.

Fence the old writers before endpoint cutover; never run two independent restored
pipelines as if their progress were one history. Retain the old cluster and backup
until an explicitly reviewed rollback/cutover decision. Restoring only one service's
schema from a different time can lose acknowledged handoffs and is not this procedure.

## Recovery objectives and limits

No production RPO, RTO, backup cadence, storage provider, encryption/key management
or expiry is approved by this local milestone. Set these before deployment. RPO
is at least the age of the last successfully verified snapshot without WAL/PITR;
RTO includes provisioning, transfer, restore, ANALYZE, validation and service catchup.
Reported small-fixture timings measure local dump/restore commands only.

The rehearsal checks logical restoration across fresh clusters on the same host,
same PostgreSQL version and architecture. It does not simulate disk destruction,
power loss, off-host recovery, point-in-time recovery, major-version upgrades or
live traffic cutover. The archive is captured after the fixture drains; resumption
of pending/in-flight work from a restored archive is not exercised by this drill
(the existing live-database restart/handoff regressions cover lease recovery).
It verifies runtime authentication and stored contracts but
does not restart application binaries against the restored synthetic workload.
These are explicit release limitations, not implied PASS results.
