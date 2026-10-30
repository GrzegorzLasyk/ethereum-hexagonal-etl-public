# Stage 12.4: readiness and service-owned database validation

## Contract

All three services keep their local HTTP routes and configured loopback listener.
`GET /ready` returns 200 only if the lifecycle is active and the owning database
passes the current binary's readiness contract. All negative results return 503
with an empty body. SQL errors, relation names, connection strings, credentials
and server diagnostics are not returned or logged by the checker.

`/live` still reflects process lifecycle, independently of database/schema state.
`/metrics` keeps Stage 12.3 behavior, including its explicit database snapshot
indicator. A metrics snapshot is not a schema readiness attestation. A healthy
receiver needs no downstream service or live Ethereum provider; there are no RPC,
gRPC or cross-service schema calls in readiness.

`*_HEALTH_ADDRESS` remains opt-in for standalone commands; the fixture configures
loopback ports 8091/8092/8093. Only literal IPv4/IPv6 loopback addresses are allowed.
Wildcard, non-loopback and DNS/hostname binds are rejected. The handler constructor
is separate for controlled tests; production composition always uses `health.Start`
to enforce listener restrictions. No remote listener/authentication mode is added.

## Checks and boundaries

Each service owns a `ReadinessChecker` application port and a PostgreSQL adapter
with a static compatibility manifest. The shared `internal/adapters/dbreadiness`
package contains only technical PostgreSQL checking logic. Domain and application
code import neither pgx, HTTP nor readiness infrastructure. Composition roots wire
the owning Store's check into the existing HTTP adapter.

Checks run in a read-only, repeatable-read local transaction:

1. The connection is usable, database is `ethereum_etl`, current and session roles
   are the expected service runtime. Its configured restricted role attributes and
   absence of memberships are verified. CONNECT must exist; database CREATE/TEMP
   must not. This detects role/configuration drift without using a migrator login.
2. The owning schema exists, belongs to the owning migrator, and permits runtime
   USAGE but not CREATE.
3. Required service tables exist as ordinary tables, have the expected owner,
   permit required SELECT/INSERT/DELETE operations and have no RLS enabled.
   The migration projection must exist as a view. DELETE is allowed only on
   Extractor active membership, as already approved by ADR 0004. TRUNCATE,
   REFERENCES and TRIGGER table privileges are not allowed.
4. Required columns exist with the expected PostgreSQL types. Effective INSERT and
   UPDATE column privileges must match the approved table/column grants, including
   narrowly mutable queue/cursor/Loader observation fields. Immutable payload
   updates and writes to the migration projection cause failure. Privilege checks
   include effective grants rather than merely looking for an ACL entry (see
   [PostgreSQL privilege inquiry functions](https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-INFO-ACCESS-TABLE)).
5. Required named operational indexes and table primary-key indexes exist and are
   valid and ready. No application rows are scanned or modified by these checks.
6. The migration projection contains exactly the expected filenames and SHA-256
   checksums. Missing, additional, duplicate or mismatched entries fail. Metadata
   queries refer only to the owning schema; system catalogs are used to inspect
   its objects and current role, not other services' application schemas.

The checker returns a fixed `service database is not ready` error for any failure.
The HTTP layer emits only the status. There is no ready-result cache: a subsequent
probe observes repaired privileges/schema without requiring a service restart.

## Migration metadata and upgrading

Existing `schema_migrations` tables remain private to migrators. New migrations:

- Extractor: `0005_readiness_metadata.sql`.
- Transformer and Loader: `0004_readiness_metadata.sql`.

Each creates its own `schema_readiness` view with `security_barrier=true`, projecting
only `version` and `checksum`. The owning migrator supplies the view's read access;
runtime gets SELECT on the view only. Runtime cannot read the base metadata table
or write through the view. The original migration files/checksums are unchanged.
No source/history rows, application tables, retry rules or privileges to other
schemas are changed. This is local operational introspection, not a shared store.

Apply the new migrations with each owning migrator using the existing migration
runner before expecting the updated binary to become ready. Never grant runtime
migration or repair privileges to bypass a failed check. For the persistent fixture,
use the commands in the [README](../../README.md#schema-readiness-stage-124).
All isolated PostgreSQL/E2E runners apply and verify the new migrations automatically.

Expected migration identities are compiled into each PostgreSQL adapter's
`readiness_policy.go`. `TestReadinessMigrationManifest` checks that its filenames
and hashes match the repository's migration files. A future approved migration
requires updating this manifest and relevant object/privilege expectations.
An older schema-aware binary rejects a newer migration set: coordinate migration
and binary upgrades; this is intentionally not a rolling-upgrade compatibility
protocol. No automatic migrations are performed by `/ready`.

## Deadlines and shutdown

The HTTP readiness call and PostgreSQL checker each impose a maximum one-second
deadline, respecting any earlier caller deadline. Connection acquisition and all
queries share that deadline; rollback cleanup is separately bounded to 100 ms.
A deadline or cancellation cannot be converted to 200 even if a check returns nil
late. Shutdown cancels in-flight readiness calls and rechecks lifecycle state
before the response. Closing the listener first marks its lifecycle stopping,
then uses the existing bounded server shutdown. `/live` returns 503 during that
stopping lifecycle; database failures alone never change liveness.

The existing HTTP read-header (one second), write (three seconds) and shutdown
(three seconds) bounds remain. The production database adapter propagates context
cancellation. No detached background probe or unbounded retry is started.

## Interpretation and limits

Readiness is a point-in-time compatibility/permission observation, not a lock on
future DDL or a guarantee that the next write will succeed. A role or schema can
change immediately after a successful check. Concurrent migration or catalog lock
contention can temporarily return 503; retry after the migration/repair completes.

This is not a complete SQL schema equivalence or adversarial database attestation.
It checks required column types and index identity/validity, not every index
expression, CHECK/FK definition, default, trigger, extension, grant option or
out-of-band administrator modification. It does not inspect other schemas to
perform a cross-service security audit. Existing isolation regressions remain in
the PostgreSQL runners; the broader [security review](release-stage12.md) records the remaining deployment limitations.

Readiness does not assert a drained queue, fresh head, finality, canonicality,
receiver availability or throughput. It does not pause workers, repair state,
change transaction boundaries, alter at-least-once delivery or change business
behavior. Existing startup validation may still fail before the listener exists
(e.g. unreachable database at process start); a process that never started has
no HTTP readiness response.

## Tests

Unit tests cover fixed-error redaction, read-only/deadline settings, HTTP failure
and recovery, deadline expiry even after a nil check result, cancellation during
shutdown, disabled listeners, loopback IPv4/IPv6, rejected wildcard/remote/hostname
addresses, occupied ports, idempotent close, and preservation of metrics/liveness.

Each mandatory PostgreSQL suite exercises actual runtime-role checks through the
production HTTP handler: missing view/table/column/index, changed column type,
missing/mismatched migration, missing SELECT/INSERT/UPDATE/schema USAGE, forbidden
immutable writes/DELETE/schema CREATE/metadata writes, then repair and recovery.
A real exclusive lock on migration metadata tests bounded timeout and recovery;
canceled calls and a migrator connection cannot become ready.

The full fixture E2E checks startup with the required migrations, readiness during
PostgreSQL outage/recovery and restarts, downstream independence, existing metrics,
correlated logs, durable ETL data and simulated reorg. No real Infura calls or
new deployment stack are involved.

## Stage 13.5 forward schema update

Transformer and Loader also require `0005_recovery_operations.sql`, its immutable
service-owned audit table/primary key and exact SELECT/INSERT grants. Inbox
`max_attempts` becomes an allowed mutable column for explicit audited repair.
The runtime still cannot update/delete audit rows or access another schema.
Old binaries' exact readiness inventories are intentionally incompatible; deploy
matching migrations/binaries through the documented maintenance procedure.
