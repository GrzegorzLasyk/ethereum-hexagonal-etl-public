#!/usr/bin/env bash
# Run only against the local development database with the Compose tools profile.
set -euo pipefail

services=(extractor transformer loader)
probe="stage2_probe_$$"
work=$(mktemp -d)

sql() {
    local role=$1 query=$2 password_var
    password_var="${role^^}_PASSWORD"
    PGUSER="$role" PGPASSWORD="${!password_var}" \
        psql -X -w -qAt -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate -c "$query"
}

assert_value() {
    local role=$1 query=$2 expected=$3 actual
    actual=$(sql "$role" "$query")
    if [[ "$actual" != "$expected" ]]; then
        echo "Unexpected query result for $role: $actual (expected $expected)" >&2
        exit 1
    fi
}

denied() {
    local role=$1 query=$2 output
    if output=$(sql "$role" "$query" 2>&1); then
        echo "Unexpected permission granted to $role: $query" >&2
        exit 1
    fi
    if [[ "$output" != *42501* ]]; then
        echo "Expected insufficient_privilege (42501), received: $output" >&2
        exit 1
    fi
}

migrate() {
    local service=$1 root=${2:-/migrations} password_var
    password_var="${service^^}_MIGRATOR_PASSWORD"
    PGUSER="${service}_migrator" PGPASSWORD="${!password_var}" \
        MIGRATIONS_ROOT="$root" bash /scripts/migrate.sh "$service"
}

cleanup() {
    local service
    for service in "${services[@]}"; do
        sql "${service}_migrator" \
            "DROP TABLE IF EXISTS $service.$probe;
             DROP FUNCTION IF EXISTS $service.$probe();" >/dev/null 2>&1 || true
    done
    rm -rf "$work"
}
trap cleanup EXIT

for service in "${services[@]}"; do
    expected_migrations=6
    if [[ "$service" == extractor ]]; then expected_migrations=5; fi
    migrate "$service"
    before=$(sql "${service}_migrator" \
        "SELECT version || checksum || applied_at::text FROM $service.schema_migrations ORDER BY version")
    migrate "$service"
    after=$(sql "${service}_migrator" \
        "SELECT version || checksum || applied_at::text FROM $service.schema_migrations ORDER BY version")
    [[ "$before" == "$after" ]]
    assert_value "${service}_migrator" \
        "SELECT count(*) FROM $service.schema_migrations" "$expected_migrations"

    # Concurrent re-application must serialize without duplicate version rows.
    migrate "$service" >"$work/concurrent-1" 2>&1 &
    first=$!
    migrate "$service" >"$work/concurrent-2" 2>&1 &
    second=$!
    wait "$first"
    wait "$second"
    assert_value "${service}_migrator" \
        "SELECT count(*) FROM $service.schema_migrations" "$expected_migrations"

    for kind in migrator runtime; do
        role="${service}_$kind"
        assert_value "$role" "SELECT current_database()" ethereum_etl
        assert_value "$role" "SELECT current_user" "$role"
        assert_value "$role" \
            "SELECT NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls OR rolinherit)
             FROM pg_roles WHERE rolname = current_user" t
        assert_value "$role" \
            "SELECT has_database_privilege(current_user, current_database(), 'CREATE')
                 OR has_database_privilege(current_user, current_database(), 'TEMP')" f
        assert_value "$role" \
            "SELECT has_schema_privilege(current_user, '$service', 'USAGE')" t
        denied "$role" "CREATE TABLE public.$probe (id integer)"
        denied "$role" "CREATE SCHEMA $probe"
        denied "$role" "CREATE TEMP TABLE $probe (id integer)"
        denied "$role" "SET ROLE postgres"
    done

    assert_value "${service}_migrator" \
        "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = '$service'" "${service}_migrator"
    denied "${service}_runtime" "SET ROLE ${service}_migrator"
    denied "${service}_runtime" "CREATE TABLE $service.$probe (id integer)"
    denied "${service}_runtime" "SELECT * FROM $service.schema_migrations"
    denied "${service}_runtime" "DELETE FROM $service.schema_migrations"
    assert_value "${service}_runtime" "SELECT count(*) FROM $service.schema_readiness" "$expected_migrations"
    denied "${service}_runtime" "DELETE FROM $service.schema_readiness"
    denied "${service}_runtime" "UPDATE $service.schema_readiness SET checksum=repeat('0',64)"


    sql "${service}_migrator" \
        "CREATE TABLE $service.$probe (id bigserial PRIMARY KEY, value text);
         CREATE FUNCTION $service.$probe() RETURNS integer LANGUAGE sql AS 'SELECT 1';" >/dev/null
    # New objects are private until their owning migration explicitly grants access.
    denied "${service}_runtime" "SELECT * FROM $service.$probe"
    denied "${service}_runtime" "SELECT $service.$probe()"
    sql "${service}_migrator" \
        "GRANT SELECT, INSERT, UPDATE, DELETE ON $service.$probe TO ${service}_runtime;
         GRANT USAGE ON SEQUENCE $service.${probe}_id_seq TO ${service}_runtime;" >/dev/null
    assert_value "${service}_runtime" \
        "INSERT INTO $service.$probe(value) VALUES ('probe') RETURNING id" 1
    assert_value "${service}_runtime" \
        "UPDATE $service.$probe SET value = 'updated' WHERE id = 1 RETURNING value" updated
    assert_value "${service}_runtime" "SELECT count(*) FROM $service.$probe" 1
    assert_value "${service}_runtime" "DELETE FROM $service.$probe RETURNING id" 1
    denied "${service}_runtime" "ALTER TABLE $service.$probe ADD COLUMN forbidden integer"
    denied "${service}_runtime" "TRUNCATE $service.$probe"
    denied "${service}_runtime" "DROP TABLE $service.$probe"

    # Verify TCP authentication is not accidentally configured as trust.
    if PGUSER="${service}_runtime" PGPASSWORD="intentionally-wrong" \
        psql -X -w -qAt -c 'SELECT 1' >"$work/auth" 2>&1; then
        echo "Wrong password unexpectedly accepted for $service." >&2
        exit 1
    fi
    grep -q "password authentication failed" "$work/auth"
    echo "PASS: $service authentication, ownership, runtime restrictions, migration replay/concurrency"
done

for service in "${services[@]}"; do
    for other in "${services[@]}"; do
        [[ "$service" == "$other" ]] && continue
        for kind in migrator runtime; do
            role="${service}_$kind"
            assert_value "$role" \
                "SELECT has_schema_privilege(current_user, '$other', 'USAGE')
                     OR has_schema_privilege(current_user, '$other', 'CREATE')" f
            denied "$role" "SELECT * FROM $other.$probe"
            denied "$role" "INSERT INTO $other.$probe(value) VALUES ('forbidden')"
            denied "$role" "CREATE TABLE $other.forbidden (id integer)"
            denied "$role" "SELECT nextval('$other.${probe}_id_seq')"
            denied "$role" "SELECT $other.$probe()"
            denied "$role" "SET ROLE ${other}_migrator"
            denied "$role" "SET ROLE ${other}_runtime"
        done
    done
done
echo "PASS: cross-schema reads, writes, DDL, sequences, functions, and role escalation denied"

# Mutated migration files are detected before changing durable state.
cp -R /migrations "$work/migrations"
printf '\n-- intentional test mutation\n' >> "$work/migrations/extractor/0001_schema_privileges.sql"
if migrate extractor "$work/migrations" >"$work/checksum" 2>&1; then
    echo "Changed migration checksum unexpectedly accepted." >&2
    exit 1
fi
grep -q "Applied migration checksum changed" "$work/checksum"
cp /migrations/extractor/0001_schema_privileges.sql "$work/migrations/extractor/"
printf 'CREATE TABLE extractor.%s_rollback (id integer);\n' "$probe" \
    > "$work/migrations/extractor/9998_rollback_probe.sql"
printf 'SELECT 1 / 0;\n' > "$work/migrations/extractor/9999_intentional_failure.sql"
if migrate extractor "$work/migrations" >"$work/rollback" 2>&1; then
    echo "Failing migration unexpectedly committed." >&2
    exit 1
fi
grep -q "division by zero" "$work/rollback"
assert_value extractor_migrator \
    "SELECT to_regclass('extractor.${probe}_rollback') IS NULL" t
assert_value extractor_migrator "SELECT count(*) FROM extractor.schema_migrations" 5
echo "PASS: changed checksums rejected and failing migration batch rolled back"

# Drop probes before checking the exact stage-owned table inventory.
cleanup
trap - EXIT
for service in "${services[@]}"; do
    expected_tables=schema_migrations
    if [[ "$service" == extractor ]]; then
        expected_tables=active_blocks,extraction_checkpoints,outbox_events,raw_blocks,reindex_jobs,replays,rewinds,schema_migrations
    elif [[ "$service" == transformer ]]; then
        expected_tables=inbox_events,outbox_events,recovery_operations,schema_migrations,transformed_blocks
    elif [[ "$service" == loader ]]; then
        expected_tables=blocks,inbox_events,logs,receipts,recovery_operations,schema_migrations,transactions
    fi
    assert_value "${service}_migrator" \
        "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname = '$service'" "$expected_tables"
done
echo "PASS: exact stage-owned tables remain; no test probes or unplanned tables"
