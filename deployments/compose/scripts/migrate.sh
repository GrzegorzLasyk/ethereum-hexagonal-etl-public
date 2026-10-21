#!/usr/bin/env bash
set -euo pipefail

service=${1:-}
case "$service" in
    extractor|transformer|loader) ;;
    *) echo "Usage: migrate.sh extractor|transformer|loader" >&2; exit 2 ;;
esac
if [[ "${PGUSER:-}" != "${service}_migrator" || "${PGDATABASE:-}" != ethereum_etl ]]; then
    echo "Migrations require the owning migrator login and ethereum_etl database." >&2
    exit 2
fi

migration_dir="${MIGRATIONS_ROOT:-/migrations}/$service"
shopt -s nullglob
files=("$migration_dir"/*.sql)
if (( ${#files[@]} == 0 )); then
    echo "No migration files found for $service." >&2
    exit 1
fi

script=$(mktemp)
trap 'rm -f "$script"' EXIT
cat > "$script" <<'SQL'
BEGIN;
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(174727, hashtext(:'service'));
CREATE TABLE IF NOT EXISTS :"service".schema_migrations (
    version text PRIMARY KEY,
    checksum text NOT NULL CHECK (length(checksum) = 64),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON :"service".schema_migrations FROM PUBLIC;
SQL

# Lexically sorted, fixed-width filenames provide deterministic ordering.
for file in "${files[@]}"; do
    version=${file##*/}
    if [[ ! "$version" =~ ^[0-9]{4}_[a-z0-9_]+\.sql$ ]]; then
        echo "Invalid migration filename: $version" >&2
        exit 1
    fi
    checksum=$(sha256sum "$file")
    checksum=${checksum%% *}
    printf "\\set version %s\n\\set checksum %s\n" "$version" "$checksum" >> "$script"
    cat >> "$script" <<'SQL'
SELECT EXISTS (
    SELECT 1 FROM :"service".schema_migrations WHERE version = :'version'
) AS applied,
NOT EXISTS (
    SELECT 1 FROM :"service".schema_migrations
    WHERE version = :'version' AND checksum <> :'checksum'
) AS unchanged
\gset
\if :unchanged
\else
    DO $$ BEGIN RAISE EXCEPTION 'Applied migration checksum changed'; END $$;
\endif
\if :applied
    \echo Already applied :version
\else
    \echo Applying :version
SQL
    # Copy SQL into the transaction so paths cannot become psql commands.
    cat "$file" >> "$script"
    cat >> "$script" <<'SQL'

INSERT INTO :"service".schema_migrations (version, checksum)
VALUES (:'version', :'checksum');
\endif
SQL
done
printf 'COMMIT;\n' >> "$script"
psql -X --no-password --set=ON_ERROR_STOP=1 --set="service=$service" --file="$script"
