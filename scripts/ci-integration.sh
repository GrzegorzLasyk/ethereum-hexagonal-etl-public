#!/usr/bin/env bash
# Existing runners own credentials, deadlines and disposable Compose cleanup.
# A green Go exit alone is insufficient: skipped/missing tests also exit zero.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# != 1 ]]; then
  echo 'Usage: bash scripts/ci-integration.sh <extractor-postgres|transformer-postgres|loader-postgres|handoff-postgres|fixture-e2e|storage-release>' >&2
  exit 2
fi
runner=$1
case "$1" in
  extractor-postgres|transformer-postgres|loader-postgres) expected=(TestPostgresIntegration) ;;
  handoff-postgres) expected=(TestSeedHandoff TestHandoff) ;;
  fixture-e2e) expected=(TestFixtureE2E)
    if [[ ${ETL_STORAGE_VERIFY:-0} == 1 ]]; then expected+=(TestFixtureE2E/storage_release); fi ;;
  storage-release)
    runner=fixture-e2e
    export ETL_STORAGE_VERIFY=1 ETL_TEST_RETAIN=1
    expected=(TestFixtureE2E TestFixtureE2E/storage_release) ;;
  *) echo 'Unknown integration suite' >&2; exit 2 ;;
esac

ci_output=$(mktemp)
trap 'rm -f "$ci_output"' EXIT
# Runner output is already redacted. Never dump Compose environments or .env.
go run "./test/integration/cmd/$runner" 2>&1 | tee "$ci_output"
if grep -Eq '^[[:space:]]*--- SKIP:' "$ci_output"; then
  echo 'Integration gate failed: a test was skipped' >&2
  exit 1
fi
for test_name in "${expected[@]}"; do
  if ! grep -Eq "^[[:space:]]*--- PASS: ${test_name} [(]" "$ci_output"; then
    printf 'Integration gate failed: missing PASS for %s\n' "$test_name" >&2
    exit 1
  fi
done
