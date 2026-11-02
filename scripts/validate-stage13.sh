#!/usr/bin/env bash
# One offline acceptance pass. No live RPC/capture, no volume deletion.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# != 1 ]]; then
  echo 'Usage: bash scripts/validate-stage13.sh LOCAL_AUTHENTIC_FIXTURE_DIRECTORY' >&2
  exit 2
fi
fixtures=$1
for block in 1000000 22431084 12965000; do
  if [[ ! -f "$fixtures/$block.json" || -L "$fixtures/$block.json" ]]; then
    echo "Required authentic fixture missing or nonregular: $block" >&2
    exit 2
  fi
done
umask 077
report_dir=$(mktemp -d "${TMPDIR:-/tmp}/ethereum-stage13-validation.XXXXXX")
printf 'Validation logs: %s\n' "$report_dir"
export ETL_TEST_RETAIN=1 ETHEREUM_LIVE_MAINNET_OPT_IN=false ETHEREUM_CAPTURE_OPT_IN=false
failed=0
run_gate() {
  local name=$1
  shift
  printf 'Running %s\n' "$name"
  if "$@" > "$report_dir/$name.log" 2>&1; then
    printf 'PASS %s\n' "$name" | tee -a "$report_dir/results.txt"
  else
    printf 'FAIL %s (see retained log)\n' "$name" | tee -a "$report_dir/results.txt"
    failed=1
  fi
}
format_check() {
  local unformatted
  # run_gate invokes this function in an if condition, disabling errexit here.
  # Check the pipeline explicitly so a tool failure cannot become an empty PASS.
  if ! unformatted=$(rg --files -0 -g '*.go' -g '!bin/**' | xargs -0 gofmt -l); then
    return 1
  fi
  [[ -z "$unformatted" ]] || { printf '%s\n' "$unformatted"; return 1; }
}
fixture_digests() {
  shasum -a 256 "$fixtures/1000000.json" "$fixtures/22431084.json" "$fixtures/12965000.json"
}
fixture_digests > "$report_dir/fixtures-before.sha256"
run_gate formatting format_check
run_gate build go build ./...
run_gate unit go test -count=1 -timeout=10m ./...
if [[ ${ETL_FINAL_VALIDATION:-0} == 1 ]]; then
  run_gate targeted-race go test -race -count=1 -timeout=10m -tags perfbaseline \
    ./internal/adapters/grpccontract ./internal/adapters/metrics ./internal/adapters/baseline \
    ./services/extractor-service/internal/adapters/outbound/jsonrpc \
    ./services/extractor-service/internal/adapters/outbound/grpc \
    ./services/transformer-service/internal/adapters/outbound/grpc \
    ./services/loader-service/internal/adapters/outbound/postgres ./tools/mainnet
else
  run_gate race go test -race -count=1 -timeout=10m ./...
fi
run_gate vet go vet ./...
run_gate protobuf sh scripts/generate-proto.sh --check
run_gate architecture go test -count=1 -v ./test/architecture
fixture_suite=fixture-e2e
if [[ ${ETL_FINAL_VALIDATION:-0} == 1 ]]; then fixture_suite=storage-release; fi
for suite in extractor-postgres transformer-postgres loader-postgres handoff-postgres "$fixture_suite"; do
  run_gate "$suite" bash scripts/ci-integration.sh "$suite"
done
run_gate authentic-offline go run ./test/integration/cmd/handoff-postgres --mainnet-fixtures "$fixtures"
fixture_digests > "$report_dir/fixtures-after.sha256"
run_gate immutable-fixtures diff -u "$report_dir/fixtures-before.sha256" "$report_dir/fixtures-after.sha256"
run_gate diff git diff --check
printf '%s\n' 'NOT RUN live-mainnet-e2e: separate explicit user execution required' | tee -a "$report_dir/results.txt"
printf 'Offline validation failures: %s. Logs retained: %s\n' "$failed" "$report_dir"
exit "$failed"
