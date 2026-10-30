#!/usr/bin/env bash
# Explicit phases only. Offline/performance never invoke the Mainnet operator.
set -euo pipefail
cd "$(dirname "$0")/.."
usage() {
  echo 'Usage: bash scripts/validate-final.sh offline --opt-in FIXTURE_DIRECTORY | performance --opt-in NEW_REPORT_DIRECTORY [baseline flags] | live --opt-in PRIVATE_CONFIG' >&2
  exit 2
}
[[ $# -ge 3 && $2 == --opt-in ]] || usage
phase=$1
shift 2
trap 'printf "FAIL %s: phase aborted; subsequent commands were not run\n" "$phase" >&2' ERR
export ETL_TEST_RETAIN=1
case "$phase" in
  offline)
    [[ $# == 1 ]] || usage
    export ETL_FINAL_VALIDATION=1 ETL_STORAGE_VERIFY=1
    echo 'NOT RUN performance-baselines: separate performance phase required'
    echo 'NOT RUN manual-release-review: final checklist required'
    bash scripts/validate-stage13.sh "$1"
    ;;
  performance)
    out=$1
    shift
    export ETHEREUM_LIVE_MAINNET_OPT_IN=false ETHEREUM_CAPTURE_OPT_IN=false
    # Existing runner validates bounded workers/repetitions and forbids CI.
    echo 'NOT RUN live-mainnet-e2e: separate live phase required'
    go run ./test/integration/cmd/handoff-postgres --baseline --opt-in --out "$out" "$@"
    echo 'PASS requested-offline-performance-scenarios (review integrity reports)'
    ;;
  live)
    [[ $# == 1 ]] || usage
    [[ -z ${CI:-} && -z ${GITHUB_ACTIONS:-} ]] || { echo 'Live validation is forbidden in CI' >&2; exit 2; }
    # Existing private config opt-in, chain/range, 100-attempt and 2m guards apply.
    # Prerequisite: operator has checked/upgraded the retained Mainnet project.
    go run ./tools/mainnet e2e "$1"
    go run ./tools/mainnet verify "$1"
    echo 'PASS controlled-live-e2e-and-verification'
    echo 'NOT RUN manual-release-review: retain evidence and review recovery checklist'
    ;;
  *) usage ;;
esac
