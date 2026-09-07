#!/bin/bash
# Runs all three layers in one command. No network, no real credentials, safe to run any time.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${GPT_ADS_FEED_PYTHON:-python3}"
FAIL=0

run() {
  local label="$1"; shift
  echo "── $label ──"
  # -B keeps __pycache__ from being written
  if ! "$PY" -B "$@"; then
    FAIL=1
    echo "   ^ this layer failed"
  fi
  echo
}

run "unit self-test (function level)" "$HERE/../feed_sync.py" --self-test
run "end-to-end (orchestration path)" "$HERE/test_e2e.py"
run "guard branches (missing dependency / missing credentials)" "$HERE/test_guards.py"

if [[ $FAIL -ne 0 ]]; then
  echo "there are failures, do not publish"
  exit 1
fi
echo "all three layers green"
