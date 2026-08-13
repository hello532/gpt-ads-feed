#!/bin/bash
# 一条命令跑完三层验证。不出网、不用真凭证，随时可跑。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${GPT_ADS_FEED_PYTHON:-python3}"
FAIL=0

run() {
  local label="$1"; shift
  echo "── $label ──"
  # -B 避免生成 __pycache__
  if ! "$PY" -B "$@"; then
    FAIL=1
    echo "   ↑ 这一层失败"
  fi
  echo
}

run "单元自检（函数级）" "$HERE/../feed_sync.py" --self-test
run "端到端（编排链路）" "$HERE/test_e2e.py"
run "守卫分支（缺依赖/缺凭证）" "$HERE/test_guards.py"

if [[ $FAIL -ne 0 ]]; then
  echo "有失败项，别发布"
  exit 1
fi
echo "三层全绿"
