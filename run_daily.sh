#!/bin/bash
# launchd 入口。launchd 不读 shell profile，所以凭证必须在这里显式加载。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${GPT_ADS_FEED_ENV:-$HOME/.config/gpt-ads-feed/env}"
CONFIG="${GPT_ADS_FEED_CONFIG:-$HERE/config.json}"
LOG_DIR="${GPT_ADS_FEED_LOG_DIR:-$HOME/Library/Logs/gpt-ads-feed}"

mkdir -p "$LOG_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "缺少凭证文件 $ENV_FILE（参考 env.example）" >&2
  exit 2
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "缺少配置文件 $CONFIG（参考 config.example.json）" >&2
  exit 2
fi

# 只导出 KEY=VALUE 行，忽略注释与空行；不 echo 任何值
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PY="${GPT_ADS_FEED_PYTHON:-python3}"
exec "$PY" "$HERE/feed_sync.py" --config "$CONFIG" "$@" \
  >> "$LOG_DIR/feed_sync.log" 2>&1
