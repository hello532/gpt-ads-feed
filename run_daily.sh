#!/bin/bash
# launchd entry point. launchd does not read a shell profile, so credentials must be loaded explicitly here.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${GPT_ADS_FEED_ENV:-$HOME/.config/gpt-ads-feed/env}"
CONFIG="${GPT_ADS_FEED_CONFIG:-$HERE/config.json}"
LOG_DIR="${GPT_ADS_FEED_LOG_DIR:-$HOME/Library/Logs/gpt-ads-feed}"

mkdir -p "$LOG_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "credentials file $ENV_FILE is missing (see env.example)" >&2
  exit 2
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "config file $CONFIG is missing (see config.example.json)" >&2
  exit 2
fi

# Export KEY=VALUE lines only, ignoring comments and blank lines; never echo a value
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

PY="${GPT_ADS_FEED_PYTHON:-python3}"
exec "$PY" "$HERE/feed_sync.py" --config "$CONFIG" "$@" \
  >> "$LOG_DIR/feed_sync.log" 2>&1
