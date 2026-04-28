#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${CONFIG_PATH:-config.yaml}"
LOG_DIR="${LOG_DIR:-reports/job_logs}"
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi
mkdir -p "$LOG_DIR"

exec "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
  --config "$CONFIG_PATH" \
  --profile daily \
  --go \
  --lock-file reports/daily.lock \
  --log-rotate-bytes "${LOG_ROTATE_BYTES:-10485760}" \
  "$@" \
  >> "$LOG_DIR/daily.log" 2>&1
