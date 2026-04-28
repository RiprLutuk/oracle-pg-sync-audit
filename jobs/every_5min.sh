#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${CONFIG_PATH:-config.yaml}"
LOG_DIR="${LOG_DIR:-reports/job_logs}"
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
ALERT_COMMAND="${ALERT_COMMAND:-}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python"
fi
mkdir -p "$LOG_DIR"

run_job() {
  "$PYTHON_BIN" -m oracle_pg_sync.ops sync \
    --config "$CONFIG_PATH" \
    --profile every_5min \
    --go \
    --lock-file reports/every_5min.lock \
    --log-rotate-bytes "${LOG_ROTATE_BYTES:-10485760}" \
    "$@"
}

set +e
run_job "$@" >> "$LOG_DIR/every_5min.log" 2>&1
status=$?
set -e
if [[ "$status" -ne 0 && -n "$ALERT_COMMAND" ]]; then
  ALERT_MESSAGE="oracle-pg-sync every_5min failed exit_code=$status log=$LOG_DIR/every_5min.log" \
    bash -c "$ALERT_COMMAND" || true
fi
exit "$status"
