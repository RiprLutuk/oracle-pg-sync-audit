#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$SCRIPT_DIR/every_5min.sh" \
  --direction postgres-to-oracle \
  --tables public.address \
  --mode upsert \
  --key-columns address_id \
  --incremental-column last_update
