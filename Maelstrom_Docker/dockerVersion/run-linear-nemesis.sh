#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

export KEYSPACE="${KEYSPACE:-3}"
export CLIENTS="${CLIENTS:-6}"
export DURATION_S="${DURATION_S:-45}"
export RPC_TIMEOUT_S="${RPC_TIMEOUT_S:-3.0}"
export VERIFY_TIMEOUT_S="${VERIFY_TIMEOUT_S:-15.0}"
export READ_PCT="${READ_PCT:-0.35}"
export WRITE_PCT="${WRITE_PCT:-0.35}"
export NEMESIS_INTERVAL_S="${NEMESIS_INTERVAL_S:-6.0}"
export SEED="${SEED:-$(date +%s)}"

exec python3 "$ROOT_DIR/omni_nemesis_linear_test.py"
