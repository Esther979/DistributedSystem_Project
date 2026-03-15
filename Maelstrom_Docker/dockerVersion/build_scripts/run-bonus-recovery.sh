#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CRASH_TARGET="${CRASH_TARGET:-n1}"
TIME_LIMIT="${TIME_LIMIT:-60}"
RATE="${RATE:-80}"
CONCURRENCY="${CONCURRENCY:-8}"
TOPOLOGY="${TOPOLOGY:-tree}"

echo "[run-bonus-recovery] reset + start docker cluster"
./build_scripts/reset-cluster.sh
./build_scripts/start-cluster.sh

echo "[run-bonus-recovery] start workload in background"
(
  TIME_LIMIT="$TIME_LIMIT" RATE="$RATE" CONCURRENCY="$CONCURRENCY" TOPOLOGY="$TOPOLOGY" \
  ./build_scripts/run-maelstrom-lin-kv.sh
) &
WORKLOAD_PID=$!

echo "[run-bonus-recovery] workload pid=$WORKLOAD_PID"
sleep 10

echo "[run-bonus-recovery] crash target: $CRASH_TARGET"
./build_scripts/nemesis.sh crash "$CRASH_TARGET"
sleep 5
./build_scripts/nemesis.sh resume "$CRASH_TARGET"

echo "[run-bonus-recovery] waiting for workload to finish"
wait "$WORKLOAD_PID" || true

echo "[run-bonus-recovery] done"