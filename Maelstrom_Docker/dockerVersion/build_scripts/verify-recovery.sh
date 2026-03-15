#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CRASH_TARGET="${CRASH_TARGET:-n1}"
SLEEP_BEFORE_CRASH="${SLEEP_BEFORE_CRASH:-8}"
SLEEP_DOWN_TIME="${SLEEP_DOWN_TIME:-5}"
SLEEP_AFTER_RESUME="${SLEEP_AFTER_RESUME:-8}"

echo "[verify-recovery] fresh reset"
./build_scripts/reset-cluster.sh

echo "[verify-recovery] starting cluster"
./build_scripts/start-cluster.sh

echo
echo "[verify-recovery] now start your workload in another terminal"
echo "Examples:"
echo "  ./build_scripts/run-maelstrom-lin-kv.sh"
echo "  ./build_scripts/run-maelstrom-partition.sh"
echo
echo "Will crash ${CRASH_TARGET} after ${SLEEP_BEFORE_CRASH}s ..."
sleep "$SLEEP_BEFORE_CRASH"

echo "[verify-recovery] crashing ${CRASH_TARGET}"
./build_scripts/nemesis.sh crash "$CRASH_TARGET"

echo "[verify-recovery] ${CRASH_TARGET} down for ${SLEEP_DOWN_TIME}s"
sleep "$SLEEP_DOWN_TIME"

echo "[verify-recovery] resuming ${CRASH_TARGET}"
./build_scripts/nemesis.sh resume "$CRASH_TARGET"

echo "[verify-recovery] waiting ${SLEEP_AFTER_RESUME}s for recovery"
sleep "$SLEEP_AFTER_RESUME"

echo
echo "[verify-recovery] tail logs:"
./build_scripts/nemesis.sh logs n1 || true
./build_scripts/nemesis.sh logs n2 || true
./build_scripts/nemesis.sh logs n3 || true

echo
echo "[verify-recovery] done"
echo "Check for:"
echo "  - restart restore logs"
echo "  - recovered applied_idx"
echo "  - node catches up after resume"
echo "  - committed writes remain readable"