#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# 这个脚本给你持续随机 crash/restart。
# 它更接近项目要求里的 fault injection，但不负责历史判定。

DURATION="${DURATION:-60}"
SLEEP_SECS="${SLEEP_SECS:-5}"
TARGETS=(n1 n2 n3)
ACTIONS=(crash restart)

END_TS=$(( $(date +%s) + DURATION ))

while [[ $(date +%s) -lt $END_TS ]]; do
  IDX=$(( RANDOM % 3 ))
  AIDX=$(( RANDOM % 2 ))
  TARGET="${TARGETS[$IDX]}"
  ACTION="${ACTIONS[$AIDX]}"

  echo "[nemesis-loop] $ACTION $TARGET"
  ./build_scripts/nemesis.sh "$ACTION" "$TARGET" || true
  sleep "$SLEEP_SECS"
done

echo "nemesis loop finished"