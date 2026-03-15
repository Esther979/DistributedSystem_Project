#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FOLLOWER_TARGET="${FOLLOWER_TARGET:-n2}"
SLEEP_AFTER_START="${SLEEP_AFTER_START:-8}"
SLEEP_DOWN="${SLEEP_DOWN:-5}"
SLEEP_AFTER_RESUME="${SLEEP_AFTER_RESUME:-8}"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    echo "Error: neither 'docker compose' nor 'docker-compose' is available."
    exit 1
  fi
}

container_name() {
  case "$1" in
    n1) echo "omnipaxos-n1" ;;
    n2) echo "omnipaxos-n2" ;;
    n3) echo "omnipaxos-n3" ;;
    *)
      echo "unknown node: $1" >&2
      exit 1
      ;;
  esac
}

reset_cluster() {
  echo "[bonus-smoke] reset cluster"
  compose down -v || true
  compose build
}

start_cluster() {
  echo "[bonus-smoke] start cluster"
  compose up -d
  compose ps
}

crash_node() {
  local node="$1"
  local c
  c="$(container_name "$node")"
  echo "[bonus-smoke] docker kill $c"
  docker kill "$c"
}

resume_node() {
  local node="$1"
  echo "[bonus-smoke] resume $node"
  compose up -d "$node"
}

tail_logs() {
  echo
  echo "========== n1 logs =========="
  docker logs --tail 120 omnipaxos-n1 || true
  echo
  echo "========== n2 logs =========="
  docker logs --tail 120 omnipaxos-n2 || true
  echo
  echo "========== n3 logs =========="
  docker logs --tail 120 omnipaxos-n3 || true
}

find_leader() {
  # 这是启发式查找：从最近日志里抓 leader 相关字样
  # 如果你日志关键字不同，改这里的 grep 即可
  local nodes=("n1" "n2" "n3")
  local best_node=""
  local best_score=0

  for n in "${nodes[@]}"; do
    local c
    c="$(container_name "$n")"
    local score
    score="$(docker logs --tail 300 "$c" 2>&1 | grep -Ei "leader|became leader|elected" | wc -l | tr -d ' ')"
    if [[ "$score" -gt "$best_score" ]]; then
      best_score="$score"
      best_node="$n"
    fi
  done

  if [[ -z "$best_node" ]]; then
    echo "n1"
  else
    echo "$best_node"
  fi
}

show_storage_tree() {
  echo
  echo "========== storage check =========="
  for n in n1 n2 n3; do
    local c
    c="$(container_name "$n")"
    echo "--- $n (/data) ---"
    docker exec "$c" sh -lc 'find /data -maxdepth 3 -type f | sort | head -100' || true
  done
}

echo "[bonus-smoke] phase 1: reset + start"
reset_cluster
start_cluster

echo "[bonus-smoke] wait ${SLEEP_AFTER_START}s for startup"
sleep "$SLEEP_AFTER_START"

echo
echo "[bonus-smoke] phase 2: crash follower candidate ${FOLLOWER_TARGET}"
crash_node "$FOLLOWER_TARGET"

echo "[bonus-smoke] keep it down ${SLEEP_DOWN}s"
sleep "$SLEEP_DOWN"

echo "[bonus-smoke] resume ${FOLLOWER_TARGET}"
resume_node "$FOLLOWER_TARGET"

echo "[bonus-smoke] wait ${SLEEP_AFTER_RESUME}s for follower recovery"
sleep "$SLEEP_AFTER_RESUME"

echo
echo "[bonus-smoke] phase 3: detect leader from logs"
LEADER_NODE="$(find_leader)"
echo "[bonus-smoke] detected leader candidate: ${LEADER_NODE}"

echo
echo "[bonus-smoke] phase 4: crash leader candidate ${LEADER_NODE}"
crash_node "$LEADER_NODE"

echo "[bonus-smoke] keep leader down ${SLEEP_DOWN}s"
sleep "$SLEEP_DOWN"

echo "[bonus-smoke] resume leader ${LEADER_NODE}"
resume_node "$LEADER_NODE"

echo "[bonus-smoke] wait ${SLEEP_AFTER_RESUME}s for leader recovery"
sleep "$SLEEP_AFTER_RESUME"

tail_logs
show_storage_tree

echo
echo "[bonus-smoke] done"
echo "Check logs for:"
echo "  - fresh start / restart"
echo "  - restored snapshot / applied_idx"
echo "  - recovery replay"
echo "  - leader re-election"
echo "  - resumed node catches up"