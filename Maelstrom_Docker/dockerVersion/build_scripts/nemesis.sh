#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

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

ACTION="${1:-}"
TARGET="${2:-}"

if [[ -z "$ACTION" ]]; then
  echo "usage: $0 <crash|resume|restart|logs|ps> [n1|n2|n3]"
  exit 1
fi

case "${TARGET:-}" in
  n1) CONTAINER="omnipaxos-n1" ;;
  n2) CONTAINER="omnipaxos-n2" ;;
  n3) CONTAINER="omnipaxos-n3" ;;
  "") ;;
  *)
    echo "unknown target: $TARGET"
    exit 1
    ;;
esac

case "$ACTION" in
  crash)
    [[ -n "${TARGET:-}" ]] || { echo "target required"; exit 1; }
    echo "[nemesis] docker kill $CONTAINER"
    docker kill "$CONTAINER"
    ;;

  resume)
    [[ -n "${TARGET:-}" ]] || { echo "target required"; exit 1; }
    echo "[nemesis] docker compose up -d $TARGET"
    compose up -d "$TARGET"
    ;;

  restart)
    [[ -n "${TARGET:-}" ]] || { echo "target required"; exit 1; }
    echo "[nemesis] restarting $TARGET"
    docker kill "$CONTAINER" || true
    sleep 1
    compose up -d "$TARGET"
    ;;

  logs)
    [[ -n "${TARGET:-}" ]] || { echo "target required"; exit 1; }
    docker logs --tail 200 "$CONTAINER"
    ;;

  ps)
    if [[ -n "${TARGET:-}" ]]; then
      docker ps --filter "name=$CONTAINER"
    else
      docker ps
    fi
    ;;

  *)
    echo "unknown action: $ACTION"
    exit 1
    ;;
esac