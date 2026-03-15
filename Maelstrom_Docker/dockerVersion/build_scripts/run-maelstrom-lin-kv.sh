#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NODE_COUNT="${NODE_COUNT:-3}"
TIME_LIMIT="${TIME_LIMIT:-30}"
RATE="${RATE:-50}"
CONCURRENCY="${CONCURRENCY:-6}"
TOPOLOGY="${TOPOLOGY:-tree}"

JAR_PATH="${JAR_PATH:-./maelstrom/maelstrom/lib/maelstrom.jar}"
BIN="${BIN:-./target/release/kv-node}"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "Error: maelstrom jar not found: $JAR_PATH"
  exit 1
fi

if [[ ! -x "$BIN" ]]; then
  echo "Error: node binary not found or not executable: $BIN"
  echo "Try building first with: cargo build --release"
  exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  echo "Error: java not found in PATH"
  exit 1
fi

mkdir -p store

echo "[maelstrom-lin-kv] running base lin-kv test (headless, jar mode)"
java -Djava.awt.headless=true -jar "$JAR_PATH" test \
  -w lin-kv \
  --bin "$BIN" \
  --node-count "$NODE_COUNT" \
  --time-limit "$TIME_LIMIT" \
  --rate "$RATE" \
  --concurrency "$CONCURRENCY" \
  --topology "$TOPOLOGY"