#!/bin/bash

# Function to clean up any running server container
cleanup() {
    docker kill server > /dev/null 2>&1
}
cleanup

set -e

# Pull the server image if specified
if [ "$PULL_IMAGE" = "true" ]; then
    docker pull "$SERVER_IMAGE"
else
    # Sleep to avoid connecting to any currently shutting down servers
    sleep 2
fi

OUTPUT_DIR=./results
CONTAINER_OUTPUT_DIR=/app
CONFIG_FILE=./server-config.toml
CONTAINER_CONFIG_FILE=/server-config.toml
CLUSTER_CONFIG_FILE=./cluster-config.toml
CONTAINER_CLUSTER_CONFIG_FILE=/cluster-config.toml

# Generate output directory
mkdir -p "$OUTPUT_DIR"

# Generate configuration files
echo -e "$SERVER_CONFIG_TOML" > "$CONFIG_FILE"
echo -e "$CLUSTER_CONFIG_TOML" > "$CLUSTER_CONFIG_FILE"

# Ensure the container is killed when this script exits.
# Note: will only work with ssh with -t flag
trap cleanup EXIT SIGHUP SIGINT SIGPIPE SIGTERM SIGQUIT

# Run the Docker container
docker run \
    --init \
    --name server \
    -p "$LISTEN_PORT:$LISTEN_PORT" \
    --env RUST_LOG="$RUST_LOG" \
    --env SERVER_CONFIG_FILE="$CONTAINER_CONFIG_FILE" \
    --env CLUSTER_CONFIG_FILE="$CONTAINER_CLUSTER_CONFIG_FILE" \
    -v "$CONFIG_FILE:$CONTAINER_CONFIG_FILE" \
    -v "$CLUSTER_CONFIG_FILE:$CONTAINER_CLUSTER_CONFIG_FILE" \
    -v "$OUTPUT_DIR:$CONTAINER_OUTPUT_DIR" \
    --rm \
    "$SERVER_IMAGE" \
    2> "$OUTPUT_DIR/xerr-server-$SERVER_ID.log"
