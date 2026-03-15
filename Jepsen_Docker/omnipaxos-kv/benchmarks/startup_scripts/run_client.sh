#!/bin/bash

# Function to clean up any running client container
cleanup() {
    docker kill client > /dev/null 2>&1
}
cleanup

set -e

# Optionally pull a fresh image if needed.
if [ "$PULL_IMAGE" = "true" ]; then
    docker pull "${CLIENT_IMAGE}"
else
    # A brief pause to avoid connecting to shutting down servers.
    sleep 2
fi


CONFIG_FILE=./client-config.toml
CONTAINER_CONFIG_FILE=/client-config.toml
OUTPUT_DIR=./results
CONTAINER_OUTPUT_DIR=/app

# Generate output directory
mkdir -p "${OUTPUT_DIR}"

# Generate the configuration file.
echo -e "${CLIENT_CONFIG_TOML}" > "${CONFIG_FILE}"

# Ensure the container is killed when this script exits.
# Note: will only work with ssh with -t flag
trap cleanup EXIT SIGHUP SIGINT SIGPIPE SIGTERM SIGQUIT

# Run the Docker container.
docker run \
    --init \
    --name=client \
    --rm \
    --env RUST_LOG="${RUST_LOG}" \
    --env CONFIG_FILE="${CONTAINER_CONFIG_FILE}" \
    -v "${CONFIG_FILE}:${CONTAINER_CONFIG_FILE}" \
    -v "${OUTPUT_DIR}:${CONTAINER_OUTPUT_DIR}" \
    -t \
    "${CLIENT_IMAGE}"
