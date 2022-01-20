#!/bin/sh
set -e

SCRIPT_PATH=$(realpath "$0")
SCRIPT_PATH=$(dirname $SCRIPT_PATH)
echo $SCRIPT_PATH

docker build -t gcp_pubsub_emulator - < ${SCRIPT_PATH}/../dockerfiles/Dockerfile.gcp_pubsub

docker run -p 127.0.0.1:8538:8538 --rm gcp_pubsub_emulator
