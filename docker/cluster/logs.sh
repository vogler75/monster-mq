#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <container_number>"
    echo "Example: $0 1"
    exit 1
fi

CONTAINER_NR=$1
CONTAINER_NAME="cluster-monster${CONTAINER_NR}-1"
DELAY=1

while true; do
    docker logs -f "$CONTAINER_NAME" 
    if [ $? -ne 0 ]; then
        echo "Container $CONTAINER_NAME not found or error occurred. Retrying in ${DELAY}s..."
    else
        echo "Connection lost. Reconnecting in ${DELAY}s..."
    fi
    sleep "$DELAY"
done
