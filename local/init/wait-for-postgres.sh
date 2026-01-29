#!/bin/sh
# wait-for-postgres.sh
# Wait until Postgres is accepting connections.
# Usage: ./wait-for-postgres.sh host port user timeout_seconds

HOST=${1:-postgres}
PORT=${2:-5432}
USER=${3:-system}
TIMEOUT=${4:-60}

echo "Waiting for Postgres at ${HOST}:${PORT} as user ${USER} (timeout ${TIMEOUT}s)..."

start_ts=$(date +%s)
while true; do
  if pg_isready -h "$HOST" -p "$PORT" -U "$USER" >/dev/null 2>&1; then
    echo "Postgres is ready."
    exit 0
  fi
  now=$(date +%s)
  elapsed=$((now - start_ts))
  if [ "$elapsed" -ge "$TIMEOUT" ]; then
    echo "Timed out waiting for Postgres after ${TIMEOUT}s" >&2
    exit 1
  fi
  sleep 1
done
