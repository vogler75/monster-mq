#!/bin/bash
set -euo pipefail

# Start the Vite dev server, listening on all interfaces so the dashboard is
# reachable from other machines (http://<this-host>:5173).
#
# Note the `--` before the flags: `npm run dev --host` does NOT work, because
# npm consumes --host as one of its own options instead of forwarding it to
# Vite. Everything after `--` is passed through.
#
# Usage:
#   ./run.sh                          # expose on all interfaces, port 5173
#   ./run.sh --port 8080              # ...on a different port
#   ./run.sh --local                  # localhost only (Vite's default)
#   VITE_LOCAL_GRAPHQL_TARGET=http://broker-host:4000 ./run.sh
#                                     # proxy /graphql to a specific broker

cd "$(dirname "$0")"

EXPOSE=true
ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local)
      EXPOSE=false
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ "$EXPOSE" = true ]]; then
  ARGS=(--host "${ARGS[@]+"${ARGS[@]}"}")
  # Vite refuses requests whose Host header it does not recognise (DNS-rebinding
  # protection), which breaks access by hostname even when --host is set.
  # vite.config.js reads this to relax that check while we are deliberately
  # exposing the dev server.
  export VITE_EXPOSE=1
fi

exec npm run dev -- "${ARGS[@]+"${ARGS[@]}"}"
