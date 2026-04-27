#!/bin/bash
# MonsterMQ Edge run script (Go broker).
# Usage: ./run.sh [SCRIPT_OPTIONS] [-- BROKER_OPTIONS]
#
# Script options (before --):
#   -c,  -clean       Run "go clean -cache" before anything else (forces a
#                     full recompile next build)
#   -b,  -build       Build the binary (CGO_ENABLED=0) before starting
#   -n,  -norun       Don't start the broker (combine with -b/-c for no-run)
#   -d,  -dashboard   Build the dashboard and serve it from dashboard/dist
#   -nk, -nokill      Don't kill any already-running monstermq-edge first
#   -h,  -help        Show this help
#
# Broker options (after --):
#   Anything after -- is passed straight to the binary.
#   See ./bin/monstermq-edge -h for available flags (-config, -log-level, -version).
#
# Examples:
#   ./run.sh                       Start with config.yaml (or config.yaml.example)
#   ./run.sh -b                    Build, then start
#   ./run.sh -c -b                 Clean cache, rebuild from scratch, then start
#   ./run.sh -b -n                 Build only
#   ./run.sh -b -- -config foo.yaml
#   ./run.sh -d                    Serve the dashboard from filesystem

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

BIN="bin/monstermq-edge"
CLEAN=false
BUILD=false
NORUN=false
DASHBOARD_DEV=false
NOKILL=false
REMAINING=()
SAW_SEP=false

for arg in "$@"; do
    if [ "$SAW_SEP" = true ]; then
        REMAINING+=("$arg")
        continue
    fi
    case "$arg" in
        -c|-clean)        CLEAN=true ;;
        -b|-build)        BUILD=true ;;
        -n|-norun)        NORUN=true ;;
        -d|-dashboard)    DASHBOARD_DEV=true ;;
        -nk|-nokill)      NOKILL=true ;;
        -h|-help|--help)  sed -n '2,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        --)               SAW_SEP=true ;;
        *)                REMAINING+=("$arg") ;;
    esac
done

if [ "$CLEAN" = true ]; then
    echo "Cleaning Go build cache..."
    go clean -cache
fi

if [ "$BUILD" = true ]; then
    echo "Building monstermq-edge..."
    mkdir -p bin
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$BIN" ./cmd/monstermq-edge
    echo "Built: $BIN ($(du -h "$BIN" | cut -f1))"
fi

if [ "$DASHBOARD_DEV" = true ]; then
    DASH="$(cd "$SCRIPT_DIR/../dashboard" 2>/dev/null && pwd || true)"
    if [ -n "$DASH" ] && [ -f "$DASH/package.json" ]; then
        echo "Building dashboard at $DASH..."
        (cd "$DASH" && npm install --silent && npm run build)
        if [ -d "$DASH/dist" ]; then
            export MONSTERMQ_DASHBOARD_PATH="$DASH/dist"
            echo "Dashboard dist available at: $DASH/dist"
            echo "(set Dashboard.Path in config.yaml to: $DASH/dist)"
        fi
    else
        echo "Dashboard sibling project not found, skipping."
    fi
fi

[ "$NORUN" = true ] && exit 0

if [ ! -x "$BIN" ]; then
    echo "Binary $BIN not found. Run with -b first." >&2
    exit 1
fi

if [ "$NOKILL" = false ]; then
    EXISTING=$(pgrep -f "$BIN" || true)
    if [ -n "$EXISTING" ]; then
        echo "Killing existing instances: $EXISTING"
        kill $EXISTING 2>/dev/null || true
        for i in 1 2 3 4 5; do
            sleep 0.5
            STILL=$(pgrep -f "$BIN" || true)
            [ -z "$STILL" ] && break
        done
        STILL=$(pgrep -f "$BIN" || true)
        [ -n "$STILL" ] && kill -9 $STILL 2>/dev/null || true
    fi
fi

# Pick a config: explicit -config wins; otherwise prefer config.yaml then the example.
HAS_CONFIG=false
for arg in "${REMAINING[@]}"; do
    [ "$arg" = "-config" ] && HAS_CONFIG=true
done
if [ "$HAS_CONFIG" = false ]; then
    if [ -f "config.yaml" ]; then
        REMAINING=("-config" "config.yaml" "${REMAINING[@]}")
    elif [ -f "config.yaml.example" ]; then
        REMAINING=("-config" "config.yaml.example" "${REMAINING[@]}")
    fi
fi

echo "Starting: $BIN ${REMAINING[*]}"
exec "$BIN" "${REMAINING[@]}"
