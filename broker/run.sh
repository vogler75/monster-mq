#!/bin/bash

# Check for -build option
BUILD_FIRST=false
REMAINING_ARGS=()

for arg in "$@"; do
    case $arg in
        -build)
            BUILD_FIRST=true
            ;;
        *)
            REMAINING_ARGS+=("$arg")
            ;;
    esac
done

# If -build option is specified, run Maven build first
if [ "$BUILD_FIRST" = true ]; then
    echo "Building MonsterMQ..."
    mvn clean package
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
    echo "Build completed successfully."
fi

# Kill any existing MonsterMQ instances
echo "Checking for existing MonsterMQ instances..."
EXISTING_PIDS=$(pgrep -f "at.rocworks.MonsterKt")

if [ ! -z "$EXISTING_PIDS" ]; then
    echo "Found existing MonsterMQ processes: $EXISTING_PIDS"
    echo "Killing existing instances..."

    # First try graceful shutdown with SIGTERM
    kill $EXISTING_PIDS 2>/dev/null

    # Wait up to 5 seconds for graceful shutdown
    for i in {1..5}; do
        sleep 1
        REMAINING_PIDS=$(pgrep -f "at.rocworks.MonsterKt")
        if [ -z "$REMAINING_PIDS" ]; then
            echo "Existing instances terminated gracefully."
            break
        fi
        echo "Waiting for graceful shutdown... ($i/5)"
    done

    # Force kill if still running
    REMAINING_PIDS=$(pgrep -f "at.rocworks.MonsterKt")
    if [ ! -z "$REMAINING_PIDS" ]; then
        echo "Force killing remaining processes: $REMAINING_PIDS"
        kill -9 $REMAINING_PIDS 2>/dev/null
        sleep 1
    fi

    echo "All existing MonsterMQ instances stopped."
else
    echo "No existing MonsterMQ instances found."
fi

echo "Starting MonsterMQ..."
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt "${REMAINING_ARGS[@]}"

