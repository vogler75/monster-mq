#!/bin/bash

# MonsterMQ Run Script
# Usage: ./run.sh [OPTIONS] [MONSTER_OPTIONS]
#
# Script Options:
#   -build    Build the project with Maven before starting
#   -dev      Run in development mode (serves dashboard from filesystem)
#
# Monster Options:
#   All remaining arguments are passed to MonsterMQ
#   See: java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -help
#
# Examples:
#   ./run.sh                           # Start with default config
#   ./run.sh -build                    # Build first, then start  
#   ./run.sh -dev                      # Development mode
#   ./run.sh -dev -config test.yaml    # Development mode with custom config
#   ./run.sh -build -dev -cluster      # Build, dev mode, cluster enabled

# Check for options
BUILD_FIRST=false
DEV_MODE=false
REMAINING_ARGS=()

for arg in "$@"; do
    case $arg in
        -build)
            BUILD_FIRST=true
            ;;
        -dev)
            DEV_MODE=true
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

# Detect if Java is GraalVM
JAVA_VERSION=$(java -version 2>&1)
if echo "$JAVA_VERSION" | grep -q "GraalVM"; then
    echo "Detected GraalVM - enabling JVMCI for optimal JavaScript performance"
    JAVA_OPTS="-XX:+EnableJVMCI -XX:+UseJVMCICompiler"
else
    echo "Using standard JVM (GraalVM not detected)"
    JAVA_OPTS=""
fi

JAVA_OPTS="$JAVA_OPTS --enable-native-access=ALL-UNNAMED"

# Prepare development options
DEV_OPTS=""
if [ "$DEV_MODE" = true ]; then
    echo "Running in development mode - serving dashboard from filesystem"
    DEV_OPTS="-dashboardPath src/main/resources/dashboard"
fi

# Start MonsterMQ
echo java $JAVA_OPTS -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt $DEV_OPTS "${REMAINING_ARGS[@]}"
java $JAVA_OPTS -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt $DEV_OPTS "${REMAINING_ARGS[@]}"
