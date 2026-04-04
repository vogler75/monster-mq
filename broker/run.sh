#!/bin/bash

# MonsterMQ Run Script
# Usage: ./run.sh [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]
#
# Script Options (before --):
#   -build    Build the project with Maven before starting
#
# Monster Options (after --):
#   All arguments after -- are passed to MonsterMQ
#   See: java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -help
#
# Examples:
#   ./run.sh                                              # Start with default config
#   ./run.sh -build                                       # Build first, then start
#   ./run.sh -- -cluster                                  # Start with monster options
#   ./run.sh -- -log FINEST                               # Start with temporary CLI log override
#   ./run.sh -build -- -cluster                           # Build and start in cluster mode
#   ./run.sh -build -- -archiveConfigs archives.json       # Build and import archive configs

# Parse script options (before --) and monster options (after --)
BUILD_FIRST=false
NO_RUN=false
NO_KILL=false
DASHBOARD_DEV=false
REMAINING_ARGS=()
FOUND_SEPARATOR=false

for arg in "$@"; do
    if [ "$FOUND_SEPARATOR" = true ]; then
        REMAINING_ARGS+=("$arg")
    elif [ "$arg" = "--" ]; then
        FOUND_SEPARATOR=true
    else
        case $arg in
            -build|-b)
                BUILD_FIRST=true
                ;;
            -norun|-n)
                NO_RUN=true
                ;;
            -nokill|-nk)
                NO_KILL=true
                ;;
            -dashboard|-d)
                DASHBOARD_DEV=true
                ;;
            -help|--help|-h)
                echo "MonsterMQ Run Script"
                echo ""
                echo "Usage: ./run.sh [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]"
                echo ""
                echo "Script Options (before --):"
                echo "  -build, -b          Build dashboard and broker with Maven before starting"
                echo "  -norun, -n          Do not run the broker (useful with -b for build-only)"
                echo "  -dashboard, -d      Build and serve dashboard from dashboard/dist/ (for live development)"
                echo "  -nokill, -nk        Do not kill existing instances before starting"
                echo "  -help, --help, -h   Show this help message"
                echo ""
                echo "Monster Options (after --):"
                echo "  All arguments after -- are passed directly to MonsterMQ."
                echo "  Use -- -help to see MonsterMQ options."
                echo ""
                echo "Examples:"
                echo "  ./run.sh                                        Start with default config"
                echo "  ./run.sh -b                                     Build first, then start"
                echo "  ./run.sh -b -n                                  Build only, do not start"
                echo "  ./run.sh -- -cluster                           Start with broker options"
                echo "  ./run.sh -b -- -cluster                         Build and start in cluster mode"
                echo "  ./run.sh -- -log FINEST                         Start with temporary CLI log override"
                echo "  ./run.sh -b -- -archiveConfigs archives.json    Build and import configs"
                echo "  ./run.sh -d                                       Serve dashboard from filesystem"
                echo "  ./run.sh -- -help                               Show MonsterMQ help"
                exit 0
                ;;
            *)
                REMAINING_ARGS+=("$arg")
                ;;
        esac
    fi
done

# If -build option is specified, build dashboard and broker
if [ "$BUILD_FIRST" = true ]; then
    # Build the dashboard
    DASHBOARD_DIR="$(cd "$(dirname "$0")/../dashboard" && pwd)"
    RESOURCES_DIR="$(cd "$(dirname "$0")" && pwd)/src/main/resources/dashboard"
    if [ -f "$DASHBOARD_DIR/package.json" ]; then
        echo "Building dashboard..."
        (cd "$DASHBOARD_DIR" && npm install && npm run build)
        if [ $? -ne 0 ]; then
            echo "Dashboard build failed!"
            exit 1
        fi
        echo "Copying dashboard to broker resources..."
        rm -rf "$RESOURCES_DIR"
        cp -r "$DASHBOARD_DIR/dist" "$RESOURCES_DIR"
        echo "Dashboard build completed."
    else
        echo "Dashboard not found, skipping."
    fi

    # Build the broker
    echo "Building MonsterMQ..."
    mvn package -DskipTests
    if [ $? -ne 0 ]; then
        echo "Build failed!"
        exit 1
    fi
    echo "Build completed successfully."
fi

# Exit if -norun is set
if [ "$NO_RUN" = true ]; then
    exit 0
fi

# Kill any existing MonsterMQ instances (skip in cluster mode or with -nokill)
SKIP_KILL=$NO_KILL
if [ "$SKIP_KILL" = false ]; then
    for arg in "${REMAINING_ARGS[@]}"; do
        if [ "$arg" = "-cluster" ]; then
            SKIP_KILL=true
            break
        fi
    done
fi

EXISTING_PIDS=""
if [ "$SKIP_KILL" = true ]; then
    echo "Skipping kill of existing instances."
else
    echo "Checking for existing MonsterMQ instances..."
    EXISTING_PIDS=$(pgrep -f "at.rocworks.MonsterKt")
fi

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

# Serve dashboard from filesystem for development
if [ "$DASHBOARD_DEV" = true ]; then
    DASHBOARD_DIR="$(cd "$(dirname "$0")/../dashboard" && pwd)"
    DASHBOARD_DIST="$DASHBOARD_DIR/dist"
    
    # If -build was NOT specified, build the dashboard now if requested by -d
    if [ "$BUILD_FIRST" = false ]; then
        if [ -f "$DASHBOARD_DIR/package.json" ]; then
            echo "Always build requested by -d, building dashboard..."
            (cd "$DASHBOARD_DIR" && npm install && npm run build)
            if [ $? -ne 0 ]; then
                echo "Dashboard build failed!"
                exit 1
            fi
        else
            echo "Warning: Dashboard directory or package.json not found, skipping build."
        fi
    fi

    if [ -d "$DASHBOARD_DIST" ]; then
        echo "Dashboard serving from filesystem: $DASHBOARD_DIST"
        REMAINING_ARGS+=("-dashboardPath" "$DASHBOARD_DIST")
    else
        echo "Warning: dashboard/dist/ not found after build attempt."
    fi
fi

# Note: Protobuf version upgraded to 4.28.3 for Java 21+ compatibility

# Start MonsterMQ
echo java $JAVA_OPTS -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt "${REMAINING_ARGS[@]}"
java $JAVA_OPTS -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt "${REMAINING_ARGS[@]}"
