#!/bin/bash

# build.sh - Build script for MonsterMQ Main Broker, Desktop Apps, and Docker Images
#
# Usage:
#   ./build.sh               Build all artifacts locally (broker zip, desktop apps, docker image)
#   ./build.sh --broker      Build Java broker zip bundle only
#   ./build.sh --desktop     Build Electron desktop apps only
#   ./build.sh --docker      Build local Docker image only
#   ./build.sh --publish     Build everything and publish to GitHub & Docker Hub

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

RAW_VERSION=$(head -n 1 version.txt | tr -d '\r' | tr -d '\n')
VERSION=$(echo "$RAW_VERSION" | cut -d'+' -f1)

BUILD_BROKER=false
BUILD_DESKTOP=false
BUILD_DOCKER=false
PUBLISH=false
CLEAN=false
EXPLICIT_TARGET=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all            Build all artifacts (default)"
    echo "  --broker         Build standalone Java broker bundle (zip)"
    echo "  --desktop        Build Electron desktop dashboard apps (mac/win)"
    echo "  --docker         Build local Docker image (native platform)"
    echo "  -p, --publish    Trigger ./publish.sh after building"
    echo "  --clean          Clean output build directories"
    echo "  -h, --help       Show this help message"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            BUILD_BROKER=true
            BUILD_DESKTOP=true
            BUILD_DOCKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --broker)
            BUILD_BROKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --desktop)
            BUILD_DESKTOP=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --docker)
            BUILD_DOCKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        -p|--publish)
            PUBLISH=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Default to building all if no target or clean specified
if [ "$EXPLICIT_TARGET" = false ] && [ "$CLEAN" = false ]; then
    BUILD_BROKER=true
    BUILD_DESKTOP=true
    BUILD_DOCKER=true
fi

echo -e "${GREEN}=== MonsterMQ Main Build Pipeline (v${VERSION}) ===${NC}"

if [ "$CLEAN" = true ]; then
    echo -e "${YELLOW}Cleaning output build directories...${NC}"
    rm -rf broker/target
    rm -rf dashboard/dist
    rm -rf dashboard/dist-desktop
    rm -rf releases/monstermq-broker-*.zip
    rm -rf docker/target
    echo -e "${GREEN}✓ Clean complete${NC}"
fi

if [ "$BUILD_BROKER" = false ] && [ "$BUILD_DESKTOP" = false ] && [ "$BUILD_DOCKER" = false ]; then
    echo -e "${GREEN}No build targets specified. Clean operation finished.${NC}"
    exit 0
fi


# 1. Build Broker Zip Bundle
if [ "$BUILD_BROKER" = true ]; then
    echo -e "${GREEN}[1/3] Building Web Dashboard and Java Broker...${NC}"
    
    echo -e "${YELLOW}Building web dashboard frontend...${NC}"
    (cd dashboard && npm ci && npm run build)
    
    echo -e "${YELLOW}Copying web dashboard to broker resources...${NC}"
    rm -rf broker/src/main/resources/dashboard
    mkdir -p broker/src/main/resources/dashboard
    cp -r dashboard/dist/* broker/src/main/resources/dashboard/
    rm -f broker/src/main/resources/dashboard/config/brokers.json
    
    echo -e "${YELLOW}Compiling Java broker with Maven...${NC}"
    (cd broker && mvn package -DskipTests)
    
    echo -e "${YELLOW}Packaging broker zip bundle...${NC}"
    mkdir -p releases
    BUNDLE_NAME="monstermq-broker-${VERSION}"
    STAGE_DIR="releases/${BUNDLE_NAME}"
    rm -rf "$STAGE_DIR"
    mkdir -p "$STAGE_DIR"
    
    cp broker/target/broker-1.0-SNAPSHOT.jar "${STAGE_DIR}/monstermq-broker-${VERSION}.jar"
    cp -r broker/target/dependencies "${STAGE_DIR}/dependencies"
    
    if [ -f "config.yaml.example" ]; then
        cp config.yaml.example "${STAGE_DIR}/config.yaml"
    fi
    
    cat << 'EOF' > "${STAGE_DIR}/run.sh"
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
JAR_FILE=$(ls monstermq-broker-*.jar 2>/dev/null | head -n 1)
exec java -classpath "${JAR_FILE}:dependencies/*" at.rocworks.MonsterKt "$@"
EOF
    chmod +x "${STAGE_DIR}/run.sh"

    cat << 'EOF' > "${STAGE_DIR}/run.bat"
@echo off
set SCRIPT_DIR=%~dp0
cd /d %SCRIPT_DIR%
java -classpath "monstermq-broker-*.jar;dependencies/*" at.rocworks.MonsterKt %*
EOF

    cat << EOF > "${STAGE_DIR}/README.md"
# MonsterMQ Broker v${VERSION}

## How to Run

Requirements: Java 21+

Linux / macOS:
  ./run.sh

Windows:
  run.bat
EOF

    ZIP_PATH="releases/${BUNDLE_NAME}.zip"
    rm -f "$ZIP_PATH"
    (cd releases && zip -r "${BUNDLE_NAME}.zip" "${BUNDLE_NAME}")
    rm -rf "$STAGE_DIR"
    
    echo -e "${GREEN}✓ Broker bundle created: ${YELLOW}${ZIP_PATH}${NC}"
fi

# 2. Build Desktop Dashboard Apps
if [ "$BUILD_DESKTOP" = true ]; then
    echo -e "${GREEN}[2/3] Building Desktop Dashboard Apps...${NC}"
    ./dashboard/build-desktop.sh --all
    echo -e "${GREEN}✓ Desktop apps built in dashboard/dist-desktop/${NC}"
fi

# 3. Build Docker Image (Local)
if [ "$BUILD_DOCKER" = true ]; then
    echo -e "${GREEN}[3/3] Building Local Docker Image...${NC}"
    (cd docker && ./build -n)
    echo -e "${GREEN}✓ Local Docker image built${NC}"
fi

echo -e "${GREEN}=== Build Complete ===${NC}"

if [ "$PUBLISH" = true ]; then
    echo -e "${YELLOW}Triggering release publication...${NC}"
    ./publish.sh
fi
