#!/usr/bin/env bash
# ==============================================================================
# MonsterMQ Java Broker - Installer Script
#
# Quick install:
#   curl -fsSL https://raw.githubusercontent.com/vogler75/monster-mq/main/install.sh | bash
#
# Options:
#   -d, --dir <path>       Target installation directory (default: ./monstermq)
#   -v, --version <tag>    Install specific version (e.g. 1.8.27, default: latest)
#   --start                Start broker immediately after installation
#   -y, --yes              Non-interactive mode (auto-confirm)
#   -h, --help             Show this help message
# ==============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

REPO="vogler75/monster-mq"
INSTALL_DIR="${MONSTERMQ_DIR:-./monstermq}"
REQUESTED_VERSION="${MONSTERMQ_VERSION:-latest}"
START_AFTER_INSTALL=false
AUTO_CONFIRM=false

usage() {
    echo -e "${BOLD}MonsterMQ Java Broker Installer${NC}"
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -d, --dir <path>       Installation directory (default: ./monstermq)"
    echo "  -v, --version <tag>    Specific version (e.g. 1.8.27, default: latest)"
    echo "  --start                Start broker immediately after install"
    echo "  -y, --yes              Non-interactive mode"
    echo "  -h, --help             Show help"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        -v|--version)
            REQUESTED_VERSION="$2"
            shift 2
            ;;
        --start)
            START_AFTER_INSTALL=true
            shift
            ;;
        -y|--yes)
            AUTO_CONFIRM=true
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

echo -e "${BLUE}${BOLD}"
echo "  __  __                  _              __  __  ____ "
echo " |  \/  | ___  _ __  ___ | |_  ___  _ __|  \/  |/ __ \\"
echo " | |\/| |/ _ \| '_ \/ __|| __|/ _ \| '__| |\/| | / / |"
echo " | |  | | (_) | | | \__ \| |_|  __/| |  | |  | | \ \_|"
echo " |_|  |_|\___/|_| |_|___/ \__|\___||_|  |_|  |_|\___\_\\"
echo -e "${NC}"
echo -e "${BOLD}MonsterMQ Java Broker Installation${NC}"
echo "----------------------------------------------------"

# 1. Detect OS and architecture
OS_TYPE="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH_TYPE="$(uname -m)"
echo -e "Platform detected : ${BOLD}${OS_TYPE} (${ARCH_TYPE})${NC}"

# 2. Check Java 21+ requirement
echo -n "Checking Java... "
if ! command -v java >/dev/null 2>&1; then
    echo -e "${RED}not found${NC}"
    echo ""
    echo -e "${RED}${BOLD}Error: Java is not installed or not in your PATH.${NC}"
    echo -e "MonsterMQ requires ${BOLD}Java 21 or higher${NC}."
    echo ""
    echo -e "${YELLOW}To install Java 21+ on your system:${NC}"
    case "$OS_TYPE" in
        darwin*)
            echo "  brew install openjdk@21"
            ;;
        linux*)
            if command -v apt-get >/dev/null 2>&1; then
                echo "  sudo apt update && sudo apt install openjdk-21-jre-headless"
            elif command -v dnf >/dev/null 2>&1; then
                echo "  sudo dnf install java-21-openjdk-headless"
            elif command -v pacman >/dev/null 2>&1; then
                echo "  sudo pacman -S jre21-openjdk-headless"
            elif command -v apk >/dev/null 2>&1; then
                echo "  sudo apk add openjdk21-jre-headless"
            else
                echo "  Install OpenJDK 21 via your package manager or SDKMAN (sdk install java 21-tem)"
            fi
            ;;
        *)
            echo "  Download Java 21 from: https://adoptium.net/temurin/releases/?version=21"
            ;;
    esac
    echo "  Or using SDKMAN: sdk install java 21-tem"
    echo ""
    exit 1
fi

JAVA_VER_STR="$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')"
if [ -z "$JAVA_VER_STR" ]; then
    JAVA_VER_STR="$(java -version 2>&1 | head -n 1)"
fi

# Extract major version number (e.g. "21.0.2" -> 21, "1.8.0_292" -> 8, "25" -> 25)
JAVA_MAJOR=$(echo "$JAVA_VER_STR" | sed -E 's/^1\.//' | sed -E 's/[^0-9].*//')

if [ -z "$JAVA_MAJOR" ] || [ "$JAVA_MAJOR" -lt 21 ]; then
    echo -e "${RED}Java ${JAVA_VER_STR} found (too old)${NC}"
    echo ""
    echo -e "${RED}${BOLD}Error: Java 21 or higher is required, but found version ${JAVA_VER_STR}.${NC}"
    echo -e "${YELLOW}Please upgrade Java to version 21+ and run this installer again.${NC}"
    exit 1
fi
echo -e "${GREEN}Java ${JAVA_VER_STR} (OK)${NC}"

# 3. Check download tools
HTTP_GET=""
if command -v curl >/dev/null 2>&1; then
    HTTP_GET="curl"
elif command -v wget >/dev/null 2>&1; then
    HTTP_GET="wget"
else
    echo -e "${RED}Error: neither 'curl' nor 'wget' was found on your system.${NC}"
    exit 1
fi

if ! command -v unzip >/dev/null 2>&1 && ! command -v tar >/dev/null 2>&1; then
    echo -e "${RED}Error: 'unzip' is required to extract MonsterMQ.${NC}"
    exit 1
fi

# 4. Resolve release version and download URL
echo -n "Resolving latest release... "
TAG=""
DOWNLOAD_URL=""

if [ "$REQUESTED_VERSION" = "latest" ]; then
    API_URL="https://api.github.com/repos/${REPO}/releases/latest"
    if [ "$HTTP_GET" = "curl" ]; then
        RELEASE_JSON="$(curl -sSL "$API_URL" || true)"
    else
        RELEASE_JSON="$(wget -qO- "$API_URL" || true)"
    fi

    # Extract tag name
    TAG=$(echo "$RELEASE_JSON" | grep -o '"tag_name": *"[^"]*"' | head -n 1 | cut -d'"' -f4)

    # Extract monstermq-broker-*.zip asset download URL
    DOWNLOAD_URL=$(echo "$RELEASE_JSON" | grep -o '"browser_download_url": *"[^"]*monstermq-broker-[^"]*\.zip"' | head -n 1 | cut -d'"' -f4)

    # Fallback if GitHub API rate limit was hit
    if [ -z "$TAG" ] || [ -z "$DOWNLOAD_URL" ]; then
        # Follow redirect on latest release
        if [ "$HTTP_GET" = "curl" ]; then
            REDIRECT_URL="$(curl -s -L -I -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest" || true)"
        else
            REDIRECT_URL="$(wget -q -S --spider "https://github.com/${REPO}/releases/latest" 2>&1 | awk '/Location/ {print $2}' | tail -n 1 || true)"
        fi
        TAG=$(basename "$REDIRECT_URL")
        if [ -n "$TAG" ] && [ "$TAG" != "latest" ]; then
            VERSION_NUM="${TAG#v}"
            DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/monstermq-broker-${VERSION_NUM}.zip"
        fi
    fi
else
    TAG="v${REQUESTED_VERSION#v}"
    VERSION_NUM="${TAG#v}"
    DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/monstermq-broker-${VERSION_NUM}.zip"
fi

if [ -z "$TAG" ] || [ -z "$DOWNLOAD_URL" ]; then
    echo -e "${RED}failed${NC}"
    echo -e "${RED}Error: Could not find MonsterMQ release ${REQUESTED_VERSION} on GitHub.${NC}"
    echo "Check available releases at: https://github.com/${REPO}/releases"
    exit 1
fi
echo -e "${GREEN}${TAG}${NC}"

# 5. Confirmation
TARGET_PATH="$(mkdir -p "$INSTALL_DIR" && cd "$INSTALL_DIR" && pwd)"
echo -e "Install target    : ${BOLD}${TARGET_PATH}${NC}"
echo -e "Download package  : ${YELLOW}${DOWNLOAD_URL}${NC}"

if [ "$AUTO_CONFIRM" = false ] && [ -t 0 ]; then
    echo ""
    read -p "Proceed with installation? [Y/n] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo -e "${YELLOW}Installation cancelled.${NC}"
        exit 0
    fi
fi

# 6. Download and extract
TEMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t 'monstermq')"
ZIP_FILE="${TEMP_DIR}/monstermq-broker.zip"

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo -e "\nDownloading MonsterMQ ${TAG}..."
if [ "$HTTP_GET" = "curl" ]; then
    curl -fL --progress-bar "$DOWNLOAD_URL" -o "$ZIP_FILE"
else
    wget --progress=bar -O "$ZIP_FILE" "$DOWNLOAD_URL"
fi

echo "Extracting bundle into ${TARGET_PATH}..."
if command -v unzip >/dev/null 2>&1; then
    unzip -q -o "$ZIP_FILE" -d "$TEMP_DIR/extracted"
else
    tar -xf "$ZIP_FILE" -C "$TEMP_DIR/extracted"
fi

# The zip may contain a top-level directory (e.g. monstermq-broker-X.Y.Z/)
EXTRACTED_CONTENT="$(ls "$TEMP_DIR/extracted")"
FIRST_ENTRY="$TEMP_DIR/extracted/${EXTRACTED_CONTENT}"

if [ -d "$FIRST_ENTRY" ] && [ $(echo "$EXTRACTED_CONTENT" | wc -w) -eq 1 ]; then
    cp -r "$FIRST_ENTRY"/* "$TARGET_PATH/"
else
    cp -r "$TEMP_DIR/extracted"/* "$TARGET_PATH/"
fi

# 7. Post-install setup
chmod +x "${TARGET_PATH}/run.sh" 2>/dev/null || true
mkdir -p "${TARGET_PATH}/sqlite"
mkdir -p "${TARGET_PATH}/log"

# Ensure config.yaml and yaml-json-schema.json
if [ ! -f "${TARGET_PATH}/yaml-json-schema.json" ]; then
    SCHEMA_URL="https://raw.githubusercontent.com/${REPO}/main/broker/yaml-json-schema.json"
    if [ "$HTTP_GET" = "curl" ]; then
        curl -sSL "$SCHEMA_URL" -o "${TARGET_PATH}/yaml-json-schema.json" 2>/dev/null || true
    else
        wget -qO "${TARGET_PATH}/yaml-json-schema.json" "$SCHEMA_URL" 2>/dev/null || true
    fi
fi

if [ ! -f "${TARGET_PATH}/config.yaml" ]; then
    CONFIG_URL="https://raw.githubusercontent.com/${REPO}/main/broker/example-config.yaml"
    if [ "$HTTP_GET" = "curl" ]; then
        curl -sSL "$CONFIG_URL" -o "${TARGET_PATH}/config.yaml" 2>/dev/null || true
    else
        wget -qO "${TARGET_PATH}/config.yaml" "$CONFIG_URL" 2>/dev/null || true
    fi
fi

if [ ! -f "${TARGET_PATH}/config.yaml" ] || [ ! -s "${TARGET_PATH}/config.yaml" ]; then
    cat << 'EOF' > "${TARGET_PATH}/config.yaml"
TCP: 1883
WS: 1884

NodeName: local
DefaultStoreType: SQLITE

QueuedMessagesEnabled: true
AllowRootWildcardSubscription: true

SQLite:
  Path: "sqlite"
  EnableWAL: true

GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql

MCP:
  Enabled: true
  Port: 3000

UserManagement:
  Enabled: false

Metrics:
  Enabled: true

Logging:
  Memory:
    Enabled: true
    Entries: 1000

Features:
  OpcUa: true
  OpcUaServer: true
  MqttClient: true
  Kafka: true
  Nats: true
  Redis: true
  RedisServer: true
  Telegram: true
  WinCCOa: true
  WinCCUa: true
  Plc4x: true
  Neo4j: true
  JdbcLogger: true
  InfluxDBLogger: true
  TimeBaseLogger: true
  SparkplugB: true
  FlowEngine: true
  Agents: true
  GenAi: true
  Mcp: true
  KafkaServer: true
  SchemaPolicy: true
  TopicNamespace: true
  DeviceImportExport: true
  Zenoh: true
  Hmi: true
EOF
    fi
fi

echo -e "\n${GREEN}${BOLD}✓ MonsterMQ ${TAG} installed successfully!${NC}"
echo "----------------------------------------------------"
echo -e "${BOLD}Quickstart:${NC}"
echo -e "  cd ${INSTALL_DIR}"
echo -e "  ./run.sh"
echo ""
echo -e "${BOLD}Endpoints:${NC}"
echo -e "  • Web Dashboard & GraphQL : ${BLUE}http://localhost:4000/${NC}"
echo -e "  • MQTT Broker             : ${BLUE}mqtt://localhost:1883${NC}"
echo -e "  • MQTT WebSocket          : ${BLUE}ws://localhost:1884${NC}"
echo -e "  • MCP AI Server           : ${BLUE}http://localhost:3000/${NC}"
echo "----------------------------------------------------"

if [ "$START_AFTER_INSTALL" = true ]; then
    echo -e "${GREEN}Starting MonsterMQ...${NC}"
    cd "$TARGET_PATH"
    exec ./run.sh
fi
