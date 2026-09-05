#!/usr/bin/env bash
# ==============================================================================
# MonsterMQ Setup Launcher (macOS & Linux)
#
# Downloads and runs the latest native MonsterMQ Setup tool (interactive web wizard
# with schema-driven config editor and Java 21+ verification).
#
# Quick run:
#   curl -fsSL https://raw.githubusercontent.com/vogler75/monster-mq/main/setup.sh | bash
#
# Options (passed to installer):
#   -cli          Run in terminal CLI mode instead of launching web browser
#   -unattended   Run non-interactive automatic installation
#   -dir <path>   Target installation directory
#   -version <v>  Specific release version (default: latest)
# ==============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

REPO="vogler75/monster-mq"
TEMP_DIR=""

cleanup() {
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT INT TERM

echo -e "${BLUE}${BOLD}"
echo "  __  __                  _              __  __  ____ "
echo " |  \/  | ___  _ __  ___ | |_  ___  _ __|  \/  |/ __ \\"
echo " | |\/| |/ _ \| '_ \/ __|| __|/ _ \| '__| |\/| | / / |"
echo " | |  | | (_) | | | \__ \| |_|  __/| |  | |  | | \ \_|"
echo " |_|  |_|\___/|_| |_|___/ \__|\___||_|  |_|  |_|\___\_\\"
echo "                                      Setup Launcher"
echo -e "${NC}"

# 1. Detect OS & Architecture
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

BINARY_NAME=""
case "$OS" in
    darwin*)
        case "$ARCH" in
            arm64|aarch64) BINARY_NAME="setup-mac-arm64" ;;
            x86_64|amd64)  BINARY_NAME="setup-mac-x64" ;;
            *)
                echo -e "${RED}Error: Unsupported macOS architecture: ${ARCH}${NC}"
                exit 1
                ;;
        esac
        ;;
    linux*)
        case "$ARCH" in
            x86_64|amd64)  BINARY_NAME="setup-linux-amd64" ;;
            arm64|aarch64) BINARY_NAME="setup-linux-arm64" ;;
            *)
                echo -e "${RED}Error: Unsupported Linux architecture: ${ARCH}${NC}"
                exit 1
                ;;
        esac
        ;;
    *)
        echo -e "${RED}Error: Unsupported operating system: ${OS}.${NC}"
        echo -e "For Windows, run in PowerShell:"
        echo -e "  ${YELLOW}irm https://raw.githubusercontent.com/${REPO}/main/setup.ps1 | iex${NC}"
        exit 1
        ;;
esac

echo -e "Platform detected : ${BOLD}${OS} (${ARCH})${NC} -> ${GREEN}${BINARY_NAME}${NC}"

# 2. Check download tools
HTTP_GET=""
if command -v curl >/dev/null 2>&1; then
    HTTP_GET="curl"
elif command -v wget >/dev/null 2>&1; then
    HTTP_GET="wget"
else
    echo -e "${RED}Error: Neither 'curl' nor 'wget' was found.${NC}"
    exit 1
fi

# 3. Resolve Release Version
echo -n "Resolving latest release... "
TAG=""
DOWNLOAD_URL=""

API_URL="https://api.github.com/repos/${REPO}/releases/latest"
if [ "$HTTP_GET" = "curl" ]; then
    RELEASE_JSON="$(curl -sSL "$API_URL" || true)"
else
    RELEASE_JSON="$(wget -qO- "$API_URL" || true)"
fi

TAG=$(echo "$RELEASE_JSON" | grep -o '"tag_name": *"[^"]*"' | head -n 1 | cut -d'"' -f4)
DOWNLOAD_URL=$(echo "$RELEASE_JSON" | grep -o "\"browser_download_url\": *\"[^\"]*${BINARY_NAME}\"" | head -n 1 | cut -d'"' -f4)

# Fallback redirect resolution if GitHub API rate limit was hit
if [ -z "$TAG" ] || [ -z "$DOWNLOAD_URL" ]; then
    if [ "$HTTP_GET" = "curl" ]; then
        REDIRECT_URL="$(curl -s -L -I -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest" || true)"
    else
        REDIRECT_URL="$(wget -q -S --spider "https://github.com/${REPO}/releases/latest" 2>&1 | awk '/Location/ {print $2}' | tail -n 1 || true)"
    fi
    TAG=$(basename "$REDIRECT_URL")
    if [ -n "$TAG" ] && [ "$TAG" != "latest" ]; then
        DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${BINARY_NAME}"
    fi
fi

if [ -z "$TAG" ] || [ -z "$DOWNLOAD_URL" ]; then
    echo -e "${RED}failed${NC}"
    echo -e "${RED}Error: Could not resolve download URL for ${BINARY_NAME} from GitHub.${NC}"
    echo "Check available releases at: https://github.com/${REPO}/releases"
    exit 1
fi

echo -e "${GREEN}${TAG}${NC}"

# 4. Download Setup Executable
TEMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t 'monstermq-setup')"
SETUP_BIN="${TEMP_DIR}/${BINARY_NAME}"

echo -e "Downloading MonsterMQ Setup (${TAG})..."
if [ "$HTTP_GET" = "curl" ]; then
    curl -fL --progress-bar "$DOWNLOAD_URL" -o "$SETUP_BIN"
else
    wget --progress=bar -O "$SETUP_BIN" "$DOWNLOAD_URL"
fi

chmod +x "$SETUP_BIN"

# 5. Launch Setup
echo -e "${GREEN}Launching MonsterMQ Setup...${NC}\n"
"$SETUP_BIN" "$@"
