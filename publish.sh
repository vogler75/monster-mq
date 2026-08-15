#!/bin/bash

# publish.sh - Upload release assets to GitHub Release and push multi-arch Docker images to Docker Hub
#
# Usage:
#   ./publish.sh --all        Publish all GitHub release assets and Docker Hub images
#   ./publish.sh --github     Publish all GitHub release assets (broker zip, desktop apps, setup)
#   -- Target-specific GitHub options --
#   ./publish.sh --broker     Publish standalone broker bundle (.zip) to GitHub only
#   ./publish.sh --dashboard  Publish desktop dashboard apps (dmg/exe) to GitHub only
#   ./publish.sh --setup      Publish cross-platform Go setup executables to GitHub only
#   -- Docker option --
#   ./publish.sh --docker     Build and push Docker images to Docker Hub only
#   -- Flags --
#   ./publish.sh -y           Auto-confirm publishing (non-interactive)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

RAW_VERSION=$(head -n 1 version.txt | tr -d '\r' | tr -d '\n')
VERSION=$(echo "$RAW_VERSION" | cut -d'+' -f1)
TAG="v${VERSION}"

PUBLISH_BROKER=false
PUBLISH_DASHBOARD=false
PUBLISH_SETUP=false
PUBLISH_DOCKER=false
EXPLICIT_TARGET=false
AUTO_CONFIRM=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all            Publish everything (all GitHub release assets + Docker Hub images)"
    echo "  --github         Publish all GitHub release assets (broker zip, desktop dashboard, setup executables)"
    echo "  --broker, -b     Publish standalone broker bundle (.zip) to GitHub release only"
    echo "  --dashboard, -d  Publish desktop dashboard apps (.dmg / .exe) to GitHub release only"
    echo "  --setup, -s      Publish Go setup executables (setup.exe, setup-mac, setup-linux) to GitHub only"
    echo "  --docker         Build multi-arch Docker image and push to Docker Hub only"
    echo "  -y, --yes        Auto-confirm publishing without asking"
    echo "  -h, --help       Show this help message"
    echo ""
    exit 0
}

if [ $# -eq 0 ]; then
    usage
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            PUBLISH_BROKER=true
            PUBLISH_DASHBOARD=true
            PUBLISH_SETUP=true
            PUBLISH_DOCKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --github)
            PUBLISH_BROKER=true
            PUBLISH_DASHBOARD=true
            PUBLISH_SETUP=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --broker|-b)
            PUBLISH_BROKER=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --dashboard|--desktop|-d)
            PUBLISH_DASHBOARD=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --setup|--installer|-s)
            PUBLISH_SETUP=true
            EXPLICIT_TARGET=true
            shift
            ;;
        --docker)
            PUBLISH_DOCKER=true
            EXPLICIT_TARGET=true
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

if [ "$EXPLICIT_TARGET" = false ]; then
    PUBLISH_BROKER=true
    PUBLISH_DASHBOARD=true
    PUBLISH_SETUP=true
    PUBLISH_DOCKER=true
fi

echo -e "${GREEN}=== MonsterMQ Main Publish Pipeline (Target: ${TAG}) ===${NC}"
echo "Selected publish targets:"
[ "$PUBLISH_BROKER" = true ] && echo -e "  • ${BLUE}Broker Bundle (.zip)${NC} -> GitHub Release"
[ "$PUBLISH_DASHBOARD" = true ] && echo -e "  • ${BLUE}Desktop Dashboard (DMG / Setup)${NC} -> GitHub Release"
[ "$PUBLISH_SETUP" = true ] && echo -e "  • ${BLUE}Setup Executables (Go installer)${NC} -> GitHub Release"
[ "$PUBLISH_DOCKER" = true ] && echo -e "  • ${BLUE}Docker Hub Multi-Arch Images${NC}"
echo ""

# Confirm publication if interactive
if [ "$AUTO_CONFIRM" = false ]; then
    read -p "Are you sure you want to publish ${TAG} with selected targets? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Publish cancelled by user.${NC}"
        exit 0
    fi
fi

# 1. GitHub Releases Publishing
if [ "$PUBLISH_BROKER" = true ] || [ "$PUBLISH_DASHBOARD" = true ] || [ "$PUBLISH_SETUP" = true ]; then
    echo -e "${GREEN}[1/2] Preparing Assets for GitHub Release ${TAG}...${NC}"

    if ! command -v gh &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI ('gh') is not installed.${NC}"
        exit 1
    fi

    if ! gh auth status &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI is not authenticated. Run 'gh auth login'.${NC}"
        exit 1
    fi

    # Verify git tag on remote
    if ! git ls-remote --tags origin "$TAG" | grep -q "$TAG"; then
        echo -e "${YELLOW}Warning: Tag ${TAG} is not on remote. Pushing tag now...${NC}"
        git push origin "$TAG" || {
            echo -e "${RED}Failed to push tag ${TAG} to origin.${NC}"
            exit 1
        }
    fi

    RELEASE_FILES=()

    # 1a. Broker Zip
    if [ "$PUBLISH_BROKER" = true ]; then
        BROKER_ZIP="dist/monstermq-broker-${VERSION}.zip"
        if [ ! -f "$BROKER_ZIP" ]; then
            echo -e "${YELLOW}Broker package ${BROKER_ZIP} not found. Building it now...${NC}"
            ./build.sh --broker
        fi

        if [ -f "$BROKER_ZIP" ]; then
            RELEASE_FILES+=("$BROKER_ZIP")
        else
            echo -e "${RED}Error: Failed to find or build ${BROKER_ZIP}.${NC}"
            exit 1
        fi
    fi

    # 1b. Desktop Dashboard Apps
    if [ "$PUBLISH_DASHBOARD" = true ]; then
        DESKTOP_FILES=()
        shopt -s nullglob
        for f in dashboard/dist-desktop/MonsterMQ-Dashboard*.dmg dashboard/dist-desktop/MonsterMQ-Dashboard*-setup.exe; do
            if [[ "$f" != *.blockmap ]]; then
                DESKTOP_FILES+=("$f")
            fi
        done
        shopt -u nullglob

        if [ ${#DESKTOP_FILES[@]} -eq 0 ]; then
            echo -e "${YELLOW}Desktop dashboard apps not found. Building them now...${NC}"
            ./build.sh --desktop
            shopt -s nullglob
            for f in dashboard/dist-desktop/MonsterMQ-Dashboard*.dmg dashboard/dist-desktop/MonsterMQ-Dashboard*-setup.exe; do
                if [[ "$f" != *.blockmap ]]; then
                    DESKTOP_FILES+=("$f")
                fi
            done
            shopt -u nullglob
        fi

        for f in "${DESKTOP_FILES[@]}"; do
            RELEASE_FILES+=("$f")
        done
    fi

    # 1c. Setup Executables
    if [ "$PUBLISH_SETUP" = true ]; then
        SETUP_FILES=()
        shopt -s nullglob
        for f in dist/setup* installer/bin/setup*; do
            if [[ -f "$f" ]]; then
                SETUP_FILES+=("$f")
            fi
        done
        shopt -u nullglob

        if [ ${#SETUP_FILES[@]} -eq 0 ]; then
            echo -e "${YELLOW}Setup executables not found. Building them now...${NC}"
            ./build.sh --setup
            shopt -s nullglob
            for f in dist/setup* installer/bin/setup*; do
                if [[ -f "$f" ]]; then
                    SETUP_FILES+=("$f")
                fi
            done
            shopt -u nullglob
        fi

        for f in "${SETUP_FILES[@]}"; do
            RELEASE_FILES+=("$f")
        done
    fi

    # Deduplicate release files list
    UNIQUE_FILES=()
    declare -A SEEN_FILES
    for file in "${RELEASE_FILES[@]}"; do
        BASE=$(basename "$file")
        if [ -z "${SEEN_FILES[$BASE]}" ]; then
            SEEN_FILES[$BASE]=1
            UNIQUE_FILES+=("$file")
        fi
    done

    if [ ${#UNIQUE_FILES[@]} -eq 0 ]; then
        echo -e "${RED}Error: No release artifacts selected or found to upload.${NC}"
        exit 1
    fi

    echo -e "${GREEN}Release artifacts to upload to GitHub:${NC}"
    for file in "${UNIQUE_FILES[@]}"; do
        echo "  - $file"
    done

    if gh release view "$TAG" &> /dev/null; then
        echo -e "${YELLOW}Uploading artifacts to existing GitHub release ${TAG}...${NC}"
        gh release upload "$TAG" "${UNIQUE_FILES[@]}" --clobber
    else
        echo -e "${YELLOW}Creating new GitHub release ${TAG}...${NC}"
        RELEASE_NOTES="releases/${TAG}.txt"
        if [ -f "$RELEASE_NOTES" ]; then
            gh release create "$TAG" "${UNIQUE_FILES[@]}" --title "Release ${TAG}" --notes-file "$RELEASE_NOTES"
        else
            gh release create "$TAG" "${UNIQUE_FILES[@]}" --title "Release ${TAG}" --generate-notes
        fi
    fi
    echo -e "${GREEN}✓ GitHub Release published successfully!${NC}"
fi

# 2. Push Multi-Arch Docker Image to Docker Hub
if [ "$PUBLISH_DOCKER" = true ]; then
    echo -e "${GREEN}[2/2] Publishing Multi-Arch Docker Images to Docker Hub...${NC}"
    (cd docker && ./build -c -y --clean)
    echo -e "${GREEN}✓ Docker Hub images published successfully!${NC}"
fi

echo -e "\n${GREEN}=== Publish Pipeline Complete ===${NC}"
