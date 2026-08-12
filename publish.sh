#!/bin/bash

# publish.sh - Upload release assets to GitHub Release and push multi-arch Docker images to Docker Hub
#
# Usage:
#   ./publish.sh               Publish GitHub Release assets and Docker Hub images
#   ./publish.sh --github-only Upload release assets to GitHub only
#   ./publish.sh --docker-only Build and push Docker images to Docker Hub only
#   ./publish.sh -y            Auto-confirm publishing (non-interactive)

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
TAG="v${VERSION}"

PUBLISH_GITHUB=false
PUBLISH_DOCKER=false
AUTO_CONFIRM=false

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --all            Publish both GitHub Release and Docker Hub (default)"
    echo "  --github-only    Publish GitHub Release assets only"
    echo "  --docker-only    Build multi-arch Docker image and push to Docker Hub only"
    echo "  -y, --yes        Auto-confirm publishing without asking"
    echo "  -h, --help       Show this help message"
    echo ""
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            PUBLISH_GITHUB=true
            PUBLISH_DOCKER=true
            shift
            ;;
        --github-only)
            PUBLISH_GITHUB=true
            shift
            ;;
        --docker-only)
            PUBLISH_DOCKER=true
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

if [ "$PUBLISH_GITHUB" = false ] && [ "$PUBLISH_DOCKER" = false ]; then
    PUBLISH_GITHUB=true
    PUBLISH_DOCKER=true
fi

echo -e "${GREEN}=== MonsterMQ Main Publish Pipeline (Target: ${TAG}) ===${NC}"

# Confirm publication if interactive
if [ "$AUTO_CONFIRM" = false ]; then
    read -p "Are you sure you want to publish ${TAG} to GitHub/Docker Hub? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Publish cancelled by user.${NC}"
        exit 0
    fi
fi

# 1. Publish to GitHub Releases
if [ "$PUBLISH_GITHUB" = true ]; then
    echo -e "${GREEN}[1/2] Publishing Assets to GitHub Release ${TAG}...${NC}"

    if ! command -v gh &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI ('gh') is not installed.${NC}"
        exit 1
    fi

    if ! gh auth status &> /dev/null; then
        echo -e "${RED}Error: GitHub CLI is not authenticated. Run 'gh auth login'.${NC}"
        exit 1
    fi

    # Verify git tag pushed to remote
    if ! git ls-remote --tags origin "$TAG" | grep -q "$TAG"; then
        echo -e "${YELLOW}Warning: Tag ${TAG} is not on remote. Pushing tag now...${NC}"
        git push origin "$TAG" || {
            echo -e "${RED}Failed to push tag ${TAG} to origin.${NC}"
            exit 1
        }
    fi

    # Gather release files
    RELEASE_FILES=()

    BROKER_ZIP="dist/monstermq-broker-${VERSION}.zip"
    if [ -f "$BROKER_ZIP" ]; then
        RELEASE_FILES+=("$BROKER_ZIP")
    else
        echo -e "${YELLOW}Warning: ${BROKER_ZIP} not found. Run ./build.sh --broker first.${NC}"
    fi

    shopt -s nullglob
    for f in dashboard/dist-desktop/MonsterMQ-Dashboard*.dmg dashboard/dist-desktop/MonsterMQ-Dashboard*-setup.exe; do
        if [[ "$f" == *.blockmap ]]; then
            continue
        fi
        RELEASE_FILES+=("$f")
    done
    shopt -u nullglob

    if [ ${#RELEASE_FILES[@]} -eq 0 ]; then
        echo -e "${RED}Error: No release artifacts found to upload. Run ./build.sh first.${NC}"
        exit 1
    fi

    echo -e "${GREEN}Found release artifacts to upload:${NC}"
    for file in "${RELEASE_FILES[@]}"; do
        echo "  - $file"
    done

    if gh release view "$TAG" &> /dev/null; then
        echo -e "${YELLOW}Uploading artifacts to existing GitHub release ${TAG}...${NC}"
        gh release upload "$TAG" "${RELEASE_FILES[@]}" --clobber
    else
        echo -e "${YELLOW}Creating new GitHub release ${TAG}...${NC}"
        RELEASE_NOTES="releases/${TAG}.txt"
        if [ -f "$RELEASE_NOTES" ]; then
            gh release create "$TAG" "${RELEASE_FILES[@]}" --title "Release ${TAG}" --notes-file "$RELEASE_NOTES"
        else
            gh release create "$TAG" "${RELEASE_FILES[@]}" --title "Release ${TAG}" --generate-notes
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

echo -e "${GREEN}=== Publish Complete ===${NC}"
