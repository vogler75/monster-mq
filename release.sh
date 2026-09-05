#!/bin/bash

# release.sh - Automated release tag script for MonsterMQ Main
#
# Usage:
#   ./release.sh               # Auto-increments patch version (e.g. 1.8.31 -> 1.8.32)
#   ./release.sh 1.9.0         # Sets explicit version 1.9.0
#   ./release.sh --retag       # Re-adjusts latest version tag to HEAD commit (aliases: -r, --readjust)
#   ./release.sh -h, --help    # Shows this help message

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

CURRENT_VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')
BASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d'+' -f1)

MODE="bump" # "bump", "explicit", "retag"
NEW_VERSION=""

usage() {
    echo -e "${BOLD}MonsterMQ Release Script${NC}"
    echo ""
    echo "Usage: $0 [options | version]"
    echo ""
    echo "Options:"
    echo "  (no arguments)            Auto-increment patch version (e.g. 1.8.31 -> 1.8.32)"
    echo "  <version>                 Set explicit version (e.g. 1.9.0)"
    echo "  -r, --retag, --readjust   Re-adjust/move the latest version tag (v${BASE_VERSION}) to HEAD"
    echo "  -h, --help                Show this help message"
    echo ""
    exit 0
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

if [ "$1" = "-r" ] || [ "$1" = "--retag" ] || [ "$1" = "--re-tag" ] || [ "$1" = "--readjust" ] || [ "$1" = "--re-adjust" ] || [ "$1" = "--current" ]; then
    MODE="retag"
    NEW_VERSION="$BASE_VERSION"
elif [ -n "$1" ]; then
    MODE="explicit"
    NEW_VERSION="$1"
else
    MODE="bump"
    IFS='.' read -r MAJOR MINOR PATCH <<< "$BASE_VERSION"
    if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$PATCH" ]; then
        echo -e "${RED}Error: Invalid version format in version.txt. Expected format: X.Y.Z${NC}"
        echo -e "${RED}Current content: '$CURRENT_VERSION'${NC}"
        exit 1
    fi
    NEW_PATCH=$((PATCH + 1))
    NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"
fi

echo -e "${GREEN}=== MonsterMQ Release Script ===${NC}"
echo -e "${YELLOW}Current version : ${BASE_VERSION}${NC}"
if [ "$MODE" = "retag" ]; then
    echo -e "${BLUE}Mode            : Re-adjust tag v${BASE_VERSION} to HEAD${NC}"
else
    echo -e "${GREEN}Target version  : ${NEW_VERSION}${NC}"
fi

if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release cancelled${NC}"
        exit 1
    fi
fi

if [ "$MODE" != "retag" ] && git rev-parse "v${NEW_VERSION}" >/dev/null 2>&1; then
    echo -e "${RED}Error: Tag v${NEW_VERSION} already exists.${NC}"
    echo -e "To move or re-adjust this tag to HEAD, run:"
    echo -e "  ${YELLOW}./release.sh --retag${NC}"
    exit 1
fi

GIT_SHA=$(git rev-parse --short HEAD)
VERSION_WITH_SHA="${NEW_VERSION}+${GIT_SHA}"
echo -e "${GREEN}Git SHA         : ${GIT_SHA}${NC}"

# 1. Update version.txt
echo "$VERSION_WITH_SHA" > version.txt
echo -e "${GREEN}✓ Updated version.txt to ${VERSION_WITH_SHA}${NC}"

mkdir -p broker/src/main/resources
cp version.txt broker/src/main/resources/version.txt
echo -e "${GREEN}✓ Copied version.txt to broker resources${NC}"

# 2. Generate / Update Release Notes
RELEASE_NOTES_FILE="releases/v${NEW_VERSION}.txt"
mkdir -p releases
echo "Release v${NEW_VERSION}" > "$RELEASE_NOTES_FILE"
echo "Built from commit: ${GIT_SHA}" >> "$RELEASE_NOTES_FILE"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RELEASE_NOTES_FILE"
echo "" >> "$RELEASE_NOTES_FILE"

if [ "$MODE" = "retag" ]; then
    PREV_TAG=$(git tag --sort=-v:refname 2>/dev/null | grep -v "^v${NEW_VERSION}$" | head -n 1 || echo "")
    if [ -n "$PREV_TAG" ]; then
        echo "Changes since ${PREV_TAG}:" >> "$RELEASE_NOTES_FILE"
        echo "---" >> "$RELEASE_NOTES_FILE"
        git log "${PREV_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
    else
        echo "Changes:" >> "$RELEASE_NOTES_FILE"
        echo "---" >> "$RELEASE_NOTES_FILE"
        git log --oneline -n 25 >> "$RELEASE_NOTES_FILE"
    fi
else
    echo "Changes since v${BASE_VERSION}:" >> "$RELEASE_NOTES_FILE"
    echo "---" >> "$RELEASE_NOTES_FILE"
    LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
    if [ -n "$LAST_TAG" ]; then
        git log "${LAST_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
    else
        echo "Initial release" >> "$RELEASE_NOTES_FILE"
    fi
fi

echo -e "${GREEN}✓ Updated release notes: ${RELEASE_NOTES_FILE}${NC}"

# 3. Commit version files
git add version.txt broker/src/main/resources/version.txt "$RELEASE_NOTES_FILE"
if [ "$MODE" = "retag" ]; then
    COMMIT_MSG="chore(release): re-adjust version.txt and release notes for v${NEW_VERSION}"
else
    COMMIT_MSG="Bump version to ${NEW_VERSION}"
fi

git commit -m "$COMMIT_MSG" || {
    echo -e "${YELLOW}No new changes to commit (files might already be up-to-date)${NC}"
}

# 4. Create or Force-Move Tag
TAG_NAME="v${NEW_VERSION}"
if [ "$MODE" = "retag" ]; then
    echo -e "${YELLOW}Re-adjusting git tag ${TAG_NAME} to HEAD (force)...${NC}"
    git tag -f -a "${TAG_NAME}" -m "Release version ${NEW_VERSION}"
    echo -e "${GREEN}✓ Updated git tag ${TAG_NAME} to HEAD${NC}"
else
    echo -e "${YELLOW}Creating git tag ${TAG_NAME}...${NC}"
    git tag -a "${TAG_NAME}" -m "Release version ${NEW_VERSION}"
    echo -e "${GREEN}✓ Created git tag ${TAG_NAME}${NC}"
fi

echo ""
echo -e "${GREEN}=== Release Tag Complete ===${NC}"
echo -e "${GREEN}Version ${NEW_VERSION} tagged successfully on $(git rev-parse --short HEAD).${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Build artifacts locally : ./build.sh"
echo "  2. Publish release assets  : ./publish.sh"
echo "  3. Or build & publish      : ./build.sh --publish"
if [ "$MODE" = "retag" ]; then
    echo "  4. Push commits & tag      : git push origin HEAD && git push origin -f ${TAG_NAME}"
else
    echo "  4. Push commits & tag      : git push origin HEAD && git push origin ${TAG_NAME}"
fi
