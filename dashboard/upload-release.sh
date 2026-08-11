#!/bin/bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MonsterMQ Dashboard Desktop Release Upload ===${NC}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Resolve location of version.txt (check ../version.txt relative to script, or current dir)
if [ -f "$SCRIPT_DIR/../version.txt" ]; then
    VERSION_FILE="$SCRIPT_DIR/../version.txt"
elif [ -f "$SCRIPT_DIR/version.txt" ]; then
    VERSION_FILE="$SCRIPT_DIR/version.txt"
elif [ -f "../version.txt" ]; then
    VERSION_FILE="../version.txt"
elif [ -f "version.txt" ]; then
    VERSION_FILE="version.txt"
else
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

# Read version string from version.txt
RAW_VERSION=$(head -n 1 "$VERSION_FILE" | tr -d '\r' | tr -d '\n')
if [ -z "$RAW_VERSION" ]; then
    echo -e "${RED}Error: version.txt is empty${NC}"
    exit 1
fi

# Strip git SHA suffix if present (e.g., 1.8.25+3bd79a89 -> 1.8.25)
VERSION=$(echo "$RAW_VERSION" | cut -d'+' -f1)
TAG="v${VERSION}"

echo -e "${YELLOW}Broker Version: ${VERSION}${NC}"
echo -e "${YELLOW}GitHub Release Tag: ${TAG}${NC}"

# Navigate to dashboard directory
cd "$SCRIPT_DIR"

# Ensure package.json version matches broker version
echo -e "${GREEN}Updating dashboard package.json version to ${VERSION}...${NC}"
npm version "$VERSION" --no-git-tag-version --allow-same-version

# Verify GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}Error: GitHub CLI ('gh') is not installed or not in PATH.${NC}"
    exit 1
fi

# Check gh authentication
if ! gh auth status &> /dev/null; then
    echo -e "${RED}Error: GitHub CLI is not authenticated. Please run 'gh auth login'.${NC}"
    exit 1
fi

# Build desktop packages for macOS and Windows
echo -e "${GREEN}Building desktop app packages (mac, win)...${NC}"
./build-desktop.sh --all

# Collect built release artifacts matching MonsterMQ-Dashboard*
RELEASE_FILES=()

# Search for matching binaries in dist-desktop/
shopt -s nullglob
for f in dist-desktop/MonsterMQ-Dashboard*.dmg dist-desktop/MonsterMQ-Dashboard*.zip dist-desktop/MonsterMQ-Dashboard*.exe; do
    # Ignore blockmap files
    if [[ "$f" == *.blockmap ]]; then
        continue
    fi
    RELEASE_FILES+=("$f")
done
shopt -u nullglob

if [ ${#RELEASE_FILES[@]} -eq 0 ]; then
    echo -e "${RED}Error: No desktop release artifacts found in dist-desktop/${NC}"
    exit 1
fi

echo -e "${GREEN}Found release artifacts to upload:${NC}"
for file in "${RELEASE_FILES[@]}"; do
    echo "  - $file"
done

# Upload or Create GitHub Release
if gh release view "$TAG" &> /dev/null; then
    echo -e "${YELLOW}Uploading desktop artifacts to existing GitHub release ${TAG}...${NC}"
    gh release upload "$TAG" "${RELEASE_FILES[@]}" --clobber
    echo -e "${GREEN}✓ Desktop release artifacts uploaded to ${TAG}!${NC}"
else
    echo -e "${YELLOW}Release ${TAG} does not exist on GitHub yet. Creating release ${TAG}...${NC}"
    # Check for release notes file in ../releases/
    RELEASE_NOTES="$SCRIPT_DIR/../releases/${TAG}.txt"
    if [ -f "$RELEASE_NOTES" ]; then
        gh release create "$TAG" "${RELEASE_FILES[@]}" --title "Release ${TAG}" --notes-file "$RELEASE_NOTES"
    else
        gh release create "$TAG" "${RELEASE_FILES[@]}" --title "Release ${TAG}" --generate-notes
    fi
    echo -e "${GREEN}✓ Release ${TAG} created and desktop artifacts uploaded!${NC}"
fi

echo -e "${GREEN}=== Release Upload Complete ===${NC}"
