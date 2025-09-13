#!/bin/bash

# release.sh - Automated release script for MonsterMQ
# This script:
# 1. Reads version from version.txt
# 2. Increments the patch version
# 3. Adds git SHA for tracking
# 4. Creates a git tag
# 5. Builds the release JAR

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MonsterMQ Release Script ===${NC}"

# Check if version.txt exists
if [ ! -f "version.txt" ]; then
    echo -e "${RED}Error: version.txt not found${NC}"
    exit 1
fi

# Read current version from version.txt
CURRENT_VERSION=$(head -n 1 version.txt | tr -d '\n' | tr -d '\r')

# Extract base version (without git SHA if present)
# Format can be either "1.0.2" or "1.0.2+abc1234"
BASE_VERSION=$(echo "$CURRENT_VERSION" | cut -d'+' -f1)

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$BASE_VERSION"

# Validate version components
if [ -z "$MAJOR" ] || [ -z "$MINOR" ] || [ -z "$PATCH" ]; then
    echo -e "${RED}Error: Invalid version format in version.txt. Expected format: X.Y.Z${NC}"
    echo -e "${RED}Current content: '$CURRENT_VERSION'${NC}"
    exit 1
fi

# Increment patch version
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

# Get current git SHA (short version)
GIT_SHA=$(git rev-parse --short HEAD)

# Create version string with git SHA
VERSION_WITH_SHA="${NEW_VERSION}+${GIT_SHA}"

echo -e "${YELLOW}Current version: ${BASE_VERSION}${NC}"
echo -e "${GREEN}New version: ${NEW_VERSION}${NC}"
echo -e "${GREEN}Git SHA: ${GIT_SHA}${NC}"

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Release cancelled${NC}"
        exit 1
    fi
fi

# Check if tag already exists
if git rev-parse "v${NEW_VERSION}" >/dev/null 2>&1; then
    echo -e "${RED}Error: Tag v${NEW_VERSION} already exists${NC}"
    echo -e "${YELLOW}Please manually update version.txt if you need a different version${NC}"
    exit 1
fi

# Update version.txt with new version (including git SHA for reference)
echo "$VERSION_WITH_SHA" > version.txt
echo -e "${GREEN}✓ Updated version.txt to ${VERSION_WITH_SHA}${NC}"

# Create release notes file
RELEASE_NOTES_FILE="releases/v${NEW_VERSION}.txt"
mkdir -p releases
echo "Release v${NEW_VERSION}" > "$RELEASE_NOTES_FILE"
echo "Built from commit: ${GIT_SHA}" >> "$RELEASE_NOTES_FILE"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')" >> "$RELEASE_NOTES_FILE"
echo "" >> "$RELEASE_NOTES_FILE"
echo "Changes since v${BASE_VERSION}:" >> "$RELEASE_NOTES_FILE"
echo "---" >> "$RELEASE_NOTES_FILE"

# Get commit messages since last tag
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LAST_TAG" ]; then
    git log "${LAST_TAG}..HEAD" --oneline >> "$RELEASE_NOTES_FILE"
else
    echo "Initial release" >> "$RELEASE_NOTES_FILE"
fi

echo -e "${GREEN}✓ Created release notes: ${RELEASE_NOTES_FILE}${NC}"

# Add version.txt and release notes to git
git add version.txt "$RELEASE_NOTES_FILE"
git commit -m "Bump version to ${NEW_VERSION}" || {
    echo -e "${YELLOW}No changes to commit (files might already be staged)${NC}"
}

# Create git tag
echo -e "${YELLOW}Creating git tag v${NEW_VERSION}...${NC}"
git tag -a "v${NEW_VERSION}" -m "Release version ${NEW_VERSION}"
echo -e "${GREEN}✓ Created git tag v${NEW_VERSION}${NC}"

# Build release JAR
echo -e "${YELLOW}Building release JAR...${NC}"
cd broker
mvn clean package

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Release build successful${NC}"
    echo -e "${GREEN}JAR location: broker/target/broker-1.0-SNAPSHOT.jar${NC}"
    cd ..
else
    echo -e "${RED}✗ Release build failed${NC}"
    cd ..
    exit 1
fi


echo ""
echo -e "${GREEN}=== Release Complete ===${NC}"
echo -e "${GREEN}Version ${NEW_VERSION} has been tagged and built${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Test the release JAR"
echo "  2. Push the tag to remote: git push origin v${NEW_VERSION}"
echo "  3. Push the commits: git push"
echo "  4. Create a GitHub release if desired"
echo ""
echo -e "${YELLOW}To undo this release:${NC}"
echo "  git tag -d v${NEW_VERSION}"
echo "  git reset HEAD~1"
echo "  Edit version.txt back to ${CURRENT_VERSION}"