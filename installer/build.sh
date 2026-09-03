#!/bin/bash
# build.sh - Cross-compiler for MonsterMQ Setup Executables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== Building MonsterMQ Setup Binaries ===${NC}"

# Ensure schema.json is up to date
if [ -f "../broker/yaml-json-schema.json" ]; then
    cp -f ../broker/yaml-json-schema.json schema.json
elif [ ! -f "schema.json" ]; then
    echo -e "${RED}Error: schema.json not found and ../broker/yaml-json-schema.json is missing!${NC}"
    exit 1
fi

mkdir -p bin

# 1. Windows AMD64 (setup.exe)
echo -e "${YELLOW}Compiling Windows x64 (setup.exe)...${NC}"
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup.exe .
echo -e "${GREEN}✓ bin/setup.exe${NC}"

# 2. Windows ARM64 (setup-win-arm64.exe)
echo -e "${YELLOW}Compiling Windows ARM64 (setup-win-arm64.exe)...${NC}"
GOOS=windows GOARCH=arm64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup-win-arm64.exe .
echo -e "${GREEN}✓ bin/setup-win-arm64.exe${NC}"

# 3. macOS ARM64 (Apple Silicon)
echo -e "${YELLOW}Compiling macOS ARM64 (setup-mac-arm64)...${NC}"
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup-mac-arm64 .
chmod +x bin/setup-mac-arm64
echo -e "${GREEN}✓ bin/setup-mac-arm64${NC}"

# 4. macOS x64 (Intel)
echo -e "${YELLOW}Compiling macOS x64 (setup-mac-x64)...${NC}"
GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup-mac-x64 .
chmod +x bin/setup-mac-x64
echo -e "${GREEN}✓ bin/setup-mac-x64${NC}"

# 5. Linux AMD64
echo -e "${YELLOW}Compiling Linux x64 (setup-linux-amd64)...${NC}"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup-linux-amd64 .
chmod +x bin/setup-linux-amd64
echo -e "${GREEN}✓ bin/setup-linux-amd64${NC}"

# 6. Linux ARM64
echo -e "${YELLOW}Compiling Linux ARM64 (setup-linux-arm64)...${NC}"
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o bin/setup-linux-arm64 .
chmod +x bin/setup-linux-arm64
echo -e "${GREEN}✓ bin/setup-linux-arm64${NC}"

echo -e "\n${GREEN}=== Build Complete! Output binaries in installer/bin/ ===${NC}"
ls -lh bin/
