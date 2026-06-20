#!/bin/bash
set -euo pipefail

# Build script for MonsterMQ Desktop Apps
cd "$(dirname "$0")"

BUILD_MAC=false
BUILD_WIN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mac|mac|-m)
      BUILD_MAC=true
      shift
      ;;
    --win|win|-w)
      BUILD_WIN=true
      shift
      ;;
    --all|all)
      BUILD_MAC=true
      BUILD_WIN=true
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--mac] [--win] [--all]"
      exit 1
      ;;
  esac
done

# If no specific platform was selected, build both by default
if [[ "$BUILD_MAC" = false && "$BUILD_WIN" = false ]]; then
  BUILD_MAC=true
  BUILD_WIN=true
fi

echo "=== Building MonsterMQ Desktop App ==="

# Copy the app logo if available
mkdir -p build
if [[ -f ../logos/Logo-v2.png ]]; then
  cp ../logos/Logo-v2.png build/icon.png
  echo "Application icon copied to dashboard/build/icon.png"
  if command -v sips &> /dev/null; then
    sips -z 512 512 build/icon.png &> /dev/null
    echo "Resized application icon to 512x512 pixels"
  fi
fi

echo "Installing npm dependencies..."
npm install

echo "Building web dashboard assets..."
npm run build

# Construct builder arguments
BUILD_FLAGS="--x64 --arm64 --publish never"
if [[ "$BUILD_MAC" = true ]]; then
  BUILD_FLAGS="$BUILD_FLAGS --mac"
fi
if [[ "$BUILD_WIN" = true ]]; then
  BUILD_FLAGS="$BUILD_FLAGS --win"
fi

echo "Packaging desktop app with flags: $BUILD_FLAGS"
npx electron-builder $BUILD_FLAGS

# Post-processing rename for x64 macOS files to be clearly labeled for Intel/x64 (old macs)
if [[ "$BUILD_MAC" = true ]]; then
  echo "Renaming macOS x64 build artifacts for clarity..."
  VERSION=$(node -e "console.log(require('./package.json').version)")
  if [[ -f "dist-desktop/MonsterMQ-${VERSION}.dmg" ]]; then
    mv "dist-desktop/MonsterMQ-${VERSION}.dmg" "dist-desktop/MonsterMQ-${VERSION}-intel-x64.dmg"
    echo "Renamed dist-desktop/MonsterMQ-${VERSION}.dmg to dist-desktop/MonsterMQ-${VERSION}-intel-x64.dmg"
  fi
  if [[ -f "dist-desktop/MonsterMQ-${VERSION}-mac.zip" ]]; then
    mv "dist-desktop/MonsterMQ-${VERSION}-mac.zip" "dist-desktop/MonsterMQ-${VERSION}-intel-x64.zip"
    echo "Renamed dist-desktop/MonsterMQ-${VERSION}-mac.zip to dist-desktop/MonsterMQ-${VERSION}-intel-x64.zip"
  fi
  if [[ -f "dist-desktop/MonsterMQ-${VERSION}.dmg.blockmap" ]]; then
    mv "dist-desktop/MonsterMQ-${VERSION}.dmg.blockmap" "dist-desktop/MonsterMQ-${VERSION}-intel-x64.dmg.blockmap"
  fi
  if [[ -f "dist-desktop/MonsterMQ-${VERSION}-mac.zip.blockmap" ]]; then
    mv "dist-desktop/MonsterMQ-${VERSION}-mac.zip.blockmap" "dist-desktop/MonsterMQ-${VERSION}-intel-x64.zip.blockmap"
  fi
fi

echo "=== Build Completed Successfully ==="
echo "Desktop packages are located in dist-desktop/"
