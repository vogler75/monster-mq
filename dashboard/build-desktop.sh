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

# Skip macOS build if running on Linux (macOS DMG packaging requires macOS sips utility)
if [[ "$BUILD_MAC" = true && "$(uname -s)" != "Darwin" ]]; then
  echo "Notice: Skipping macOS desktop build on Linux (macOS DMG packaging requires macOS)."
  BUILD_MAC=false
fi

echo "=== Building MonsterMQ Desktop App ==="

# Sync package.json version with broker version in version.txt if available
if [[ -f "../version.txt" ]]; then
  BROKER_VERSION=$(head -n 1 "../version.txt" | tr -d '\r' | tr -d '\n' | cut -d'+' -f1)
  if [[ -n "$BROKER_VERSION" ]]; then
    echo "Syncing package.json version from ../version.txt: $BROKER_VERSION"
    npm version "$BROKER_VERSION" --no-git-tag-version --allow-same-version > /dev/null
  fi
elif [[ -f "version.txt" ]]; then
  BROKER_VERSION=$(head -n 1 "version.txt" | tr -d '\r' | tr -d '\n' | cut -d'+' -f1)
  if [[ -n "$BROKER_VERSION" ]]; then
    echo "Syncing package.json version from version.txt: $BROKER_VERSION"
    npm version "$BROKER_VERSION" --no-git-tag-version --allow-same-version > /dev/null
  fi
fi

# Copy the app logo if available and build/icon.png doesn't exist
mkdir -p build
if [[ ! -f build/icon.png ]]; then
  if [[ -f appicon.png ]]; then
    cp appicon.png build/icon.png
    echo "Application icon copied from dashboard/appicon.png to dashboard/build/icon.png"
  elif [[ -f appicon-option1.png ]]; then
    cp appicon-option1.png build/icon.png
    echo "Application icon copied from dashboard/appicon-option1.png to dashboard/build/icon.png"
  elif [[ -f ../logos/appicon.png ]]; then
    cp ../logos/appicon.png build/icon.png
    echo "Application icon copied from logos/appicon.png to dashboard/build/icon.png"
  elif [[ -f ../logos/Logo-v2.png ]]; then
    cp ../logos/Logo-v2.png build/icon.png
    echo "Application icon copied to dashboard/build/icon.png"
  fi
  if [[ -f build/icon.png ]] && command -v sips &> /dev/null; then
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
  if [[ "$(uname -s)" != "Darwin" ]] && ! command -v wine &> /dev/null; then
    echo "Notice: wine not detected on Linux. Building Windows zip target."
    BUILD_FLAGS="$BUILD_FLAGS --config.win.target=zip"
  fi
fi

if [[ "$BUILD_MAC" = false && "$BUILD_WIN" = false ]]; then
  echo "No target platforms enabled for desktop build."
  exit 0
fi

echo "Packaging desktop app with flags: $BUILD_FLAGS"
CSC_IDENTITY_AUTO_DISCOVERY=false npx electron-builder $BUILD_FLAGS

# Post-processing rename for macOS and Windows build artifacts for consistency (without version numbers)
if [[ "$BUILD_MAC" = true ]]; then
  echo "Checking macOS build artifacts..."
  if [[ -f "dist-desktop/MonsterMQ-Dashboard-x64.dmg" ]]; then
    mv "dist-desktop/MonsterMQ-Dashboard-x64.dmg" "dist-desktop/MonsterMQ-Dashboard-mac-x64.dmg"
    echo "Renamed dist-desktop/MonsterMQ-Dashboard-x64.dmg to dist-desktop/MonsterMQ-Dashboard-mac-x64.dmg"
  fi
  if [[ -f "dist-desktop/MonsterMQ-Dashboard-arm64.dmg" ]]; then
    mv "dist-desktop/MonsterMQ-Dashboard-arm64.dmg" "dist-desktop/MonsterMQ-Dashboard-mac-arm64.dmg"
    echo "Renamed dist-desktop/MonsterMQ-Dashboard-arm64.dmg to dist-desktop/MonsterMQ-Dashboard-mac-arm64.dmg"
  fi
fi

if [[ "$BUILD_WIN" = true ]]; then
  echo "Checking Windows build artifacts..."
  if [[ -f "dist-desktop/MonsterMQ-Dashboard Setup.exe" ]]; then
    mv "dist-desktop/MonsterMQ-Dashboard Setup.exe" "dist-desktop/MonsterMQ-Dashboard-win-x64-setup.exe"
    echo "Renamed Setup exe to dist-desktop/MonsterMQ-Dashboard-win-x64-setup.exe"
  fi
  if [[ -f "dist-desktop/MonsterMQ-Dashboard Setup arm64.exe" ]]; then
    mv "dist-desktop/MonsterMQ-Dashboard Setup arm64.exe" "dist-desktop/MonsterMQ-Dashboard-win-arm64-setup.exe"
    echo "Renamed Setup exe to dist-desktop/MonsterMQ-Dashboard-win-arm64-setup.exe"
  fi
fi

echo "=== Build Completed Successfully ==="
echo "Desktop packages are located in dist-desktop/"
