#!/bin/bash
set -euo pipefail

# Root wrapper for uploading desktop app releases to GitHub releases
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/dashboard/upload-release.sh" "$@"
