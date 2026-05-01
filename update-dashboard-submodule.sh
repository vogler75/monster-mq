#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

mode="${1:-}"

if [[ "${mode}" != "" && "${mode}" != "--recorded" ]]; then
  echo "Usage: $0 [--recorded]"
  echo
  echo "Without arguments, clone/update dashboard to the latest commit from its configured branch."
  echo "With --recorded, update dashboard to the exact commit recorded by this repo."
  exit 2
fi

if [[ ! -f .gitmodules ]]; then
  echo "Error: .gitmodules not found. Run this script from the monster-mq checkout."
  exit 1
fi

echo "Synchronizing dashboard submodule URL..."
git submodule sync -- dashboard

if [[ "${mode}" == "--recorded" ]]; then
  echo "Cloning/updating dashboard submodule to recorded commit..."
  git submodule update --init --recursive dashboard
  echo
  echo "Dashboard submodule is at recorded commit:"
else
  echo "Cloning/updating dashboard submodule to latest remote commit..."
  git submodule update --init --remote --merge dashboard
  echo
  echo "Dashboard submodule is at:"
fi

git submodule status dashboard
