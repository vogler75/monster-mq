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

dashboard_mode="$(git ls-files -s -- dashboard | awk 'NR == 1 { print $1 }')"

if [[ "${dashboard_mode}" != "160000" ]]; then
  echo
  echo "Warning: dashboard is configured in .gitmodules, but it is not tracked as a git submodule."
  echo "The parent repository currently tracks dashboard files directly, so git submodule commands"
  echo "cannot update it or print a submodule status."
  echo

  if [[ ! -d dashboard ]]; then
    echo "Error: dashboard directory does not exist."
    exit 1
  fi

  if ! git -C dashboard rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: dashboard exists, but it is not a git checkout."
    exit 1
  fi

  if [[ -n "$(git -C dashboard status --porcelain)" ]]; then
    echo "Error: dashboard checkout has local changes. Commit or stash them before updating."
    git -C dashboard status --short
    exit 1
  fi

  if [[ "${mode}" == "--recorded" ]]; then
    echo "Error: --recorded requires dashboard to be a real git submodule in the parent index."
    exit 1
  fi

  echo "Updating nested dashboard checkout from its remote..."
  git -C dashboard fetch origin

  dashboard_branch="$(git -C dashboard remote show origin | awk '/HEAD branch/ { print $NF }')"
  if [[ -z "${dashboard_branch}" ]]; then
    dashboard_branch="$(git -C dashboard symbolic-ref --quiet --short refs/remotes/origin/HEAD | sed 's#^origin/##')"
  fi
  if [[ -z "${dashboard_branch}" ]]; then
    echo "Error: could not determine dashboard remote default branch."
    exit 1
  fi

  git -C dashboard checkout --detach "origin/${dashboard_branch}"
  echo
  echo "Dashboard checkout is at:"
  git -C dashboard log -1 --oneline
  exit 0
fi

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
