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

dashboard_path="$(git config --file .gitmodules --get submodule.dashboard.path || true)"
dashboard_url="$(git config --file .gitmodules --get submodule.dashboard.url || true)"
dashboard_branch="$(git config --file .gitmodules --get submodule.dashboard.branch || true)"

if [[ -z "${dashboard_path}" ]]; then
  dashboard_path="dashboard"
fi
if [[ "${dashboard_path}" != "dashboard" ]]; then
  echo "Error: expected dashboard submodule path to be 'dashboard', got '${dashboard_path}'."
  exit 1
fi
if [[ -z "${dashboard_url}" ]]; then
  echo "Error: dashboard URL is missing from .gitmodules."
  exit 1
fi
if [[ -z "${dashboard_branch}" ]]; then
  dashboard_branch="main"
fi

dashboard_mode="$(git ls-files -s -- dashboard | awk 'NR == 1 { print $1 }')"

if [[ "${dashboard_mode}" == "160000" ]]; then
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
  exit 0
fi

if [[ "${mode}" == "--recorded" ]]; then
  echo "Error: --recorded requires dashboard to be a real git submodule in the parent index."
  exit 1
fi

if [[ -n "${dashboard_mode}" ]]; then
  echo
  echo "Warning: dashboard is configured in .gitmodules, but it is not tracked as a git submodule."
  echo "The parent repository currently tracks dashboard files directly, so git submodule commands"
  echo "cannot update it or print a submodule status."
  echo
fi

if [[ ! -d dashboard ]]; then
  echo "Cloning dashboard from ${dashboard_url}..."
  git clone --branch "${dashboard_branch}" --single-branch "${dashboard_url}" dashboard
  echo
  echo "Dashboard checkout is at:"
  git -C dashboard log -1 --oneline
  exit 0
fi

if ! git -C dashboard rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: dashboard exists, but it is not a git checkout."
  exit 1
fi

dashboard_abs_path="$(cd dashboard && pwd -P)"
dashboard_git_root="$(git -C dashboard rev-parse --show-toplevel)"
dashboard_git_root="$(cd "${dashboard_git_root}" && pwd -P)"
if [[ "${dashboard_git_root}" != "${dashboard_abs_path}" ]]; then
  echo "Error: dashboard is not a nested git checkout; it is part of the parent repository."
  echo "Remove dashboard first or convert it to a real submodule before updating it with this script."
  exit 1
fi

if [[ -n "$(git -C dashboard status --porcelain)" ]]; then
  echo "Error: dashboard checkout has local changes. Commit or stash them before updating."
  git -C dashboard status --short
  exit 1
fi

echo "Updating dashboard checkout from ${dashboard_url}..."
current_url="$(git -C dashboard remote get-url origin 2>/dev/null || true)"
if [[ -z "${current_url}" ]]; then
  git -C dashboard remote add origin "${dashboard_url}"
elif [[ "${current_url}" != "${dashboard_url}" ]]; then
  git -C dashboard remote set-url origin "${dashboard_url}"
fi

git -C dashboard fetch origin "${dashboard_branch}"
if git -C dashboard show-ref --verify --quiet "refs/heads/${dashboard_branch}"; then
  git -C dashboard checkout "${dashboard_branch}"
else
  git -C dashboard checkout --track "origin/${dashboard_branch}"
fi
git -C dashboard merge --ff-only "origin/${dashboard_branch}"

echo
echo "Dashboard checkout is at:"
git -C dashboard log -1 --oneline
