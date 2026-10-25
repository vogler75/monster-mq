"""Paths and build steps shared by the MonsterMQ build, docker, publish and run commands."""

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from mmqlib import docker, fs, ui
from mmqlib.proc import CommandError, capture, require, run
from mmqlib.version import read_version_file

IMAGE = "rocworks/monstermq"
MAVEN_BUILDER_IMAGE = "monstermq-maven-builder:graalvm21"
DASHBOARD_VOLUME = "monstermq-dashboard-build"
BROKER_VOLUME = "monstermq-broker-build"
M2_VOLUME = "monstermq-m2-cache"
BROKER_JAR = "broker-1.0-SNAPSHOT.jar"
NODE_IMAGE = "node:24-slim"

# Dashboard checkout locations, relative to the repo root: a symlink inside the repo or
# a sibling checkout (union of what build.sh, docker/build.sh and broker/run.sh searched).
DASHBOARD_CANDIDATES = [
    "dashboard", "../dashboard", "../../dashboard",
    "monster-mq-dashboard", "../monster-mq-dashboard", "../../monster-mq-dashboard",
]

SETUP_BINARIES = [
    # (GOOS, GOARCH, output name)
    ("windows", "amd64", "setup.exe"),
    ("windows", "arm64", "setup-win-arm64.exe"),
    ("darwin", "arm64", "setup-mac-arm64"),
    ("darwin", "amd64", "setup-mac-x64"),
    ("linux", "amd64", "setup-linux-amd64"),
    ("linux", "arm64", "setup-linux-arm64"),
]


class TaskError(ui.Abort):
    """A failed precondition; reported as a plain message (no traceback, no command line)."""


@dataclass(frozen=True)
class Repo:
    root: Path

    @property
    def broker(self) -> Path:
        return self.root / "broker"

    @property
    def broker_target(self) -> Path:
        return self.broker / "target"

    @property
    def resources(self) -> Path:
        return self.broker / "src" / "main" / "resources"

    @property
    def dist(self) -> Path:
        return self.root / "dist"

    @property
    def docker_dir(self) -> Path:
        return self.root / "docker"

    @property
    def setup_dir(self) -> Path:
        return self.root / "setup"

    def version(self) -> str:
        """Version from version.txt without build metadata (X.Y.Z or X.Y.Z-dev)."""
        return read_version_file(self.root / "version.txt").split("+", 1)[0].strip()

    def bundle_name(self, version: str) -> str:
        return f"monstermq-broker-{version}"

    def bundle_zip(self, version: str) -> Path:
        return self.dist / f"{self.bundle_name(version)}.zip"

    def find_dashboard(self) -> Optional[Path]:
        for cand in DASHBOARD_CANDIDATES:
            path = self.root / cand
            if (path / "package.json").is_file():
                return path.resolve()
        return None

    def require_dashboard(self) -> Path:
        path = self.find_dashboard()
        if path is None:
            raise TaskError("Dashboard checkout not found! Clone it next to this repository:\n"
                               "  git clone https://github.com/vogler75/monster-mq-dashboard.git ../dashboard\n"
                               "or create a link named 'dashboard' in the repository root.")
        return path


def is_dev(version: str) -> bool:
    return version.endswith(("-dev", "-beta", "-alpha"))


class HelpFormatter(argparse.RawDescriptionHelpFormatter):
    pass


def parser(prog: str, description: str, epilog: str = "") -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog=f"make.py {prog}", description=description, epilog=epilog,
                                   formatter_class=HelpFormatter)


# ---------------------------------------------------------------------------
# Build steps
# ---------------------------------------------------------------------------

def stamp_git_sha(repo: Repo) -> None:
    """The broker reports <version>+<sha>: git-sha.txt is generated (gitignored);
    resources/version.txt is committed and never written by the build."""
    sha_file = repo.resources / "git-sha.txt"
    try:
        sha = capture(["git", "-C", str(repo.root), "rev-parse", "--short", "HEAD"])
    except (CommandError, OSError):
        sha = ""
    if sha:
        fs.write_text(sha_file, sha + "\n")
        ui.detail(f"Stamped broker build commit {sha}")
    else:
        fs.remove(sha_file)


def npm_build(dashboard: Path) -> None:
    """npm run build in the dashboard checkout.

    Dependencies are only installed (npm ci) when node_modules is missing, e.g. a fresh
    checkout in CI. An existing node_modules is never reinstalled: npm ci deletes it first,
    which fails on Windows while a dashboard dev server holds esbuild.exe open, and npm install
    may rewrite the dashboard's package-lock.json.
    """
    if not (dashboard / "node_modules").is_dir():
        run(["npm", "ci"], cwd=dashboard)
    run(["npm", "run", "build"], cwd=dashboard)


def _newest_mtime(paths) -> float:
    newest = 0.0
    for root in paths:
        if root.is_file():
            newest = max(newest, root.stat().st_mtime)
        elif root.is_dir():
            for p in root.rglob("*"):
                if p.is_file():
                    newest = max(newest, p.stat().st_mtime)
    return newest


def dashboard_build_available(dashboard: Path) -> bool:
    """True if the dashboard checkout already has a build in dist/ (warns when it looks stale)."""
    index = dashboard / "dist" / "index.html"
    if not index.is_file():
        return False
    # The newest file in dist/ is the build time; some files keep their source timestamps
    # when Vite copies them, so index.html alone is not reliable.
    sources = [dashboard / "src", dashboard / "package.json", dashboard / "vite.config.js"]
    if _newest_mtime(sources) > _newest_mtime([dashboard / "dist"]):
        ui.warn(f"dashboard sources changed after {dashboard / 'dist'} was built; to include them, "
                f"rebuild it in {dashboard} with 'python make.py build --web'")
    return True


def container_npm_build(dashboard: Path) -> None:
    """npm ci + build in node:24-slim; node_modules lives in a named volume so the
    host never gets root-owned files."""
    node_modules = dashboard / "node_modules"
    docker.remove_unwritable_dir(node_modules, dashboard, "node_modules")
    node_modules.mkdir(exist_ok=True)
    script = "set -e && npm ci && npm run build"
    if hasattr(os, "getuid"):
        script += f" && chown -R {os.getuid()}:{os.getgid()} /app/dist"
    run(["docker", "run", "--rm",
         "-v", f"{docker.host_path(dashboard)}:/app",
         "-v", f"{DASHBOARD_VOLUME}:/app/node_modules",
         "-w", "/app", NODE_IMAGE, "bash", "-c", script])
    fs.rmdir_if_empty(node_modules)


def copy_dashboard(repo: Repo, dashboard: Path) -> None:
    src = dashboard / "dist"
    if not src.is_dir():
        raise TaskError(f"{src} not found after the dashboard build")
    dest = repo.resources / "dashboard"
    fs.rmtree(dest)
    fs.copytree(src, dest)
    fs.remove(dest / "config" / "brokers.json")


def build_dashboard(repo: Repo, container: bool) -> None:
    """Copy the dashboard build into broker resources (building it first only if the dashboard
    checkout has none yet) and stamp the git sha."""
    dashboard = repo.require_dashboard()
    if dashboard_build_available(dashboard):
        ui.step(f"Using existing web dashboard build from {dashboard / 'dist'}")
    elif container:
        ui.step(f"No dashboard build found; building it in Docker ({NODE_IMAGE}) from {dashboard}...")
        container_npm_build(dashboard)
    else:
        ui.step(f"No dashboard build found; building web dashboard from {dashboard}...")
        require("npm")
        npm_build(dashboard)
    ui.step("Copying web dashboard to broker resources...")
    copy_dashboard(repo, dashboard)
    stamp_git_sha(repo)


def maven_package(repo: Repo) -> None:
    ui.step("Compiling Java broker with Maven...")
    docker.remove_unwritable_dir(repo.broker_target, repo.broker, "target")
    run(["mvn", "package", "-DskipTests"], cwd=repo.broker)


def container_maven_package(repo: Repo, dest: Path) -> None:
    """mvn package in the GraalVM 21 builder image; copy jar + dependencies to dest."""
    ui.step("Compiling Java broker with Maven in Docker (GraalVM 21)...")
    if not docker.image_exists(MAVEN_BUILDER_IMAGE):
        ui.step("Building Maven builder image (first time only)...")
        run(["docker", "build", "-t", MAVEN_BUILDER_IMAGE,
             "-f", str(repo.docker_dir / "Dockerfile.maven-builder"), str(repo.docker_dir)])
    target = repo.broker_target
    docker.remove_unwritable_dir(target, repo.broker, "target")
    target.mkdir(parents=True, exist_ok=True)
    maven_opts = os.environ.get("MAVEN_OPTS") or "-XX:MaxRAMPercentage=75.0"
    try:
        run(["docker", "run", "--rm",
             "-v", f"{docker.host_path(repo.broker)}:/build",
             "-v", f"{BROKER_VOLUME}:/build/target",
             "-v", f"{M2_VOLUME}:/root/.m2",
             "-e", f"MAVEN_OPTS={maven_opts} -Duser.home=/root",
             "-w", "/build", MAVEN_BUILDER_IMAGE, "mvn", "package", "-DskipTests"])
    finally:
        fs.rmdir_if_empty(target)
    ui.step(f"Copying jar and dependencies from the build volume to {dest}...")
    fs.rmtree(dest)
    dest.mkdir(parents=True)
    docker.copy_from_volume(BROKER_VOLUME, MAVEN_BUILDER_IMAGE, "/build/target",
                            [BROKER_JAR, "dependencies"], dest)
