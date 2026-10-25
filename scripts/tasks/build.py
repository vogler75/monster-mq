"""make.py build / clean: broker zip bundle, Go setup executables, local Docker image."""

import shutil
from pathlib import Path
from typing import Dict, List

from mmqlib import docker as dk
from mmqlib import fs, ui
from mmqlib.cli import Command
from mmqlib.proc import require, run

from . import dockerimage
from .common import (TaskError, BROKER_JAR, BROKER_VOLUME, DASHBOARD_VOLUME, SETUP_BINARIES, Repo,
                     build_dashboard, container_maven_package, maven_package, parser)

# ---------------------------------------------------------------------------
# Files generated into the broker bundle
# ---------------------------------------------------------------------------

RUN_SH = """#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
JAR_FILE=$(ls monstermq-broker-*.jar 2>/dev/null | head -n 1)
exec java -classpath "${JAR_FILE}:dependencies/*" at.rocworks.MonsterKt "$@"
"""

RUN_BAT = """@echo off
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
set "JAR_FILE="
for %%f in ("monstermq-broker-*.jar") do set "JAR_FILE=%%f"
if not defined JAR_FILE (
    echo Error: monstermq-broker-*.jar not found.
    exit /b 1
)
java -classpath "%JAR_FILE%;dependencies/*" at.rocworks.MonsterKt %*
"""

README_MD = """# MonsterMQ Broker v{version}

## How to Run

Requirements: Java 21+

Linux / macOS:
  ./run.sh

Windows:
  run.bat

Web Dashboard & GraphQL:
  http://localhost:4000/

MQTT Broker:
  mqtt://localhost:1883
"""

AGENTS_MD = """# MonsterMQ Agent Guide

This file provides guidance to AI coding and DevOps agents working with this MonsterMQ broker instance.

## What MonsterMQ Is

MonsterMQ is a high-performance, industrial-grade MQTT broker (MQTT 3.1.1 and MQTT 5.0) built on Vert.x with multi-database persistence, embedded web dashboard, GraphQL API, Model Context Protocol (MCP) server for AI tools, and industrial protocol bridging (OPC UA, PLC4X, SparkplugB, WinCC OA/Unified, Kafka, NATS, Redis).

## Quick Start & Running

Requirements: **Java 21+** (`java -version`)

- **Linux / macOS**: `./run.sh` (or `./run.sh -config config.yaml`)
- **Windows**: `run.bat` (or `run.bat -config config.yaml`)

## Endpoints

| Service | Port / Path | Description |
| :--- | :--- | :--- |
| **MQTT TCP** | `tcp://localhost:1883` | Standard plain MQTT protocol |
| **MQTT WebSocket** | `ws://localhost:1884` | MQTT over WebSocket for web apps |
| **GraphQL & Dashboard** | `http://localhost:4000/` | Web Dashboard & `/graphql` API endpoint |
| **MCP Server** | `http://localhost:3000/` | Model Context Protocol for AI tool integration |

## Configuration & Schema (`yaml-json-schema.json`)

The broker configuration is defined in `config.yaml`.
The draft-07 JSON schema is provided in `yaml-json-schema.json` in this directory.

### Key Configuration Concepts:
- **`DefaultStoreType`**: Default database backend (`SQLITE`, `POSTGRES`, `MONGODB`, `CRATEDB`, `MEMORY`). Defaults to `SQLITE`.
- **`SQLite.Path`**: Directory for SQLite database files (defaults to `"sqlite"`).
- **`Features`**: Map of feature flags (`OpcUa`, `MqttClient`, `FlowEngine`, `Agents`, `GenAi`, `Mcp`, `Plc4x`, `SparkplugB`, etc.).
- **`UserManagement`**: Authentication and ACL rules (`Enabled: false` by default for quickstart).
- **`GraphQL`**: Controls web dashboard and GraphQL API (`Port: 4000`, `Path: /graphql`).
- **`MCP`**: Controls Model Context Protocol server (`Port: 3000`).

## Database Backends

- **SQLite (Default)**: Zero external setup. Database files are stored locally under `./sqlite/`.
- **PostgreSQL**: Set `DefaultStoreType: POSTGRES` and configure connection details under `Postgres: { Url, User, Pass }`.
- **MongoDB**: Set `DefaultStoreType: MONGODB` and configure `MongoDB: { Url, Database }`.

## CLI Arguments

- `-config <file>`: Specify custom configuration YAML (defaults to `config.yaml` or `GATEWAY_CONFIG` env var).
- `-cluster`: Enable Hazelcast cluster mode.
- `-log <level>`: Set log level (`INFO`, `FINE`, `FINER`, `FINEST`, `ALL`).
- `-workerPoolSize <num>`: Vert.x worker thread pool size.
"""


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def package_bundle(repo: Repo, version: str) -> Path:
    """Stage dist/monstermq-broker-<version>/ from broker/target and zip it."""
    ui.step("Packaging broker zip bundle...")
    jar = repo.broker_target / BROKER_JAR
    deps = repo.broker_target / "dependencies"
    if not jar.is_file() or not deps.is_dir():
        raise TaskError(f"{jar} or {deps} missing; the Maven build did not produce them")
    name = repo.bundle_name(version)
    stage = repo.dist / name
    fs.rmtree(stage)
    stage.mkdir(parents=True)
    try:
        shutil.copy2(jar, stage / f"{name}.jar")
        fs.copytree(deps, stage / "dependencies")
        for cfg in ("example-config.yaml", "example-config-sqlite.yaml"):
            if (repo.broker / cfg).is_file():
                shutil.copy2(repo.broker / cfg, stage / "config.yaml")
                break
        (stage / "sqlite").mkdir()
        (stage / "log").mkdir()
        for extra in ("yaml-json-schema.json", "logging.properties"):
            if (repo.broker / extra).is_file():
                shutil.copy2(repo.broker / extra, stage / extra)
        fs.write_text(stage / "run.sh", RUN_SH, executable=True)
        fs.write_text(stage / "run.bat", RUN_BAT, newline="\r\n")  # cmd.exe wants CRLF
        fs.write_text(stage / "README.md", README_MD.format(version=version))
        fs.write_text(stage / "AGENTS.md", AGENTS_MD)
        zip_path = repo.bundle_zip(version)
        fs.remove(zip_path)
        fs.zip_dir(stage, zip_path, executables=["run.sh"])
    finally:
        fs.rmtree(stage)
    ui.ok(f"Broker bundle created: {zip_path.relative_to(repo.root).as_posix()}")
    return zip_path


def build_broker(repo: Repo, version: str, container: bool) -> Path:
    ui.header("[1/3] Building Web Dashboard and Java Broker")
    if container:
        require("docker")
    else:
        require("npm")
        require("mvn")
    build_dashboard(repo, container)
    if container:
        container_maven_package(repo, repo.broker_target)
    else:
        maven_package(repo)
    return package_bundle(repo, version)


def build_setup(repo: Repo) -> List[Path]:
    """Cross-compile the Go installer to setup/bin and copy the binaries to dist/."""
    ui.header("[2/3] Building Cross-Platform Setup Executables (Go)")
    require("go")
    setup = repo.setup_dir
    schema = repo.broker / "yaml-json-schema.json"
    if schema.is_file():
        shutil.copyfile(schema, setup / "schema.json")
    elif not (setup / "schema.json").is_file():
        raise TaskError("setup/schema.json not found and broker/yaml-json-schema.json is missing")
    bin_dir = setup / "bin"
    bin_dir.mkdir(exist_ok=True)
    for goos, goarch, name in SETUP_BINARIES:
        ui.step(f"Compiling {goos}/{goarch} ({name})...")
        out = bin_dir / name
        run(["go", "build", "-buildvcs=false", "-ldflags=-s -w", "-o", f"bin/{name}", "."],
            cwd=setup, env={"GOOS": goos, "GOARCH": goarch, "CGO_ENABLED": "0"})
        if goos != "windows":
            fs.make_executable(out)
    repo.dist.mkdir(exist_ok=True)
    copied = []
    for _, _, name in SETUP_BINARIES:
        shutil.copy2(bin_dir / name, repo.dist / name)
        copied.append(repo.dist / name)
    ui.ok("Setup executables copied to dist/")
    return copied


def clean(repo: Repo) -> None:
    ui.step("Cleaning output build directories...")
    dk.remove_unwritable_dir(repo.broker_target, repo.broker, "target")
    for path in (repo.broker_target, repo.root / "dashboard" / "dist", repo.dist,
                 repo.docker_dir / "target"):
        fs.rmtree(path)
    dk.volume_rm(DASHBOARD_VOLUME, BROKER_VOLUME)
    ui.ok("Clean complete")


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------

def main(repo: Repo, argv: List[str]) -> int:
    p = parser("build", "Build MonsterMQ artifacts into dist/ (and a local Docker image).",
               "Without a target, --all is assumed. Artifacts:\n"
               "  dist/monstermq-broker-<version>.zip   (--broker)\n"
               "  dist/setup*, setup/bin/setup*          (--setup)\n"
               "  rocworks/monstermq:<version>[-jdk21]   (--docker, local image, never pushed)")
    p.add_argument("--all", action="store_true", help="build broker zip, setup executables and docker image")
    p.add_argument("--broker", action="store_true", help="build the standalone Java broker bundle (zip)")
    p.add_argument("--setup", "--installer", dest="setup", action="store_true",
                   help="cross-compile the Go setup executables (setup.exe, setup-mac-*, setup-linux-*)")
    p.add_argument("--docker", action="store_true", help="build the local Docker image (native platform)")
    p.add_argument("-t", "--testing", action="store_true",
                   help="build the local Docker image tagged rocworks/monstermq:testing")
    p.add_argument("-c", "--container", action="store_true",
                   help="build dashboard and broker inside Docker containers (Node.js / GraalVM 21)")
    p.add_argument("--clean", action="store_true", help="clean output directories (alone: clean only)")
    if not argv:
        p.print_help()
        return 0
    a = p.parse_args(argv)

    broker, setup, image = a.broker or a.all, a.setup or a.all, a.docker or a.testing or a.all
    if not (broker or setup or image) and not a.clean:
        broker = setup = image = True

    version = repo.version()
    ui.header(f"MonsterMQ Main Build Pipeline (v{version})")
    if a.clean:
        clean(repo)
    if not (broker or setup or image):
        ui.ok("No build targets specified. Clean operation finished.")
        return 0
    if image:
        dk.require()  # fail before spending minutes on the other targets

    if broker:
        build_broker(repo, version, a.container)
    if setup:
        build_setup(repo)
    if image:
        ui.header("[3/3] Building Local Docker Image" + (" (testing)" if a.testing else ""))
        dockerimage.build_image(
            repo,
            testing=a.testing,
            # broker already compiled in step 1 -> just package it; otherwise build it (in a container with -c)
            docker_only=broker,
            container=a.container and not broker,
            publish="no",
            clean=False,
        )
        ui.ok("Local Docker image built")
    ui.header("Build Complete")
    return 0


def commands(repo: Repo) -> Dict[str, Command]:
    return {
        "build": Command("build broker zip, setup executables and/or local docker image",
                         lambda argv: main(repo, argv)),
        "clean": Command("remove build outputs (same as 'build --clean')",
                         lambda argv: main(repo, ["--clean", *argv])),
    }
