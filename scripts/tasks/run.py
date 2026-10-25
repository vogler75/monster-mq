"""make.py run: (optionally build and) start the broker from broker/target for development."""

import os
import signal
import subprocess
import time
from typing import Dict, List

from mmqlib import ui
from mmqlib.cli import Command
from mmqlib.proc import capture, require, run, which

from .common import TaskError, Repo, build_dashboard, npm_build

MAIN_CLASS = "at.rocworks.MonsterKt"

HELP = """usage: python make.py run [OPTIONS] [-- BROKER_OPTIONS]

Start MonsterMQ from broker/target (working directory: broker/, so relative
paths like -config configs/x.yaml are resolved there).

Options:
  -build, -b          build dashboard and package broker with Maven before starting
  -compile, -c        only compile broker (mvn compile, no package, no dashboard);
                      reuses target/dependencies
  -norun, -n          do not run the broker (with -b/-c: build only)
  -dashboard, -d      build the dashboard and serve it from its dist/ folder (live development)
  -nokill, -nk        do not stop running MonsterMQ instances first
                      (they are never stopped when -cluster is given)
  -help, --help, -h   show this help

Every other argument, and everything after --, is passed to MonsterMQ.

Examples:
  python make.py run                                    start with the default config
  python make.py run -b                                 build first, then start
  python make.py run -c                                 compile only (fast), then start
  python make.py run -b -n                              build only, do not start
  python make.py run -- -cluster                        start in cluster mode
  python make.py run -- -log FINEST                     temporary CLI log level override
  python make.py run -b -- -archiveConfigs archives.json
  python make.py run -d                                 serve dashboard from the filesystem
  python make.py run -- -help                           show MonsterMQ options
"""


def _find_pids() -> List[int]:
    """PIDs of running MonsterMQ JVMs (matched on the main class in the command line)."""
    me = os.getpid()
    if os.name == "nt":
        # Exclude this PowerShell itself: its own command line contains the pattern.
        script = ("Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*%s*' "
                  "-and $_.ProcessId -ne $PID } | ForEach-Object { $_.ProcessId }" % MAIN_CLASS)
        shell = which("powershell") or which("pwsh")
        if shell is None:
            return []
        out = capture([shell, "-NoProfile", "-NonInteractive", "-Command", script], check=False)
    else:
        if which("pgrep") is None:
            return []
        out = capture(["pgrep", "-f", MAIN_CLASS], check=False)
    pids = []
    for tok in out.split():
        if tok.isdigit() and int(tok) != me:
            pids.append(int(tok))
    return pids


def _kill(pid: int, force: bool) -> None:
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid)] + (["/F"] if force else []),
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.kill(pid, signal.SIGKILL if force else signal.SIGTERM)
    except OSError:
        pass


def stop_running_instances() -> None:
    ui.step("Checking for existing MonsterMQ instances...")
    pids = _find_pids()
    if not pids:
        ui.detail("No existing MonsterMQ instances found.")
        return
    ui.warn(f"Found existing MonsterMQ processes: {', '.join(map(str, pids))}; stopping them...")
    if os.name == "nt":
        # A console JVM ignores taskkill without /F, so Windows goes straight to a forced stop.
        for pid in pids:
            _kill(pid, force=True)
        time.sleep(1)
    else:
        for pid in pids:
            _kill(pid, force=False)
        for i in range(1, 6):
            time.sleep(1)
            if not _find_pids():
                ui.detail("Existing instances terminated gracefully.")
                break
            ui.detail(f"Waiting for graceful shutdown... ({i}/5)")
        remaining = _find_pids()
        if remaining:
            ui.warn(f"Force killing remaining processes: {', '.join(map(str, remaining))}")
            for pid in remaining:
                _kill(pid, force=True)
            time.sleep(1)
    ui.ok("All existing MonsterMQ instances stopped.")


def parse(argv: List[str]):
    opts = {"build": False, "compile": False, "norun": False, "nokill": False, "dashboard": False, "help": False}
    names = {
        "build": ("-build", "-b", "--build"),
        "compile": ("-compile", "-c", "--compile"),
        "norun": ("-norun", "-n", "--norun"),
        "nokill": ("-nokill", "-nk", "--nokill"),
        "dashboard": ("-dashboard", "-d", "--dashboard"),
        "help": ("-help", "--help", "-h", "/?"),
    }
    lookup = {flag: key for key, flags in names.items() for flag in flags}
    broker_args: List[str] = []
    it = iter(argv)
    for arg in it:
        if arg == "--":
            broker_args.extend(it)
            break
        key = lookup.get(arg)
        if key:
            opts[key] = True
        else:
            broker_args.append(arg)
    return opts, broker_args


def main(repo: Repo, argv: List[str]) -> int:
    opts, broker_args = parse(argv)
    if opts["help"]:
        print(HELP)
        return 0
    broker = repo.broker

    if opts["build"]:
        require("npm")
        require("mvn")
        build_dashboard(repo, container=False)
        ui.step("Building MonsterMQ...")
        run(["mvn", "package", "-DskipTests"], cwd=broker)
        ui.ok("Build completed successfully.")
    elif opts["compile"]:
        if not (broker / "target" / "dependencies").is_dir():
            raise TaskError("broker/target/dependencies not found; run with -b once to populate it")
        require("mvn")
        ui.step("Compiling MonsterMQ (no package)...")
        run(["mvn", "compile", "-DskipTests"], cwd=broker)
        ui.ok("Compile completed successfully.")

    if opts["norun"]:
        return 0

    require("java")
    if opts["nokill"] or "-cluster" in broker_args:
        ui.detail("Skipping kill of existing instances.")
    else:
        stop_running_instances()

    java_opts: List[str] = []
    version_out = subprocess.run([which("java") or "java", "-version"], capture_output=True, text=True,
                                 errors="replace")
    if "GraalVM" in (version_out.stdout + version_out.stderr):
        ui.detail("Detected GraalVM - enabling JVMCI for optimal JavaScript performance")
        java_opts += ["-XX:+EnableJVMCI", "-XX:+UseJVMCICompiler"]
    else:
        ui.detail("Using standard JVM (GraalVM not detected)")
    java_opts += ["--enable-native-access=ALL-UNNAMED", "-Dpolyglot.engine.WarnInterpreterOnly=false"]

    if opts["dashboard"]:
        dashboard = repo.require_dashboard()
        if not opts["build"]:
            require("npm")
            ui.step(f"Building dashboard from {dashboard} (-d always builds)...")
            npm_build(dashboard)
        dist = dashboard / "dist"
        if not dist.is_dir():
            raise TaskError(f"{dist} not found after the dashboard build")
        ui.detail(f"Dashboard serving from filesystem: {dist}")
        broker_args += ["-dashboardPath", str(dist)]

    classpath = os.pathsep.join(["target/classes", "target/dependencies/*"])
    cmd = ["java", *java_opts, "-classpath", classpath, MAIN_CLASS, *broker_args]
    ui.step("Starting MonsterMQ...")
    try:
        return run(cmd, cwd=broker, check=False)
    except KeyboardInterrupt:
        return 130  # the JVM got the Ctrl+C as well and shuts down by itself


def commands(repo: Repo) -> Dict[str, Command]:
    return {"run": Command("build (optional) and start the broker for development",
                           lambda argv: main(repo, argv))}
