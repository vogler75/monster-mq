"""make.py docker: build the rocworks/monstermq images locally, or multi-arch and push."""

import shutil
import zipfile
from pathlib import Path
from typing import Dict, List

from mmqlib import docker as dk
from mmqlib import fs, ui
from mmqlib.cli import Command
from mmqlib.proc import require, run, succeeds
from mmqlib.version import VersionError

from .common import (TaskError, BROKER_JAR, BROKER_VOLUME, DASHBOARD_VOLUME, IMAGE, Repo, build_dashboard,
                     container_maven_package, is_dev, maven_package, parser)

PLATFORMS = "linux/amd64,linux/arm64"
BUILDER_NAME = "monstermq-builder"
COMPAT_BASE_IMAGE = "ghcr.io/graalvm/jdk-community:21"


def broker_inputs_available(repo: Repo, version: str) -> bool:
    """What a -d (docker only) build needs: broker/target or the dist zip."""
    t = repo.broker_target
    return ((t / BROKER_JAR).is_file() and (t / "dependencies").is_dir()) or repo.bundle_zip(version).is_file()


def stage_from_broker(repo: Repo, version: str, target: Path) -> None:
    """Copy jar + dependencies into docker/target from broker/target, else from the dist zip."""
    fs.rmtree(target)
    target.mkdir(parents=True)
    bt = repo.broker_target
    if (bt / BROKER_JAR).is_file() and (bt / "dependencies").is_dir():
        ui.step("Copying jar and dependencies from broker/target...")
        shutil.copy2(bt / BROKER_JAR, target / BROKER_JAR)
        fs.copytree(bt / "dependencies", target / "dependencies")
        return
    zip_path = repo.bundle_zip(version)
    if not zip_path.is_file():
        raise TaskError(f"neither broker/target nor dist/{zip_path.name} found.\n"
                           "Build the broker first: python make.py build --broker")
    ui.step(f"Extracting jar and dependencies from dist/{zip_path.name}...")
    prefix = f"{repo.bundle_name(version)}/"
    jar_name = f"{prefix}{repo.bundle_name(version)}.jar"
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(jar_name) as src, open(target / BROKER_JAR, "wb") as dst:
            shutil.copyfileobj(src, dst)
        for info in zf.infolist():
            if info.filename.startswith(prefix + "dependencies/") and not info.is_dir():
                rel = info.filename[len(prefix):]
                out = target.joinpath(*rel.split("/"))
                out.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(out, "wb") as dst:
                    shutil.copyfileobj(src, dst)


def _tags(version: str, testing: bool, dev: bool):
    if testing:
        return [f"{IMAGE}:testing"], []
    main = [f"{IMAGE}:{version}"] + ([] if dev else [f"{IMAGE}:latest"])
    compat = [f"{IMAGE}:{version}-jdk21"] + ([] if dev else [f"{IMAGE}:latest-jdk21"])
    return main, compat


def _tag_args(tags: List[str]) -> List[str]:
    out: List[str] = []
    for t in tags:
        out += ["-t", t]
    return out


def check_hub_tags(version: str, assume_yes: bool) -> None:
    """Warn before overwriting immutable version tags on Docker Hub (moving tags are skipped)."""
    ui.step("Checking Docker Hub for existing tags...")
    existing = [t for t in (version, f"{version}-jdk21") if dk.hub_tag_exists(IMAGE, t)]
    if not existing:
        ui.ok("No conflicting tags on Docker Hub")
        return
    ui.warn("These tags already exist on Docker Hub:")
    for t in existing:
        ui.detail(f"{IMAGE}:{t}")
    if assume_yes:
        ui.warn("-y given, overwriting")
    else:
        ui.confirm("Overwrite existing tag(s)?")


def build_image(repo: Repo, testing: bool, docker_only: bool, container: bool, publish: str,
                clean: bool) -> None:
    """publish: 'no' (local image), 'yes' (multi-arch build + push), 'ask'."""
    try:
        version = repo.version()
    except VersionError:  # no version.txt
        version = "testing"
    dev = is_dev(version)
    ui.step(f"Building Docker image for MonsterMQ {'(testing)' if testing else 'v' + version}...")
    dk.require()
    target = repo.docker_dir / "target"

    if clean:
        ui.step("Cleaning output directories...")
        dk.remove_unwritable_dir(repo.broker_target, repo.broker, "target")
        fs.rmtree(repo.broker_target)
        local_dashboard = repo.root / "dashboard"
        fs.rmtree(local_dashboard / "dist")
        dk.remove_unwritable_dir(local_dashboard / "node_modules", local_dashboard, "node_modules")
        fs.rmtree(local_dashboard / "node_modules")
        fs.rmtree(target)
        dk.volume_rm(DASHBOARD_VOLUME, BROKER_VOLUME)
        ui.ok("Output directories and build volumes cleaned")

    # Decide about pushing before the (long) build: buildx builds and pushes in one go.
    if publish == "yes":
        push = True
    elif publish == "no":
        push = False
    else:
        push = ui.ask("Build multi-arch and push to Docker Hub?")
    bx = dk.buildx()
    if push and bx is None:
        raise TaskError("pushing multi-arch images requires docker buildx")
    if push and not testing:
        check_hub_tags(version, assume_yes=(publish == "yes"))

    try:
        if not docker_only:
            if not container:
                require("npm")
                require("mvn")
            build_dashboard(repo, container)
            if container:
                container_maven_package(repo, target)  # straight into docker/target
            else:
                maven_package(repo)
        else:
            ui.step("Skipping Maven build (-d)")
        if docker_only or not container:
            stage_from_broker(repo, version, target)

        main_tags, compat_tags = _tags(version, testing, dev)
        if push:
            assert bx is not None
            if succeeds(bx + ["inspect", BUILDER_NAME]):
                run(bx + ["use", BUILDER_NAME])
            else:
                ui.step(f"Creating buildx builder '{BUILDER_NAME}'...")
                run(bx + ["create", "--name", BUILDER_NAME, "--use"])
            ui.step(f"Building and pushing multi-arch images ({PLATFORMS})...")
            run(bx + ["build", "--platform", PLATFORMS, "--push", *_tag_args(main_tags), "."],
                cwd=repo.docker_dir)
            if compat_tags:
                ui.step("Building and pushing multi-arch compat image (GraalVM JDK 21)...")
                run(bx + ["build", "--platform", PLATFORMS, "--push",
                          "--build-arg", f"BASE_IMAGE={COMPAT_BASE_IMAGE}", *_tag_args(compat_tags), "."],
                    cwd=repo.docker_dir)
            ui.ok("Multi-arch images built and pushed:")
        else:
            env = None
            if bx is not None:
                build = bx + ["build", "--load"]
            else:
                ui.warn("buildx not available, using the legacy builder")
                build = ["docker", "build"]
                env = {"DOCKER_BUILDKIT": "0"}
            ui.step("Building Docker image (local, native platform only)...")
            run(build + _tag_args(main_tags) + ["."], cwd=repo.docker_dir, env=env)
            if compat_tags:
                ui.step("Building compat Docker image (GraalVM JDK 21)...")
                run(build + ["--build-arg", f"BASE_IMAGE={COMPAT_BASE_IMAGE}", *_tag_args(compat_tags), "."],
                    cwd=repo.docker_dir, env=env)
            ui.ok("Docker image(s) built (native platform):")
        for t in main_tags + compat_tags:
            ui.detail(t)
        if not push:
            ui.info("Images were built for the native platform only. To build multi-arch and push: "
                    "python make.py docker -d -y (or python make.py publish --docker)")
    finally:
        fs.rmtree(target)


def main(repo: Repo, argv: List[str]) -> int:
    p = parser("docker", "Build the rocworks/monstermq Docker images (locally, or multi-arch + push).",
               "Without -n or -y you are asked whether to push.\n"
               "Former wrappers:  docker/build-testing.sh = make.py docker -c -t -n\n"
               "                  docker/build-version.sh = make.py docker -c -y --clean")
    p.add_argument("-t", "--testing", action="store_true", help="build the 'testing' image")
    p.add_argument("-d", dest="docker_only", action="store_true",
                   help="docker only: skip dashboard/Maven, use broker/target or the dist zip")
    p.add_argument("-c", "--container", action="store_true", help="build Maven inside a Docker container (GraalVM 21)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("-n", dest="publish", action="store_const", const="no", help="do not push (local image only)")
    g.add_argument("-y", dest="publish", action="store_const", const="yes",
                   help="build multi-arch and push to Docker Hub without asking")
    p.add_argument("--clean", "-clean", action="store_true", help="clean output directories before the build")
    a = p.parse_args(argv)
    build_image(repo, testing=a.testing, docker_only=a.docker_only, container=a.container,
                publish=a.publish or "ask", clean=a.clean)
    return 0


def commands(repo: Repo) -> Dict[str, Command]:
    return {"docker": Command("build docker images locally (-n) or multi-arch and push (-y)",
                              lambda argv: main(repo, argv))}
