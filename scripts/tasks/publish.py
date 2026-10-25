"""make.py publish: upload release assets to GitHub and push Docker images to Docker Hub.

Everything that can be checked is checked before the first upload, so a publish
either does nothing or runs with all prerequisites in place:
  - GitHub releases are refused for dev versions,
  - the release tag must already exist on origin and point at HEAD (it is never
    pushed or force-pushed from here; that is 'make.py release' / 'make.py retag'),
  - the artifacts must exist and must have been built from HEAD,
  - gh must be authenticated, docker + buildx must be available.
Uploads use 'gh release upload --clobber', so re-running a publish is safe.
"""

import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from mmqlib import docker as dk
from mmqlib import github, ui
from mmqlib.cli import Command
from mmqlib.git import Git, GitError

from . import dockerimage
from .common import BROKER_JAR, Repo, TaskError, is_dev, parser

REMOTE = "origin"


class PublishError(TaskError):
    pass


@dataclass
class Plan:
    version: str
    broker: bool = False
    setup: bool = False
    docker: bool = False
    testing: bool = False
    files: List[Path] = field(default_factory=list)

    @property
    def tag(self) -> str:
        return f"v{self.version}"

    @property
    def github(self) -> bool:
        return self.broker or self.setup


# ---------------------------------------------------------------------------
# Checks (read-only)
# ---------------------------------------------------------------------------

def jar_git_sha(jar_bytes: bytes) -> Optional[str]:
    """The git-sha.txt resource the build stamped into the broker jar."""
    try:
        with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
            return jar.read("git-sha.txt").decode("utf-8").strip() or None
    except (KeyError, zipfile.BadZipFile):
        return None


def bundle_git_sha(repo: Repo, version: str) -> Optional[str]:
    name = repo.bundle_name(version)
    try:
        with zipfile.ZipFile(repo.bundle_zip(version)) as zf:
            return jar_git_sha(zf.read(f"{name}/{name}.jar"))
    except (KeyError, zipfile.BadZipFile, FileNotFoundError):
        return None


def check_built_from(what: str, sha: Optional[str], head: str) -> None:
    if sha is None:
        ui.warn(f"{what}: no git-sha.txt inside, cannot verify which commit it was built from")
    elif not head.startswith(sha):
        raise PublishError(f"{what} was built from commit {sha}, but HEAD is {head[:len(sha)]}.\n"
                           "Rebuild it from the release commit: python make.py build --broker")


def check_tag(git: Git, tag: str) -> str:
    """The release tag must exist on origin and point at HEAD. Returns HEAD."""
    head = git.head()
    try:
        remote = git.remote_tags(REMOTE)
    except GitError as e:
        raise PublishError(f"cannot read tags from {REMOTE}: {e}")
    ref = remote.get(tag)
    if ref is None:
        raise PublishError(f"tag {tag} does not exist on {REMOTE}.\n"
                           "Create and push it with 'python make.py release' "
                           "(or check 'python make.py status' / 'python make.py repair').")
    if ref.commit != head:
        raise PublishError(f"tag {tag} on {REMOTE} points at {ref.commit[:10]}, but HEAD is {head[:10]}.\n"
                           "Check out the tagged commit, or move the tag with 'python make.py retag'.")
    local = git.local_tags().get(tag)
    if local is not None and local.commit != ref.commit:
        ui.warn(f"local tag {tag} differs from {REMOTE}; {REMOTE} is what the release uses")
    ui.ok(f"Tag {tag} is on {REMOTE} and points at HEAD ({head[:10]})")
    return head


def collect_files(repo: Repo, plan: Plan) -> List[Path]:
    files: List[Path] = []
    if plan.broker:
        zip_path = repo.bundle_zip(plan.version)
        if not zip_path.is_file():
            raise PublishError(f"broker bundle dist/{zip_path.name} not found.\n"
                               "Build it first: python make.py build --broker")
        files.append(zip_path)
    if plan.setup:
        found = [f for d in (repo.dist, repo.setup_dir / "bin")
                 for f in sorted(d.glob("setup*")) if f.is_file()]
        if not found:
            raise PublishError("setup executables not found in dist/ or setup/bin/.\n"
                               "Build them first: python make.py build --setup")
        files += found
    unique: Dict[str, Path] = {}
    for f in files:  # dist/ wins over setup/bin/ for the same asset name
        unique.setdefault(f.name, f)
    return list(unique.values())


def preflight(repo: Repo, plan: Plan, git: Git) -> None:
    ui.step("Checking prerequisites (nothing is uploaded yet)...")
    head = None
    if plan.github or (plan.docker and not plan.testing):
        if not git.is_repo():
            raise PublishError(f"{repo.root} is not a git repository")
        head = check_tag(git, plan.tag)
    if plan.github:
        plan.files = collect_files(repo, plan)
        if plan.broker:
            check_built_from(f"dist/{repo.bundle_zip(plan.version).name}",
                             bundle_git_sha(repo, plan.version), head)
        github.require_gh()
        ui.ok("gh is installed and authenticated")
    if plan.docker:
        dk.require()
        if dk.buildx() is None:
            raise PublishError("docker buildx is required to build and push multi-arch images")
        if not dockerimage.broker_inputs_available(repo, plan.version):
            raise PublishError(f"neither broker/target nor dist/{repo.bundle_zip(plan.version).name} found.\n"
                               "Build the broker first: python make.py build --broker")
        if not plan.testing:
            jar = repo.broker_target / BROKER_JAR
            if jar.is_file():  # docker stages broker/target in preference to the zip
                check_built_from("broker/target/" + BROKER_JAR, jar_git_sha(jar.read_bytes()), head)
            else:
                check_built_from(f"dist/{repo.bundle_zip(plan.version).name}",
                                 bundle_git_sha(repo, plan.version), head)
        ui.ok("docker and buildx are available")


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

def publish_github(repo: Repo, plan: Plan) -> None:
    ui.header(f"[1/2] GitHub Release {plan.tag}")
    for f in plan.files:
        ui.detail(f.relative_to(repo.root).as_posix())
    if github.release_exists(plan.tag, cwd=repo.root):
        ui.step(f"Uploading artifacts to existing GitHub release {plan.tag}...")
        github.upload(plan.tag, plan.files, cwd=repo.root)
        github.undraft(plan.tag, cwd=repo.root)
    else:
        ui.step(f"Creating GitHub release {plan.tag}...")
        notes = repo.root / "releases" / f"{plan.tag}.txt"
        github.create(plan.tag, plan.files, f"Release {plan.tag}",
                      notes if notes.is_file() else None, cwd=repo.root)
    ui.ok("GitHub release published")


def publish_docker(repo: Repo, plan: Plan) -> None:
    ui.header("[2/2] Docker Hub multi-arch images" + (" (testing)" if plan.testing else ""))
    dockerimage.build_image(repo, testing=plan.testing, docker_only=True, container=False,
                            publish="yes", clean=False)
    ui.ok("Docker Hub images published")


def main(repo: Repo, argv: List[str], git: Optional[Git] = None) -> int:
    p = parser("publish", "Upload release assets to GitHub and push multi-arch images to Docker Hub.",
               "Without a target, everything is published. Build first with 'python make.py build'.\n"
               "Dev versions can only publish the Docker 'testing' image.")
    p.add_argument("--all", action="store_true", help="GitHub release assets and Docker Hub images")
    p.add_argument("--github", action="store_true", help="all GitHub release assets (broker zip, setup executables)")
    p.add_argument("-b", "--broker", action="store_true", help="broker bundle (.zip) to the GitHub release")
    p.add_argument("-s", "--setup", "--installer", dest="setup", action="store_true",
                   help="Go setup executables to the GitHub release")
    p.add_argument("--docker", action="store_true", help="build multi-arch images and push to Docker Hub")
    p.add_argument("-t", "--testing", action="store_true",
                   help="build multi-arch image tagged 'testing' and push to Docker Hub")
    p.add_argument("-y", "--yes", action="store_true", help="don't ask for confirmation")
    if not argv:
        p.print_help()
        return 0
    a = p.parse_args(argv)

    plan = Plan(version=repo.version())
    plan.broker = a.all or a.github or a.broker
    plan.setup = a.all or a.github or a.setup
    plan.docker = a.all or a.docker or a.testing
    plan.testing = a.testing
    if not (plan.broker or plan.setup or plan.docker):
        plan.broker = plan.setup = plan.docker = True

    if is_dev(plan.version):
        if plan.github:
            raise PublishError(
                f"version {plan.version} is a development version; GitHub releases cannot be published for it.\n"
                "Finalize the release first: python make.py release\n"
                "To publish a development Docker image tagged 'testing': python make.py publish --testing")
        plan.testing = True  # dev images only ever go out as 'testing'

    if plan.testing and not plan.github:
        ui.header("MonsterMQ Publish Pipeline (target: rocworks/monstermq:testing)")
        ui.detail("Docker Hub multi-arch image -> rocworks/monstermq:testing")
    else:
        ui.header(f"MonsterMQ Publish Pipeline (target: {plan.tag})")
        if plan.broker:
            ui.detail("Broker bundle (.zip) -> GitHub release")
        if plan.setup:
            ui.detail("Setup executables -> GitHub release")
        if plan.docker:
            ui.detail("Docker Hub multi-arch images" + (" -> rocworks/monstermq:testing" if plan.testing else ""))

    preflight(repo, plan, git or Git(repo.root))

    what = "rocworks/monstermq:testing to Docker Hub" if plan.testing and not plan.github \
        else f"{plan.tag} with the selected targets"
    ui.confirm(f"Publish {what}?", assume_yes=a.yes)

    if plan.github:
        publish_github(repo, plan)
    if plan.docker:
        publish_docker(repo, plan)
    ui.header("Publish Pipeline Complete")
    return 0


def commands(repo: Repo) -> Dict[str, Command]:
    return {"publish": Command("upload release assets to GitHub, push images to Docker Hub",
                               lambda argv: main(repo, argv))}
