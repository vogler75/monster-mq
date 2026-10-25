"""Docker / buildx helpers."""

import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import List, Optional

from . import ui
from .proc import CommandError, capture, run, succeeds, which


def available() -> bool:
    """docker CLI present and the daemon reachable."""
    return which("docker") is not None and succeeds(["docker", "info"])


def require() -> None:
    if which("docker") is None:
        raise CommandError(["docker"], 127, "'docker' not found on PATH")
    if not succeeds(["docker", "info"]):
        raise CommandError(["docker", "info"], 1, "the Docker daemon is not reachable (is Docker running?)")


def buildx() -> Optional[List[str]]:
    """The buildx command: standalone 'docker-buildx' (e.g. Homebrew) or the 'docker buildx' plugin."""
    if which("docker-buildx"):
        return ["docker-buildx"]
    if succeeds(["docker", "buildx", "version"]):
        return ["docker", "buildx"]
    return None


def image_exists(image: str) -> bool:
    return succeeds(["docker", "image", "inspect", image])


def volume_rm(*names: str) -> None:
    """Remove volumes, ignoring ones that don't exist."""
    if which("docker") is None:
        return
    for name in names:
        succeeds(["docker", "volume", "rm", name])


def host_path(path: Path) -> str:
    """Path for a bind mount (-v host:container)."""
    return str(Path(path).resolve())


def remove_unwritable_dir(path: Path, mount_parent: Path, inside: str) -> None:
    """A root-owned directory left by an earlier container build can't be removed by the
    user; try rmdir, else delete it from inside an alpine container. POSIX only - on
    Windows (Docker Desktop) bind-mounted files are always owned by the user."""
    path = Path(path)
    if os.name == "nt" or not path.is_dir() or os.access(str(path), os.W_OK):
        return
    try:
        path.rmdir()
        return
    except OSError:
        pass
    succeeds(["docker", "run", "--rm", "-v", f"{host_path(mount_parent)}:/mnt", "alpine",
              "rm", "-rf", f"/mnt/{inside}"])


def copy_from_volume(volume: str, image: str, mount: str, items: List[str], dest: Path) -> None:
    """Copy items (paths below `mount`) out of a named volume into dest."""
    cid = capture(["docker", "create", "-v", f"{volume}:{mount}", image, "sh"])
    try:
        for item in items:
            run(["docker", "cp", f"{cid}:{mount}/{item}", str(dest) + os.sep])
    finally:
        succeeds(["docker", "rm", cid])


def hub_tag_exists(repo: str, tag: str) -> bool:
    """True if Docker Hub has repo:tag (public API, no auth)."""
    url = f"https://hub.docker.com/v2/repositories/{repo}/tags/{tag}/"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.status == 200
    except urllib.error.HTTPError:
        return False
    except (urllib.error.URLError, OSError) as e:
        ui.warn(f"could not query Docker Hub for {repo}:{tag}: {e}")
        return False
