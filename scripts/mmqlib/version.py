"""Semantic version parsing and version file handling."""

import json
import os
import re
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

_VERSION_RE = re.compile(
    r"^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?$"
)


class VersionError(ValueError):
    pass


@dataclass(frozen=True)
class Version:
    major: int
    minor: int
    patch: int
    pre: Optional[str] = None

    @classmethod
    def parse(cls, text: str) -> "Version":
        """Parse X.Y.Z[-pre][+build], with an optional leading 'v'. Build metadata is dropped."""
        m = _VERSION_RE.match(text.strip())
        if not m:
            raise VersionError(f"invalid version '{text.strip()}' (expected X.Y.Z or X.Y.Z-dev)")
        return cls(int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4))

    def __str__(self) -> str:
        base = f"{self.major}.{self.minor}.{self.patch}"
        return f"{base}-{self.pre}" if self.pre else base

    @property
    def is_prerelease(self) -> bool:
        return self.pre is not None

    @property
    def tag(self) -> str:
        return f"v{self}"

    def final(self) -> "Version":
        return replace(self, pre=None)

    def next_patch(self) -> "Version":
        return Version(self.major, self.minor, self.patch + 1)

    def as_dev(self) -> "Version":
        return replace(self, pre="dev")

    def sort_key(self):
        # A pre-release sorts before its final version: 1.2.3-dev < 1.2.3.
        return (self.major, self.minor, self.patch, 0 if self.pre else 1, self.pre or "")

    def __lt__(self, other: "Version") -> bool:
        return self.sort_key() < other.sort_key()

    def __le__(self, other: "Version") -> bool:
        return self.sort_key() <= other.sort_key()


def try_parse(text: str) -> Optional[Version]:
    try:
        return Version.parse(text)
    except VersionError:
        return None


def write_text_atomic(path: Path, text: str) -> None:
    """Write via a temp file and rename, so a crash never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def read_version_file(path: Path) -> str:
    """Return the first line of a version file, stripped."""
    if not path.is_file():
        raise VersionError(f"{path.name} not found in {path.parent}")
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[0].strip() if lines else ""


def write_version_file(path: Path, version: Version) -> None:
    write_text_atomic(path, f"{version}\n")


def update_npm_version(directory: Path, version: Version) -> list:
    """Set the version in package.json / package-lock.json without needing node. Returns changed paths."""
    changed = []
    pkg = directory / "package.json"
    if pkg.is_file():
        data = json.loads(pkg.read_text(encoding="utf-8"))
        if data.get("version") != str(version):
            data["version"] = str(version)
            write_text_atomic(pkg, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
            changed.append(pkg)
    lock = directory / "package-lock.json"
    if lock.is_file():
        data = json.loads(lock.read_text(encoding="utf-8"))
        dirty = False
        if data.get("version") != str(version):
            data["version"] = str(version)
            dirty = True
        root = data.get("packages", {}).get("")
        if isinstance(root, dict) and root.get("version") != str(version):
            root["version"] = str(version)
            dirty = True
        if dirty:
            write_text_atomic(lock, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
            changed.append(lock)
    return changed
