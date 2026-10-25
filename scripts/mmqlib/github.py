"""GitHub releases via the gh CLI."""

from pathlib import Path
from typing import List, Optional, Sequence

from .proc import CommandError, run, succeeds, which


def require_gh() -> None:
    """gh installed and authenticated - checked before anything is uploaded."""
    if which("gh") is None:
        raise CommandError(["gh"], 127, "GitHub CLI ('gh') is not installed")
    if not succeeds(["gh", "auth", "status"]):
        raise CommandError(["gh", "auth", "status"], 1,
                           "GitHub CLI is not authenticated; run 'gh auth login'")


def release_exists(tag: str, cwd: Optional[Path] = None) -> bool:
    return succeeds(["gh", "release", "view", tag], cwd=cwd)


def upload(tag: str, files: Sequence[Path], cwd: Optional[Path] = None) -> None:
    """Upload assets to an existing release; --clobber makes re-runs idempotent."""
    run(["gh", "release", "upload", tag, *[str(f) for f in files], "--clobber"], cwd=cwd)


def undraft(tag: str, cwd: Optional[Path] = None) -> None:
    run(["gh", "release", "edit", tag, "--draft=false"], cwd=cwd, check=False)


def create(tag: str, files: Sequence[Path], title: str, notes_file: Optional[Path] = None,
           cwd: Optional[Path] = None) -> None:
    args: List[str] = ["gh", "release", "create", tag, *[str(f) for f in files], "--title", title]
    if notes_file is not None:
        args += ["--notes-file", str(notes_file)]
    else:
        args.append("--generate-notes")
    # --verify-tag: never let gh create a missing tag on the default branch
    args += ["--draft=false", "--verify-tag"]
    run(args, cwd=cwd)
