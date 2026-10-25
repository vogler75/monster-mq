"""Thin, explicit wrapper around the git CLI."""

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from . import ui


class GitError(Exception):
    def __init__(self, args: Sequence[str], returncode: int, stderr: str):
        self.args_ = list(args)
        self.returncode = returncode
        self.stderr = stderr.strip()
        msg = f"git {' '.join(self.args_)} failed (exit {returncode})"
        if self.stderr:
            msg += f":\n{self.stderr}"
        super().__init__(msg)


@dataclass(frozen=True)
class TagRef:
    """A tag as stored in a repo: the tag object (for annotated tags) and the commit it points at."""
    object: str
    commit: str


class Git:
    def __init__(self, repo: Path, verbose: bool = False):
        self.repo = Path(repo)
        self.verbose = verbose

    # -- plumbing ---------------------------------------------------------

    def run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        if self.verbose:
            ui.detail("$ git " + " ".join(args))
        proc = subprocess.run(["git", "-C", str(self.repo), *args], capture_output=True,
                              text=True, encoding="utf-8", errors="replace")
        if check and proc.returncode != 0:
            raise GitError(args, proc.returncode, proc.stderr or proc.stdout)
        return proc

    def out(self, *args: str) -> str:
        return self.run(*args).stdout.strip()

    def ok(self, *args: str) -> bool:
        return self.run(*args, check=False).returncode == 0

    # -- repository state -------------------------------------------------

    def is_repo(self) -> bool:
        return self.ok("rev-parse", "--is-inside-work-tree")

    def git_dir(self) -> Path:
        return Path(self.out("rev-parse", "--absolute-git-dir"))

    def toplevel(self) -> Path:
        return Path(self.out("rev-parse", "--show-toplevel"))

    def current_branch(self) -> Optional[str]:
        """Branch name, or None when HEAD is detached."""
        proc = self.run("symbolic-ref", "--short", "-q", "HEAD", check=False)
        return proc.stdout.strip() if proc.returncode == 0 else None

    def head(self) -> str:
        return self.out("rev-parse", "HEAD")

    def short(self, rev: str = "HEAD") -> str:
        return self.out("rev-parse", "--short", rev)

    def rev_parse(self, rev: str) -> Optional[str]:
        proc = self.run("rev-parse", "--verify", "-q", rev, check=False)
        return proc.stdout.strip() if proc.returncode == 0 else None

    def operations_in_progress(self) -> List[str]:
        gd = self.git_dir()
        markers = {
            "merge": gd / "MERGE_HEAD",
            "rebase": gd / "rebase-merge",
            "rebase (apply)": gd / "rebase-apply",
            "cherry-pick": gd / "CHERRY_PICK_HEAD",
            "revert": gd / "REVERT_HEAD",
            "bisect": gd / "BISECT_LOG",
        }
        return [name for name, path in markers.items() if path.exists()]

    def status_lines(self) -> List[str]:
        """Porcelain status including untracked files."""
        out = self.run("status", "--porcelain", "--untracked-files=normal").stdout
        return [line for line in out.splitlines() if line.strip()]

    def config(self, key: str) -> Optional[str]:
        proc = self.run("config", "--get", key, check=False)
        return proc.stdout.strip() or None

    def ahead_behind(self, local: str, upstream: str) -> Tuple[int, int]:
        out = self.out("rev-list", "--left-right", "--count", f"{local}...{upstream}")
        ahead, behind = out.split()
        return int(ahead), int(behind)

    def log_oneline(self, rev_range: str, limit: Optional[int] = None) -> List[str]:
        args = ["log", "--oneline", "--no-decorate"]
        if limit:
            args.append(f"-n{limit}")
        args.append(rev_range)
        out = self.out(*args)
        return out.splitlines() if out else []

    def last_commit_touching(self, path: str) -> Optional[str]:
        out = self.out("log", "-n1", "--format=%H", "--", path)
        return out or None

    def file_at(self, rev: str, path: str) -> Optional[str]:
        proc = self.run("show", f"{rev}:{path}", check=False)
        return proc.stdout if proc.returncode == 0 else None

    def is_ancestor(self, ancestor: str, rev: str) -> bool:
        return self.ok("merge-base", "--is-ancestor", ancestor, rev)

    # -- tags -------------------------------------------------------------

    def local_tags(self) -> Dict[str, TagRef]:
        out = self.out("for-each-ref", "refs/tags",
                       "--format=%(refname:strip=2) %(objectname) %(*objectname)")
        tags = {}
        for line in out.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                name, obj = parts[0], parts[1]
                commit = parts[2] if len(parts) > 2 else obj  # lightweight tags have no peeled object
                tags[name] = TagRef(obj, commit)
        return tags

    def remote_tags(self, remote: str) -> Dict[str, TagRef]:
        """Tags on the remote, read with ls-remote so local tags are never touched."""
        out = self.out("ls-remote", "--tags", remote)
        objects: Dict[str, str] = {}
        peeled: Dict[str, str] = {}
        for line in out.splitlines():
            sha, ref = line.split("\t", 1)
            name = ref[len("refs/tags/"):]
            if name.endswith("^{}"):
                peeled[name[:-3]] = sha
            else:
                objects[name] = sha
        return {n: TagRef(o, peeled.get(n, o)) for n, o in objects.items()}

    def remote_branch_sha(self, remote: str, branch: str) -> Optional[str]:
        out = self.out("ls-remote", "--heads", remote, f"refs/heads/{branch}")
        return out.split("\t", 1)[0] if out else None

    def fetch_branch(self, remote: str, branch: str) -> None:
        # --no-tags: we read remote tags via ls-remote and never rewrite local tags implicitly.
        self.run("fetch", "--no-tags", remote, f"+refs/heads/{branch}:refs/remotes/{remote}/{branch}")

    def describe_last_tag(self, rev: str = "HEAD", pattern: str = "v[0-9]*") -> Optional[str]:
        proc = self.run("describe", "--tags", "--abbrev=0", f"--match={pattern}", rev, check=False)
        return proc.stdout.strip() if proc.returncode == 0 else None
