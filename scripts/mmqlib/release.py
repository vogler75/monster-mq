"""Release tooling: tag releases and advance dev versions without ever leaving git half-done.

Every command that changes something follows the same shape:

1. Preflight (read-only): right branch, clean tree, no merge/rebase in progress,
   in sync with the remote, version and tag are consistent. Any problem aborts
   before anything is touched.
2. Plan: print exactly what will happen; --dry-run stops here.
3. Local changes inside a Transaction (version files, release notes, commit, tag).
4. One `git push --atomic` with exactly the refs involved - branch and tag land
   together or not at all.

If anything fails (or Ctrl+C) before the push succeeded, the Transaction resets the
branch to the original commit, restores or deletes the tags it touched and removes
files it created, so the repository is exactly as it was before the run.
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from . import ui
from .git import Git, GitError, TagRef
from .proc import which
from .version import (Version, VersionError, read_version_file, try_parse,
                      update_npm_version, write_text_atomic, write_version_file)


class ReleaseError(Exception):
    pass


@dataclass
class ReleaseConfig:
    name: str
    version_file: str = "version.txt"
    mirror_files: List[str] = field(default_factory=list)  # committed copies of the version file
    npm: bool = False                 # also keep package.json / package-lock.json in sync
    npm_dev: bool = True               # ...also for dev versions (False: only on release)
    releases_dir: str = "releases"     # release notes go to <releases_dir>/vX.Y.Z.txt
    branch: str = "main"
    remote: str = "origin"
    next_steps: List[str] = field(default_factory=list)


@dataclass
class Context:
    cfg: ReleaseConfig
    root: Path
    git: Git
    dry_run: bool = False
    yes: bool = False

    @property
    def version_path(self) -> Path:
        return self.root / self.cfg.version_file

    @property
    def remote(self) -> str:
        return self.cfg.remote

    @property
    def branch(self) -> str:
        return self.cfg.branch


# ---------------------------------------------------------------------------
# Locking and transactions
# ---------------------------------------------------------------------------

@contextmanager
def release_lock(git: Git):
    """Prevent two release runs on the same repo at once (e.g. two mbp jobs)."""
    path = git.git_dir() / "mmq-release.lock"
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise ReleaseError(f"another release run holds the lock {path}.\n"
                           "If no other run is active, delete that file and try again.")
    with os.fdopen(fd, "w") as f:
        f.write(str(os.getpid()))
    try:
        yield
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


class Transaction:
    """Records local changes so they can be undone if the run fails before the push."""

    def __init__(self, git: Git):
        self.git = git
        self.orig_head = git.head()
        self.tags: Dict[str, Optional[str]] = {}  # tag name -> previous tag object (None = didn't exist)
        self.new_files: List[Path] = []
        self.pushed = False

    def will_write(self, path: Path) -> None:
        if not path.exists():
            self.new_files.append(path)

    def set_tag(self, name: str, target: str, message: str, force: bool = False) -> None:
        if name not in self.tags:
            self.tags[name] = self.git.rev_parse(f"refs/tags/{name}")
        args = ["tag", "-a"] + (["-f"] if force else []) + [name, "-m", message, target]
        self.git.run(*args)

    def fetch_tag(self, remote: str, name: str) -> None:
        if name not in self.tags:
            self.tags[name] = self.git.rev_parse(f"refs/tags/{name}")
        self.git.run("fetch", "--no-tags", remote, f"refs/tags/{name}:refs/tags/{name}")

    def rollback(self) -> List[str]:
        problems = []
        for name, previous in self.tags.items():
            try:
                if previous:
                    self.git.run("update-ref", f"refs/tags/{name}", previous)
                elif self.git.rev_parse(f"refs/tags/{name}"):
                    self.git.run("tag", "-d", name)
            except GitError as e:
                problems.append(str(e))
        try:
            self.git.run("reset", "--hard", self.orig_head)
        except GitError as e:
            problems.append(str(e))
        for path in self.new_files:
            try:
                if path.exists():
                    path.unlink()
            except OSError as e:
                problems.append(f"could not remove {path}: {e}")
        return problems


def run_transaction(ctx: Context, body: Callable[[Transaction], None]) -> None:
    with release_lock(ctx.git):
        tx = Transaction(ctx.git)
        try:
            body(tx)
        except BaseException as exc:
            if tx.pushed:
                raise  # the remote already has the result; undoing locally would only desync
            reason = "interrupted" if isinstance(exc, KeyboardInterrupt) else str(exc)
            ui.step(("Interrupted" if isinstance(exc, KeyboardInterrupt) else "Failed")
                    + "; rolling back local changes...")
            problems = tx.rollback()
            if problems:
                raise ReleaseError(f"{reason}\n\nROLLBACK INCOMPLETE, please inspect the repository:\n"
                                   + "\n".join(problems)) from exc
            ui.ok(f"Rolled back: HEAD is {ctx.git.short(tx.orig_head)} again and no tags were left behind.")
            if isinstance(exc, KeyboardInterrupt):
                raise
            raise ReleaseError(f"{reason}\nNothing was pushed; the repository is unchanged.") from exc


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

@dataclass
class RepoState:
    head: str
    local_tags: Dict[str, TagRef]
    remote_tags: Dict[str, TagRef]

    def tag_names(self) -> Iterable[str]:
        return set(self.local_tags) | set(self.remote_tags)


def preflight(ctx: Context) -> RepoState:
    g = ctx.git
    ui.step("Checking repository state...")
    if not g.is_repo():
        raise ReleaseError(f"{ctx.root} is not a git repository")
    ops = g.operations_in_progress()
    if ops:
        raise ReleaseError(f"a {', '.join(ops)} is in progress; finish or abort it first")
    branch = g.current_branch()
    if branch is None:
        raise ReleaseError(f"HEAD is detached; check out '{ctx.branch}' first")
    if branch != ctx.branch:
        raise ReleaseError(f"on branch '{branch}', releases are made from '{ctx.branch}'")
    if not g.config("user.name") or not g.config("user.email"):
        raise ReleaseError("git user.name / user.email are not set")
    dirty = g.status_lines()
    if dirty:
        listing = "\n".join("  " + line for line in dirty[:15])
        more = f"\n  ... and {len(dirty) - 15} more" if len(dirty) > 15 else ""
        raise ReleaseError(f"working tree is not clean; commit or stash these first:\n{listing}{more}")

    g.fetch_branch(ctx.remote, ctx.branch)
    upstream = f"{ctx.remote}/{ctx.branch}"
    ahead, behind = g.ahead_behind("HEAD", upstream)
    if ahead and behind:
        raise ReleaseError(f"'{ctx.branch}' has diverged from {upstream} "
                           f"({ahead} local, {behind} remote commits); reconcile first")
    if behind:
        raise ReleaseError(f"'{ctx.branch}' is {behind} commit(s) behind {upstream}; run 'git pull' first")
    if ahead:
        commits = "\n".join("  " + c for c in g.log_oneline(f"{upstream}..HEAD", limit=10))
        raise ReleaseError(f"'{ctx.branch}' has {ahead} unpushed commit(s); push or reset them first:\n{commits}")

    return RepoState(g.head(), g.local_tags(), g.remote_tags(ctx.remote))


def read_current(ctx: Context) -> Tuple[str, Version]:
    raw = read_version_file(ctx.version_path)
    return raw, Version.parse(raw)


def latest_release(tag_names: Iterable[str], below: Optional[Version] = None) -> Optional[Version]:
    versions = [v for v in (try_parse(t) for t in tag_names if t.startswith("v"))
                if v is not None and not v.is_prerelease and (below is None or v < below)]
    return max(versions, key=Version.sort_key) if versions else None


def apply_version(ctx: Context, tx: Transaction, version: Version) -> List[Path]:
    changed = []
    for path in [ctx.version_path] + [ctx.root / m for m in ctx.cfg.mirror_files]:
        if not path.exists() or read_version_file(path) != str(version):
            tx.will_write(path)
            write_version_file(path, version)
            changed.append(path)
    if ctx.cfg.npm and (ctx.cfg.npm_dev or not version.is_prerelease):
        changed.extend(update_npm_version(ctx.root, version))
    return changed


def write_release_notes(ctx: Context, tx: Transaction, version: Version, state: RepoState) -> Path:
    g = ctx.git
    path = ctx.root / ctx.cfg.releases_dir / f"{version.tag}.txt"
    lines = [
        f"Release {version.tag}",
        f"Built from commit: {g.short('HEAD')}",
        f"Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    prev = latest_release(state.tag_names(), below=version)
    prev_ref = None
    if prev:
        ref = state.local_tags.get(prev.tag) or state.remote_tags.get(prev.tag)
        if ref and g.rev_parse(f"{ref.commit}^{{commit}}"):
            prev_ref = ref.commit
    if prev_ref:
        lines += [f"Changes since {prev.tag}:", "---"] + g.log_oneline(f"{prev_ref}..HEAD")
    else:
        lines += ["Changes:", "---"] + g.log_oneline("HEAD", limit=25)
    tx.will_write(path)
    write_text_atomic(path, "\n".join(lines) + "\n")
    return path


def commit_files(ctx: Context, paths: List[Path], message: str, allow_empty: bool = False) -> bool:
    """Stage exactly these files and commit. Returns False if nothing changed (only when allowed)."""
    g = ctx.git
    top = g.toplevel().resolve()
    rels = sorted({p.resolve().relative_to(top).as_posix() for p in paths})
    if rels:
        g.run("add", "--", *rels)
    staged = set(g.out("diff", "--cached", "--name-only").splitlines())
    unexpected = staged - set(rels)
    if unexpected:
        raise ReleaseError("unexpected files staged: " + ", ".join(sorted(unexpected)))
    if not staged:
        if allow_empty:
            return False
        raise ReleaseError("nothing to commit - the version files already have the target content")
    g.run("commit", "-m", message)
    return True


def _remote_has(ctx: Context, expect: Dict[str, str]) -> bool:
    g = ctx.git
    tags = None
    for ref, sha in expect.items():
        if ref.startswith("refs/heads/"):
            if g.remote_branch_sha(ctx.remote, ref[len("refs/heads/"):]) != sha:
                return False
        elif ref.startswith("refs/tags/"):
            tags = tags if tags is not None else g.remote_tags(ctx.remote)
            ref_ = tags.get(ref[len("refs/tags/"):])
            if ref_ is None or ref_.commit != sha:
                return False
    return True


def atomic_push(ctx: Context, tx: Transaction, refspecs: List[str], expect: Dict[str, str],
                extra: Iterable[str] = ()) -> None:
    """Push all refspecs in one atomic transaction; `expect` maps remote refs to the commit they must end at."""
    g = ctx.git
    args = ["push", "--atomic", *extra, ctx.remote, *refspecs]
    ui.step(f"Pushing to {ctx.remote}: {' '.join(refspecs)}")
    try:
        g.run(*args)
    except GitError as e:
        # The connection can drop after the remote already accepted the push.
        try:
            if _remote_has(ctx, expect):
                ui.warn("push reported an error, but the remote has exactly the expected refs; continuing")
                tx.pushed = True
                return
        except GitError:
            pass
        raise ReleaseError(f"push to {ctx.remote} failed:\n{e.stderr}") from None
    tx.pushed = True
    try:
        if not _remote_has(ctx, expect):
            ui.warn(f"push succeeded but {ctx.remote} does not show the expected refs yet; "
                    "check with 'make.py status'")
    except GitError as e:
        ui.warn(f"could not verify the push: {e}")


def show_plan(rows: List[Tuple[str, str]]) -> None:
    width = max(len(k) for k, _ in rows)
    for key, value in rows:
        ui.info(f"  {key.ljust(width)} : {value}")
    ui.info("")


def github_release_assets(ctx: Context, tag: str) -> Optional[int]:
    """Number of assets on the GitHub release for `tag`, 0 if there is no release, None if unknown."""
    if not which("gh"):
        ui.warn("GitHub CLI 'gh' not found; cannot check whether a GitHub release exists for " + tag)
        return None
    proc = subprocess.run([which("gh"), "release", "view", tag, "--json", "assets"], cwd=ctx.root,
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        if "not found" in (proc.stderr or "").lower():
            return 0
        ui.warn(f"could not query GitHub release {tag}: {proc.stderr.strip()}")
        return None
    try:
        return len(json.loads(proc.stdout).get("assets", []))
    except ValueError:
        return None


def is_first_release(current: Version, tag_names: Iterable[str]) -> bool:
    """A final version in a repo that has no release tags at all has simply never been released."""
    return not current.is_prerelease and latest_release(tag_names) is None


def check_released_version_tagged(current: Version, state: RepoState) -> None:
    if is_first_release(current, state.tag_names()):
        return
    if not current.is_prerelease and current.tag not in state.tag_names():
        raise ReleaseError(f"version file says {current} but tag {current.tag} does not exist.\n"
                           "The last release did not finish. Run 'make.py status' and 'make.py repair'.")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def cmd_release(ctx: Context, args) -> int:
    state = preflight(ctx)
    raw, current = read_current(ctx)

    if args.version:
        target = Version.parse(args.version)
        if target.is_prerelease:
            raise ReleaseError(f"{target} is a pre-release; use 'make.py dev {target}' for dev versions")
    elif current.is_prerelease:
        target = current.final()
    elif is_first_release(current, state.tag_names()):
        target = current
    else:
        check_released_version_tagged(current, state)
        target = current.next_patch()

    tag = target.tag
    where = [n for n, tags in (("locally", state.local_tags), (ctx.remote, state.remote_tags)) if tag in tags]
    if where:
        raise ReleaseError(f"tag {tag} already exists ({' and '.join(where)}).\n"
                           "Use 'make.py retag' to move it to HEAD, or choose another version.")
    latest = latest_release(state.tag_names())
    if latest and target <= latest:
        raise ReleaseError(f"{target} is not newer than the latest release {latest.tag}")

    files = ([ctx.cfg.version_file] + ctx.cfg.mirror_files
             + (["package.json", "package-lock.json"] if ctx.cfg.npm else []))
    ui.header(f"{ctx.cfg.name} release")
    show_plan([
        ("Current version", raw),
        ("Release version", str(target)),
        ("Commit", f"'Bump version to {target}' ({', '.join(files)}, {ctx.cfg.releases_dir}/{tag}.txt)"),
        ("Tag", f"{tag} (annotated) on that commit"),
        ("Push", f"git push --atomic {ctx.remote} HEAD:refs/heads/{ctx.branch} refs/tags/{tag}"),
    ])
    if ctx.dry_run:
        ui.info("Dry run: nothing was changed.")
        return 0
    ui.confirm(f"Release {tag}?", ctx.yes)

    def body(tx: Transaction) -> None:
        changed = apply_version(ctx, tx, target)
        notes = write_release_notes(ctx, tx, target, state)
        commit_files(ctx, changed + [notes], f"Bump version to {target}")
        tx.set_tag(tag, "HEAD", f"Release version {target}")
        head = ctx.git.head()
        atomic_push(ctx, tx, [f"HEAD:refs/heads/{ctx.branch}", f"refs/tags/{tag}"],
                    {f"refs/heads/{ctx.branch}": head, f"refs/tags/{tag}": head})

    run_transaction(ctx, body)
    ui.ok(f"Released {tag} on {ctx.git.short()} and pushed to {ctx.remote}")
    if ctx.cfg.next_steps:
        ui.info("\nNext steps:")
        for i, s in enumerate(ctx.cfg.next_steps, 1):
            ui.info(f"  {i}. {s}")
    return 0


def cmd_dev(ctx: Context, args) -> int:
    state = preflight(ctx)
    raw, current = read_current(ctx)

    if args.version:
        target = Version.parse(args.version)
        if not target.is_prerelease:
            target = target.as_dev()
    elif current.is_prerelease:
        # preflight guarantees there are no unpushed commits, so this really is a no-op
        ui.ok(f"Already on development version {current} and in sync with {ctx.remote}; nothing to do.")
        return 0
    else:
        check_released_version_tagged(current, state)
        target = current.next_patch().as_dev()

    if target == current:
        ui.ok(f"Version is already {current}; nothing to do.")
        return 0
    latest = latest_release(state.tag_names())
    if latest and target.final() <= latest:
        raise ReleaseError(f"{target} would not lead to a version newer than the latest release {latest.tag}")

    ui.header(f"{ctx.cfg.name} development version")
    show_plan([
        ("Current version", raw),
        ("Dev version", str(target)),
        ("Commit", f"'chore(version): prepare next version {target}'"),
        ("Push", f"git push --atomic {ctx.remote} HEAD:refs/heads/{ctx.branch}"),
    ])
    if ctx.dry_run:
        ui.info("Dry run: nothing was changed.")
        return 0

    def body(tx: Transaction) -> None:
        changed = apply_version(ctx, tx, target)
        commit_files(ctx, changed, f"chore(version): prepare next version {target}")
        head = ctx.git.head()
        atomic_push(ctx, tx, [f"HEAD:refs/heads/{ctx.branch}"], {f"refs/heads/{ctx.branch}": head})

    run_transaction(ctx, body)
    ui.ok(f"Advanced to {target} and pushed to {ctx.remote}")
    return 0


def cmd_retag(ctx: Context, args) -> int:
    state = preflight(ctx)
    _, current = read_current(ctx)
    target = Version.parse(args.version) if args.version else current
    if target.is_prerelease:
        raise ReleaseError(f"version file is {current}; retag moves the tag of a released version.\n"
                           "Pass the version explicitly, e.g. 'make.py retag 1.2.3'.")
    tag = target.tag
    local = state.local_tags.get(tag)
    remote = state.remote_tags.get(tag)
    if not local and not remote:
        raise ReleaseError(f"tag {tag} does not exist; use 'make.py release' to create a release")
    if remote and remote.commit == state.head and local and local.commit == state.head:
        ui.ok(f"{tag} already points at HEAD; nothing to do.")
        return 0
    if remote and local and remote.object != local.object:
        ui.warn(f"local {tag} differs from {ctx.remote}; the remote one will be replaced")

    assets = github_release_assets(ctx, tag)
    if assets and not args.force:
        raise ReleaseError(f"GitHub release {tag} already has {assets} asset(s) built from the old commit.\n"
                           "Re-run with --force to move the tag anyway, then re-publish the assets.")

    old = remote.commit if remote else (local.commit if local else "")
    ui.header(f"{ctx.cfg.name} retag")
    show_plan([
        ("Tag", tag),
        ("From", ctx.git.short(old) if old else "(not on remote)"),
        ("To", f"HEAD ({ctx.git.short()})"),
        ("Release notes", f"regenerate {ctx.cfg.releases_dir}/{tag}.txt, commit if changed"),
        ("Push", f"git push --atomic --force-with-lease (tag only replaced if {ctx.remote} still has the old one)"),
    ])
    if ctx.dry_run:
        ui.info("Dry run: nothing was changed.")
        return 0
    ui.confirm(f"Move {tag} to HEAD?", ctx.yes)

    def body(tx: Transaction) -> None:
        notes = write_release_notes(ctx, tx, target, state)
        committed = commit_files(ctx, [notes], f"chore(release): update release notes for {tag}",
                                 allow_empty=True)
        tx.set_tag(tag, "HEAD", f"Release version {target}", force=True)
        head = ctx.git.head()
        refspecs, expect = [], {f"refs/tags/{tag}": head}
        if committed:
            refspecs.append(f"HEAD:refs/heads/{ctx.branch}")
            expect[f"refs/heads/{ctx.branch}"] = head
        refspecs.append(f"refs/tags/{tag}")
        lease = f"--force-with-lease=refs/tags/{tag}:{remote.object if remote else ''}"
        atomic_push(ctx, tx, refspecs, expect, extra=[lease])

    run_transaction(ctx, body)
    ui.ok(f"{tag} now points at {ctx.git.short()} on {ctx.remote}")
    if assets:
        ui.warn(f"Re-publish the release assets for {tag}; the existing ones were built from the old commit.")
    return 0


# -- status / repair ---------------------------------------------------------

@dataclass
class Finding:
    level: str                      # "error", "warn" or "info"
    message: str
    fix: Optional[str] = None       # description of the repair, if one is available
    apply: Optional[Callable[[Transaction], Tuple[List[str], Dict[str, str], bool]]] = None
    # apply returns (refspecs to push, expected remote refs, needs branch push)


def inspect(ctx: Context) -> Tuple[List[Tuple[str, str]], List[Finding]]:
    g = ctx.git
    info: List[Tuple[str, str]] = []
    findings: List[Finding] = []

    branch = g.current_branch()
    info.append(("Branch", branch or "(detached HEAD)"))
    if branch != ctx.branch:
        findings.append(Finding("warn", f"not on '{ctx.branch}'"))
    ops = g.operations_in_progress()
    if ops:
        findings.append(Finding("error", f"{', '.join(ops)} in progress"))
    dirty = g.status_lines()
    info.append(("Working tree", "clean" if not dirty else f"{len(dirty)} changed/untracked file(s)"))

    upstream = f"{ctx.remote}/{ctx.branch}"
    ahead = behind = 0
    try:
        g.fetch_branch(ctx.remote, ctx.branch)
        ahead, behind = g.ahead_behind("HEAD", upstream)
        info.append((f"vs {upstream}", f"{ahead} ahead, {behind} behind"))
    except GitError as e:
        findings.append(Finding("error", f"cannot reach {ctx.remote}: {e.stderr}"))
    if behind and ahead:
        findings.append(Finding("error", f"diverged from {upstream}; reconcile manually"))
    elif behind:
        findings.append(Finding("warn", f"{behind} commit(s) behind {upstream}; run 'git pull'"))
    elif ahead:
        commits = ", ".join(g.log_oneline(f"{upstream}..HEAD", limit=5))
        findings.append(Finding("warn", f"{ahead} unpushed commit(s): {commits}",
                                fix=f"push {ctx.branch} to {ctx.remote}",
                                apply=lambda tx: ([], {}, True)))

    raw = read_version_file(ctx.version_path)
    current = Version.parse(raw)
    info.append(("Version file", raw))
    local_tags = g.local_tags()
    try:
        remote_tags = g.remote_tags(ctx.remote)
    except GitError:
        remote_tags = {}
    latest = latest_release(set(local_tags) | set(remote_tags))
    info.append(("Latest release tag", latest.tag if latest else "(none)"))

    out_of_sync = [m for m in ctx.cfg.mirror_files
                   if not (ctx.root / m).exists() or read_version_file(ctx.root / m) != str(current)]
    if raw != str(current) or out_of_sync:
        def normalize(tx: Transaction, v=current):
            changed = apply_version(ctx, tx, v)
            commit_files(ctx, changed, f"chore(version): normalize version files to {v}")
            return [], {}, True
        problems = []
        if raw != str(current):
            problems.append(f"{ctx.cfg.version_file} uses a legacy format '{raw}' (build metadata or 'v' prefix)")
        if out_of_sync:
            problems.append(f"{', '.join(out_of_sync)} not in sync with {ctx.cfg.version_file}")
        findings.append(Finding("warn", "; ".join(problems),
                                fix=f"write '{current}' to all version files and commit", apply=normalize))

    tag = current.tag
    if current.is_prerelease:
        if current.final().tag in set(local_tags) | set(remote_tags):
            findings.append(Finding("error", f"dev version {current} but {current.final().tag} is already "
                                             f"released; run 'make.py dev {current.next_patch()}'"))
    else:
        local, remote = local_tags.get(tag), remote_tags.get(tag)
        if is_first_release(current, set(local_tags) | set(remote_tags)):
            findings.append(Finding("info", f"no releases yet; 'make.py release' will release {current} "
                                            "as the first version"))
        elif not local and not remote:
            commit = g.last_commit_touching(ctx.cfg.version_file)
            content = g.file_at(commit, ctx.cfg.version_file) if commit else None
            if commit and content and try_parse(content.splitlines()[0]) == current:
                def tag_it(tx: Transaction, c=commit, t=tag, v=current):
                    tx.set_tag(t, c, f"Release version {v}")
                    return [f"refs/tags/{t}"], {f"refs/tags/{t}": c}, False
                findings.append(Finding("error", f"version file says {current} but tag {tag} does not exist "
                                                 "(release did not finish)",
                                        fix=f"tag {g.short(commit)} (the commit that set {current}) "
                                            f"as {tag} and push it",
                                        apply=tag_it))
            else:
                findings.append(Finding("error", f"version file says {current} but tag {tag} does not exist, "
                                                 "and the release commit could not be identified"))
        elif local and not remote:
            findings.append(Finding("warn", f"tag {tag} exists locally but not on {ctx.remote}",
                                    fix=f"push {tag}",
                                    apply=lambda tx, t=tag, c=local.commit:
                                    ([f"refs/tags/{t}"], {f"refs/tags/{t}": c}, False)))
        elif remote and not local:
            def fetch(tx: Transaction, t=tag):
                tx.fetch_tag(ctx.remote, t)
                return [], {}, False
            findings.append(Finding("info", f"tag {tag} exists on {ctx.remote} but not locally",
                                    fix=f"fetch {tag}", apply=fetch))
        elif local.object != remote.object:
            findings.append(Finding("error", f"local {tag} ({g.short(local.commit)}) differs from "
                                             f"{ctx.remote} ({remote.commit[:7]}); decide which is right, "
                                             "then delete the wrong one or use 'make.py retag'"))

    local_only = sorted(t for t in set(local_tags) - set(remote_tags) if t.startswith("v") and t != tag)
    if local_only:
        findings.append(Finding("info", "other tags only present locally: " + ", ".join(local_only[:10])))
    return info, findings


def _print_findings(findings: List[Finding]) -> None:
    if not findings:
        ui.ok("Everything is consistent.")
        return
    for f in findings:
        text = f.message + (f"\n      repair: {f.fix}" if f.fix else "")
        {"error": ui.error, "warn": ui.warn}.get(f.level, lambda m: ui.info(f"[info] {m}"))(text)


def cmd_status(ctx: Context, args) -> int:
    ui.header(f"{ctx.cfg.name} release status")
    info, findings = inspect(ctx)
    show_plan(info)
    _print_findings(findings)
    return 1 if any(f.level == "error" for f in findings) else 0


def cmd_repair(ctx: Context, args) -> int:
    g = ctx.git
    ui.header(f"{ctx.cfg.name} release repair")
    info, findings = inspect(ctx)
    show_plan(info)
    _print_findings(findings)
    fixes = [f for f in findings if f.apply]
    if not fixes:
        ui.info("\nNothing that can be repaired automatically.")
        return 1 if any(f.level == "error" for f in findings) else 0
    if g.operations_in_progress() or g.status_lines() or g.current_branch() != ctx.branch:
        raise ReleaseError(f"repair needs a clean working tree on '{ctx.branch}' with no merge/rebase in progress")
    ahead, behind = g.ahead_behind("HEAD", f"{ctx.remote}/{ctx.branch}")
    if behind:
        raise ReleaseError("branch is behind the remote; pull first")

    ui.info("\nPlanned repairs:")
    for i, f in enumerate(fixes, 1):
        ui.info(f"  {i}. {f.fix}")
    ui.info(f"  then one atomic push to {ctx.remote}\n")
    if ctx.dry_run:
        ui.info("Dry run: nothing was changed.")
        return 0
    ui.confirm("Apply these repairs?", ctx.yes)

    def body(tx: Transaction) -> None:
        refspecs: List[str] = []
        expect: Dict[str, str] = {}
        push_branch = ahead > 0
        for f in fixes:
            r, e, b = f.apply(tx)
            refspecs += r
            expect.update(e)
            push_branch = push_branch or b
        if push_branch:
            head = g.head()
            refspecs.insert(0, f"HEAD:refs/heads/{ctx.branch}")
            expect[f"refs/heads/{ctx.branch}"] = head
        if refspecs:
            atomic_push(ctx, tx, refspecs, expect)

    run_transaction(ctx, body)
    ui.ok("Repairs applied.")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

USAGE = """\
make.py release [version]     finalize X.Y.Z-dev -> X.Y.Z (or bump the patch), commit, tag, push
make.py dev [version]         advance to the next development version (X.Y.Z+1-dev) and push
make.py retag [version]       move the release tag to HEAD (safe, uses --force-with-lease)
make.py status                show version/tag/remote state and any inconsistencies
make.py repair                fix what 'status' found (asks first)

options: -n/--dry-run  -y/--yes  -v/--verbose  (retag also: --force)"""

COMMANDS = {"dev": cmd_dev, "retag": cmd_retag, "status": cmd_status, "repair": cmd_repair}
ALIASES = {"--dev": "dev", "--next": "dev", "next": "dev",
           "-r": "retag", "--retag": "retag", "--readjust": "retag", "--rebase": "retag"}


def main(cfg: ReleaseConfig, root: Path, argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    command = "release"
    if argv and (argv[0] in COMMANDS or argv[0] in ALIASES):
        command = ALIASES.get(argv[0], argv[0])
        argv = argv[1:]

    parser = argparse.ArgumentParser(prog="make.py", usage=USAGE,
                                     description=f"{cfg.name} release tool")
    if command in ("release", "dev", "retag"):
        parser.add_argument("version", nargs="?", help="explicit version")
    parser.add_argument("-n", "--dry-run", action="store_true", help="show the plan, change nothing")
    parser.add_argument("-y", "--yes", action="store_true", help="don't ask for confirmation")
    parser.add_argument("-v", "--verbose", action="store_true", help="print every git command")
    if command == "retag":
        parser.add_argument("--force", action="store_true",
                            help="retag even if the GitHub release already has assets")
    parser.add_argument("--remote", default=cfg.remote)
    parser.add_argument("--branch", default=cfg.branch)
    args = parser.parse_args(argv)
    if not hasattr(args, "version"):
        args.version = None

    cfg = ReleaseConfig(**{**cfg.__dict__, "remote": args.remote, "branch": args.branch})
    ctx = Context(cfg, Path(root), Git(Path(root), verbose=args.verbose),
                  dry_run=args.dry_run, yes=args.yes)
    handler = cmd_release if command == "release" else COMMANDS[command]
    try:
        return handler(ctx, args)
    except (ReleaseError, VersionError, GitError, ui.Abort) as e:
        ui.error(str(e))
        return 1
    except KeyboardInterrupt:
        ui.error("interrupted")
        return 130


def release_commands(cfg: ReleaseConfig, root: Path) -> Dict[str, "Command"]:
    """The release-related commands for a repository's make.py."""
    from .cli import Command
    return {
        "release": Command("finalize the dev version (or bump patch), tag and push",
                           lambda argv: main(cfg, root, argv)),
        "dev": Command("advance to the next development version and push",
                       lambda argv: main(cfg, root, ["dev", *argv])),
        "retag": Command("move the release tag to HEAD (safe)",
                         lambda argv: main(cfg, root, ["retag", *argv])),
        "status": Command("show version/tag/remote state and inconsistencies",
                          lambda argv: main(cfg, root, ["status", *argv])),
        "repair": Command("fix what 'status' found (asks first)",
                          lambda argv: main(cfg, root, ["repair", *argv])),
    }
