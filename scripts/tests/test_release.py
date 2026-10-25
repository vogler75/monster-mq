"""Tests for mmqlib.release against throwaway repos with a bare 'origin'.

Run from the repo root:  python -m unittest discover -s scripts/tests -v
"""

import contextlib
import io
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True  # keep __pycache__ out of the working tree
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mmqlib import release as rel  # noqa: E402
from mmqlib.git import Git, GitError  # noqa: E402
from mmqlib.version import Version, VersionError  # noqa: E402

CFG = rel.ReleaseConfig(name="Test")


def sh(cwd, *args) -> str:
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed: {proc.stderr}")
    return proc.stdout.strip()


def write_hook(git_dir: Path, name: str, body: str = "exit 1") -> None:
    hook = git_dir / "hooks" / name
    hook.parent.mkdir(parents=True, exist_ok=True)
    hook.write_text(f"#!/bin/sh\n{body}\n", newline="\n")
    hook.chmod(0o755)


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="mmq-release-test-"))
        self.origin = self.tmp / "origin.git"
        self.work = self.tmp / "work"
        sh(self.tmp, "init", "--bare", "-b", "main", str(self.origin))
        self.work = self.clone("work")
        (self.work / "version.txt").write_text("1.0.0-dev\n", newline="\n")
        (self.work / "README.md").write_text("test\n", newline="\n")
        sh(self.work, "add", ".")
        sh(self.work, "commit", "-m", "init")
        sh(self.work, "push", "-u", "origin", "main")
        self.output = ""

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def clone(self, name: str) -> Path:
        path = self.tmp / name
        sh(self.tmp, "clone", "-q", str(self.origin), str(path))
        sh(path, "config", "user.name", "Test")
        sh(path, "config", "user.email", "test@example.com")
        sh(path, "config", "core.autocrlf", "false")
        return path

    cfg = CFG

    def run_tool(self, *argv, root=None) -> int:
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
            code = rel.main(self.cfg, root or self.work, list(argv))
        self.output = out.getvalue()
        return code

    def assertTool(self, expected: int, *argv):
        code = self.run_tool(*argv)
        self.assertEqual(code, expected, f"exit {code}, output:\n{self.output}")

    def commit_file(self, repo: Path, name: str, content: str, msg: str, push: bool = True) -> str:
        (repo / name).write_text(content, newline="\n")
        sh(repo, "add", name)
        sh(repo, "commit", "-m", msg)
        if push:
            sh(repo, "push", "-q", "origin", "HEAD:main")
        return sh(repo, "rev-parse", "HEAD")

    def snapshot(self, repo=None):
        repo = repo or self.work
        return {
            "head": sh(repo, "rev-parse", "HEAD"),
            "tags": sh(repo, "for-each-ref", "refs/tags", "--format=%(refname) %(objectname)"),
            "status": sh(repo, "status", "--porcelain"),
            "version": (repo / "version.txt").read_text(),
            "remote": sh(self.tmp, "ls-remote", str(self.origin)),
        }

    def remote_tag_commit(self, tag: str):
        return Git(self.work).remote_tags("origin").get(tag)

    def version(self, repo=None) -> str:
        return ((repo or self.work) / "version.txt").read_text().strip()


class ReleaseTests(RepoTestCase):
    def test_release_finalizes_dev_version(self):
        self.assertTool(0, "-y")
        self.assertEqual(self.version(), "1.0.0")
        head = sh(self.work, "rev-parse", "HEAD")
        self.assertEqual(self.remote_tag_commit("v1.0.0").commit, head)
        self.assertEqual(sh(self.tmp, "ls-remote", str(self.origin), "refs/heads/main").split()[0], head)
        self.assertEqual(sh(self.work, "cat-file", "-t", "v1.0.0"), "tag")  # annotated
        notes = (self.work / "releases" / "v1.0.0.txt").read_text()
        self.assertIn("Release v1.0.0", notes)
        self.assertEqual(sh(self.work, "status", "--porcelain"), "")

    def test_release_bumps_patch_from_final(self):
        self.assertTool(0, "-y")
        self.assertTool(0, "-y")
        self.assertEqual(self.version(), "1.0.1")
        self.assertIn("Changes since v1.0.0", (self.work / "releases" / "v1.0.1.txt").read_text())

    def test_explicit_version(self):
        self.assertTool(0, "2.0.0", "-y")
        self.assertIsNotNone(self.remote_tag_commit("v2.0.0"))

    def test_explicit_version_must_be_newer(self):
        self.assertTool(0, "-y")
        before = self.snapshot()
        self.assertTool(1, "0.9.0", "-y")
        self.assertEqual(self.snapshot(), before)

    def test_dry_run_changes_nothing(self):
        before = self.snapshot()
        self.assertTool(0, "--dry-run")
        self.assertEqual(self.snapshot(), before)

    def test_non_tty_without_yes_refuses(self):
        before = self.snapshot()
        with mock.patch("sys.stdin.isatty", return_value=False):
            self.assertTool(1)
        self.assertIn("-y", self.output)
        self.assertEqual(self.snapshot(), before)


class PreflightTests(RepoTestCase):
    def assertRefused(self, *argv):
        before = self.snapshot()
        self.assertTool(1, *argv)
        self.assertEqual(self.snapshot(), before)

    def test_untracked_file_refused(self):
        (self.work / "stray.txt").write_text("x")
        self.assertRefused("-y")
        self.assertIn("stray.txt", self.output)

    def test_modified_file_refused(self):
        (self.work / "README.md").write_text("changed\n")
        self.assertRefused("-y")

    def test_behind_origin_refused(self):
        other = self.clone("other")
        self.commit_file(other, "a.txt", "a", "other change")
        self.assertRefused("-y")
        self.assertIn("behind", self.output)

    def test_ahead_of_origin_refused(self):
        self.commit_file(self.work, "a.txt", "a", "local only", push=False)
        self.assertRefused("-y")
        self.assertIn("unpushed", self.output)

    def test_wrong_branch_refused(self):
        sh(self.work, "checkout", "-q", "-b", "feature")
        self.assertRefused("-y")

    def test_detached_head_refused(self):
        sh(self.work, "checkout", "-q", "--detach")
        self.assertRefused("-y")

    def test_tag_already_on_remote_refused(self):
        other = self.clone("other")
        sh(other, "tag", "v1.0.0")
        sh(other, "push", "-q", "origin", "v1.0.0")
        self.assertRefused("-y")
        self.assertIn("already exists", self.output)

    def test_lock_prevents_concurrent_runs(self):
        lock = Git(self.work).git_dir() / "mmq-release.lock"
        lock.write_text("123")
        self.assertRefused("-y")
        self.assertTrue(lock.exists(), "a foreign lock must not be removed")


class RollbackTests(RepoTestCase):
    def test_push_rejected_rolls_back(self):
        write_hook(self.origin, "pre-receive")
        before = self.snapshot()
        self.assertTool(1, "-y")
        self.assertEqual(self.snapshot(), before)
        self.assertFalse((self.work / "releases" / "v1.0.0.txt").exists())
        self.assertIn("unchanged", self.output)

    def test_commit_hook_failure_rolls_back(self):
        write_hook(self.work / ".git", "pre-commit")
        before = self.snapshot()
        self.assertTool(1, "-y")
        self.assertEqual(self.snapshot(), before)

    def test_interrupt_rolls_back(self):
        before = self.snapshot()
        with mock.patch.object(rel, "atomic_push", side_effect=KeyboardInterrupt):
            self.assertTool(130, "-y")
        self.assertEqual(self.snapshot(), before)

    def test_dev_push_rejected_rolls_back(self):
        self.assertTool(0, "-y")
        write_hook(self.origin, "pre-receive")
        before = self.snapshot()
        self.assertTool(1, "dev")
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.version(), "1.0.0")

    def test_push_error_after_remote_accepted_counts_as_success(self):
        real_run = Git.run

        def flaky(self_, *args, check=True):
            result = real_run(self_, *args, check=check)
            if args and args[0] == "push":
                raise GitError(args, 128, "connection reset")
            return result

        with mock.patch.object(Git, "run", flaky):
            self.assertTool(0, "-y")
        self.assertEqual(self.remote_tag_commit("v1.0.0").commit, sh(self.work, "rev-parse", "HEAD"))
        self.assertIn("expected refs", self.output)


class DevTests(RepoTestCase):
    def test_dev_after_release(self):
        self.assertTool(0, "-y")
        self.assertTool(0, "dev")
        self.assertEqual(self.version(), "1.0.1-dev")
        self.assertEqual(sh(self.tmp, "ls-remote", str(self.origin), "refs/heads/main").split()[0],
                         sh(self.work, "rev-parse", "HEAD"))

    def test_legacy_dev_flag(self):
        self.assertTool(0, "-y")
        self.assertTool(0, "--dev")
        self.assertEqual(self.version(), "1.0.1-dev")

    def test_dev_explicit(self):
        self.assertTool(0, "-y")
        self.assertTool(0, "dev", "1.1.0")
        self.assertEqual(self.version(), "1.1.0-dev")

    def test_dev_noop_when_already_dev(self):
        before = self.snapshot()
        self.assertTool(0, "dev")
        self.assertEqual(self.snapshot(), before)

    def test_dev_with_unpushed_commit_is_not_silent(self):
        # The old script printed "Already on a development version" and exited 0 here.
        self.commit_file(self.work, "a.txt", "a", "stuck", push=False)
        self.assertTool(1, "dev")
        self.assertIn("unpushed", self.output)

    def test_dev_refuses_untagged_release(self):
        self.assertTool(0, "-y")
        self.assertTool(0, "dev")
        self.commit_file(self.work, "version.txt", "1.0.1\n", "Release")
        before = self.snapshot()
        self.assertTool(1, "dev")
        self.assertEqual(self.snapshot(), before)
        self.assertIn("repair", self.output)


class RetagTests(RepoTestCase):
    def setUp(self):
        super().setUp()
        patcher = mock.patch.object(rel, "github_release_assets", return_value=None)
        self.assets = patcher.start()
        self.addCleanup(patcher.stop)

    def test_retag_moves_tag_to_head(self):
        self.assertTool(0, "-y")
        self.commit_file(self.work, "fix.txt", "fix", "hotfix")
        self.assertTool(0, "retag", "-y")
        head = sh(self.work, "rev-parse", "HEAD")
        self.assertEqual(self.remote_tag_commit("v1.0.0").commit, head)
        self.assertIn("hotfix", (self.work / "releases" / "v1.0.0.txt").read_text())

    def test_retag_refused_when_release_has_assets(self):
        self.assertTool(0, "-y")
        self.commit_file(self.work, "fix.txt", "fix", "hotfix")
        self.assets.return_value = 3
        before = self.snapshot()
        self.assertTool(1, "retag", "-y")
        self.assertEqual(self.snapshot(), before)
        self.assertTool(0, "retag", "-y", "--force")

    def test_retag_lease_protects_against_concurrent_move(self):
        self.assertTool(0, "-y")
        self.commit_file(self.work, "fix.txt", "fix", "hotfix")
        other = self.clone("other")
        real_push = rel.atomic_push

        def someone_else_moves_tag_first(*args, **kwargs):
            sh(other, "fetch", "-q", "--tags", "origin")
            sh(other, "tag", "-f", "-a", "v1.0.0", "-m", "theirs", "HEAD~1")
            sh(other, "push", "-q", "--force", "origin", "refs/tags/v1.0.0")
            return real_push(*args, **kwargs)

        before = self.snapshot()
        with mock.patch.object(rel, "atomic_push", someone_else_moves_tag_first):
            self.assertTool(1, "retag", "-y")
        after = self.snapshot()
        self.assertEqual(after["head"], before["head"])
        self.assertEqual(after["tags"], before["tags"], "local tag must be restored")


class StatusRepairTests(RepoTestCase):
    def make_dashboard_state(self):
        """What dashboard looked like: a manual 'Release' commit with +sha, never tagged, more work on top."""
        self.assertTool(0, "-y")
        self.assertTool(0, "dev")
        release_commit = self.commit_file(self.work, "version.txt", "1.0.1+abc1234\n", "Release")
        self.commit_file(self.work, "feature.txt", "f", "later work")
        return release_commit

    def test_status_clean_repo(self):
        self.assertTool(0, "status")

    def test_status_detects_missing_tag(self):
        self.make_dashboard_state()
        self.assertTool(1, "status")
        self.assertIn("tag v1.0.1 does not exist", self.output)

    def test_repair_tags_release_commit_and_normalizes_version(self):
        release_commit = self.make_dashboard_state()
        self.assertTool(0, "repair", "-y")
        self.assertEqual(self.remote_tag_commit("v1.0.1").commit, release_commit)
        self.assertEqual(self.version(), "1.0.1")
        self.assertTool(0, "status")
        self.assertTool(0, "dev")
        self.assertEqual(self.version(), "1.0.2-dev")

    def test_repair_pushes_local_only_tag(self):
        self.assertTool(0, "-y")
        sh(self.tmp, "--git-dir", str(self.origin), "tag", "-d", "v1.0.0")
        self.assertTool(0, "repair", "-y")
        self.assertIsNotNone(self.remote_tag_commit("v1.0.0"))

    def test_repair_dry_run_changes_nothing(self):
        self.make_dashboard_state()
        before = self.snapshot()
        self.assertTool(0, "repair", "--dry-run")
        self.assertEqual(self.snapshot(), before)


class FirstReleaseTests(RepoTestCase):
    def setUp(self):
        super().setUp()
        self.commit_file(self.work, "version.txt", "0.1.0\n", "set version")

    def test_untagged_final_version_in_new_repo_is_released_as_is(self):
        self.assertTool(0, "status")
        self.assertIn("no releases yet", self.output)
        self.assertTool(0, "-y")
        self.assertEqual(self.version(), "0.1.0")
        self.assertIsNotNone(self.remote_tag_commit("v0.1.0"))

    def test_dev_allowed_before_first_release(self):
        self.assertTool(0, "dev")
        self.assertEqual(self.version(), "0.1.1-dev")


class NpmTests(RepoTestCase):
    def setUp(self):
        super().setUp()
        pkg = '{\n  "name": "x",\n  "version": "0.9.0"\n}\n'
        lock = '{\n  "name": "x",\n  "version": "0.9.0",\n  "packages": {\n    "": {\n      "version": "0.9.0"\n    }\n  }\n}\n'
        (self.work / "package-lock.json").write_text(lock, newline="\n")
        sh(self.work, "add", "package-lock.json")
        self.commit_file(self.work, "package.json", pkg, "add package.json")

    def pkg_version(self, name="package.json"):
        import json
        return json.loads((self.work / name).read_text())["version"]

    def test_release_and_dev_update_package_files(self):
        self.cfg = rel.ReleaseConfig(name="Test", npm=True)
        self.assertTool(0, "-y")
        self.assertEqual(self.pkg_version(), "1.0.0")
        self.assertEqual(self.pkg_version("package-lock.json"), "1.0.0")
        self.assertTool(0, "dev")
        self.assertEqual(self.pkg_version(), "1.0.1-dev")
        self.assertEqual(sh(self.work, "status", "--porcelain"), "")

    def test_npm_dev_false_keeps_package_json_at_release(self):
        self.cfg = rel.ReleaseConfig(name="Test", npm=True, npm_dev=False)
        self.assertTool(0, "-y")
        self.assertTool(0, "dev")
        self.assertEqual(self.version(), "1.0.1-dev")
        self.assertEqual(self.pkg_version(), "1.0.0")


class MirrorTests(RepoTestCase):
    MIRROR = "res/version.txt"

    def setUp(self):
        super().setUp()
        (self.work / "res").mkdir()
        self.commit_file(self.work, self.MIRROR, "1.0.0-dev\n", "add mirror")
        self.cfg = rel.ReleaseConfig(name="Test", mirror_files=[self.MIRROR])

    def mirror(self) -> str:
        return (self.work / self.MIRROR).read_text().strip()

    def test_release_and_dev_keep_mirror_in_sync(self):
        self.assertTool(0, "-y")
        self.assertEqual(self.mirror(), "1.0.0")
        self.assertIn(self.MIRROR, sh(self.work, "show", "--name-only", "--format=", "HEAD"))
        self.assertTool(0, "dev")
        self.assertEqual(self.mirror(), "1.0.1-dev")

    def test_repair_syncs_drifted_mirror(self):
        self.commit_file(self.work, self.MIRROR, "0.9.9+abc\n", "drift")
        self.assertTool(0, "status")
        self.assertIn("not in sync", self.output)
        self.assertTool(0, "repair", "-y")
        self.assertEqual(self.mirror(), "1.0.0-dev")


class VersionTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(str(Version.parse("1.8.35+61ef687")), "1.8.35")
        self.assertEqual(str(Version.parse("v0.5.14")), "0.5.14")
        self.assertEqual(Version.parse("1.2.3-dev").pre, "dev")
        with self.assertRaises(VersionError):
            Version.parse("1.2")

    def test_ordering(self):
        self.assertLess(Version.parse("1.2.3-dev"), Version.parse("1.2.3"))
        self.assertLess(Version.parse("1.2.3"), Version.parse("1.2.10"))
        self.assertEqual(rel.latest_release(["v1.2.9", "v1.2.10", "v1.3.0-dev", "x"]), Version.parse("1.2.10"))


if __name__ == "__main__":
    unittest.main()
