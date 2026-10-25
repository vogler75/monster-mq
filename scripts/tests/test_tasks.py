"""Tests for the make.py build/publish/run tasks. Nothing here talks to GitHub or Docker Hub.

Run from the repo root:  python -m unittest discover -s scripts/tests -v
"""

import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mmqlib import fs  # noqa: E402
from mmqlib.git import Git  # noqa: E402
from tasks import publish, run  # noqa: E402
from tasks.build import package_bundle  # noqa: E402
from tasks.common import BROKER_JAR, Repo  # noqa: E402


def git(cwd, *args) -> str:
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed: {proc.stderr}")
    return proc.stdout.strip()


def quiet():
    return contextlib.redirect_stdout(io.StringIO())


def make_jar(path: Path, sha: str = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        if sha:
            z.writestr("git-sha.txt", sha + "\n")


class TempRepo(unittest.TestCase):
    """A git repo with a bare 'origin', version.txt and a broker/target layout."""

    version = "1.2.3"

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="mmq-tasks-"))
        self.addCleanup(fs.rmtree, self.tmp)
        self.origin = self.tmp / "origin.git"
        git(self.tmp, "init", "--bare", "-q", str(self.origin))
        self.root = self.tmp / "work"
        self.root.mkdir()
        git(self.root, "init", "-q", "-b", "main")
        git(self.root, "config", "user.name", "t")
        git(self.root, "config", "user.email", "t@example.com")
        git(self.root, "config", "commit.gpgsign", "false")
        git(self.root, "config", "tag.gpgsign", "false")
        git(self.root, "remote", "add", "origin", str(self.origin))
        (self.root / "version.txt").write_text(self.version + "\n")
        git(self.root, "add", ".")
        git(self.root, "commit", "-q", "-m", "init")
        self.repo = Repo(self.root)
        self.head = git(self.root, "rev-parse", "HEAD")

    def tag_and_push(self, rev="HEAD"):
        git(self.root, "tag", "-a", f"v{self.version}", "-m", "r", rev)
        git(self.root, "push", "-q", "origin", "main", f"v{self.version}")

    def build_bundle(self, sha):
        t = self.repo.broker_target
        make_jar(t / BROKER_JAR, sha)
        (t / "dependencies").mkdir(parents=True, exist_ok=True)
        (t / "dependencies" / "dep.jar").write_bytes(b"x")
        with quiet():
            return package_bundle(self.repo, self.version)

    def publish(self, *argv):
        with quiet():
            return publish.main(self.repo, list(argv), git=Git(self.root))


class BundleTest(TempRepo):
    def test_layout_permissions_and_names(self):
        (self.root / "broker").mkdir(exist_ok=True)
        (self.root / "broker" / "example-config.yaml").write_text("a: 1\n")
        zip_path = self.build_bundle("abc1234")
        self.assertEqual(zip_path, self.root / "dist" / "monstermq-broker-1.2.3.zip")
        with zipfile.ZipFile(zip_path) as z:
            names = set(z.namelist())
            base = "monstermq-broker-1.2.3/"
            for n in ("", "sqlite/", "log/", "dependencies/", "dependencies/dep.jar", "run.sh", "run.bat",
                      "README.md", "AGENTS.md", "config.yaml", "monstermq-broker-1.2.3.jar"):
                self.assertIn(base + n, names)
            self.assertEqual(z.getinfo(base + "run.sh").external_attr >> 16, 0o100755)
            self.assertEqual(z.getinfo(base + "README.md").external_attr >> 16, 0o100644)
            self.assertNotIn(b"\r\n", z.read(base + "run.sh"))
            self.assertIn(b"\r\n", z.read(base + "run.bat"))
        self.assertFalse((self.root / "dist" / "monstermq-broker-1.2.3").exists())  # stage removed
        self.assertEqual(publish.bundle_git_sha(self.repo, self.version), "abc1234")


class PublishTest(TempRepo):
    def test_no_args_prints_help(self):
        self.assertEqual(self.publish(), 0)

    def test_dev_version_refuses_github(self):
        (self.root / "version.txt").write_text("1.2.4-dev\n")
        with self.assertRaisesRegex(publish.PublishError, "development version"):
            self.publish("--github", "-y")

    def test_tag_missing_on_origin(self):
        git(self.root, "push", "-q", "origin", "main")
        with self.assertRaisesRegex(publish.PublishError, "does not exist on origin"):
            self.publish("--broker", "-y")

    def test_tag_not_on_head_never_pushes(self):
        self.tag_and_push()
        (self.root / "x.txt").write_text("x")
        git(self.root, "add", ".")
        git(self.root, "commit", "-q", "-m", "later")
        before = git(self.root, "ls-remote", "--tags", "origin")
        with self.assertRaisesRegex(publish.PublishError, "make.py retag"):
            self.publish("--broker", "-y")
        self.assertEqual(before, git(self.root, "ls-remote", "--tags", "origin"))

    def test_missing_artifacts(self):
        self.tag_and_push()
        with self.assertRaisesRegex(publish.PublishError, "not found"):
            self.publish("--broker", "-y")
        with self.assertRaisesRegex(publish.PublishError, "setup executables not found"):
            self.publish("--setup", "-y")

    def test_bundle_from_other_commit_refused(self):
        self.tag_and_push()
        self.build_bundle("0000000")
        with self.assertRaisesRegex(publish.PublishError, "built from commit 0000000"):
            self.publish("--broker", "-y")

    def test_gh_checked_before_upload(self):
        self.tag_and_push()
        self.build_bundle(self.head[:7])
        with mock.patch("mmqlib.github.which", return_value=None), \
                mock.patch("mmqlib.github.run") as gh_run:
            with self.assertRaisesRegex(Exception, "'gh'"):
                self.publish("--broker", "-y")
        gh_run.assert_not_called()

    def test_upload_to_existing_release_is_idempotent(self):
        self.tag_and_push()
        self.build_bundle(self.head[:7])
        (self.root / "dist" / "setup.exe").write_bytes(b"x")
        (self.root / "setup" / "bin").mkdir(parents=True)
        (self.root / "setup" / "bin" / "setup.exe").write_bytes(b"y")       # duplicate name: dist wins
        (self.root / "setup" / "bin" / "setup-linux-amd64").write_bytes(b"z")
        with mock.patch("mmqlib.github.require_gh"), \
                mock.patch("mmqlib.github.release_exists", return_value=True), \
                mock.patch("mmqlib.github.run") as gh_run:
            self.assertEqual(self.publish("--github", "-y"), 0)
        upload = gh_run.call_args_list[0][0][0]
        self.assertEqual(upload[:4], ["gh", "release", "upload", "v1.2.3"])
        self.assertIn("--clobber", upload)
        files = [Path(f).relative_to(self.root).as_posix() for f in upload[4:-1]]
        self.assertEqual(files, ["dist/monstermq-broker-1.2.3.zip", "dist/setup.exe", "setup/bin/setup-linux-amd64"])

    def test_docker_checked_before_github_upload(self):
        self.tag_and_push()
        self.build_bundle(self.head[:7])
        (self.root / "dist" / "setup.exe").write_bytes(b"x")
        with mock.patch("mmqlib.github.require_gh"), \
                mock.patch("mmqlib.docker.which", return_value=None), \
                mock.patch("mmqlib.github.run") as gh_run:
            with self.assertRaisesRegex(Exception, "'docker' not found"):
                self.publish("--all", "-y")
        gh_run.assert_not_called()

    def test_confirmation_needs_terminal(self):
        self.tag_and_push()
        self.build_bundle(self.head[:7])
        with mock.patch("mmqlib.github.require_gh"), mock.patch("mmqlib.github.run") as gh_run, \
                mock.patch("sys.stdin", io.StringIO()):
            with self.assertRaisesRegex(Exception, "no terminal"):
                self.publish("--broker")
        gh_run.assert_not_called()


class RunArgsTest(unittest.TestCase):
    def test_parse(self):
        opts, rest = run.parse(["-b", "-nk", "-cluster", "-config", "x.yaml", "--", "-b", "-help"])
        self.assertTrue(opts["build"] and opts["nokill"])
        self.assertFalse(opts["help"])
        self.assertEqual(rest, ["-cluster", "-config", "x.yaml", "-b", "-help"])


if __name__ == "__main__":
    unittest.main()
