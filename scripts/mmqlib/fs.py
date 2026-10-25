"""File system helpers that behave the same on Windows, macOS and Linux."""

import os
import shutil
import stat
import zipfile
from pathlib import Path
from typing import Iterable, Optional


def _on_rm_error(func, path, _exc):
    # Windows refuses to delete read-only files (e.g. inside .git or node_modules); clear the flag and retry.
    os.chmod(path, stat.S_IWRITE)
    func(path)


def rmtree(path: Path) -> None:
    """Remove a directory tree (or file / symlink) if it exists."""
    path = Path(path)
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path, onerror=_on_rm_error)


def remove(path: Path) -> None:
    """Remove a file if it exists."""
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


def rmdir_if_empty(path: Path) -> None:
    try:
        Path(path).rmdir()
    except OSError:
        pass


def copytree(src: Path, dst: Path) -> None:
    """Copy src to dst (dst may exist; existing files are overwritten)."""
    shutil.copytree(src, dst, dirs_exist_ok=True)


def write_text(path: Path, text: str, newline: str = "\n", executable: bool = False) -> None:
    """Write text with fixed line endings (independent of the OS running the build)."""
    if newline != "\n":
        text = text.replace("\n", newline)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:  # newline="": write verbatim
        f.write(text)
    if executable:
        make_executable(path)


def make_executable(path: Path) -> None:
    mode = os.stat(path).st_mode
    os.chmod(path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def zip_dir(src_dir: Path, zip_path: Path, executables: Iterable[str] = ()) -> None:
    """Zip src_dir so entries start with '<src_dir name>/' (like `cd parent && zip -r x.zip name`).

    Directory entries are included (so empty dirs survive) and Unix permissions are
    stored explicitly: 0755 for directories and for names in `executables`
    (paths relative to src_dir, forward slashes), 0644 otherwise. The result is the
    same no matter which OS builds it.
    """
    src_dir = Path(src_dir)
    execs = set(executables)
    base = src_dir.name
    tmp = zip_path.with_name(zip_path.name + ".tmp")
    remove(tmp)
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        def add_dir(rel: str) -> None:
            info = zipfile.ZipInfo(f"{base}/{rel}/" if rel else f"{base}/")
            info.external_attr = (0o40755 << 16) | 0x10
            zf.writestr(info, b"")

        add_dir("")
        for root, dirs, files in os.walk(src_dir):
            dirs.sort()
            rel_root = Path(root).relative_to(src_dir).as_posix()
            rel_root = "" if rel_root == "." else rel_root
            for d in dirs:
                add_dir(f"{rel_root}/{d}" if rel_root else d)
            for name in sorted(files):
                rel = f"{rel_root}/{name}" if rel_root else name
                full = Path(root) / name
                info = zipfile.ZipInfo.from_file(full, f"{base}/{rel}")
                mode = 0o755 if rel in execs else 0o644
                info.external_attr = (0o100000 | mode) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                with open(full, "rb") as fsrc, zf.open(info, "w") as fdst:
                    shutil.copyfileobj(fsrc, fdst, 1024 * 1024)
    os.replace(tmp, zip_path)


def read_zip_member(zip_path: Path, name: str) -> Optional[bytes]:
    try:
        with zipfile.ZipFile(zip_path) as zf:
            return zf.read(name)
    except (KeyError, zipfile.BadZipFile, FileNotFoundError):
        return None


__all__ = ["rmtree", "remove", "rmdir_if_empty", "copytree", "write_text", "make_executable",
           "zip_dir", "read_zip_member"]
