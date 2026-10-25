"""Running external commands the same way on Windows, macOS and Linux."""

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Mapping, Optional, Sequence, Union

from . import ui

PathLike = Union[str, Path]


class CommandError(Exception):
    def __init__(self, cmd: Sequence[str], returncode: int, output: str = ""):
        self.cmd = list(cmd)
        self.returncode = returncode
        self.output = output.strip()
        msg = f"command failed (exit {returncode}): {' '.join(self.cmd)}"
        if self.output:
            msg += f"\n{self.output}"
        super().__init__(msg)


def _quote(arg: str) -> str:
    """Quote an argument for display only (commands are never run through a shell)."""
    return f'"{arg}"' if (not arg or any(ch in arg for ch in ' 	"')) else arg


def which(name: str) -> Optional[str]:
    """Resolve a tool on PATH, including Windows wrappers like npm.cmd or mvn.cmd."""
    return shutil.which(name)


def require(name: str) -> str:
    path = which(name)
    if path is None:
        raise CommandError([name], 127, f"'{name}' not found on PATH")
    return path


def run(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None, echo: bool = True,
        check: bool = True, env: Optional[Mapping[str, str]] = None) -> int:
    """Run a command with output streaming to the console. Never uses a shell.

    `env` adds/overrides variables on top of the current environment.
    """
    args: List[str] = [str(c) for c in cmd]
    exe = which(args[0])
    if exe:
        args[0] = exe
    if echo:
        ui.detail("$ " + " ".join(_quote(str(c)) for c in cmd))
    full_env = None
    if env:
        full_env = dict(os.environ)
        full_env.update(env)
    sys.stdout.flush()  # keep our output in order with the child's when redirected
    sys.stderr.flush()
    proc = subprocess.run(args, cwd=cwd, env=full_env)
    if check and proc.returncode != 0:
        raise CommandError(args, proc.returncode)
    return proc.returncode


def succeeds(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None) -> bool:
    """True if the command exists and exits 0; output is discarded."""
    args = [str(c) for c in cmd]
    exe = which(args[0])
    if exe is None:
        return False
    args[0] = exe
    try:
        proc = subprocess.run(args, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        return False
    return proc.returncode == 0


def capture(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None, check: bool = True) -> str:
    """Run a command and return its stdout (stripped)."""
    args = [str(c) for c in cmd]
    exe = which(args[0])
    if exe:
        args[0] = exe
    proc = subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                          encoding="utf-8", errors="replace")
    if check and proc.returncode != 0:
        raise CommandError(args, proc.returncode, proc.stderr or proc.stdout)
    return proc.stdout.strip()
