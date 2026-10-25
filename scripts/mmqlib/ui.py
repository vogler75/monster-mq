"""Console output and prompts."""

import os
import sys


class Abort(Exception):
    """Raised when the user declines or a prompt cannot be answered."""


def _setup_stream(stream):
    # Never crash on characters the console encoding can't show (e.g. cp1252 when piped).
    try:
        stream.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass


_setup_stream(sys.stdout)
_setup_stream(sys.stderr)


def _color_enabled() -> bool:
    if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
        return False
    if os.name == "nt":
        os.system("")  # enables ANSI escape processing in the Windows console
    return True


_COLOR = _color_enabled()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOR else text


def bold(text: str) -> str:
    return _c("1", text)


def header(text: str) -> None:
    print(_c("1;32", f"=== {text} ==="))


def info(text: str) -> None:
    print(text)


def step(text: str) -> None:
    print(_c("1;33", text))


def ok(text: str) -> None:
    print(_c("32", f"[ok] {text}"))


def warn(text: str) -> None:
    print(_c("33", f"[warn] {text}"))


def error(text: str) -> None:
    print(_c("31", f"[error] {text}"), file=sys.stderr)


def detail(text: str) -> None:
    print(_c("2", f"  {text}"))


def confirm(question: str, assume_yes: bool = False) -> None:
    """Ask a y/n question; raise Abort on 'no'.

    Without a terminal (e.g. run from mbp or CI) there is nobody to answer, so
    this fails instead of hanging unless assume_yes is set.
    """
    if assume_yes:
        return
    if not sys.stdin.isatty():
        raise Abort("no terminal to confirm on; re-run with -y to proceed non-interactively")
    answer = _input(f"{question} [y/N] ")
    if answer not in ("y", "yes"):
        raise Abort("cancelled by user")


def ask(question: str) -> bool:
    """Ask a y/n question and return the answer; raise Abort if there is no terminal."""
    if not sys.stdin.isatty():
        raise Abort(f"no terminal to answer '{question}'; pass the flag that answers it (see --help)")
    return _input(f"{question} [y/N] ") in ("y", "yes")


def _input(prompt: str) -> str:
    # On Windows isatty() is also true for NUL (e.g. '< /dev/null'), so EOF must abort as well.
    try:
        return input(prompt).strip().lower()
    except EOFError:
        print()
        raise Abort("no answer on stdin; re-run with -y (or the flag that answers the question)")
