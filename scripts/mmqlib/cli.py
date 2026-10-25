"""One entry point per repository: `make.py <command> [options]`."""

import sys
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from . import ui
from .proc import CommandError


@dataclass
class Command:
    help: str
    run: Callable[[List[str]], Optional[int]]


def _usage(prog: str, title: str, commands: Dict[str, Command]) -> str:
    width = max(len(name) for name in commands)
    lines = [f"{title}", "", f"usage: python {prog} <command> [options]", "", "commands:"]
    lines += [f"  {name.ljust(width)}  {cmd.help}" for name, cmd in commands.items()]
    lines += ["", f"Run 'python {prog} <command> --help' for the options of a command."]
    return "\n".join(lines)


def run_cli(prog: str, title: str, commands: Dict[str, Command], argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(_usage(prog, title, commands))
        return 0 if argv else 2
    name, rest = argv[0], argv[1:]
    cmd = commands.get(name)
    if cmd is None:
        ui.error(f"unknown command '{name}'")
        print(_usage(prog, title, commands))
        return 2
    try:
        return cmd.run(rest) or 0
    except KeyboardInterrupt:
        ui.error("interrupted")
        return 130
    except (CommandError, ui.Abort) as e:
        ui.error(str(e))
        return 1
