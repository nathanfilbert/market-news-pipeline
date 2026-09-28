#!/usr/bin/env python3
"""Claude Code PreToolUse hook: refuse broad git staging.

`git add -A` once committed a vim swap file of .env (with API keys) to this public repo.
This blocks the commands that stage everything, so files are always staged by explicit path:

  git add -A / --all / --no-ignore-removal / -u / --update / . / :/ / *
  git commit -a / --all (including combined flags like -am)

Reads the hook payload (JSON) on stdin. Exit 2 blocks the command and shows stderr to Claude;
exit 0 allows it. Heredoc bodies (e.g. commit messages that mention these commands) are ignored.
"""

import json
import re
import shlex
import sys

BROAD_ADD_ARGS = {"-A", "--all", "--no-ignore-removal", "-u", "--update", ".", "./", ":/", "*"}
# Options whose next token is a value, not a flag (so `-m "-a thing"` isn't read as -a).
VALUE_OPTIONS = {
    "-m", "--message", "-F", "--file", "-C", "-c", "--reuse-message", "--reedit-message",
    "--author", "--date", "--fixup", "--squash", "--cleanup", "-t", "--template",
    "--pathspec-from-file", "--trailer",
}  # fmt: skip
GIT_GLOBAL_VALUE_OPTIONS = {"-C", "-c", "--git-dir", "--work-tree", "--namespace"}
OPERATORS = {"&&", "||", ";", "|", "&", "(", ")", ";;", "\n"}
HEREDOC = re.compile(r"<<-?\s*(['\"]?)(\w+)\1[^\n]*\n.*?^\s*\2\s*$", re.S | re.M)

MESSAGE = (
    "Blocked: {what} stages files wholesale. In this repo, stage explicit paths instead "
    "(e.g. `git add src/mnp/foo.py tests/test_foo.py`), then check `git status` before "
    "committing. A broad `git add -A` once committed a .env swap file with API keys."
)


def _segments(command: str) -> list[list[str]]:
    command = HEREDOC.sub("", command)
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    segments, current = [], []
    for token in lexer:
        if token in OPERATORS:
            if current:
                segments.append(current)
            current = []
        else:
            current.append(token)
    if current:
        segments.append(current)
    return segments


def _violation(tokens: list[str]) -> str | None:
    if "git" not in tokens:
        return None
    i = tokens.index("git") + 1
    while i < len(tokens) and tokens[i].startswith("-"):  # git global options
        i += 2 if tokens[i] in GIT_GLOBAL_VALUE_OPTIONS else 1
    if i >= len(tokens):
        return None
    sub, args = tokens[i], tokens[i + 1 :]

    j = 0
    while j < len(args):
        arg = args[j]
        if arg == "--":
            rest = args[j + 1 :]
            if sub == "add" and any(a in BROAD_ADD_ARGS for a in rest):
                return f"`git add -- {' '.join(rest)}`"
            break
        if arg in VALUE_OPTIONS:
            j += 2
            continue
        if sub == "add":
            short = arg.startswith("-") and not arg.startswith("--")
            if arg in BROAD_ADD_ARGS or (short and ("A" in arg or "u" in arg)):
                return f"`git add {arg}`"
        if sub == "commit":
            short = arg.startswith("-") and not arg.startswith("--")
            if arg == "--all" or (short and "a" in arg.split("=")[0]):
                return f"`git commit {arg}`"
        j += 1
    return None


def check(command: str) -> str | None:
    """A description of the broad staging command in `command`, or None if it's fine."""
    try:
        segments = _segments(command)
    except ValueError:  # unbalanced quotes: fall back to a plain pattern check
        m = re.search(
            r"\bgit\s+(add\s+(-A|--all|-u|\.)(\s|$)|commit\s+(-\w*a\w*|--all)\b)", command
        )
        return f"`{m.group(0).strip()}`" if m else None
    for tokens in segments:
        if found := _violation(tokens):
            return found
    return None


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0
    command = (payload.get("tool_input") or {}).get("command") or ""
    if found := check(command):
        print(MESSAGE.format(what=found), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
