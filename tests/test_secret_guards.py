"""Guards against committing credentials: the Claude Code hook and the git pre-commit hook."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys

import pytest

from mnp.config import PROJECT_ROOT

CLAUDE_HOOK = PROJECT_ROOT / ".claude" / "hooks" / "block_broad_git_add.py"
PRE_COMMIT = PROJECT_ROOT / ".githooks" / "pre-commit"
# Fake values shaped like real keys; never real credentials.
FAKE_JEV = "ap_" + "x" * 30
FAKE_FINNHUB = "da1" + "y" * 20


def _load_claude_hook():
    spec = importlib.util.spec_from_file_location("block_broad_git_add", CLAUDE_HOOK)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "command",
    [
        "git add -A",
        "git add --all",
        "git add .",
        "git add -u",
        "git add -Av",
        "git add -- .",
        "cd repo && git add -A && git commit -m x",
        "git -C /some/repo add --all",
        "git commit -a -m 'msg'",
        "git commit -am 'msg'",
        "git commit --all",
    ],
)
def test_claude_hook_blocks_broad_staging(command):
    assert _load_claude_hook().check(command)


@pytest.mark.parametrize(
    "command",
    [
        "git add src/mnp/cli.py tests/test_cli.py",
        "git status --short",
        "git commit -m 'avoid git add -A'",
        "git commit -m '-a is a flag'",
        "git commit -q -F - <<'EOF'\nNever run `git add -A` here\nEOF",
        "git log --all --oneline",
        "uv run pytest -q",
        "echo 'git add -A'",
    ],
)
def test_claude_hook_allows_explicit_staging(command):
    assert _load_claude_hook().check(command) is None


def test_claude_hook_protocol():
    def run(command: str) -> subprocess.CompletedProcess:
        payload = json.dumps({"tool_name": "Bash", "tool_input": {"command": command}})
        return subprocess.run(
            [sys.executable, str(CLAUDE_HOOK)], input=payload, capture_output=True, text=True
        )

    blocked = run("git add -A")
    assert blocked.returncode == 2  # exit 2 = block, stderr goes to Claude
    assert "stage explicit paths" in blocked.stderr
    assert run("git add README.md").returncode == 0
    malformed = subprocess.run([sys.executable, str(CLAUDE_HOOK)], input="not json", text=True)
    assert malformed.returncode == 0  # never block on a payload it can't read


def test_claude_settings_register_the_hook():
    settings = json.loads((PROJECT_ROOT / ".claude" / "settings.json").read_text())
    [entry] = settings["hooks"]["PreToolUse"]
    assert entry["matcher"] == "Bash"
    assert "block_broad_git_add.py" in entry["hooks"][0]["command"]


@pytest.fixture
def repo(tmp_path):
    """A throwaway git repo using this project's pre-commit hook and gitleaks config."""
    (tmp_path / ".githooks").mkdir()
    shutil.copy(PRE_COMMIT, tmp_path / ".githooks" / "pre-commit")
    shutil.copy(PROJECT_ROOT / ".gitleaks.toml", tmp_path / ".gitleaks.toml")
    env = os.environ | {
        "MNP_SKIP_GITLEAKS": "1",  # checks 1-2 are ours; gitleaks may not be installed in CI
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }

    def git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["git", *args], cwd=tmp_path, env=env, capture_output=True, text=True)

    git("init", "-q")
    git("config", "core.hooksPath", ".githooks")
    git("add", ".githooks/pre-commit", ".gitleaks.toml")
    assert git("commit", "-q", "-m", "init").returncode == 0

    def commit(name: str, data: bytes) -> subprocess.CompletedProcess:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        git("add", "-f", name)  # -f: stage even if a .gitignore would hide it
        return git("commit", "-q", "-m", f"add {name}")

    return commit


@pytest.mark.parametrize(
    ("name", "data", "reason"),
    [
        (".env", b"JEV_API_KEY=\n", ".env file"),
        (".env.local", b"X=1\n", ".env variant"),
        (".env.swp", b"b0VIM 9.1\x00\x00\x00", "vim swap file"),
        ("config/.sources.yaml.swp", b"\x00", "vim swap file"),
        ("notes.txt~", b"old", "editor backup file"),
        ("deploy/server.pem", b"-----BEGIN", "private key"),
        # binary content (like a swap file) hiding a key: gitleaks skips binaries
        ("blob.bin", b'\x00\x01JEV_API_KEY="' + FAKE_JEV.encode() + b'"\x00', "Jev/Finnhub"),
        ("src/settings.py", f'FINNHUB_API_KEY = "{FAKE_FINNHUB}"\n'.encode(), "Jev/Finnhub"),
        ("config.yaml", f"typesafe_token: {FAKE_JEV}\n".encode(), "Jev/Finnhub"),
    ],
)
def test_pre_commit_blocks_credentials(repo, name, data, reason):
    result = repo(name, data)
    assert result.returncode != 0
    assert "Commit blocked" in result.stderr
    assert reason in result.stderr


@pytest.mark.parametrize(
    ("name", "data"),
    [
        (".env.example", b"JEV_API_KEY=\nFINNHUB_API_KEY=\n"),
        ("src/config.py", b"jev_api_key: SecretStr | None = None\n"),
        ("README.md", b"Set `FINNHUB_API_KEY` in .env.\n"),
        ("src/app.py", b"print('hello')\n"),
    ],
)
def test_pre_commit_allows_normal_files(repo, name, data):
    result = repo(name, data)
    assert result.returncode == 0, result.stderr


def test_repo_tree_is_clean():
    """No tracked file in this repository fails the name or provider-key checks."""
    env = os.environ | {"MNP_SKIP_GITLEAKS": "1"}
    result = subprocess.run(
        [sys.executable, str(PRE_COMMIT), "--tracked"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
