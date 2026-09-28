# Keeping secrets out of git

API keys live only in `.env`, which git ignores. On 2026-09-27 a vim swap file of `.env`
(`.env.swp`) was committed by a broad `git add -A` and pushed to this public repo, exposing the
Jev and Finnhub keys; they were rotated. Three layers now guard against a repeat.

## 1. Claude Code hook

`.claude/settings.json` runs `.claude/hooks/block_broad_git_add.py` before every shell command
Claude Code runs in this project. It refuses commands that stage everything (`git add -A`,
`--all`, `.`, `-u`, `git commit -a`) and tells Claude to stage explicit paths. It only affects
Claude Code sessions, not your own terminal. Review or disable it with `/hooks` in Claude Code.

## 2. Git pre-commit hook

`.githooks/pre-commit` checks staged files before every commit and blocks:

- `.env` and variants (`.env.local`, …; `.env.example` is allowed), editor swap and backup
  files (`*.swp`, `*~`), private keys (`*.pem`, `id_rsa`, …);
- Jev/TypeSafe/Finnhub key assignments anywhere in a file, including binary files such as swap
  files, which gitleaks skips;
- anything [gitleaks](https://github.com/gitleaks/gitleaks) finds with `.gitleaks.toml` (its
  default rules plus this project's).

Enable it once per clone, and install gitleaks (without it the first two checks still run):

```bash
git config core.hooksPath .githooks
```

If it blocks a commit, unstage the file with `git restore --staged <file>`. `git commit
--no-verify` skips the hook; never use it to commit a real secret.

## 3. CI

The `secrets` job in `.github/workflows/ci.yml` runs the same checks on every pull request and
push to `main`, over every tracked file and all of git history, so a commit made without the
local hook still can't merge.

## If a secret is committed anyway

1. **Rotate it immediately** at the provider. Assume anything pushed to a public repo is
   already copied; removing it from history does not un-leak it.
2. Remove the file from the repo and add a guard (a `.gitignore` entry or a hook rule) so it
   can't happen again.
3. Optionally rewrite history to purge it (needs a force-push to `main`; coordinate first).
