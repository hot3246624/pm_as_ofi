# Xuan Frontier Private Repo Migration - 2026-06-10

## Decision

This workspace is currently a linked worktree of `pm_as_ofi`:

- Workspace: `/Users/hot/web3Scientist/pm_as_ofi-xuan-frontier`
- Git dir: `/Users/hot/web3Scientist/pm_as_ofi/.git/worktrees/pm_as_ofi-xuan-frontier`
- Current remote: `https://github.com/hot3246624/pm_as_ofi.git`

To make it an independent private GitHub project, prefer a clean snapshot migration:

1. Export current `HEAD` with `git archive`.
2. Initialize a new standalone repo from that snapshot.
3. Create a brand-new private GitHub repo.
4. Push the standalone repo as `main`.

This avoids making the new GitHub project a branch or fork of `pm_as_ofi`.

## Current Blocker

`gh auth status` currently reports the `hot3246624` token is invalid. Remote GitHub repo creation cannot be completed until GitHub CLI is re-authenticated or an empty private repo URL is provided.

Required user-side action:

```bash
gh auth login -h github.com
```

Alternative: create an empty private GitHub repo manually and provide its URL.

## Recommended Repo Name

Recommended private repo name:

```text
xuan-frontier
```

## Migration Commands After GitHub Auth Works

Run from a shell with write access to `/Users/hot/web3Scientist`:

```bash
set -euo pipefail

SRC=/Users/hot/web3Scientist/pm_as_ofi-xuan-frontier
DST=/Users/hot/web3Scientist/xuan-frontier

test ! -e "$DST"
mkdir -p "$DST"

cd "$SRC"
git archive --format=tar HEAD | tar -x -C "$DST"

cd "$DST"
git init -b main
git add .
git add -f Cargo.lock
git commit -m "Initial xuan frontier private snapshot"

gh repo create hot3246624/xuan-frontier --private --source=. --remote=origin --push
```

After this, the new repo is independent. It will not be a branch of `pm_as_ofi`, and it will not use `/Users/hot/web3Scientist/pm_as_ofi/.git/worktrees/...`.

## What This Snapshot Includes

The snapshot includes tracked files at current `HEAD`, including:

- S9Q guarded execute switch source.
- S9Q fixture-source reconciliation hard gate.
- S8/S9 native runtime and adapter source as committed.
- `Cargo.lock` for reproducible standalone Rust builds.
- `scripts/b_strategy_storage_retention.sh` for B-owned remote staging retention.
- This handoff/migration documentation if committed before migration.

The snapshot intentionally excludes untracked runtime artifacts such as `.tmp_xuan/`, raw local caches, and ignored build outputs. Persist required artifact hashes in documentation instead of committing large temporary files.

## Post-Migration Checks

In the new standalone repo:

```bash
git remote -v
git status --short
python3 -m py_compile scripts/b_strategy_canary_s8a_one_run_orchestrator.py
cargo fmt --check
cargo test s8a_order_adapter --lib
cargo test btc_completion_controller --lib
```

Expected current test state:

- `s8a_order_adapter`: 37 passed
- `btc_completion_controller`: 47 passed
