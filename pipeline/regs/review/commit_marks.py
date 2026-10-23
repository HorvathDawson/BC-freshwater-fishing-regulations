"""COMMIT MARKS (T135, curation-review PLAN §4 phase 4, §5): put the reviewer's marks into git — the
marks store and NOTHING ELSE.

    python -m pipeline.regs.review.commit_marks [-m "review: region 1 P4"] [--dry-run]

The store (`data/curated/regulations/review/marks.jsonl`) is append-only, so the file is the
history; git holds it after each commit. The commit is `git commit --only -m <msg> -- <store>`:
whatever else is staged or edited in the tree stays exactly as it was (staged files stay staged and
uncommitted), so a reviewer's commit never sweeps up someone else's work. The review screen prints
this command; it never runs it (review mode writes one thing, a mark).
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          check=check)


def added_marks(repo: Path, store: Path) -> int:
    """How many lines the store gained since HEAD (a new store: all of them)."""
    rel = str(store.resolve().relative_to(repo.resolve()))
    got = _git(repo, "diff", "--numstat", "HEAD", "--", rel, check=False)
    if got.returncode == 0 and got.stdout.strip():
        return int(got.stdout.split()[0])
    tracked = _git(repo, "ls-files", "--error-unmatch", "--", rel, check=False).returncode == 0
    if tracked:
        return 0
    return sum(1 for ln in store.read_text(encoding="utf-8").splitlines() if ln.strip())


def commit_marks(repo: Path, store: Path, message: str | None = None, *,
                 dry_run: bool = False) -> dict:
    """Commit the store alone. `{committed, added, message, command}`; nothing to commit when the
    store is as HEAD has it. Raises ValueError for a store outside the repo or not valid marks."""
    from pipeline.regs.review.marks import load
    repo, store = Path(repo), Path(store)
    try:
        rel = str(store.resolve().relative_to(repo.resolve()))
    except ValueError:
        raise ValueError(f"{store} is not inside the repository {repo}") from None
    load(store)                                   # a store that does not load is never committed
    n = added_marks(repo, store)
    msg = message or f"review: {n} mark{'s' if n != 1 else ''}"
    cmd = ["git", "commit", "--only", "-m", msg, "--", rel]
    if n == 0:
        return {"committed": False, "added": 0, "message": msg, "command": cmd}
    if dry_run:
        return {"committed": False, "added": n, "message": msg, "command": cmd}
    _git(repo, "add", "--", rel)
    _git(repo, *cmd[1:])
    return {"committed": True, "added": n, "message": msg, "command": cmd}


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.commit_marks",
                                 description="commit the review marks store, and nothing else")
    ap.add_argument("-m", "--message")
    ap.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    repo = Path(_git(Path(a.marks).resolve().parent, "rev-parse", "--show-toplevel").stdout.strip())
    got = commit_marks(repo, a.marks, a.message, dry_run=a.dry_run)
    if not got["added"]:
        print("nothing to commit: the marks store is as HEAD has it")
    elif got["committed"]:
        print(f"committed {got['added']} mark(s): {got['message']}")
    else:
        print("would run: " + " ".join(got["command"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
