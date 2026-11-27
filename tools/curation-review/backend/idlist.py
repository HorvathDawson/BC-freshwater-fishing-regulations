"""ONLY THESE ENTRIES (T414, in T239): the review list the yearly synopsis diff writes
(`python -m pipeline.tools.synopsis_diff OLD NEW --ids-out FILE`, one entry id per line) limits the
entry queue to those ids. The screen opens it as `/?ids=<file>` (a path under the repo, or absolute
inside it); the hash stays the open entry.

    read(path)          the file's ids, in order — blank lines and `#` comments skipped, a repeat
                        kept once (and counted)
    view(path, rows)    the ids, the progress over the list, and every id that matches NO entry —
                        shown, never dropped (owner 2026-11-03: no silent fallbacks)

Read only.
"""
from __future__ import annotations

from pathlib import Path


class Refused(ValueError):
    """The list cannot be read: said to the reviewer, never an empty list."""


def resolve(path: str, root: Path) -> Path:
    """`path` relative to the repo root (or absolute), and it must stay inside the repo."""
    if not path or not path.strip():
        raise Refused("no ids file named (?ids=<file>)")
    root = root.resolve()
    p = Path(path.strip()).expanduser()
    p = (p if p.is_absolute() else root / p).resolve()
    if p != root and root not in p.parents:
        raise Refused(f"{path}: the ids file must be inside the repo ({root})")
    if not p.is_file():
        raise Refused(f"{path}: no such file")
    return p


def read(path: str, root: Path) -> tuple[Path, list[str], int]:
    """(the file, its ids in order, how many repeats were dropped)."""
    p = resolve(path, root)
    try:
        text = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as ex:
        raise Refused(f"{path}: cannot be read ({ex})") from ex
    ids: list[str] = []
    seen: set[str] = set()
    repeats = 0
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s in seen:
            repeats += 1
            continue
        seen.add(s)
        ids.append(s)
    return p, ids, repeats


def view(path: str, root: Path, rows: list[dict], done: tuple[str, ...]) -> dict:
    """The list against the queue's rows (unfiltered): `reviewed` counts a mark in force (`done`:
    verified, wrong, unsure, deferred — a mark gone stale is to do again), `verified` the
    human-reviewed ones; `missing` every id no entry has, in the file's order."""
    p, ids, repeats = read(path, root)
    by_id = {r["entry_id"]: r for r in rows}
    found = [i for i in ids if i in by_id]
    try:
        shown = str(p.relative_to(root.resolve()))
    except ValueError:
        shown = str(p)
    return {
        "path": shown,
        "ids": ids,
        "total": len(ids),
        "found": len(found),
        "reviewed": sum(1 for i in found if by_id[i]["verify"] in done),
        "verified": sum(1 for i in found if by_id[i]["verify"] == "verified"),
        "missing": [i for i in ids if i not in by_id],
        "repeats": repeats,
    }
