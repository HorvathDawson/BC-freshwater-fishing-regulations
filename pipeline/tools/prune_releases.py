"""PRUNE OLD RELEASES (T72, audit N10). Dry run by default: it prints what it keeps and what it would remove.

    python -m pipeline.tools.prune_releases            # the list, nothing removed
    python -m pipeline.tools.prune_releases --yes      # remove them

Kept: the release `LATEST` names, the one `PUBLISHED` names (what R2 serves), the newest other release (one to go
back to), and any `--keep <id>`. Everything else `r-*` under data/release goes. `tiles/` and the pointers are never
touched. A release is a cut of the pipeline's outputs, so a removed one is rebuilt, not restored.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable, List, Set, Tuple

from pipeline.common.curated import GENERATED
from pipeline.deliver.release.manifest import LATEST, latest, releases


def _named(root: Path, name: str) -> Set[str]:
    f = root / name
    return {f.read_text().strip()} if f.is_file() else set()


def plan(root: Path, keep: Iterable[str] = ()) -> Tuple[List[Path], List[Path]]:
    """(kept, removed) release folders under `root`, newest first. A folder with no manifest is not a release and is
    left alone."""
    dirs = releases(root)
    cur = latest(root)
    named = _named(root, "PUBLISHED") | set(keep) | ({cur.name} if cur else set())
    previous = next((d.name for d in dirs if d.name not in named), None)
    held = named | ({previous} if previous else set())
    return [d for d in dirs if d.name in held], [d for d in dirs if d.name not in held]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", type=Path, default=None, help="default: data/release")
    ap.add_argument("--keep", action="append", default=[], help="a release id to keep as well (repeatable)")
    ap.add_argument("--yes", action="store_true", help="remove (without it, print the list only)")
    a = ap.parse_args(argv)
    root = a.root or GENERATED.release
    kept, gone = plan(root, a.keep)
    if not (root / LATEST).is_file():
        print(f"note: no {root / LATEST} — the newest release by mtime stands in for it")
    for d in kept:
        print(f"keep    {d.name}")
    for d in gone:
        print(f"{'remove ' if a.yes else 'would remove'} {d.name}")
        if a.yes:
            shutil.rmtree(d)
    print(f"{len(kept)} kept, {len(gone)} {'removed' if a.yes else 'to remove — add --yes'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
