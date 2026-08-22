"""Drop stale bare-item_id entries that the per-row re-parse has SUPERSEDED.

Before the per-row fix, several synopsis rows for one waterbody collided on `entry_id == item_id` and
only one survived (dropping the other reaches). The fix gives each row its own `item_id#<reach>` entry.
After re-parsing + ingesting those, the OLD bare `item_id` entry is a stale partial — this removes it,
but ONLY when its per-row replacements are present (so an incomplete re-parse never deletes content),
and NEVER a `locked` (human-confirmed) entry.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.prune_superseded            # apply
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.prune_superseded --dry-run  # preview
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.parsing import io


def _entries_dir() -> Path:
    return io.entries_dir()


def prune(entries_dir: Path, dry_run: bool = False) -> dict:
    report: dict[str, list] = {"removed": [], "kept_locked": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)                         # {entry_id: entry_dict}
        # bases that now have per-row replacements (entry_id "item#reach")
        superseded_bases = {eid.split("#", 1)[0] for eid in by_id if "#" in eid}
        kept: dict[str, dict] = {}
        changed = False
        for eid, e in by_id.items():
            if "#" not in eid and eid in superseded_bases:      # a bare id with per-row replacements
                if e.get("locked"):
                    report["kept_locked"].append(eid)
                    kept[eid] = e
                    continue
                report["removed"].append(eid)
                changed = True
                continue
            kept[eid] = e
        if changed and not dry_run:
            io.write_entryfile(path, region, kept.values())     # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Remove stale bare-item_id entries superseded by per-row entries.")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()
    entries_dir = Path(args.entries_dir) if args.entries_dir else _entries_dir()
    report = prune(entries_dir, dry_run=args.dry_run)
    print(f"{'[dry-run] would remove' if args.dry_run else 'removed'}: {len(report['removed'])} stale entry(ies)")
    for eid in report["removed"]:
        print(f"    - {eid}")
    if report["kept_locked"]:
        print(f"  kept {len(report['kept_locked'])} locked entry(ies) (superseded but human-confirmed): "
              f"{report['kept_locked']}")


if __name__ == "__main__":
    main()
