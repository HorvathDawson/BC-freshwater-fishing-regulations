"""Drop entries whose REGISTRY ITEM changed, so the next parse re-creates them under the right id.

`prune_superseded` handles one shape of staleness (a bare `item_id` entry replaced by per-row
`item_id#reach` entries). This handles the other: a fix that moves a row onto a DIFFERENT registry
item changes its `entry_id`, so the export writes a new entry and the old one lingers, still showing
the wrong water in the review queue. That happened when the Fraser overrides stopped resolving onto
Annacis Channel (`gnis:10494#fraser_river…` -> `gnis:39325#fraser_river…`) and when a curated-named
side channel got its own item (`gnis:39492#mcarthur_island_slough` -> `blk:355994157`).

Stale = on disk, and NOT an entry_id the current registry + overrides would produce. The entry ids are
recomputed with the exporter's own logic so this can never disagree with what a parse will write.

NEVER removes a `locked` (human-confirmed) entry — those are reported instead, so a curator can move
the content by hand rather than lose it.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.prune_remapped --dry-run   # preview
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.parsing.prune_remapped             # apply
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.matching.matcher import load_overrides
from pipeline.parsing import io
from pipeline.registry import default_registry_path, load_registry
from pipeline.curated import CURATED, SOURCE


def current_entry_ids(registry_path: str | Path, overrides_path: str | Path | None) -> dict[str, str]:
    """{entry_id: water name} the exporter would produce right now, via `batch_exporter.export`
    itself — writing its batches to a throwaway dir so the ids can never drift from a real parse."""
    import tempfile

    from pipeline.parsing.batch_exporter import export
    from pipeline.parsing.rows import load_synopsis_rows

    registry = load_registry(str(registry_path))
    rows = load_synopsis_rows()
    with tempfile.TemporaryDirectory() as tmp:
        export(rows, registry, Path(tmp), batch_size=500, overrides=load_overrides(overrides_path),
               existing_ids=set(), force=True)
        out: dict[str, str] = {}
        for item in io.load_all_batch_items(Path(tmp) / "batches").values():
            out[item["entry_id"]] = item.get("name", "")
    return out


def prune(entries_dir: Path, live_ids: dict[str, str], dry_run: bool = False) -> dict:
    report: dict[str, list] = {"removed": [], "kept_locked": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        kept: dict[str, dict] = {}
        changed = False
        for eid, e in by_id.items():
            if eid in live_ids or e.get("registry_status") == "no_registry":
                kept[eid] = e                                   # still produced / not registry-keyed
                continue
            if e.get("locked"):
                report["kept_locked"].append((region, eid, e.get("identity", {}).get("name", "")))
                kept[eid] = e
                continue
            report["removed"].append((region, eid, e.get("identity", {}).get("name", "")))
            changed = True
        if changed and not dry_run:
            io.write_entryfile(path, region, kept.values())      # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Remove entries whose registry item changed (re-parse them).")
    ap.add_argument("--registry", help=f"registry.json (default: {default_registry_path()})")
    ap.add_argument("--overrides", default=str(CURATED.regulations.overrides))
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()

    live = current_entry_ids(args.registry or default_registry_path(), args.overrides)
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = prune(entries_dir, live, dry_run=args.dry_run)

    verb = "[dry-run] would remove" if args.dry_run else "removed"
    print(f"{verb}: {len(report['removed'])} remapped entry(ies)")
    for region, eid, name in report["removed"]:
        print(f"    - [{region}] {eid}   {name}")
    if report["kept_locked"]:
        print(f"  KEPT {len(report['kept_locked'])} LOCKED entry(ies) — curator must move these by hand:")
        for region, eid, name in report["kept_locked"]:
            print(f"    ! [{region}] {eid}   {name}")
    print("\nRe-parse the removed rows (HUMAN-ONLY, spends credits):\n"
          "    bash pipeline/parsing/run_parse.sh parse-missing")


if __name__ == "__main__":
    main()
