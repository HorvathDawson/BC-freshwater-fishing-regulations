"""Re-point entries at renamed AUTO boundaries after a registry rebuild.

An auto boundary (lake / outlet / headwaters) gets a READABLE id built from the item's name —
`f"{slug(item.name)}__{slug(label)}"` in `pipeline.atlas.registry.build`. So renaming an item renames every
auto boundary on it, and any rule that bound one now references an id that no longer exists. Fixing
the registry's item names did exactly that: `mcarthur_island_slough__kamloops_lake` became
`thompson_river__kamloops_lake` when the Thompson stopped being displayed under a side channel's name.

Curated splits are unaffected — their ids come from the curation source (`split:*`), not the item name.

The remap keys on `ref`, NOT on the id string: `ref` ("lake:329..." / "split:...") is the boundary's
stable identity across builds, so old-id -> ref (from the pre-rebuild registry) -> new-id (from the
current one) is exact, and a boundary whose ref cannot be found is reported rather than guessed.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.remap_boundaries \\
        --old-registry /path/to/registry.before.json --dry-run
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.atlas.registry import default_registry_path, load_registry

# every place an Extent can hang off an entry
_EXTENT_HOLDERS = ("scope",)


def _extents(entry: dict):
    """Every extent dict in an entry — entry scope, tributary excludes, and each rule's."""
    yield from (entry.get("scope") or [])
    yield from ((entry.get("tributaries") or {}).get("excludes") or [])
    for r in entry.get("rules") or []:
        yield from (r.get("extents") or [])
        yield from (r.get("tributary_excludes") or [])


def build_remap(old_registry: dict, new_registry: dict) -> dict[str, str]:
    """{old boundary id: new boundary id} for boundaries whose READABLE id changed but whose `ref`
    (stable identity) still exists. Only ids that actually moved are included."""
    new_by_ref: dict[str, str] = {}
    for it in new_registry.values():
        for b in it.boundaries:
            if b.ref:
                new_by_ref.setdefault(b.ref, b.id)
    remap: dict[str, str] = {}
    for it in old_registry.values():
        for b in it.boundaries:
            new_id = new_by_ref.get(b.ref or "")
            if new_id and new_id != b.id:
                remap[b.id] = new_id
    return remap


def apply_remap(entries_dir: Path, remap: dict[str, str], dry_run: bool = False) -> dict:
    report: dict[str, list] = {"changed": [], "locked_changed": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        file_changed = False
        for eid, e in by_id.items():
            hits: list[tuple[str, str]] = []
            for ex in _extents(e):
                new_splits = []
                for sid in ex.get("splits") or []:
                    if sid in remap:
                        hits.append((sid, remap[sid]))
                        new_splits.append(remap[sid])
                    else:
                        new_splits.append(sid)
                if hits:
                    ex["splits"] = new_splits
            if hits:
                file_changed = True
                report["changed"].append((region, eid, e.get("locked", False), sorted(set(hits))))
                if e.get("locked"):
                    report["locked_changed"].append(eid)
        if file_changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())     # atomic, via the model
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-point entries at renamed auto boundaries.")
    ap.add_argument("--old-registry", required=True, help="registry.json from BEFORE the rebuild")
    ap.add_argument("--registry", help=f"current registry.json (default: {default_registry_path()})")
    ap.add_argument("--entries-dir", help="EntryFiles dir (default: pipeline/regs/parsing/entries).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()

    remap = build_remap(load_registry(args.old_registry),
                        load_registry(str(args.registry or default_registry_path())))
    print(f"boundaries whose readable id moved: {len(remap)}")
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = apply_remap(entries_dir, remap, dry_run=args.dry_run)

    verb = "[dry-run] would re-point" if args.dry_run else "re-pointed"
    print(f"{verb} {len(report['changed'])} entry(ies)")
    for region, eid, locked, hits in report["changed"]:
        print(f"    [{region}] {eid}{'  (LOCKED)' if locked else ''}")
        for old, new in hits:
            print(f"        {old}  ->  {new}")


if __name__ == "__main__":
    main()
