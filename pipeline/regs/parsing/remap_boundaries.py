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
from pipeline.regs.parsing.catalogue_parts.walk import iter_extents
from pipeline.regs.parsing.validate_catalogue import rename_cut_picks
from pipeline.atlas.registry import default_registry_path, load_registry

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
    report: dict[str, list] = {"changed": []}
    for path in sorted(Path(entries_dir).glob("region-*.json")):
        region = path.stem.split("region-")[1]
        by_id = io.read_entryfile(path)
        file_changed = False
        for eid, e in by_id.items():
            hits: list[tuple[str, str]] = []
            # EVERY extent (`walk.iter_extents`, T112): the entry scope, each rule's extents and
            # carve-outs, and each licensing record's. This skipped every licensing extent once, so
            # about 30 licensing split references went dead after a registry rebuild unreported.
            for _path, ex, _role in iter_extents(e):
                mine = [(sid, remap[sid]) for sid in ex.get("splits") or [] if sid in remap]
                if not mine:
                    continue            # an untouched extent is left exactly as it was
                hits += mine
                ex["splits"] = [remap.get(sid, sid) for sid in ex["splits"]]
                # A PICK FOLLOWS ITS SPLIT: `cut_pick` is keyed by split id and the model refuses
                # a key that is not among the extent's splits (T119 review).
                rename_cut_picks(ex, remap)
            if hits:
                file_changed = True
                report["changed"].append((region, eid, sorted(set(hits))))
        if file_changed and not dry_run:
            io.write_entryfile(path, region, by_id.values())     # atomic; the file is validated
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-point entries at renamed auto boundaries.")
    ap.add_argument("--old-registry", required=True, help="registry.json from BEFORE the rebuild")
    ap.add_argument("--registry", help=f"current registry.json (default: {default_registry_path()})")
    ap.add_argument("--entries-dir", help="entries dir (default: data/curated/regulations/entries/catalogue).")
    ap.add_argument("--dry-run", action="store_true", help="report only; write nothing.")
    args = ap.parse_args()

    remap = build_remap(load_registry(args.old_registry),
                        load_registry(str(args.registry or default_registry_path())))
    print(f"boundaries whose readable id moved: {len(remap)}")
    entries_dir = Path(args.entries_dir) if args.entries_dir else io.entries_dir()
    report = apply_remap(entries_dir, remap, dry_run=args.dry_run)

    verb = "[dry-run] would re-point" if args.dry_run else "re-pointed"
    print(f"{verb} {len(report['changed'])} entry(ies)")
    for region, eid, hits in report["changed"]:
        print(f"    [{region}] {eid}")
        for old, new in hits:
            print(f"        {old}  ->  {new}")


if __name__ == "__main__":
    main()
