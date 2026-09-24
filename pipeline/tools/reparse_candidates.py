"""Which entries are worth DELETING so `run_parse.sh parse` re-parses them?

Not "everything unresolved". Most unresolved rules are not the parser's fault and a
re-parse would produce exactly the same output:

  no_extents          the parser REFUSED to guess a reach with no boundary to bind to
                      ("500 m upstream of Causeway Road"). It needs a curated split, and
                      re-parsing would either reproduce the refusal or invent a binding.
  no_sections_for_items  the lake has no geometry — a graph-build gap.
  cut_not_found / cuts_collapsed / empty_after_scope / area_id_dangling
                      curation or resolver issues, untouched by re-parsing.

The group worth credits: entries parsed as `no_registry` because the registry did not yet
contain their water, and does now — Little Stawamus Creek, Endako River, Hidden Lake. Their
`matched` is empty, so only a re-parse (or attaching the item by hand in the review app) can
bind them. `parse` re-parses exactly the rows missing from the catalogue, so deleting these
entries is what queues them.

    .venv/bin/python -m pipeline.tools.reparse_candidates            # dry run
    .venv/bin/python -m pipeline.tools.reparse_candidates --apply    # back up, then delete them

`--apply` backs up every region file first, and writes through `io.write_entryfile`.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline.regs.parsing import io as parse_io
from pipeline.atlas.reach.covered import live_match_ids, make_matcher
from pipeline.atlas.registry import load_registry
from pipeline.common.curated import GENERATED


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", default=str(GENERATED.build()))
    ap.add_argument("--apply", action="store_true",
                    help="back up, then DELETE the re-parse group so `run_parse.sh parse` re-runs them")
    ap.add_argument("--backup-dir", default=str(GENERATED.regs.entries_backup))
    args = ap.parse_args()

    registry = load_registry(str(Path(args.build) / "registry.json"))
    match = make_matcher(registry)
    entries_dir = parse_io.entries_dir()

    reparse_g: dict[str, list[tuple[str, str, str]]] = {}   # region -> [(entry_id, item, name)]
    kept = Counter()

    for region in parse_io.region_ids(entries_dir):
        for e in parse_io.read_entryfile(entries_dir / f"region-{region}.json").values():
            stored = [i for i in (e.get("matched") or []) if i in registry]
            if stored:
                kept["already bound to a registry item"] += 1
                continue
            found = live_match_ids(e, registry, match)
            if not found:
                kept["still has no registry item — re-parsing cannot help"] += 1
                continue
            reparse_g.setdefault(region, []).append(
                (e["entry_id"], found[0], registry[found[0]].name))

    n_reparse = sum(len(v) for v in reparse_g.values())
    print(f"REPARSE (needs credits, or attach the item by hand): {n_reparse}")
    for region in sorted(reparse_g):
        for eid, iid, name in sorted(reparse_g[region]):
            print(f"  region {region}: {eid:<40} -> {iid} ({name})")
    print("\nnot candidates:")
    for k, v in kept.most_common():
        print(f"  {v:>5}  {k}")

    if not args.apply:
        print("\n(dry run — --apply backs up the region files and deletes the REPARSE group)")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = Path(args.backup_dir) / stamp
    backup.mkdir(parents=True, exist_ok=True)
    for p in sorted(entries_dir.glob("region-*.json")):
        shutil.copy2(p, backup / p.name)
    print(f"\nbacked up {len(list(backup.glob('*.json')))} files -> {backup}")

    for region, rows in reparse_g.items():
        path = entries_dir / f"region-{region}.json"
        entries = parse_io.read_entryfile(path)
        drop = {eid for eid, _i, _n in rows}
        parse_io.write_entryfile(path, region,
                                 [e for eid, e in entries.items() if eid not in drop])
        print(f"  region {region}: {len(entries)} -> {len(entries) - len(drop)} entries")
    print("\nNow re-parse the deleted rows (HUMAN-ONLY — spends credits):")
    print("    bash pipeline/regs/parsing/run_parse.sh parse")
    return 0


if __name__ == "__main__":
    sys.exit(main())
