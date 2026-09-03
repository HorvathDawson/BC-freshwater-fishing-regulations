"""Which entries are worth DELETING so `run_parse.sh parse-missing` re-parses them?

Not "everything unresolved". Most unresolved rules are not the parser's fault and a
re-parse would produce exactly the same output:

  no_extents          the parser REFUSED to guess a reach with no boundary to bind to
                      ("500 m upstream of Causeway Road"). It needs a curated split, and
                      re-parsing would either reproduce the refusal or invent a binding.
  no_sections_for_items  the lake has no geometry — a graph-build gap.
  cut_not_found / cuts_collapsed / empty_after_scope / area_id_dangling
                      curation or resolver issues, untouched by re-parsing.

An entry with an empty `matched` splits into two very different groups, and only the
second is worth credits:

  STAMP  the entry_id already IS the item id (`gnis:11051` -> Greenstone Creek). The link
         is provable locally and re-parsing would spend credits to rediscover a fact the
         filename already states. 376 entries.

  REPARSE the entry was parsed as `no_registry` because the registry did not yet contain
         its water, and does now — Little Stawamus Creek, Endako River, Hidden Lake. The
         entry_id encodes no item, so only a re-parse (or attaching the item by hand in
         the review app) can bind it. 3 entries.

    .venv/bin/python -m pipeline.tools.reparse_candidates            # dry run, both groups
    .venv/bin/python -m pipeline.tools.reparse_candidates --stamp    # fix group 1, FREE
    .venv/bin/python -m pipeline.tools.reparse_candidates --apply    # delete group 2

LOCKED entries are never touched. Both write modes back up every entry file first.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline.common.io.serialize import read_artifact
from pipeline.regs.parsing import io as parse_io
from pipeline.atlas.reach.covered import covered_ids, make_matcher
from pipeline.atlas.registry import load_registry
from pipeline.common.curated import GENERATED


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", default=str(GENERATED.build()))
    ap.add_argument("--stamp", action="store_true",
                    help="write `matched` for entries whose id already names the item (free)")
    ap.add_argument("--apply", action="store_true",
                    help="back up, then DELETE the re-parse group so parse-missing re-runs them")
    ap.add_argument("--backup-dir", default=str(GENERATED.regs.entries_backup))
    args = ap.parse_args()

    registry = load_registry(str(Path(args.build) / "registry.json"))
    match = make_matcher(registry)
    entries_dir = parse_io.entries_dir()

    candidates: dict[str, list[tuple[str, str]]] = {}     # region -> [(entry_id, why)]
    kept = Counter()

    for region in parse_io.region_ids(entries_dir):
        path = entries_dir / f"region-{region}.json"
        data = json.loads(path.read_text())
        for e in data["entries"]:
            if e.get("locked"):
                kept["locked — never touched"] += 1
                continue
            stored = [i for i in (e.get("matched") or []) if i in registry]
            if stored:
                kept["already bound to a registry item"] += 1
                continue
            found = covered_ids(e, registry, match)
            if not found:
                kept["still has no registry item — re-parsing cannot help"] += 1
                continue
            base = e["entry_id"].split("#")[0]
            group = "stamp" if (base in registry and base == found[0]) else "reparse"
            candidates.setdefault(group, {}).setdefault(region, []).append(
                (e["entry_id"], found[0], registry[found[0]].name))

    stamp_g = candidates.get("stamp", {})
    reparse_g = candidates.get("reparse", {})
    n_stamp = sum(len(v) for v in stamp_g.values())
    n_reparse = sum(len(v) for v in reparse_g.values())

    print(f"STAMP (free — the entry id already names the item): {n_stamp}")
    for region in sorted(stamp_g):
        ids = [r[0] for r in stamp_g[region]]
        print(f"  region {region}: {len(ids)}   e.g. {', '.join(sorted(ids)[:3])}")
    print(f"\nREPARSE (needs credits, or attach the item by hand): {n_reparse}")
    for region in sorted(reparse_g):
        for eid, iid, name in sorted(reparse_g[region]):
            print(f"  region {region}: {eid:<40} -> {iid} ({name})")
    print("\nnot candidates:")
    for k, v in kept.most_common():
        print(f"  {v:>5}  {k}")

    if not (args.stamp or args.apply):
        print("\n(dry run — --stamp fixes the first group free, --apply deletes the second)")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = Path(args.backup_dir) / stamp
    backup.mkdir(parents=True, exist_ok=True)
    for p in sorted(entries_dir.glob("region-*.json")):
        shutil.copy2(p, backup / p.name)
    print(f"\nbacked up {len(list(backup.glob('*.json')))} files -> {backup}")

    if args.stamp:
        n = 0
        for region, rows in stamp_g.items():
            path = entries_dir / f"region-{region}.json"
            data = json.loads(path.read_text())
            want = {eid: iid for eid, iid, _ in rows}
            for e in data["entries"]:
                if e["entry_id"] in want:
                    e["matched"] = [want[e["entry_id"]]]
                    n += 1
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        print(f"stamped `matched` on {n} entries (no credits spent)")

    if args.apply:
        for region, rows in reparse_g.items():
            path = entries_dir / f"region-{region}.json"
            data = json.loads(path.read_text())
            drop = {eid for eid, _i, _n in rows}
            before = len(data["entries"])
            data["entries"] = [e for e in data["entries"] if e["entry_id"] not in drop]
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
            print(f"  region {region}: {before} -> {len(data['entries'])} entries")
        print("\nNow re-parse the deleted rows (HUMAN-ONLY — spends credits):")
        print("    bash pipeline/regs/parsing/run_parse.sh parse-missing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
