"""    python -m pipeline.atlas.reach.cli --build data/generated/atlas/full --out data/generated/reaches/full
       python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_new --out data/generated/reaches/full_new \
                                    --against data/generated/reaches/full

Resolves every rule against one build, writes the tables, and (with `--against`) reports
which rules now cover different water than they did — confirmed entries first.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline.common.io.serialize import read_artifact
from pipeline.regs.parsing import io as parse_io
from pipeline.atlas.reach.build import build_reaches
from pipeline.atlas.reach.diff import diff_runs
from pipeline.atlas.reach.io import digest, write_run
from pipeline.atlas.registry import load_registry


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", required=True, help="a build dir (registry.json + graph.pkl)")
    ap.add_argument("--out", help="where to write the tables; omit for a dry run")
    ap.add_argument("--against", help="a previous --out dir to diff against")
    ap.add_argument("--entries", help="entries dir (default: the checked-in one)")
    args = ap.parse_args()

    build = Path(args.build)
    registry = load_registry(str(build / "registry.json"))
    graph = read_artifact(str(build / "graph.pkl"))
    # THE HOME REGION OF EACH STRADDLING SECTION (`registry.regions`), measured once by the atlas build
    # and read from it here. Since T183 it filters nothing — a straddler, stream piece or lake, takes
    # BOTH regions' rules (the most strict applies where the rules are read); it is attached so the
    # T115 mutation pin and the review app read the same graph.
    from pipeline.atlas.registry import regions
    home = regions.attach_from(build, graph, registry)   # the atlas's own `region_home.json`
    # WHERE A WATER THAT TOUCHES NOTHING LIES, for a watershed part (`reach.position`): read from
    # this build's geometry the first time a watershed part asks.
    from pipeline.atlas.reach import position
    position.attach(graph, build)
    lakes = len(regions.lakes_among(graph, registry, home))
    print(f"  region straddlers: {len(home) - lakes:,} stream piece(s) and {lakes:,} lake(s), each in "
          f"every region it lies in (T183)")
    # EVERY SOURCE, not just the synopsis. The reach builder is a CONSUMER of the corpus:
    # zone regulations, park closures and the salmon entries are the same kind of thing to
    # it as a river row, and a rule that binds to an area binds through the same resolver.
    # `--entries` still names ONE directory, for building against a single source in
    # isolation.
    # ABSORBED IDS ARE MAPPED HERE, at the corpus' read point (`registry.flowing.canonical_ids`):
    # a `matched` or an extent naming a polygon the registry folded into its river names the
    # river. `build_reaches` refuses one that slipped past.
    if args.entries:
        entries = parse_io.read_entries_dir(Path(args.entries), registry=registry)
    else:
        entries = parse_io.read_all_entries(registry=registry)
        srcs = [n for n, _ in parse_io.entry_sources()]
        print(f"  entries from {len(srcs)} source(s): {', '.join(srcs)}")
        for note in parse_io.skipped_sources():
            print(f"  (not an EntryFile source — {note})")

    # The DIGEST, not just the folder name. Promotion renames directories, so a run built
    # against a scratch path would otherwise stop matching its own atlas the moment it is
    # promoted — see `BuildReport.handles`.
    from pipeline.common.section_handles import digest_for
    # THE USER'S KNOWN-STEELHEAD LIST (curated; a missing file raises) — `reach.steelhead`: a
    # presence indicator, it binds no rule.
    from pipeline.atlas.reach.steelhead import load_list
    # THE TIDAL BOUNDARIES THIS ATLAS CUT (T166, `outside.tidal_splits`): the piece below each is
    # tidal water, out of every binding after the walk.
    from pipeline.atlas.reach.outside import tidal_splits
    tidal_ids = tidal_splits(build)
    result = build_reaches(entries, registry, graph, build=build.name,
                           handles=digest_for(build), steelhead_list=load_list(),
                           tidal_splits=tidal_ids)
    r = result.report
    print(f"{r.n_entries:,} entries · {r.n_rules:,} rules · {r.seconds}s")
    if r.tidal_cuts:
        print(f"  tidal        {len(r.tidal_cuts)} tidal boundar{'y' if len(r.tidal_cuts) == 1 else 'ies'}, "
              f"{sum(c['sections'] for c in r.tidal_cuts):,} section(s) below them made tidal")
        for c in r.tidal_cuts:
            if c["unplaced"]:
                print(f"    {c['split']}: {len(c['unplaced'])} piece(s) not placed, left freshwater: "
                      f"{c['unplaced'][:4]}")
    if r.tidal_emptied:
        # LOUD (T166): a rule whose whole reach lies below a tidal boundary binds nothing now
        print(f"!! {len(r.tidal_emptied)} rule(s) left with NO water by the tidal boundaries "
              "(unresolved `tidal`) — review each cut / rule:")
        for x in r.tidal_emptied:
            print(f"!!   {x['entry_id']} {x['rule_id']}  <- {', '.join(x['splits'])}")
    for k, v in sorted(r.outcomes.items()):
        print(f"  {k:<12} {v:,}")
    for k, v in sorted(r.reasons.items(), key=lambda kv: -kv[1]):
        print(f"      {k:<24} {v:,}")
    for k, v in sorted(r.diagnostics.items()):
        print(f"  {k:<12} {v:,}")
    print(f"  digest       {digest(result)}")
    if r.steelhead:
        print("  steelhead    " + " · ".join(f"{k} {v:,}" for k, v in r.steelhead.items()
                                           if isinstance(v, int)))
        cur = r.steelhead.get("curated") or []
        if cur:
            print(f"    curated list (a presence indicator): {len(cur):,} waters, "
                  f"{sum(c['sections'] for c in cur):,} sections")
    if r.licensing:
        print("  licensing")
        for k, v in sorted(r.licensing.items()):
            print(f"    {k:<30} {v:,}")
        for p in result.licensing:
            if p.placement == "unresolved":
                print(f"    UNRESOLVED {p.entry_id}#{p.record_id} ({p.kind}): {p.reason} — "
                      f"{p.detail[:80]}")

    if args.out:
        from pipeline.common.section_handles import registry_digest_for
        counts = write_run(args.out, result, entries,
                           stamps={"registry_digest": registry_digest_for(build)})
        print(f"\nwrote {args.out}: " + " · ".join(f"{k} {v:,}" for k, v in counts.items()))
        # (T211) THE DFO SALMON LOCATIONS, placed by the same `build_reach` — beside the tables, outside the rules' digest
        from pipeline.atlas.reach import dfo_salmon
        rows = dfo_salmon.resolve_all(registry, graph, outside=result.outside or None)
        d = dfo_salmon.write(Path(args.out), rows)
        print(f"  dfo salmon   {sum(r['mapped'] for r in rows):,} of {len(rows):,} locations placed (digest {d})")

    if args.against:
        print("\n" + "=" * 72)
        print(diff_runs(args.against, result).summary())
    # THE STRADDLE GATE (T67): after the tables are written, so what failed can be read — the run still
    # FAILS, naming every piece left straddling a cut the atlas should have swept.
    from pipeline.atlas.reach.build import straddle_gate
    straddle_gate(result.diagnostics)
    return 0


if __name__ == "__main__":
    sys.exit(main())
