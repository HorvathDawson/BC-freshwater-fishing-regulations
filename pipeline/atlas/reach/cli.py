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
    # THE REGION EACH STRADDLING SECTION LIES IN, for the zone rules (`registry.regions`): a stream
    # piece drawn across a region line takes its home region's standing table; a lake takes both
    # (the most strict applies where the rules are read). Measured once by the atlas build and read
    # from it here — never measured again.
    from pipeline.atlas.registry import regions
    home = regions.attach_from(build, graph, registry)   # the atlas's own `region_home.json`
    # WHERE A WATER THAT TOUCHES NOTHING LIES, for a watershed part (`reach.position`): read from
    # this build's geometry the first time a watershed part asks.
    from pipeline.atlas.reach import position
    position.attach(graph, build)
    lakes = len(regions.lakes_among(graph, registry, home))
    print(f"  region homes: {len(home) - lakes:,} straddling stream piece(s) held to the region "
          f"they lie in; {lakes:,} straddling lake(s) in every region they touch")
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
    result = build_reaches(entries, registry, graph, build=build.name,
                           handles=digest_for(build), steelhead_list=load_list())
    r = result.report
    print(f"{r.n_entries:,} entries · {r.n_rules:,} rules · {r.seconds}s")
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

    if args.against:
        print("\n" + "=" * 72)
        print(diff_runs(args.against, result).summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
