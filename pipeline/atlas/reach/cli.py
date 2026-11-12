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
    from pipeline.atlas.reach.outside import inside_lines, tidal_splits, tidal_waters
    from pipeline.atlas.splits import coast as _coast
    if _coast.built_by_coast_rule(build):
        # T678: an atlas cut by the coast rule writes THE tidal record; the reach reads it and recomputes nothing
        rec = _coast.read_record(build)
        print(f"  tidal: the atlas's tidal record ({_coast.TIDAL_RECORD}, {len(rec):,} section(s))")
        result = build_reaches(entries, registry, graph, build=build.name,
                               handles=digest_for(build), steelhead_list=load_list(), tidal_record=rec)
    else:
        # an atlas cut before T678 (no record): its tidal inputs, as it was built to be read
        print(f"  tidal: {build.name} was cut before T678 (no {_coast.TIDAL_RECORD}): reading its tidal sidecars")
        result = build_reaches(entries, registry, graph, build=build.name,
                               handles=digest_for(build), steelhead_list=load_list(),
                               tidal_splits=tidal_splits(build), tidal_waters=tidal_waters(build),
                               inside_lines=inside_lines(build))
    r = result.report
    # T546 item 7: the curated override ids this registry lacks — the matcher's `unresolved_ids` had no reader,
    # so a partly resolved override bound the ids that resolved and dropped the rest unseen
    # T632: every override, the DFO-tagged ones too — and any one of them stops the run (below, after the tables)
    from pipeline.regs.matching.matcher import dead_override_ids, dead_override_lines
    r.override_id_gaps = dead_override_ids(registry)
    print(f"{r.n_entries:,} entries · {r.n_rules:,} rules · {r.seconds}s")
    for x in r.override_id_gaps:
        print(f"{'!!' if x['resolved'] else '  '} override {x['name']} {'/'.join(x['mu'])}: {len(x['unresolved'])} "
              f"id(s) in no registry item ({x['resolved']} resolved"
              f"{' — the row binds only those' if x['resolved'] else ', the row is unmatched'}): {x['unresolved'][:3]}")
    if r.tidal_cuts:
        print(f"  tidal        {len(r.tidal_cuts)} tidal boundar{'y' if len(r.tidal_cuts) == 1 else 'ies'} / whole "
              f"water(s), {sum(c['sections'] for c in r.tidal_cuts):,} section(s) made tidal")
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
    if r.scope_unresolved:
        # T546 item 2: a row whose scope does not resolve binds nothing — listed, and a row with rules fails the run
        held = {}
        for b in result.bindings:
            if b.reason is not None and b.reason.value == "scope_unresolved":
                held[b.entry_id] = held.get(b.entry_id, 0) + 1
        print(f"  scope unresolved: {len(r.scope_unresolved)} entr{'y' if len(r.scope_unresolved) == 1 else 'ies'}")
        for eid in r.scope_unresolved:
            print(f"{'!!' if held.get(eid) else '  '}   {eid}  ({held.get(eid, 0)} rule(s) held unresolved)")
    if r.unplaced:
        # T546 item 3: LOUD — each piece the reach could not place is withheld from its rule (never placed by
        # a rule of thumb, AGENTS 12); a closure piece left out is water an angler is told is open
        by_reason: dict[str, int] = {}
        for x in r.unplaced:
            by_reason[x["reason"]] = by_reason.get(x["reason"], 0) + 1
        print(f"!! {len(r.unplaced)} piece(s) unplaced, withheld from their rule(s): "
              + ", ".join(f"{k} {v}" for k, v in sorted(by_reason.items())))
        for x in r.unplaced[:40]:
            print(f"!!   {x['entry_id']} {x['rule_id'] or '(entry scope)'}  {x['piece']}  ({x['reason']})")
        if len(r.unplaced) > 40:
            print(f"!!   … {len(r.unplaced) - 40} more (report.json `unplaced`)")
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
        dfo_diags: list = []
        # T632 (owner 2026-11-11): the run's tidal water comes out of every DFO place but those with a stated reason
        rows = dfo_salmon.resolve_all(registry, graph, outside=result.outside or None, diagnostics=dfo_diags,
                                      tidal=result.tidal, keep_tidal=dfo_salmon.load_tidal_calls())
        cut = sum(d["payload"].get("removed", 0) for d in dfo_diags if d["kind"] == "tidal")
        print(f"  dfo tidal    {sum(1 for d in dfo_diags if d['kind'] == 'tidal'):,} place(s) lost {cut:,} tidal "
              "section(s) (T632)")
        d = dfo_salmon.write(Path(args.out), rows)
        n = dfo_salmon.write_diagnostics(Path(args.out), dfo_diags)
        print(f"  dfo salmon   {sum(r['mapped'] for r in rows):,} of {len(rows):,} locations placed (digest {d}), "
              f"{n:,} diagnostic(s)")
        loud = [x for x in dfo_diags if x["kind"] == "unclassified"]
        for x in loud:
            # T546 item 4: a DFO place's piece the reach could not place, as the provincial ones are printed
            print(f"!!   dfo {x['location_id']}  unplaced {x['payload'].get('pieces')} "
                  f"({', '.join(sorted(set((x['payload'].get('reasons') or {}).values()))) or 'no reason'})")

    if args.against:
        print("\n" + "=" * 72)
        print(diff_runs(args.against, result).summary())
    # THE STRADDLE GATE (T67): after the tables are written, so what failed can be read — the run still
    # FAILS, naming every piece left straddling a cut the atlas should have swept.
    # Every gate is asked, and the run fails naming ALL that refused — one refusal must not hide the next (T632).
    from pipeline.atlas.reach.build import load_unplaced_calls, scope_gate, straddle_gate, unplaced_gate
    failed: list[str] = []

    def ask(fn, *a):          # not `gate`: that name is the catalogue's one acceptance gate (test_one_acceptance_gate)
        try:
            fn(*a)
        except (RuntimeError, SystemExit) as e:
            failed.append(f"{type(e).__name__}: {e}")

    ask(straddle_gate, result.diagnostics)
    # T546 item 2: and on a row with rules whose scope does not resolve (its rules are held unresolved)
    ask(scope_gate, result.bindings)
    # T632 (owner 2026-11-11): EVERY unplaced piece — any reason, a rule's or a DFO place's — stops the run unless the
    # owner accepted it (`unplaced_piece_calls.json` `accepted`); the DFO places are placed only with --out
    ask(unplaced_gate, list(r.unplaced) + (dfo_salmon.unplaced(dfo_diags) if args.out else []),
        load_unplaced_calls())
    # T632: and on a curated override id that names no water (no "expected" note: the override is fixed)
    if r.override_id_gaps:
        failed.append(f"DeadOverrideIds: {len(r.override_id_gaps)} book_name_links override(s) hold an id that names "
                      "no water — fix the override (T632):\n  " + "\n  ".join(dead_override_lines(r.override_id_gaps)))
    if failed:
        raise SystemExit("reach run REFUSED (the tables above are written, for reading):\n" + "\n".join(failed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
