"""THE AREA-CUT GATE (T1199, owner 2026-11-26) — asserts, never repairs.

A closure area (`areas.json` `cut: "clean"`: national parks, ecological reserves, the Chilkoot) is cut by
`clean_cut.decide_area`: at the real crossings of its edge, edge water within `EDGE_M` closed with it, no piece under
`MIN_PIECE_M`. This gate checks the BUILT stream pieces against each area's polygon, after the membership pass, and
stops the build on any of:

  G1  a MEMBER piece with more than `MIN_PIECE_M - EDGE_M` of line farther than `EDGE_M` outside the polygon (the floor
      may close at most that much beyond the band; the 1,000 m rejoin rule failed 41 pieces at 25 / 50, up to 1,107 m);
  G2  a piece that is NOT a member with any line deeper than `DEEP_M` inside the polygon (inside water left open);
  G3  a piece shorter than the floor that an area cut made (the cut is the piece's own boundary, not an alias of an
      older one); `AT_CUT_M` of slack for a chain's geometric length against its route measure;
  G4  the naming design's rule, that a cut area's part is wholly in or out: a piece that is NOT a member with an inside
      run of at least `DEEP_M` making a `SHARE` of its length, or a MEMBER piece with no line within `EDGE_M` of the
      polygon (a member the geometry does not confirm). A piece straddling its cutter's inside is the sliver gate's.
  G5  a RUN of member pieces (consecutive on one blue line) holding no run inside the polygon at least `DEEP_M` long, and
      not lying wholly inside (a run wholly inside is inside however short): a line that never truly crosses — grazes under the dip are positional error — made inside by the band (review of
      T1199: a 2 m graze once closed 2 km of a creek hugging the edge 20 m out).

Lakes and wetlands are never cut and keep the outline test (`border.mark_inside_areas`); they are not checked here.
Every number is written to `area_cut_gate.json` (pass or fail), so a build's log says what the rule did.

Also a CLI over a built atlas (`python -m pipeline.atlas.splits.area_cut_gate --atlas DIR [--out FILE]`): the same
check, reporting instead of stopping, to measure an atlas the gate did not build.
"""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.atlas.splits import clean_cut as _cc
from pipeline.common import tolerances as _tol
from pipeline.common.models import NodeKind, StreamGraph

FILENAME = "area_cut_gate.json"
#: G2 / G4: a line this deep inside, or an inside run this long, is real inside water (`tolerances.SLIVER_M`, the
#: cutter's own dip: shorter is positional error).
DEEP_M = _tol.SLIVER_M
#: G4: the share of a non-member piece inside its area that says it is not wholly out (the naming design's
#: `compose2.check`, 2026-11-26).
SHARE = 0.05
#: G3: a piece this close to the floor is at it (a chain's geometric length and its route measure differ by mm).
FLOOR_SLACK_M = _tol.AT_CUT_M
#: G5: two pieces of one line meet when their measures agree this closely (`tolerances.SAME_PLACE_M`), and an inside
#: run this much under `DEEP_M` is that run computed twice (the cutter's measure against the pieces' geometry).
_MEET_M = _tol.SAME_PLACE_M
_RUN_NOISE_M = 1e-3
#: G5: a run within this of the polygon everywhere lies wholly inside it (`tolerances.AT_CUT_M`: a line ending on a lake
#: hole's edge or on the edge itself)
_WHOLLY_IN_M = _tol.AT_CUT_M
#: How many offenders of each check are printed (all are written).
_PRINT = 25


def _parts(g):
    return list(g.geoms) if hasattr(g, "geoms") else [g]


def measure(graph: StreamGraph, geoms: dict, areas: list[tuple[str, str, object]],
            edge_m: float = _cc.EDGE_M, min_piece_m: float = _cc.MIN_PIECE_M) -> dict:
    """The four checks over every closure area: `areas` is [(area id, the name its cuts carry, polygon)]. Returns
    {"checks": {G1..G5: [offender, …]}, "areas": {area id: counts}, "worst": {…}}."""
    import shapely
    from shapely.strtree import STRtree

    allowed_beyond = min_piece_m - edge_m
    want = {aid for aid, _, _ in areas}
    member_of: dict[str, list[str]] = {}
    nids, gs = [], []
    for nid, n in graph.nodes.items():
        if n.kind != NodeKind.stream:
            continue
        for a in n.in_areas:
            if a in want:
                member_of.setdefault(a, []).append(nid)
        g = geoms.get(nid)
        if g is not None and not g.is_empty:
            nids.append(nid)
            gs.append(g)
    tree = STRtree(gs) if gs else None
    by_area_cut: dict[str, str] = {f"split:area:{name}": aid for aid, name, _ in areas}
    checks: dict[str, list] = {"G1": [], "G2": [], "G3": [], "G4": [], "G5": []}
    per: dict[str, dict] = {}
    worst = {"G1_beyond_band_m": 0.0, "G2_deep_m": 0.0, "shortest_area_piece_m": None}
    for aid, name, poly in sorted(areas, key=lambda x: x[0]):
        if poly is None or poly.is_empty:
            continue
        shapely.prepare(poly)
        band = _cc.edge_band(poly, edge_m)
        inside_near = poly.buffer(_WHOLLY_IN_M)
        shapely.prepare(inside_near)
        deep = poly.buffer(-DEEP_M)
        shapely.prepare(deep)
        c = per.setdefault(aid, {"members": 0, "member_m": 0.0, "member_beyond_band_m": 0.0, "near_outside": 0})
        members = member_of.get(aid, [])
        for nid in sorted(members):
            n = graph.nodes[nid]
            g = geoms.get(nid)
            c["members"] += 1
            c["member_m"] += n.up_m - n.down_m
            if g is None or g.is_empty:
                checks["G4"].append({"area": aid, "node": nid, "why": "member with no geometry"})
                continue
            beyond = g.difference(band).length
            c["member_beyond_band_m"] += beyond
            worst["G1_beyond_band_m"] = max(worst["G1_beyond_band_m"], round(beyond, 1))
            if beyond > allowed_beyond:
                checks["G1"].append({"area": aid, "node": nid, "length_m": round(g.length, 1),
                                     "beyond_band_m": round(beyond, 1)})
            if not band.intersects(g):
                checks["G4"].append({"area": aid, "node": nid, "why": "member with no line within the band"})
        # G5: each run of consecutive member pieces on one blue line holds real inside water
        by_blk: dict[str, list] = {}
        for nid in members:
            n = graph.nodes[nid]
            if geoms.get(nid) is not None:
                by_blk.setdefault(n.blk, []).append(n)
        for blk, ns in sorted(by_blk.items()):
            ns.sort(key=lambda n: n.down_m)
            runs, cur = [], [ns[0]]
            for n in ns[1:]:
                if abs(n.down_m - cur[-1].up_m) <= _MEET_M:
                    cur.append(n)
                else:
                    runs.append(cur)
                    cur = [n]
            runs.append(cur)
            for run in runs:
                part = shapely.line_merge(shapely.union_all([geoms[n.node_id] for n in run])).intersection(poly)
                part = shapely.line_merge(part) if part.geom_type == "MultiLineString" else part
                longest = max((p.length for p in _parts(part) if not p.is_empty), default=0.0)
                # a run lying wholly inside is inside however short (FWA's own 1.5 m line between two lakes in a park)
                if longest < DEEP_M - _RUN_NOISE_M and not inside_near.covers(
                        shapely.union_all([geoms[n.node_id] for n in run])):
                    checks["G5"].append({"area": aid, "blk": blk, "from_m": round(run[0].down_m, 1),
                                         "to_m": round(run[-1].up_m, 1), "longest_inside_run_m": round(longest, 2)})
        if tree is None:
            continue
        member_set = set(members)
        for i in sorted(tree.query(poly, predicate="intersects")):
            nid = nids[i]
            if nid in member_set:
                continue
            g = gs[i]
            c["near_outside"] += 1
            if deep.intersects(g):
                d = g.intersection(deep).length
                if d > 0.0:
                    checks["G2"].append({"area": aid, "node": nid, "length_m": round(g.length, 1),
                                         "deeper_than_m": DEEP_M, "deep_line_m": round(d, 1)})
                    worst["G2_deep_m"] = max(worst["G2_deep_m"], round(d, 1))
            part = g.intersection(poly)
            run = max((p.length for p in _parts(part) if not p.is_empty), default=0.0)
            if run >= DEEP_M and g.length > 0 and part.length / g.length >= SHARE:
                checks["G4"].append({"area": aid, "node": nid, "why": "not a member, but partly inside",
                                     "inside_m": round(part.length, 1), "share": round(part.length / g.length, 3)})
    # G3: a piece under the floor whose own boundary (not an alias) is a closure area's cut
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind != NodeKind.stream:
            continue
        ln = n.up_m - n.down_m
        hit = [by_area_cut[str(b.boundary_id)] for b in (n.lower_bound, n.upper_bound)
               if b is not None and str(b.boundary_id) in by_area_cut]
        if not hit:
            continue
        if worst["shortest_area_piece_m"] is None or ln < worst["shortest_area_piece_m"]:
            worst["shortest_area_piece_m"] = round(ln, 2)
        if ln < min_piece_m - FLOOR_SLACK_M:
            checks["G3"].append({"area": sorted(set(hit)), "node": nid, "length_m": round(ln, 2),
                                 "lower": str(getattr(n.lower_bound, "boundary_id", None)),
                                 "upper": str(getattr(n.upper_bound, "boundary_id", None))})
    for aid, c in per.items():
        c["member_m"] = round(c["member_m"], 1)
        c["member_beyond_band_m"] = round(c["member_beyond_band_m"], 1)
    return {"edge_m": edge_m, "min_piece_m": min_piece_m, "allowed_beyond_band_m": allowed_beyond,
            "deep_m": DEEP_M, "share": SHARE, "counts": {k: len(v) for k, v in checks.items()},
            "worst": worst, "checks": checks, "areas": per}


def check(graph: StreamGraph, geoms: dict, areas: list[tuple[str, str, object]], out_dir: Path | None = None,
          edge_m: float = _cc.EDGE_M, min_piece_m: float = _cc.MIN_PIECE_M) -> dict:
    """Stop the build if any closure area fails G1-G5; always write `area_cut_gate.json` to `out_dir`."""
    res = measure(graph, geoms, areas, edge_m, min_piece_m)
    if out_dir is not None:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        (Path(out_dir) / FILENAME).write_text(json.dumps(res, indent=1, sort_keys=True) + "\n")
    n = res["counts"]
    w = res["worst"]
    if not any(n.values()):
        print(f"  area-cut gate: {len(res['areas'])} closure area(s) pass G1-G5 (worst member line beyond the "
              f"{edge_m:g} m band {w['G1_beyond_band_m']} m of {res['allowed_beyond_band_m']:g} allowed; shortest "
              f"piece an area cut made {w['shortest_area_piece_m']} m)")
        return res
    for k, rows in res["checks"].items():
        for r in rows[:_PRINT]:
            print(f"  AREA-CUT {k} {r}")
    raise SystemExit(f"atlas build: area-cut gate — G1 {n['G1']}, G2 {n['G2']}, G3 {n['G3']}, G4 {n['G4']}, G5 {n['G5']} "
                     f"piece(s) (see {FILENAME}). Fix the cut rule (clean_cut.decide_area); the gate never repairs")


def closure_areas(area_defs: list[dict], catalog_polys: dict) -> list[tuple[str, str, object]]:
    """[(area id, name, polygon)] of every clean-cut area def's polygons, as the build holds them."""
    from pipeline.atlas.splits.area_catalog import area_id
    from pipeline.atlas.splits.area_splits import cut_mode
    out = []
    for ad in area_defs:
        if cut_mode(ad) != "clean":
            continue
        for name, poly in sorted((catalog_polys.get(ad["id"]) or {}).items()):
            out.append((area_id(ad.get("kind", ad["id"]), name), name, poly))
    return out


def main(argv: list[str] | None = None) -> None:
    import argparse

    from pipeline.atlas.splits.area_splits import load_area_polys, load_area_split_defs
    from pipeline.common.io.serialize import read_artifact

    ap = argparse.ArgumentParser(description="The area-cut gate over a built atlas (reports, never stops).")
    ap.add_argument("--atlas", required=True, help="a built atlas directory (graph.pkl, geometries.pkl)")
    ap.add_argument("--out", required=True, help="where to write the gate's JSON")
    a = ap.parse_args(argv)
    atlas = Path(a.atlas)
    for f in ("graph.pkl", "geometries.pkl"):
        if not (atlas / f).exists():
            raise SystemExit(f"area_cut_gate: {atlas / f} is missing — build the atlas first "
                             f"(python -m pipeline build --atlas)")
    graph = read_artifact(str(atlas / "graph.pkl"))
    geoms = read_artifact(str(atlas / "geometries.pkl"))
    defs = load_area_split_defs()
    polys = {ad["id"]: load_area_polys(None, ad) for ad in defs if ad.get("cut") == "clean"}
    res = measure(graph, geoms, closure_areas(defs, polys))
    Path(a.out).write_text(json.dumps(res, indent=1, sort_keys=True) + "\n")
    print(json.dumps({"counts": res["counts"], "worst": res["worst"]}, indent=1))


if __name__ == "__main__":
    main()
