"""T678: THE OWNER'S LINES, WITH TODAY'S RESULT — `waters/shore_held.json` from a built atlas.

The coast cut (`atlas.splits.coast`) decides every line that enters tidal water, except the ones it cannot: the two
crossings disagree, the geometric crossing finds no tidal water, a slough or an unnamed line on the Fraser's code, and
the lines the owner holds open by name (Kitimat's upstream limit; the five lines of item C). Those "stay as they are
today and are listed for the owner — build nothing that decides it" (T678). This tool runs the rule on a BUILT atlas
(today's), finds those lines, and writes each with TODAY'S RESULT — the atlas's shore record: a cut where it cut
(`keep: cut`, `cut_m`), whole where it listed the line tidal whole (`keep: whole`), else `keep: fresh` — plus where the
line is and what each method says. The build reads the file (`build.cut_shores`) and refuses an owner line it lacks.

A line ticked by the owner leaves the file (or changes its `keep`); a row with `decided` is the owner's ruling and is
kept as written on a rerun (owner A, 2026-11-11: Ewen Slough and 356242813 are tidal whole).

    python -m pipeline.atlas.splits.coast_held --atlas data/generated/atlas/full --out <file> [--report DIR]
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

#: OWNER-HELD BY NAME (T678, owner 2026-11-11): item C: lines with a build-27 cut and nothing tidal below. Answered
#: 2026-11-12 and no longer held: Kitimat 360885824 ("automatic mouth line": the coast cut decides it) and the San Juan
#: side channel 354087703 (tidal below the Deering Road bridge: a `cut` row of waters/tidal_limits.json, rank 1)
HELD_BY_NAME = {
    "354154029": "Colonial Creek: item C (a build-27 cut, nothing tidal below)",
    "356341234": "unnamed: item C (a build-27 cut, nothing tidal below)",
    "360445710": "unnamed: item C (a build-27 cut, nothing tidal below)",
    "360861076": "unnamed: item C (a build-27 cut, nothing tidal below)",
}
#: OWNER A (2026-11-11), decided: these two lines are tidal whole
DECIDED_WHOLE = {
    "356355111": "Ewen Slough: owner A 2026-11-11, the 22.7 m fresh stub at the top counts as tidal: wholly tidal",
    "356242813": "owner A 2026-11-11, the 19.4 m fresh stub at the top counts as tidal: wholly tidal",
}
ABOUT = ("T678: the lines the coast cut (`pipeline.atlas.splits.coast`) leaves to the owner, each kept as TODAY'S atlas "
         "has it (`keep`: cut at `cut_m` / whole / fresh) until the owner decides: the two crossings disagree, the "
         "geometric crossing finds no tidal water, a slough or unnamed line on the Fraser's code, or held by name "
         "(item C). `at` is the line's mouth (lon, lat); `methods` what each crossing says (route measures). "
         "Written by `python -m pipeline.atlas.splits.coast_held` from a built atlas; a row with `decided` is the "
         "owner's ruling and is kept on a rerun. Read by the atlas build (`build.cut_shores`), which refuses an "
         "owner line missing here.")


def today(atlas: Path) -> dict[str, dict]:
    """`{blk: {keep, cut_m?}}`: what the built atlas did with each line (its shore record + lines tidal whole)."""
    from pipeline.atlas.splits import shore as _shore
    rec = json.loads((atlas / _shore.RECORD).read_text(encoding="utf-8"))
    out = {}
    for r in rec["runs"]:
        if r.get("cut_m") is not None and not r.get("not_cut"):
            out[str(r["blk"])] = {"keep": "cut", "cut_m": float(r["cut_m"])}
    for b in _shore.read_inside(atlas):
        out[str(b)] = {"keep": "whole"}
    return out


def load(path) -> dict[str, dict]:
    """`{blk: row}` from a held file. Missing is refused (AGENTS 37)."""
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"coast: {p} is missing — `python -m pipeline.atlas.splits.coast_held` writes it")
    rows = json.loads(p.read_text(encoding="utf-8"))["lines"]
    out = {}
    for r in rows:
        b = str(r["blk"])
        if b in out:
            raise SystemExit(f"coast: {p}: {b} twice")
        if r.get("keep") not in ("cut", "whole", "fresh") or (r["keep"] == "cut") != ("cut_m" in r):
            raise SystemExit(f"coast: {p}: {b}: keep must be cut (with cut_m), whole or fresh")
        out[b] = r
    return out


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--atlas", required=True, help="a built atlas directory (today's)")
    ap.add_argument("--out", required=True, help="the held file to write")
    ap.add_argument("--report", help="a directory for the dry run's rows / cuts / owner list (scratch)")
    a = ap.parse_args(argv)
    import shapely
    from pyproj import Transformer

    from pipeline.atlas.registry.io import load_registry
    from pipeline.atlas.splits import coast as C
    from pipeline.atlas.splits import shore as _shore
    from pipeline.atlas.splits import tidal as _tidal
    from pipeline.common.io.serialize import read_artifact
    from pipeline.common.models import NodeKind
    from project_config import get_config
    t0 = time.time()
    atlas = Path(a.atlas)
    graph = read_artifact(str(atlas / "graph.pkl"))
    geoms = read_artifact(str(atlas / "geometries.pkl"))
    registry = load_registry(atlas / "registry.json")
    print(f"atlas loaded {time.time() - t0:.0f}s", flush=True)
    limits = _tidal.load()
    rank1 = C.rank1(limits, C.load_authority())
    id_of = {r["blk"]: r["split_id"] for r in _tidal.read_resolved(atlas) if r.get("limit") == "cut"}
    tidal = _shore.reach_tidal(registry, graph, rank1, id_of)
    governed = _shore.owner_limited(graph, tidal, rank1)
    tidal_blks = {graph.nodes[s].blk for s in tidal if s in graph.nodes and graph.nodes[s].blk}
    lines: dict[str, list] = {}
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk:
            lines.setdefault(n.blk, []).append(n)
    for v in lines.values():
        v.sort(key=lambda n: (n.down_m, n.up_m))
    chains = {}
    for c in _shore.graph_lines(graph, geoms):
        if c.blk not in chains or c.mouth_measure < chains[c.blk].mouth_measure:
            chains[c.blk] = c
    print(f"tidal {len(tidal)} sections on {len(tidal_blks)} lines; governed {len(governed)}; {time.time() - t0:.0f}s",
          flush=True)
    cg = C.CoastGeometry(get_config().fwa_coast_gpkg)
    ends = sorted(b for b, ns in lines.items() if not graph.down_adj.get(ns[0].node_id) and b in chains)
    sea = cg.in_sea([shapely.get_point(chains[b].geometry, 0) for b in ends])
    sea_mouth = frozenset(b for b, s in zip(ends, sea) if s)
    print(f"{len(ends)} lines end with no outlet, {len(sea_mouth)} of them in the sea; {time.time() - t0:.0f}s", flush=True)
    e1450 = edge1450_from_gpkg(get_config().fwa_data_gpkg, {b: ns[0].down_m for b, ns in lines.items()})
    print(f"edge-1450 crossings at {len(e1450)} line mouths; {time.time() - t0:.0f}s", flush=True)
    cache: dict = {}

    def geo(blk):
        if blk not in cache:
            ch = chains.get(blk)
            if ch is None:
                cache[blk] = (None, float("inf"))
            else:
                top = ch.mouth_measure + ch.geometry.length
                cache[blk] = C.geometric_crossing(ch.geometry, ch.mouth_measure, top,
                                                  cg.target(ch.geometry, tidal_blks, blk))
        return cache[blk]
    now = today(atlas)

    def keep_of(b):
        return dict(now.get(b) or {"keep": "fresh"})
    forced = {b: {**keep_of(b), "why": w} for b, w in HELD_BY_NAME.items() if b in lines and b not in governed}
    old = load(a.out) if Path(a.out).exists() else {}
    decided = {b: r for b, r in old.items() if r.get("decided")}
    existing, leaves_at = _shore.standing_boundaries(graph), _shore.edge_leave_measures(graph, geoms)

    def run(held):
        return C.coast_rule(graph, tidal=tidal, e1450=e1450, geo=geo, sea_mouth=sea_mouth, governed=governed,
                            held=held, existing=existing, leaves_at=leaves_at)
    pts, whole, rows, owner = run({**forced, **decided})
    # ESTUARIES DRAWN AS RIVER POLYGONS (owner item, T678): a line today's atlas made tidal (whole, or cut) that the
    # coast cut changes in kind (fresh, or whole <-> cut) or moves more than AGREE_M, and that lies in an FWA river
    # polygon: the polygon reading and the coast reading disagree there, and the owner decides. Today's result kept.
    new_cut = {p.blk: p.route_measure for p in pts}
    new_whole = {w["blk"] for w in whole}
    estuary = {}
    for b, t in sorted(now.items()):
        if b in forced or b in decided or b in governed or b not in chains:
            continue
        kind = "whole" if b in new_whole else ("cut" if b in new_cut else "fresh")
        moved = t["keep"] == "cut" and kind == "cut" and abs(new_cut[b] - t["cut_m"]) > C.AGREE_M
        if (kind == t["keep"] and not moved) or not cg.touches_river(chains[b].geometry):
            continue
        estuary[b] = {**t, "why": C.OWNER_ESTUARY + f" (the coast cut says: {kind}"
                      + (f" at {new_cut[b]:.1f}" if kind == "cut" else "") + ")"}
    print(f"estuaries drawn as river polygons: {len(estuary)} line(s) held; {time.time() - t0:.0f}s", flush=True)
    for _ in range(3):                     # holding a line can change what flows into it: settle
        pts, whole, rows, owner = run({**forced, **decided, **estuary})
        if not owner:
            break
        estuary.update({r["blk"]: {**keep_of(r["blk"]), "why": r["why"]} for r in owner})
    print(f"rule: {len(pts)} cuts, {len(whole)} whole, {len(rows)} rows, {len(owner)} owner; {time.time() - t0:.0f}s",
          flush=True)
    to_ll = Transformer.from_crs(3005, 4326, always_xy=True)

    def at(b):
        ch = chains.get(b)
        if ch is None:
            return None
        p = shapely.get_point(ch.geometry, 0)
        return [round(v, 6) for v in to_ll.transform(p.x, p.y)]
    out = []
    for b, f in sorted({**forced, **estuary}.items()):
        ce, (cgm, _) = e1450.get(b), geo(b)
        row = {"blk": b, "name": next((n.display_name for n in lines[b] if n.display_name), ""), **f, "at": at(b),
               "methods": {"edge1450_m": ce, "geometric_m": None if cgm is None else round(cgm, 2)}}
        if b in DECIDED_WHOLE:
            row.update(keep="whole", decided="2026-11-11", why=DECIDED_WHOLE[b])
            row.pop("cut_m", None)
        out.append(row)
    if owner:
        raise SystemExit(f"coast_held: {len(owner)} owner line(s) did not settle: {[r['blk'] for r in owner[:5]]}")
    for b, r in sorted(decided.items()):
        if b not in {x["blk"] for x in out}:
            out.append(r)
    out.sort(key=lambda r: r["blk"])
    Path(a.out).write_text(json.dumps({"_about": ABOUT, "lines": out}, indent=1, ensure_ascii=False) + "\n",
                           encoding="utf-8")
    print(f"wrote {a.out}: {len(out)} line(s) held for the owner")
    if a.report:
        d = Path(a.report)
        d.mkdir(parents=True, exist_ok=True)
        (d / "coast_rows.json").write_text(json.dumps(rows, indent=0) + "\n")
        (d / "coast_whole.json").write_text(json.dumps(whole, indent=0) + "\n")
        (d / "coast_cuts.json").write_text(json.dumps([{"blk": p.blk, "m": p.route_measure} for p in pts], indent=0) + "\n")
        (d / "coast_owner.json").write_text(json.dumps(owner, indent=0) + "\n")


def edge1450_from_gpkg(gpkg, mouths) -> dict[str, float]:
    """`coast.edge1450_tops` read off the gpkg's streams (EDGE_TYPE 1450 rows only; a run counts only when it starts at
    the line's mouth in `mouths`, `{blk: mouth measure}`)."""
    from types import SimpleNamespace

    import pyogrio

    from pipeline.atlas.splits import coast as C
    from pipeline.common.fwa_edge_types import WATER_CROSSING
    df = pyogrio.read_dataframe(gpkg, layer="streams", read_geometry=False,
                                columns=["BLUE_LINE_KEY", "DOWNSTREAM_ROUTE_MEASURE", "UPSTREAM_ROUTE_MEASURE", "EDGE_TYPE"],
                                where=f"EDGE_TYPE = {int(WATER_CROSSING)}")
    rows = [SimpleNamespace(blk=str(int(b)), edge_type=WATER_CROSSING, down_m=float(d), up_m=float(u))
            for b, d, u in zip(df.BLUE_LINE_KEY, df.DOWNSTREAM_ROUTE_MEASURE, df.UPSTREAM_ROUTE_MEASURE)]
    first: dict[str, float] = {}
    for r in rows:
        first[r.blk] = min(first.get(r.blk, r.down_m), r.down_m)
    tops = C.edge1450_tops(rows)
    return {b: t for b, t in tops.items() if b in mouths and abs(first[b] - mouths[b]) <= 0.5}


if __name__ == "__main__":
    main()
