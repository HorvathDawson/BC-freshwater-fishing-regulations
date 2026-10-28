"""Q154 (owner 2026-10-28): the SAMPLE the owner marks in the review app's Sloughs tab ("Small ponds (Q154)").

Read-only. Builds `sample.json` beside this file from the q154-examples.md measurement (its scratch `rows.json`
+ `live_sel.json`, one row per `river_widenings` polygon) and the FWA gpkg (outline + stream lines):

    the 20 examples of q154-examples.md §3, and 60 drawn at random (seed 154), stratified:
      caught        24  under 1 ha AND avg width <= 2 x modelled river width (not man-made): 8 per size band
                        (<0.1, 0.1-0.5, 0.5-1 ha), regions round-robin
      just missed   18  9 over the size cap (1-3 ha, ratio <= 2) + 9 over the ratio (<1 ha, ratio 2-3.5)
      clearly not   18  3 per size band (<0.1 ... >20 ha), ratio > 3.5

    python handoff/PLAN/curation-review/q154/sample.py <scratch q154 dir>
"""
from __future__ import annotations

import json
import random
import re
import sqlite3
import sys
from pathlib import Path

import shapely
from pyproj import Transformer
from shapely.ops import unary_union

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
GPKG = REPO / "data/source/bc_fisheries_data.gpkg"
MD = HERE.parent / "q154-examples.md"
CAP_HA, CAP_RATIO = 1.0, 2.0


def caught(r: dict) -> bool:
    return r["ha"] < CAP_HA and r["ratio"] is not None and r["ratio"] <= CAP_RATIO and "X" not in r["types"]


def wkb(blob):
    env = (blob[3] >> 1) & 7
    return shapely.from_wkb(bytes(blob[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def examples(R: dict) -> list[tuple[str, int, str]]:
    """(id, #, the md's own read) for the 20 table rows, matched on the map link's lat/lon."""
    at = {(round(r["lat"], 5), round(r["lon"], 5)): k for k, r in R.items()}
    out = []
    for m in re.finditer(r"^\| (\d+) \|.*?@([-\d.]+),([-\d.]+),.*\| ([^|]+) \|\s*$", MD.read_text(), re.M):
        k = at[(float(m[2]), float(m[3]))]
        out.append((k, int(m[1]), m[4].strip().replace("**", "")))
    assert len(out) == 20, len(out)
    return out


def pick(R: dict, pool: list[str], n: int, rnd: random.Random) -> list[str]:
    """n from pool, regions round-robin (1..8), random within a region."""
    by: dict[str, list[str]] = {}
    for k in sorted(pool):
        by.setdefault((R[k]["region"] or ["?"])[0], []).append(k)
    for v in by.values():
        rnd.shuffle(v)
    regs = sorted(by)
    rnd.shuffle(regs)
    out = []
    while len(out) < n and any(by.values()):
        for g in regs:
            if by[g] and len(out) < n:
                out.append(by[g].pop())
    return out


def main(scratch: Path) -> None:
    R = json.load(open(scratch / "rows.json"))
    sel = json.load(open(scratch / "live_sel.json"))
    rnd = random.Random(154)
    ex = examples(R)
    taken = {k for k, _, _ in ex}
    ok = {k: r for k, r in R.items() if k not in taken and r["ratio"] is not None}
    band = lambda lo, hi: [k for k, r in ok.items() if lo <= r["ha"] < hi]
    strata: list[tuple[str, str, list[str]]] = []
    for lo, hi in ((0, .1), (.1, .5), (.5, 1)):
        strata.append(("caught", f"{lo}-{hi} ha", pick(R, [k for k in band(lo, hi) if caught(R[k])], 8, rnd)))
    strata.append(("just missed", "over the 1 ha cap",
                   pick(R, [k for k in band(1, 3) if R[k]["ratio"] <= CAP_RATIO and "X" not in R[k]["types"]], 9, rnd)))
    strata.append(("just missed", "over the 2x width",
                   pick(R, [k for k in band(0, 1) if CAP_RATIO < R[k]["ratio"] <= 3.5 and "X" not in R[k]["types"]], 9, rnd)))
    for lo, hi in ((0, .1), (.1, .5), (.5, 1), (1, 5), (5, 20), (20, 1e9)):
        strata.append(("clearly not", f"{lo}-{hi if hi < 1e9 else ''} ha".replace("- ha", "+ ha"),
                        pick(R, [k for k in band(lo, hi) if R[k]["ratio"] > 3.5], 3, rnd)))
    sample = [(k, s, b) for s, b, ks in strata for k in ks]
    rnd.shuffle(sample)                      # mixed, so the order does not give the answer away

    db = sqlite3.connect(f"file:{GPKG}?mode=ro", uri=True)
    tr = Transformer.from_crs(3005, 4326, always_xy=True)
    ll = lambda cs: [[round(a, 6), round(b, 6)] for a, b in (tr.transform(x, y) for x, y, *_ in cs)]

    def geo(k: str) -> dict:
        wbk = int(sel[k]["wbk"])
        g = unary_union([wkb(b) for (b,) in db.execute(
            "SELECT geom FROM lakes WHERE WATERBODY_KEY=? UNION ALL SELECT geom FROM manmade WHERE WATERBODY_KEY=?",
            (wbk, wbk))])
        rings = []
        for p in getattr(g, "geoms", [g]):
            rings.append(ll(p.exterior.coords))
            rings += [ll(i.coords) for i in p.interiors]
        minx, miny, maxx, maxy = g.bounds
        pad = max(maxx - minx, maxy - miny) * 1.2 + 60
        bb = (minx - pad, miny - pad, maxx + pad, maxy + pad)
        near = g.buffer(5)
        lines = []
        for b, name in db.execute(
                "SELECT s.geom, s.GNIS_NAME FROM streams s JOIN rtree_streams_geom r ON s.fid=r.id "
                "WHERE r.maxx>=? AND r.minx<=? AND r.maxy>=? AND r.miny<=?", (bb[0], bb[2], bb[1], bb[3])):
            ln = wkb(b)
            through = ln.intersects(near)
            c = shapely.clip_by_rect(ln, *bb)
            for part in getattr(c, "geoms", [c]):
                if not part.is_empty and part.geom_type == "LineString":
                    lines.append({"coords": ll(part.coords), "through": through, "name": name})
        return {"rings": rings, "lines": lines}

    def row(k: str, src: str, stratum: str, why: str, n: int | None = None, md: str | None = None) -> dict:
        r = R[k]
        return {"id": k, "set": src, "n": n, "stratum": stratum, "why": why,
                "region": "/".join(r["region"]) or None, "river": r["river"], "order": r["order"],
                "ha": round(r["ha"], 3), "avg_width_m": round(r["sw"], 1), "length_m": round(r["sl"]),
                "elongation": round(r["el"], 1), "river_width_m": round(r["W"], 1) if r["W"] else None,
                "ratio": round(r["ratio"], 2) if r["ratio"] is not None else None,
                "man_made": "X" in r["types"], "caught": caught(r), "md_read": md,
                "lat": round(r["lat"], 6), "lon": round(r["lon"], 6), **geo(k)}

    rows = [row(k, "example", "caught" if caught(R[k]) else "not caught", f"q154-examples.md #{n}", n, md)
            for k, n, md in ex]
    rows += [row(k, "sample", s, b) for k, s, b in sample]
    doc = {"about": "Q154 sample (owner 2026-10-28): small unnamed lake polygons a river runs through "
                    "(river_widenings). Cutoff: under 1 ha AND avg width <= 2 x modelled river width, "
                    "man-made excluded. Generated by sample.py; read only.",
           "cutoff": {"max_ha": CAP_HA, "max_ratio": CAP_RATIO, "caught_total": sum(caught(r) for r in R.values()),
                      "set_total": len(R)},
           "rows": rows}
    (HERE / "sample.json").write_text(json.dumps(doc, separators=(",", ":")))
    from collections import Counter
    print(len(rows), Counter((r["set"], r["stratum"]) for r in rows), Counter(r["region"] for r in rows))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
