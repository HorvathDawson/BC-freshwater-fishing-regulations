"""TIDAL CANDIDATES, PRE-VERIFIED BY A SECOND METHOD (owner 2026-10-27: "so that I have to do less review").

    .venv/bin/python -m pipeline.tools.audit.tidal_preverify \
        [--candidates handoff/PLAN/curation-review/tidal/tidal-candidates.json] \
        [--gpkg data/source/bc_fisheries_data.gpkg] [--threshold 4.0] [--calibrate]

Every tidal candidate (`tidal_candidates`, made from the DFO tidal POLYGON's crossing + the words) is
held against an independent estimate: the DEM TIDAL LIMIT. Ground elevation (AWS Terrarium tiles,
`added_streams.dem.ElevationSampler`, z13 ~19 m/px, cached in ~/.cache/terrarium) is sampled every
`STEP_M` along the stream's FWA line from its mouth upstream; the streambed profile is taken as the
running MINIMUM of everything upstream within `ENVELOPE_M` (bank pixels and canopy only ever read
HIGH, so a min-envelope is robust to them), and the DEM tidal limit is the first measure where that
envelope rises above `THRESHOLD_M` (geodetic ~MSL; BC higher high water is ~+2.5-3 m geodetic, plus
DEM noise of a metre or two over a channel).

    pre-verified  |polygon - DEM| <= tolerance = max(300 m, 25 % of the proposal's distance above the mouth)
    disagree      outside the tolerance, and the DEM is steep enough to place its limit (says which way)
    unchecked     the DEM cannot place a limit: low gradient (a +/-1.5 m threshold error moves the
                  limit by more than the tolerance), no tiles, no line geometry, a big low-gradient river

DUPLICATES: rows naming the same water (name key) whose proposed points lie within `DUP_M` of each
other (a side channel or second blue line of one river) -> `duplicate_of` the best row (confidence,
agreement, ocean-entering, longest).

READ ONLY on every input; writes handoff/PLAN/curation-review/tidal/tidal-preverify.json, which the
curation review reads as a badge. Never a mark, never splits.json.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

import numpy as np

from pipeline.tools.audit.tidal_candidates import _gpkg_geom, _to_lonlat, point_at
import shapely

ROOT = Path(__file__).resolve().parents[3]
CANDIDATES = ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"
OUT = ROOT / "handoff/PLAN/curation-review/tidal/tidal-preverify.json"
GPKG = ROOT / "data/source/bc_fisheries_data.gpkg"

THRESHOLD_M = 4.0
THRESHOLD_ERR_M = 1.5          # how wrong the threshold (datum + DEM bias) may be
STEP_M = 25.0
ENVELOPE_M = 500.0
MIN_RANGE_M = 3000.0
BEYOND_M = 2000.0
BIG_RIVER_M = 15000.0          # a proposal this far above the mouth: low-gradient big river, not placed
TOL_MIN_M = 300.0
TOL_FRAC = 0.25
DUP_M = 300.0
TRIB_RISE_M = 2.0         # a tidal-water tributary: tidal while within this of its receiving river's level
RISE_M = 2.0              # the bed must climb this much ...
RISE_OVER_M = 500.0       # ... within this far past the limit
CONTROL_SHIFT_M = 0.0
SHORE_M = 50.0            # a DEM limit this close to the mouth may be the coastline step itself
SPREAD_FRAC = 2.0         # pre-verified only when the DEM pins its limit this tightly (x tolerance)
CONF = {"high": 3, "medium": 2, "low": 1}
AGREE = {"agree": 3, "text unlocated": 2, "polygon only": 1, "disagree": 0}


def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").casefold()).strip()


def load_lines(con, blks: set[int], top: dict[int, float]) -> dict[int, list[dict]]:
    """blk -> its segments up to the measure we sample to (one attribute pass, then geometries)."""
    segs: dict[int, list[dict]] = defaultdict(list)
    q = ("select fid, BLUE_LINE_KEY, DOWNSTREAM_ROUTE_MEASURE, LENGTH_METRE from streams where "
         f"BLUE_LINE_KEY in ({','.join(str(b) for b in sorted(blks))})")
    for fid, blk, down, length in con.execute(q):
        down = down or 0.0
        if down <= top[blk]:
            segs[blk].append({"fid": fid, "down": down, "length": length or 0.0})
    return segs


def geoms_of(con, segs: list[dict]) -> dict[int, object]:
    out = {}
    fids = [s["fid"] for s in segs]
    for k in range(0, len(fids), 900):
        chunk = fids[k:k + 900]
        for fid, b in con.execute(f"select fid, geom from streams where fid in ({','.join('?' * len(chunk))})",
                                  chunk):
            g = _gpkg_geom(b)
            if g.geom_type == "MultiLineString":
                g = shapely.line_merge(g)
                if g.geom_type == "MultiLineString":
                    g = max(g.geoms, key=lambda x: x.length)
            out[fid] = g
    return out


def profile(segs, geoms, m0: float, m1: float, dem, lonlat) -> tuple[np.ndarray, np.ndarray]:
    ms, es = [], []
    for m in np.arange(m0, m1 + 0.1, STEP_M):
        p = point_at(segs, geoms, float(m))
        if p is None:
            continue
        lon, lat = lonlat(p.x, p.y)
        e = dem.elevation(lon, lat)
        if e is None:
            continue
        ms.append(float(m) - m0)
        es.append(e)
    return np.array(ms), np.array(es)


def envelope(ms: np.ndarray, es: np.ndarray) -> np.ndarray:
    """The streambed: the min of everything upstream within ENVELOPE_M (bank pixels read high)."""
    out = np.empty_like(es)
    j = 0
    for i in range(len(ms)):
        j = max(j, i)
        while j + 1 < len(ms) and ms[j + 1] - ms[i] <= ENVELOPE_M:
            j += 1
        out[i] = es[i:j + 1].min()
    return out


def limit_at(ms, env, thr) -> float | None:
    above = np.nonzero(env > thr)[0]
    return float(ms[above[0]]) if len(above) else None


def judge(c: dict, ms, es, thr: float) -> dict:
    prop = float(c["proposed"]["above_mouth_m"])
    tol = round(max(TOL_MIN_M, TOL_FRAC * prop), 1)
    base = {"method": "DEM tidal limit", "first": "polygon crossing" + (" + words" if c.get("agreement") == "agree" else ""),
            "tolerance_m": tol, "delta_m": None, "duplicate_of": None}
    if len(ms) < 5:
        return {**base, "status": "unchecked", "why": "no DEM profile (no line geometry or no tiles)"}
    env = envelope(ms, es)
    # the reference level: the sea (0 m geodetic) for an ocean mouth; the RECEIVING river's own DEM water
    # level for a stream entering tidal water (a Fraser tributary at Mission sits on a ~3-4 m river)
    if c["enters"] == "ocean":
        ref, thr = 0.0, thr
    else:
        ref = max(0.0, float(env[0]))
        thr = ref + TRIB_RISE_M
    thr = round(thr, 1)
    base["threshold_m"] = thr
    lim = limit_at(ms, env, thr)
    lo = limit_at(ms, env, thr + THRESHOLD_ERR_M)      # a higher threshold places the limit further up
    hi = limit_at(ms, env, thr - THRESHOLD_ERR_M)
    span = ms[-1]
    if lim is None:
        return {**base, "status": "unchecked",
                "why": f"low gradient: streambed stays below {thr:g} m for the whole {span/1000:.1f} km sampled "
                       f"(ends at {float(env[-1]):.1f} m) — tidal may reach further; the DEM cannot place it"}
    # how far a +/- THRESHOLD_ERR_M error moves the DEM limit (a lower threshold is crossed no later)
    spread = max(lim - hi, (span if lo is None else lo) - lim)
    # a DECISIVE limit is a real rise: the bed keeps climbing past it (a hydro-flattened river surface or
    # a step at the shoreline then flat for km is not a tidal limit the DEM can see)
    at = float(env[np.searchsorted(ms, lim)])
    past = float(env[min(np.searchsorted(ms, lim + RISE_OVER_M), len(env) - 1)])
    if past - at < RISE_M:
        return {**base, "status": "unchecked", "delta_m": round(prop - lim, 1),
                "why": f"flat DEM: the streambed passes {thr:g} m at {lim:.0f} m but climbs only {past - at:.1f} m "
                       f"in the next {RISE_OVER_M:g} m (hydro-flattened or low gradient) — not decisive"}
    delta = round(prop - lim, 1)
    base["delta_m"] = delta
    base["spread_m"] = round(spread, 1)
    if abs(delta) <= tol and spread <= SPREAD_FRAC * tol:
        return {**base, "status": "pre-verified",
                "why": f"DEM streambed rises above {thr:g} m at {lim:.0f} m above the mouth; proposal at "
                       f"{prop:.0f} m (Δ {delta:+.0f} m, tolerance {tol:.0f} m)"}
    if abs(delta) <= tol + spread:
        return {**base, "status": "unchecked",
                "why": f"low gradient: a ±{THRESHOLD_ERR_M:g} m threshold error moves the DEM limit up to "
                       f"{spread:.0f} m; DEM limit {lim:.0f} m vs proposal {prop:.0f} m — not decisive"}
    if delta < 0:
        why = (f"DEM says tidal reaches {abs(delta):.0f} m further up (streambed below {thr:g} m until "
               f"{lim:.0f} m above the mouth; proposal at {prop:.0f} m)")
    else:
        why = (f"DEM says tidal stops {delta:.0f} m short (streambed above {thr:g} m from {lim:.0f} m "
               f"above the mouth; proposal at {prop:.0f} m)")
    if delta > 0 and lim <= SHORE_M:
        why += "; the rise is AT the shoreline — on a big estuary this can be a DEM coastline step, look"
    return {**base, "status": "disagree", "why": why}


def duplicates(cands: list[dict]) -> dict[str, str]:
    """id -> the best row's id, for rows naming one water whose proposals are within DUP_M."""
    by_name: dict[str, list[dict]] = defaultdict(list)
    for c in cands:
        by_name[_key(c["water"])].append(c)

    def rank(c):
        return (CONF.get(c["confidence"], 0), AGREE.get(c.get("agreement"), 0),
                c["enters"] == "ocean", bool((c.get("triage") or {}).get("edited")), -int(c["blk"]))

    def km(a, b):
        return math.hypot((a[0] - b[0]) * 111.32 * math.cos(math.radians(a[1])), (a[1] - b[1]) * 111.32) * 1000

    out = {}
    for rows in by_name.values():
        if len(rows) < 2:
            continue
        rows = sorted(rows, key=rank, reverse=True)
        heads: list[dict] = []
        for c in rows:
            h = next((h for h in heads if km(h["proposed"]["coord"], c["proposed"]["coord"]) <= DUP_M), None)
            if h is None:
                heads.append(c)
            else:
                out[c["id"]] = h["id"]
    return out


def run(cands_path: Path, gpkg: Path, thr: float, ids: set[str] | None = None) -> dict[str, dict]:
    from pipeline.atlas.waters.added_streams.dem import ElevationSampler
    cands = json.loads(cands_path.read_text())["candidates"]
    if ids:
        cands = [c for c in cands if c["id"] in ids]
    top = {}
    for c in cands:
        b = int(c["blk"])
        m0 = float(c["mouth"]["measure"])
        prop = float(c["proposed"]["above_mouth_m"])
        top[b] = max(top.get(b, 0.0), m0 + (max(MIN_RANGE_M, prop + BEYOND_M) if prop <= BIG_RIVER_M else 0.0))
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    lines = load_lines(con, set(top), top)
    dem = ElevationSampler()
    lonlat = _to_lonlat()
    out = {}
    for n, c in enumerate(cands, 1):
        prop = float(c["proposed"]["above_mouth_m"])
        if prop > BIG_RIVER_M:
            out[c["id"]] = {"status": "unchecked", "method": "DEM tidal limit", "first": "polygon crossing",
                            "delta_m": None, "tolerance_m": None, "duplicate_of": None,
                            "why": f"big low-gradient river (proposal {prop/1000:.0f} km above the mouth): "
                                   f"the DEM cannot place a tidal limit there"}
            continue
        segs = lines.get(int(c["blk"]), [])
        geoms = geoms_of(con, segs)
        m0 = float(c["mouth"]["measure"])
        ms, es = profile(segs, geoms, m0, m0 + max(MIN_RANGE_M, prop + BEYOND_M), dem, lonlat)
        out[c["id"]] = judge(c, ms, es, thr)
        if CONTROL_SHIFT_M:
            # NEGATIVE CONTROL: the same profile judged against a proposal moved CONTROL_SHIFT_M up the line
            moved = {**c, "proposed": {**c["proposed"], "above_mouth_m": prop + CONTROL_SHIFT_M}}
            out[c["id"]]["_control"] = judge(moved, ms, es, thr)["status"]
        out[c["id"]]["_profile"] = [round(float(x), 1) for x in es[:: max(1, len(es) // 40)]] if len(es) else []
        if n % 100 == 0:
            print(f"  {n}/{len(cands)}", flush=True)
    dup = duplicates(json.loads(cands_path.read_text())["candidates"] if ids else cands)
    for i, d in dup.items():
        if i in out:
            out[i]["duplicate_of"] = d
    return out


def main(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", type=Path, default=CANDIDATES)
    ap.add_argument("--gpkg", type=Path, default=GPKG)
    ap.add_argument("--threshold", type=float, default=THRESHOLD_M)
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--ids", nargs="*")
    ap.add_argument("--control", type=float, default=0.0, help="negative control: also judge each proposal moved this far upstream")
    ap.add_argument("--profiles", type=Path, help="also dump the sampled profiles here (debug)")
    a = ap.parse_args(argv)
    global CONTROL_SHIFT_M
    CONTROL_SHIFT_M = a.control
    items = run(a.candidates, a.gpkg, a.threshold, set(a.ids) if a.ids else None)
    if a.profiles:
        a.profiles.write_text(json.dumps({k: v.get("_profile") for k, v in items.items()}))
    if a.control:
        from collections import Counter
        print("control (proposals moved", a.control, "m up):", Counter(v.get("_control") for v in items.values()))
    for v in items.values():
        v.pop("_profile", None)
        v.pop("_control", None)
    counts = {s: sum(1 for v in items.values() if v["status"] == s) for s in ("pre-verified", "disagree", "unchecked")}
    counts["duplicates"] = sum(1 for v in items.values() if v["duplicate_of"])
    doc = {"_about": ("PRE-VERIFICATION of the tidal candidates (pipeline/tools/audit/tidal_preverify.py): each "
                      "candidate's proposed tidal point (DFO tidal polygon crossing + words) held against an "
                      "independent DEM tidal limit (Terrarium z13 streambed min-envelope crossing the threshold). "
                      "A badge for the review app, never a mark; the owner still accepts each one."),
           "generated": "2026-10-27",
           "method": (f"DEM tidal limit: Terrarium z13 sampled every {STEP_M:g} m along the FWA line from the "
                      f"mouth; streambed = min of everything within {ENVELOPE_M:g} m upstream; limit = first "
                      f"measure above {a.threshold:g} m; agree within max({TOL_MIN_M:g} m, {TOL_FRAC:.0%} of the "
                      f"proposal's distance above the mouth); unchecked when a ±{THRESHOLD_ERR_M:g} m threshold "
                      f"error moves the limit more than the tolerance (low gradient)"),
           "threshold_m": a.threshold, "counts": counts, "items": items}
    a.out.write_text(json.dumps(doc, indent=1) + "\n")
    print(counts)


if __name__ == "__main__":
    main()
