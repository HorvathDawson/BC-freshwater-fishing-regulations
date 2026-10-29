"""THE WORDS WIN (owner 2026-10-27): where the book's / DFO's words NAME the tidal boundary's landmark
(a bridge, a rail line, a dyke, a sign, a dam), the proposal goes AT THE LANDMARK — not at the
polygon's crossing, the FWA mouth or the DEM limit. Those stay as ALTERNATES the owner can switch to.

    # 1. the OSM ways around every words-candidate (Overpass, network; cached, re-run only to refresh)
    .venv/bin/python -m pipeline.tools.audit.tidal_words osm
    # 2. resolve each landmark to a point on the FWA line, move the proposal there, write the alternates
    .venv/bin/python -m pipeline.tools.audit.tidal_words apply [--dry-run]
    # 3. then re-run the pre-verify for the moved ones (printed by `apply`):
    .venv/bin/python -m pipeline.tools.audit.tidal_preverify --ids <ids>

Run order after a regeneration: tidal_candidates -> tidal_triage triage -> tidal_words apply ->
tidal_preverify. Idempotent: `apply` keeps the pre-words proposal in `proposed_before_words` and starts
from it, so a second run moves nothing.

THE LANDMARKS are resolved here, one row per candidate (`LANDMARKS`), from what the words say, because
the candidate generator's locator picks the crossing NEAREST THE POLYGON (so the polygon chose which
rail line was "the" bridge: Brunette took the SkyTrain). Here a rail bridge is the crossing of a line
whose OPERATOR the words name (CN / CP / BNSF), the lowest such crossing from the mouth; a road bridge
is the named road's crossing; a "line" is drawn from its landmark and intersected with the stream; a
curated split that IS the boundary is used as is. Every resolved point carries the verbatim words, the
source, how it was located and a confidence.

`alternates` (every candidate, words or not): the words landmark(s), every polygon crossing (the
uppermost marked), the FWA mouth, the DEM tidal limit (the pre-verify's), each {kind, label, measure,
above_mouth_m, coord, words?}. The review app shows them as selectable points; the owner's mark (the
only write) carries the one he picks.

READ ONLY on every curated input; writes only tidal-candidates.json (`proposed`, `proposed_before_words`,
`words_point`, `alternates`), tidal-osm-landmarks.json and tidal-words.md in the tidal dir.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path

import shapely
from shapely.geometry import LineString, Point

from pipeline.common.licence import odbl as _odbl
from pipeline.tools.audit.tidal_candidates import (AT_MOUTH_M, MOUTH_LABEL, REUSE_ALIAS, _to_albers,
                                                   _to_lonlat, point_at, snap)
from pipeline.tools.audit.tidal_preverify import geoms_of, load_lines

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "handoff/PLAN/curation-review/tidal"
GPKG = ROOT / "data/source/bc_fisheries_data.gpkg"
OSM_CACHE = "tidal-osm-landmarks.json"
OVERPASS = "https://overpass-api.de/api/interpreter"
#: how far above the furthest point of interest the line is read (m)
REACH_M = 4000.0
#: a landmark further than this from the stream line is not on it
SNAP_MAX_M = 400.0

SYN = "synopsis 2025-27 p.21 (PDF p.23) box \"Tidal Boundaries on Rivers in Region 2\""
CN = r"canadian national|\bcn\b|bc rail|bcr"
CP = r"canadian pacific|\bcp\b|cpkc|cp kansas"
BN = r"bnsf|burlington"

#: THE LANDMARK EACH CANDIDATE'S WORDS NAME. kind: rail (operator regex `op`, the lowest crossing from
#: the mouth unless `pick`), road (name regex `name`), split (a curated split source, by id fragment),
#: line (true west from a landmark point `from` [lon, lat], `bearing` 270), coord (a stated place,
#: snapped), keep (the words' point the generator already resolved is right: `source` kind).
#: conf: how sure the landmark IS the one the words mean (high: named + operator / name matched).
LANDMARKS: dict[str, dict] = {
    "tidal:360875983": {"water": "Capilano River", "kind": "rail", "op": CN, "words": "Capilano River: CN Railway Bridge",
                        "source": SYN, "conf": "high",
                        "also": "DFO Tidal Waters Sport Fishing Guide 2015 (Region 2 rivers): \"Capilano River: BCR Bridge\" "
                                "— the same structure (BC Rail, now CN)"},
    "tidal:356331018": {"water": "Campbell River", "kind": "rail", "op": BN, "words": "Campbell River: Burlington Northern Railway Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356353722": {"water": "Nicomekl River", "kind": "rail", "op": BN, "words": "Nicomekl River: Burlington Northern Railway Bridge",
                        "source": SYN, "conf": "high"},
    # the BNSF trestle across Mud Bay is SEAWARD of the FWA line's end: the whole stream is above it
    "tidal:356351273": {"water": "Serpentine River", "kind": "rail", "op": BN, "seaward_m": 1000.0,
                        "words": "Serpentine River: Burlington Northern Railway Bridge", "source": SYN, "conf": "medium"},
    # OSM tags no BNSF line on the Brunette: the main-line bridges at Sapperton (an untagged track and the
    # "CN New Westminster Subdivision", 16 m apart) are the only main-line pair below the SkyTrain; the
    # lower of the two is taken, medium (the CP Westminster Sub at 2412 m is CP, not BN)
    "tidal:356294414": {"water": "Brunette River", "kind": "rail", "op": BN, "fallback_ways": [93963296, 93963302],
                        "fallback_why": "OSM tags no BNSF line here: the Sapperton main-line bridge pair (untagged track + "
                                        "CN New Westminster Subdivision) taken as the BN bridge",
                        "words": "Brunette River: Burlington Northern Railway Bridge", "source": SYN, "conf": "medium"},
    # the only rail line crossing the lower Seymour (OSM: untagged branch, bridge): CN's
    "tidal:360886339": {"water": "Seymour River", "kind": "rail", "op": CN, "sole": True,
                        "words": "Seymour River: CN Railway Bridge", "source": SYN, "conf": "medium"},
    "tidal:356350005": {"water": "Nathan Creek", "kind": "rail", "op": CN, "words": "Nathan Creek: CN Railway Bridge",
                        "source": SYN, "conf": "high", "also": "DFO tidal Area 29 page: \"Nathan Creek: CNR Bridge\""},
    "tidal:356294262": {"water": "West Creek", "kind": "rail", "op": CN, "words": "West Creek: CN Railway Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356352191": {"water": "Kanaka Creek", "kind": "rail", "op": CP, "words": "Kanaka Creek: CPR Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356362384": {"water": "Stave River", "kind": "rail", "op": CP, "words": "Stave River: CPR Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356250271": {"water": "Whonnock Creek", "kind": "rail", "op": CP, "words": "Whonnock Creek: CP rail line",
                        "source": SYN, "conf": "high"},
    "tidal:356083018": {"water": "York Creek", "kind": "rail", "op": CP, "words": "York Creek: CP rail line",
                        "source": SYN, "conf": "high"},
    "tidal:356360869": {"water": "Pitt River", "kind": "rail", "op": CP, "words": "Pitt River: CPR Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356364584": {"water": "Coquitlam River", "kind": "road", "name": r"mary hill", "words": "Coquitlam River: Mary Hill Bypass Bridge",
                        "source": SYN, "conf": "high"},
    "tidal:356364114": {"water": "Fraser River", "kind": "split", "split": "fraser_river__the_cpr_bridge_at_mission",
                        "words": "Fraser River: CPR Bridge at Mission", "source": SYN, "conf": "high"},
    "tidal:360886594": {"water": "Squamish River", "kind": "line", "dyke": True,
                        "words": "Squamish River: a line running true west from the tip of the Squamish Dyke (\"Windsurfer Spit\")",
                        "source": SYN, "conf": "medium"},
    "tidal:360857589": {"water": "Chapman Creek", "kind": "road", "name": r"\b101\b|sunshine coast highway",
                        "words": "Chapman Creek: Highway 101 Bridge (DFO tidal area page; the notice: signs located below the Hwy 101 Bridge)",
                        "source": "DFO tidal Area 16 page", "conf": "high"},
    "tidal:354154938": {"water": "Nanaimo River", "kind": "road", "name": r"cedar road",
                        "words": "Nanaimo River: Subarea 17-14 takes the Nanaimo River from the Cedar Bridge down",
                        "source": "DFO tidal Area 17 page", "conf": "high"},
    "tidal:354152994": {"water": "Salmon River", "kind": "road", "name": r"sayward road", "pick": "lowest",
                        "words": "Salmon River: Salmon Bay (tidal) runs up to \"the downstream side of the first bridge upstream on the Salmon River\"",
                        "source": "DFO tidal Area 13 page (Subarea 13-34)", "conf": "high"},
    "tidal:354154936": {"water": "Nimpkish River", "kind": "road", "name": r"island highway|\b19\b", "offset_m": 100.0,
                        "words": "Nimpkish River: the tidal water boundary signs approximately 100 m upstream of the Highway 19 bridge",
                        "source": "DFO tidal Area 12 page", "conf": "high"},
    "tidal:354153927": {"water": "Sooke River", "kind": "road", "name": r"sooke road|\b14\b", "pick": "lowest",
                        "words": "The designated tidal boundary is located at the Sooke River Bridge",
                        "source": "web (DFO / Sooke River sport-fishing guidance, tidal-web.json)", "conf": "medium"},
    "tidal:354135941": {"water": "Somass River", "kind": "split", "split": "somass_river__tidal_boundary_at_papermill_dam",
                        "words": "Somass River: the tidal boundary signs at the top of the rapids at Paper Mill Dam",
                        "source": "DFO tidal Area 23 page (Subarea 23-1); curated split somass_river__tidal_boundary_at_papermill_dam",
                        "conf": "high"},
    "tidal:354152785": {"water": "Qualicum River", "kind": "split", "split": "qualicum_river__hwy_19a_bridge",
                        "words": "Hwy 19A bridge, noted \"at the tidal boundary (m~108 above mouth)\"",
                        "source": "curated split qualicum_river__hwy_19a_bridge", "conf": "medium"},
    "tidal:354154635": {"water": "Campbell River", "kind": "split", "split": "campbell_river__cement_block_boundary_sign",
                        "words": "cement block sign, noted \"where the Campbell R enters the tidal zone\"",
                        "source": "curated split campbell_river__cement_block_boundary_sign", "conf": "medium"},
    "tidal:360872960": {"water": "Tlell River", "kind": "split", "split": "tlell_river__tidal_sign_1_5_km_above_hwy_16_bridge",
                        "words": "Tlell River: tidal boundary sign 1.5 km above the Hwy 16 bridge (synopsis Tlell row; "
                                 "DFO Area 2 page says ~1 km — kept as an alternate)",
                        "source": "synopsis Tlell River row; curated split tlell_river__tidal_sign_1_5_km_above_hwy_16_bridge",
                        "conf": "medium"},
    "tidal:360887278": {"water": "Skeena River", "kind": "keep", "source_kind": "dfo_tidal_area",
                        "words": "Skeena River: a line drawn from Mowitch Point to Veitch Point",
                        "source": "DFO tidal Area 4 page", "conf": "high"},
    "tidal:354154440": {"water": "Chemainus River", "kind": "coord", "at": [-123.6909, 48.8883],
                        "words": "following the shoreline northerly to a boundary sign on the Chemainus River near the Bald Eagle "
                                 "Campground, thence across the river to the opposite bank",
                        "source": "DFO Area 17 closures PDF (archived 2026); DFO guide 2007-09 p.46",
                        "conf": "low", "approx": "the campground is pinned, not the sign itself"},
    "tidal:354155148": {"water": "Cowichan River", "kind": "keep", "source_kind": "dfo_tidal_area",
                        "words": "Cowichan River: the Tidal Water Boundary in Cowichan Bay runs from an orange boundary sign at "
                                 "the Hecate boat ramp to one near Green Point (seaward of the mouth: the river is all above it)",
                        "source": "DFO fishery notice 2015 (owner-supplied)", "conf": "high"},
}
#: candidates whose words name a boundary that cannot be placed (relative distance, no landmark):
#: listed in the report, the proposal stays
UNPLACEABLE = {
    "tidal:360881072": "Copper Creek: \"6 km downstream of the second bridge\" — relative only",
    "tidal:360873432": "Mamin River: \"~10 km downstream of the third bridge\" — relative only",
    "tidal:354155031": "Marble River: \"the Marble River tidal boundary\" — named, not located",
    "tidal:360863736": "Deena Creek: \"second bridge\" is a no-fishing boundary, not the tidal one",
    "tidal:360883982": "Dean River: \"downstream to the tidal boundary\" — names no landmark",
    "tidal:354153710": "Oyster River: \"yellow triangle signs at the estuary\" — not located (low-confidence web)",
}
CONF = {"high": 3, "medium": 2, "low": 1}


# --------------------------------------------------------------------------- the stream lines

def _lines(cands: list[dict], gpkg: Path):
    """blk -> (segs, geoms) for the lower reach of each candidate's line."""
    top: dict[int, float] = {}
    for c in cands:
        b = int(c["blk"])
        ms = [c["mouth"]["measure"], (c.get("polygon") or {}).get("measure") or 0.0,
              (c.get("proposed_before_words") or c["proposed"])["measure"]]
        top[b] = max(top.get(b, 0.0), max(m for m in ms if m is not None) + REACH_M)
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    lines = load_lines(con, set(top), top)
    out = {b: (segs, geoms_of(con, segs)) for b, segs in lines.items()}
    con.close()
    return out


def _bbox_ll(segs, geoms) -> tuple[float, float, float, float]:
    ll = _to_lonlat()
    g = shapely.union_all([geoms[s["fid"]] for s in segs if s["fid"] in geoms])
    x0, y0, x1, y1 = g.buffer(600).bounds
    a, b = ll(x0, y0)
    c, d = ll(x1, y1)
    return (min(b, d), min(a, c), max(b, d), max(a, c))


# --------------------------------------------------------------------------- OSM

def fetch_osm(cands: list[dict], gpkg: Path, out: Path) -> dict:
    """One Overpass call per candidate's lower reach: rail lines, named roads, dykes / spits."""
    lines = _lines(cands, gpkg)
    prev = json.loads(out.read_text()) if out.exists() else {}
    ways: dict[str, dict] = prev.get("ways") or {}
    done = set(prev.get("fetched") or [])
    for c in cands:
        if c["id"] in done:
            continue
        segs, geoms = lines.get(int(c["blk"]), ([], {}))
        if not segs or LANDMARKS[c["id"]]["kind"] not in ("rail", "road", "line"):
            continue
        s, w, n, e = _bbox_ll(segs, geoms)
        if LANDMARKS[c["id"]].get("dyke"):
            s -= 0.03
        bb = f"{s:.5f},{w:.5f},{n:.5f},{e:.5f}"
        q = (f"[out:json][timeout:90];(way[\"railway\"~\"^(rail|disused|abandoned|light_rail|subway)$\"]({bb});"
             f"way[\"highway\"][\"name\"]({bb});way[\"highway\"][\"ref\"]({bb});"
             f"way[\"man_made\"=\"dyke\"]({bb});way[\"name\"~\"[Ss]pit|[Dd]yke\"]({bb}););out geom tags;")
        req = urllib.request.Request(OVERPASS, data=urllib.parse.urlencode({"data": q}).encode(),
                                     headers={"User-Agent": "bcfish-tidal-words/1.0"})
        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=180) as r:
                    doc = json.loads(r.read())
                break
            except (OSError, ValueError) as e:            # 429 / 502 / 504 / timeout: back off, retry
                print(f"  {c['water']}: {e} (retry {attempt + 1})", flush=True)
                time.sleep(20 * (attempt + 1))
        else:
            continue
        for el in doc.get("elements") or []:
            if el.get("type") == "way" and el.get("geometry"):
                ways[str(el["id"])] = {"id": el["id"], "tags": el.get("tags") or {},
                                       "geometry": [[round(p["lon"], 7), round(p["lat"], 7)] for p in el["geometry"]]}
        done.add(c["id"])
        print(f"  {c['water']}: {len(doc.get('elements') or [])} ways", flush=True)
        res = {"_about": "OSM ways (Overpass, overpass-api.de) around each tidal candidate whose words name a "
                         "rail / road / line landmark; read by pipeline.tools.audit.tidal_words apply. Refresh: delete "
                         "and re-run `tidal_words osm` (it resumes: candidates in `fetched` are skipped).",
               "_licence": _odbl("every way in `ways` (id, tags, geometry)"),
               "fetched": sorted(done), "ways": ways}
        out.write_text(json.dumps(res, ensure_ascii=False) + "\n")   # after each: a failed run resumes
        time.sleep(2)
    return {"fetched": sorted(done), "ways": ways}


def _osm_geoms(osm: dict):
    al = _to_albers()
    out = []
    for w in osm["ways"].values():
        pts = [al(*p) for p in w["geometry"]]
        if len(pts) >= 2:
            out.append({**w, "g": LineString(pts)})
    return out


def _label(tags: dict) -> str:
    return " ".join(str(tags[k]) for k in ("name", "ref", "operator") if tags.get(k)) or \
        str(tags.get("railway") or tags.get("highway") or tags.get("man_made") or "way")


def crossings(segs, geoms, ways, pred) -> list[dict]:
    line = shapely.union_all([geoms[s["fid"]] for s in segs if s["fid"] in geoms])
    hits = []
    for w in ways:
        if not pred(w["tags"]) or not w["g"].intersects(line):
            continue
        x = w["g"].intersection(line)
        for p in (getattr(x, "geoms", None) or [x]):
            p = p if p.geom_type == "Point" else p.representative_point()
            m, _ = snap(segs, geoms, p)
            hits.append({"measure": round(m, 1), "way": w["id"], "label": _label(w["tags"]),
                         "bridge": w["tags"].get("bridge") not in (None, "no")})
    hits.sort(key=lambda h: (h["measure"], not h["bridge"]))
    uniq = []
    for h in hits:
        if uniq and abs(uniq[-1]["measure"] - h["measure"]) < 15:
            continue
        uniq.append(h)
    return uniq


_LABELS: dict | None = None


def _split_labels() -> dict:
    """splits.json id -> label (read only)."""
    global _LABELS
    if _LABELS is None:
        doc = json.loads((ROOT / "data/curated/waters/splits.json").read_text())
        _LABELS = {sp["id"]: sp.get("label") or sp["id"] for wb in doc.get("waterbodies") or []
                   for sp in wb.get("splits") or [] if sp.get("id")}
    return _LABELS


# --------------------------------------------------------------------------- resolving a landmark

def _rail_pred(op: str):
    rx = re.compile(op, re.I)

    def ok(t):
        if t.get("railway") not in ("rail", "disused", "abandoned"):
            return False
        if t.get("service") in ("yard", "siding", "spur", "crossover"):
            return False
        return bool(rx.search(" ".join(str(t.get(k, "")) for k in ("operator", "owner", "name", "network"))))
    return ok


def _road_pred(name: str):
    rx = re.compile(name, re.I)

    def ok(t):
        return "highway" in t and t["highway"] not in ("footway", "path", "cycleway", "steps", "track") \
            and bool(rx.search(" ".join(str(t.get(k, "")) for k in ("name", "ref"))))
    return ok


def resolve(c: dict, spec: dict, segs, geoms, ways) -> dict | None:
    """The landmark the words name, as a point on the line: {measure, coord, located_by, osm?}."""
    ll = _to_lonlat()
    m0 = float(c["mouth"]["measure"])

    def out(m, how, **kw):
        p = point_at(segs, geoms, m)
        if p is None:
            return None
        return {"measure": round(m, 1), "above_mouth_m": round(m - m0, 1),
                "coord": [round(v, 6) for v in ll(p.x, p.y)], "located_by": how, **kw}

    k = spec["kind"]
    if k in ("rail", "road"):
        pred = _rail_pred(spec["op"]) if k == "rail" else _road_pred(spec["name"])
        hits = crossings(segs, geoms, ways, pred)
        how = ""
        if not hits and spec.get("fallback_ways"):
            fw = set(spec["fallback_ways"])
            hits = \
                crossings(segs, geoms, [w for w in ways if w["id"] in fw], lambda t: True)
            how = f"; {spec['fallback_why']}"
        if not hits and spec.get("sole"):
            rails = crossings(segs, geoms, ways, lambda t: t.get("railway") == "rail"
                              and t.get("service") not in ("yard", "siding", "spur"))
            if len(rails) == 1:
                hits, how = rails, "; the only rail line crossing the lower stream (OSM names no operator)"
        if not hits and spec.get("seaward_m"):
            line = shapely.union_all([geoms[s["fid"]] for s in segs if s["fid"] in geoms])
            mp = point_at(segs, geoms, m0)
            near = [w for w in ways if pred(w["tags"]) and mp is not None and w["g"].distance(mp) <= spec["seaward_m"]
                    and not w["g"].intersects(line)]
            if near:
                w = min(near, key=lambda w: w["g"].distance(mp))
                return out(m0, f"OSM rail '{_label(w['tags'])}' (way {w['id']}) passes {w['g'].distance(mp):.0f} m "
                               "SEAWARD of the FWA mouth without crossing the line: the whole stream is above it",
                           osm_way=w["id"])
        if not hits:
            return None
        best = hits[0]                                  # the lowest from the mouth
        off = spec.get("offset_m") or 0.0
        return out(best["measure"] + off,
                   f"OSM {k} crossing '{best['label']}' (way {best['way']}) @ {best['measure']:.0f} m"
                   + (f" + {off:.0f} m upstream" if off else "") + how,
                   osm_way=best["way"], crossings=[f"{h['label']} @ {h['measure']:.0f}" for h in hits[:6]])
    if k == "split":
        s = next((s for s in c.get("sources") or [] if s["kind"] == "curated_split"
                  and spec["split"] in s.get("source", "")), None)
        if s is None or s.get("measure") is None:
            return None
        return out(float(s["measure"]), f"curated split {spec['split']} (splits.resolved.json route measure)")
    if k == "keep":
        s = next((s for s in c.get("sources") or [] if s["kind"] == spec["source_kind"]
                  and s.get("measure") is not None), None)
        return out(float(s["measure"]), s["located_by"]) if s else None
    if k == "coord":
        al = _to_albers()
        m, d = snap(segs, geoms, Point(*al(*spec["at"])))
        if d > SNAP_MAX_M:
            return None
        return out(m, f"the stated place {spec['at'][1]:.4f}, {spec['at'][0]:.4f} snapped {d:.0f} m to the line"
                   + (f" ({spec['approx']})" if spec.get("approx") else ""))
    if k == "line":
        # the dyke's seaward tip: the southernmost vertex of the dyke / spit ways near the river
        line = shapely.union_all([geoms[s["fid"]] for s in segs if s["fid"] in geoms])
        # the spit itself: the dyke / "Spit ..." ways beside the river (not inland dyke trails)
        dykes = [w for w in ways if (w["tags"].get("man_made") == "dyke"
                                     or re.search(r"\bspit\b", w["tags"].get("name", ""), re.I))
                 and w["g"].distance(line) < 300]
        if not dykes:
            return None
        tip_w, tip = None, None
        for w in dykes:
            for x, y in w["g"].coords:
                if tip is None or y < tip[1]:
                    tip_w, tip = w, (x, y)
        tlon, tlat = ll(*tip)
        al = _to_albers()
        # TRUE west: along the tip's parallel (drawn in lon/lat, not along the Albers grid)
        west = LineString([al(tlon - 0.0008 * i, tlat) for i in range(0, 101)])
        x = west.intersection(line)
        if x.is_empty:
            # the line passes seaward of the stream's mouth: the whole stream is above it
            return out(m0, f"the line true west from the dyke tip ({tlat:.5f}, {tlon:.5f}, OSM way {tip_w['id']} "
                           f"'{_label(tip_w['tags'])}') passes seaward of the FWA mouth", osm_way=tip_w["id"])
        p = x if x.geom_type == "Point" else min(getattr(x, "geoms", [x]), key=lambda g: g.distance(Point(tip)))
        m, _ = snap(segs, geoms, p if p.geom_type == "Point" else p.representative_point())
        return out(m, f"the line true west from the dyke tip ({tlat:.5f}, {tlon:.5f}, OSM way {tip_w['id']} "
                      f"'{_label(tip_w['tags'])}') crosses the line", osm_way=tip_w["id"], tip=[round(tlon, 6), round(tlat, 6)])
    return None


# --------------------------------------------------------------------------- alternates

def line_at(segs, geoms):
    """measure -> [lon, lat] on the line (None off it): the `at` `alternates` takes."""
    ll = _to_lonlat()

    def at(m):
        p = point_at(segs, geoms, m) if segs else None
        return [round(v, 6) for v in ll(p.x, p.y)] if p is not None else None
    return at


def alternates(c: dict, at, pv: dict | None) -> list[dict]:
    """EVERY POINT THE OWNER CAN SWITCH TO (the review app's Tidal tab calls this too): the words'
    landmark(s) (`supported`: the words say so), every polygon crossing (the uppermost marked), the FWA
    mouth, the DEM tidal limit (the pre-verify's). `at(measure) -> [lon, lat]` places a measure on the
    stream. Deduplicated by kind + measure (within 5 m); `proposed` marks the one the proposal sits on."""
    m0 = float(c["mouth"]["measure"])
    alts: list[dict] = []

    def add(kind, label, m, coord=None, **kw):
        coord = coord or at(m)
        if coord is None or m is None:
            return
        if any(a["kind"] == kind and abs(a["measure"] - m) < 5 for a in alts):
            return
        alts.append({"id": f"{kind}{sum(a['kind'] == kind for a in alts)}", "kind": kind, "label": label,
                     "measure": round(float(m), 1), "above_mouth_m": round(float(m) - m0, 1),
                     "coord": coord, **kw})

    wp = c.get("words_point")
    if wp:
        add("words", f"the words: {wp['words'][:80]}", wp["measure"], wp["coord"], words=wp["words"],
            source=wp["source"], located_by=wp["located_by"], confidence=wp["confidence"], supported=True)
    for s in c.get("sources") or []:
        if s.get("measure") is not None and s["kind"] != "tidal_gauge":
            add("words", f"{s['kind']}: {s['words'][:70]}", float(s["measure"]), s.get("coord"),
                words=s["words"], source=s.get("source"), located_by=s.get("located_by"),
                supported=s["kind"] != "curated_split_offset")
    for w in (c.get("triage") or {}).get("web") or []:
        loc = w.get("located") or {}
        if loc.get("measure") is not None and w.get("confidence") in ("high", "medium"):
            add("words", f"web: {w['marker'][:70]}", float(loc["measure"]), loc.get("coord"),
                words=w.get("quote") or w["marker"], source=w.get("site"), located_by=loc.get("located_by"),
                supported=True)
    poly = c.get("polygon") or {}
    cuts = sorted(poly.get("cuts") or ([poly["measure"]] if poly.get("measure") is not None else []))
    top = poly.get("measure")
    for m in cuts:
        up = top is not None and abs(m - top) < 1
        add("polygon", "polygon crossing" + (" (uppermost — the polygon's boundary)" if up else ""), m,
            poly.get("coord") if up else None, uppermost=up)
    add("mouth", "FWA mouth", m0, c["mouth"].get("coord"))
    lim = (pv or {}).get("dem_limit_m")
    if lim is None and (pv or {}).get("delta_m") is not None:
        # an older pre-verify file: delta = proposal - limit, against the proposal it judged
        lim = float(pv["judged_above_mouth_m"] if pv.get("judged_above_mouth_m") is not None
                    else c["proposed"]["above_mouth_m"]) - float(pv["delta_m"])
    if lim is not None:
        add("dem", f"DEM tidal limit (pre-verify: streambed above {pv.get('threshold_m', 4)} m)", m0 + float(lim),
            pv.get("dem_coord") if pv.get("dem_limit_m") is not None else None,
            status=pv.get("status"), why=pv.get("why"))
    pr = c["proposed"]
    for a in alts:
        a["proposed"] = abs(a["measure"] - pr["measure"]) < 1
    if not any(a["proposed"] for a in alts) and pr.get("coord"):
        # the proposal sits on none of them (a reused curated split, an owner decision): list it too
        alts.insert(0, {"id": "proposed0", "kind": "proposed", "label": f"the proposal: {pr['split_name']}",
                        "measure": round(float(pr["measure"]), 1), "above_mouth_m": pr["above_mouth_m"],
                        "coord": pr["coord"], "proposed": True})
    return alts


# --------------------------------------------------------------------------- apply

def apply(doc: dict, osm: dict, pv_items: dict, gpkg: Path) -> list[dict]:
    """Moves each words candidate's proposal to its landmark and writes `alternates` on EVERY candidate.
    Returns the moves."""
    cands = doc["candidates"]
    lines = _lines(cands, gpkg)
    ways = _osm_geoms(osm)
    moves = []
    for c in cands:
        segs, geoms = lines.get(int(c["blk"]), ([], {}))
        spec = LANDMARKS.get(c["id"])
        if spec is None or not segs:
            c["alternates"] = alternates(c, line_at(segs, geoms), pv_items.get(c["id"]))
            continue
        base = c.get("proposed_before_words") or c["proposed"]
        loc = resolve(c, spec, segs, geoms, ways)
        if loc is None:
            c["words_point"] = None
            c["alternates"] = alternates(c, line_at(segs, geoms), pv_items.get(c["id"]))
            moves.append({"id": c["id"], "water": c["water"], "moved_m": None, "why": "landmark not resolved"})
            continue
        c["words_point"] = {**loc, "words": spec["words"], "source": spec["source"], "confidence": spec["conf"],
                            **({"also": spec["also"]} if spec.get("also") else {})}
        c["proposed_before_words"] = base
        dec = ((c.get("triage") or {}).get("decision") or {}).get("boundary")
        if dec and dec != "source_text":
            # the owner decided another point (Cowichan: the river mouth): his call stands
            c["proposed"] = base
            moved = 0.0
        else:
            m0 = float(c["mouth"]["measure"])
            above = round(loc["measure"] - m0, 1)
            landmark = spec["words"].split(": ", 1)[-1] if spec["kind"] in ("rail", "road") else None
            reuse = base.get("reuse") if base.get("reuse") and abs(base["measure"] - loc["measure"]) <= 25 else None
            if reuse is None and spec["kind"] == "split":
                # the landmark IS a curated split: the proposal reuses it (alias, no new cut)
                lab = _split_labels().get(spec["split"], spec["split"])
                reuse = {"split_id": spec["split"], "label": lab, "distance_m": 0.0, "measured": "along the line",
                         "split_measure": loc["measure"], "tidal_measure": loc["measure"], "alias": REUSE_ALIAS}
                base_r = {**base, "measure": loc["measure"], "coord": loc["coord"], "reuse": reuse,
                          "split_name": f"{c['water']}: {lab}"}
            else:
                base_r = base
            name = base_r["split_name"] if reuse else (
                f"{c['water']}: {landmark} (tidal boundary)" if landmark and len(landmark) <= 60
                else f"{c['water']}: {MOUTH_LABEL}" if above <= AT_MOUTH_M else f"{c['water']}: tidal boundary")
            aliases = list(dict.fromkeys([*(base.get("aliases") or []), REUSE_ALIAS,
                                          *([landmark] if landmark else [])]))
            c["proposed"] = {**base_r, "measure": base_r["measure"] if reuse else loc["measure"],
                             "coord": base_r["coord"] if reuse else loc["coord"],
                             "above_mouth_m": round((base_r["measure"] if reuse else loc["measure"]) - m0, 1),
                             "basis": f"the words ({spec['source']}): {loc['located_by']}"
                                      + (f"; reuses curated split {reuse['split_id']}" if reuse else ""),
                             "split_label": name.split(": ", 1)[1], "split_name": name, "aliases": aliases}
            moved = round(c["proposed"]["measure"] - base["measure"], 1)
            # confidence: how sure the landmark is the one the words mean (the proposal stands on it now)
            c["confidence"] = spec["conf"]
            c["why"] = (f"the words name the landmark ({spec['words'][:80]}) and it is located; the words win over "
                        f"the polygon (owner 2026-10-27)")
        c["alternates"] = alternates(c, line_at(segs, geoms), pv_items.get(c["id"]))
        moves.append({"id": c["id"], "water": c["water"], "from_m": base["above_mouth_m"],
                      "to_m": c["proposed"]["above_mouth_m"], "moved_m": moved, "located_by": loc["located_by"],
                      "conf": spec["conf"]})
    return moves


def report(moves: list[dict]) -> str:
    L = ["# Tidal: the words win (owner 2026-10-27)", "",
         "Generated by `python -m pipeline.tools.audit.tidal_words apply`. Where the book's / DFO's words name the "
         "tidal boundary's landmark, the proposal is AT the landmark; the polygon crossing(s), the FWA mouth and the "
         "DEM limit stay as alternates the owner can switch to in the Tidal tab.", "",
         "| stream | was (m above mouth) | now | moved | located by | conf |", "|---|---|---|---|---|---|"]
    for m in sorted(moves, key=lambda m: -abs(m.get("moved_m") or 0)):
        if m.get("moved_m") is None:
            L.append(f"| {m['water']} ({m['id']}) | | | — | {m['why']} | |")
        else:
            L.append(f"| {m['water']} ({m['id']}) | {m['from_m']:.0f} | {m['to_m']:.0f} | {m['moved_m']:+.0f} m | "
                     f"{m['located_by']} | {m['conf']} |")
    L += ["", "## Capilano River (owner 2026-10-27: \"it should be at the CN railway bridge\")", "",
          "Synopsis 2025-27 p.21 box \"Tidal Boundaries on Rivers in Region 2\": **\"Capilano River: CN Railway "
          "Bridge\"** (DFO Tidal Waters guide 2015: \"Capilano River: BCR Bridge\" — the same structure, BC Rail now CN). "
          "The only rail line over the Capilano is CN's Squamish Subdivision (OSM way 70954081, operator Canadian "
          "National Railway, bridge=yes, 49.32206,-123.14004 to 49.32151,-123.13857). It crosses the FWA line 37 m above "
          "the FWA mouth: the river meets Burrard Inlet right under it (OSM's Capilano River way ends on the bridge, the "
          "coastline runs just seaward of it). So the proposal IS the bridge — it only looked like \"the mouth\" because "
          "it was labelled \"mouth (tidal boundary)\" / \"at the FWA mouth\" and the map opened zoomed out. It is now "
          "named \"Capilano River: CN Railway Bridge (tidal boundary)\" and the map opens on it; the polygon's 1049 m "
          "(Marine Drive) stays an alternate.", ""]
    L += ["## Words that name a boundary but cannot place it (proposal unchanged)", ""]
    L += [f"- {v}" for v in UNPLACEABLE.values()]
    return "\n".join(L) + "\n"


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    o = sub.add_parser("osm")
    o.add_argument("--out", type=Path, default=OUT)
    o.add_argument("--gpkg", type=Path, default=GPKG)
    p = sub.add_parser("apply")
    p.add_argument("--out", type=Path, default=OUT)
    p.add_argument("--gpkg", type=Path, default=GPKG)
    p.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    cpath = a.out / "tidal-candidates.json"
    doc = json.loads(cpath.read_text())
    if a.cmd == "osm":
        want = [c for c in doc["candidates"] if c["id"] in LANDMARKS]
        res = fetch_osm(want, a.gpkg, a.out / OSM_CACHE)
        print(f"{len(res['ways'])} ways -> {a.out / OSM_CACHE}")
        return
    osm = json.loads((a.out / OSM_CACHE).read_text())
    pvp = a.out / "tidal-preverify.json"
    pv = json.loads(pvp.read_text()).get("items") or {} if pvp.exists() else {}
    moves = apply(doc, osm, pv, a.gpkg)
    for m in moves:
        print(f"  {m['water']:<18} {m['id']:<17} "
              + (f"{m['from_m']:>8.0f} -> {m['to_m']:>8.0f} ({m['moved_m']:+.0f} m)  {m['located_by']}"
                 if m.get("moved_m") is not None else m["why"]))
    moved = [m["id"] for m in moves if m.get("moved_m")]
    print(f"{len(moves)} words candidates, {len(moved)} moved")
    if a.dry_run:
        return
    cpath.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    (a.out / "tidal-words.md").write_text(report(moves))
    print("re-run the pre-verify for:", " ".join(moved))


if __name__ == "__main__":
    main()
