"""DFO SUGGESTION PRE-VERIFY (owner 2026-10-27): re-check every DFO suggestion with a SECOND method,
independent of the one that produced it, so the owner can take the agreeing ones fast.

    .venv/bin/python -m pipeline.regs.dfo_salmon.preverify            # writes dfo-preverify.json

For each suggestion (`dfo-suggestions.json`) every point is located again by a method its first one
did not use, and the two must agree within a tolerance:

    first method                        second method
    a curated cut matched by NAME       the landmark itself: OSM bridge / weir / dam / waterfall /
                                        power line near the cut (Overpass), BC GNIS mouth of the named
                                        tributary, the FWA lake polygon by name, the notice's coordinate
    split hunt from OSM                 BC MoTI road structures (bridges) or BC GNIS (falls, mouths)
    split hunt from MoTI / notice text  OSM (Overpass)
    FWA tributary mouth (topology)      BC GNIS's MOUTH point of that tributary
    OSM waterfall                       BC GNIS falls by name
    notice coordinate                   the landmark the words name (OSM / MoTI)
    an offset ("75 m below the weir")   the anchor located as above, then the distance checked
    the whole water                     the notice names no place on the water (no landmark, offset
                                        or coordinate words) and every water it names is an item

A point AGREES within its tolerance (150 m for a point landmark, 300 m for a confluence / named
place, an offset's own distance ± max(75 m, half of it)). A suggestion is "pre-verified" when every
point agrees, "disagree" when one point's second method finds the landmark elsewhere, "unchecked"
when no second method reaches a point. Duplicates: same water + same points (split ids, new
splits within 60 m) -> `duplicate_of` the canonical (the cluster's, else the active / shortest id).

READ ONLY on every input; writes `handoff/PLAN/curation-review/dfo-preverify.json`. Network reads
(Overpass, openmaps.gov.bc.ca WFS, the warm review backend for split coordinates) are cached in
`~/.cache/bcfish-preverify` (`PREVERIFY_CACHE` overrides). Never applies anything.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Optional

from pyproj import Transformer

from pipeline.regs.dfo_salmon.suggest import GENERIC, TYPE_WORDS, _norm, coords_in, tokens

ROOT = Path(__file__).resolve().parents[3]
SUGG = ROOT / "handoff/PLAN/curation-review/dfo-suggestions.json"
OUT = ROOT / "handoff/PLAN/curation-review/dfo-preverify.json"
GPKG = ROOT / "data/source/bc_fisheries_data.gpkg"
TIDAL = ROOT / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"
API = os.environ.get("CURATION_API", "http://127.0.0.1:8787")
CACHE = Path(os.environ.get("PREVERIFY_CACHE", Path.home() / ".cache" / "bcfish-preverify"))
OVERPASS = "https://overpass-api.de/api/interpreter"
WFS = "https://openmaps.gov.bc.ca/geo/pub/wfs"
GNIS = "pub:WHSE_BASEMAPPING.GNS_GEOGRAPHICAL_NAMES_SP"
MOTI = "pub:WHSE_IMAGERY_AND_BASE_MAPS.MOT_ROAD_STRUCTURE_SP"

TOL_POINT = 150.0
TOL_CONFLUENCE = 300.0
TOL_PLACE = 500.0
DUP_M = 60.0

_TO_ALB = Transformer.from_crs(4326, 3005, always_xy=True).transform
_TO_LL = Transformer.from_crs(3005, 4326, always_xy=True).transform


# --------------------------------------------------------------------------- fetch (cached)

def _get(url: str, data: Optional[bytes] = None, *, pause: float = 0.0) -> bytes:
    CACHE.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha1(url.encode() + (data or b"")).hexdigest()
    f = CACHE / key
    if f.exists():
        return f.read_bytes()
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": "bcfish-preverify/1.0"})
            body = urllib.request.urlopen(req, timeout=90).read()
            f.write_bytes(body)
            time.sleep(pause)
            return body
        except Exception:                                   # noqa: BLE001 — retry, then give up
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"fetch failed: {url[:120]}")


_OSM: list[tuple[dict, list[tuple[float, float]]]] = []


def _osm_clauses(lon: float, lat: float, r: int = 800) -> str:
    return (f"way(around:{r},{lat},{lon})[bridge];"
            f"way(around:{r},{lat},{lon})[power=line];"
            f"node(around:{r},{lat},{lon})[natural=waterfall];"
            f"nwr(around:{r},{lat},{lon})[waterway~\"^(weir|dam)$\"];"
            f"nwr(around:{r},{lat},{lon})[man_made~\"^(weir|dam)$\"];"
            f"nwr(around:{int(1.5 * r)},{lat},{lon})[name~\"hatchery|park|lodge|resort|canyon|lagoon|fishway|falls\",i];")


def prefetch(coords: list[tuple[float, float]], chunk: int = 20) -> None:
    """Every OSM feature near every point, in a few batched Overpass calls (one per `chunk` points)."""
    seen = set()
    for i in range(0, len(coords), chunk):
        part = coords[i:i + chunk]
        q = "[out:json][timeout:300];(" + "".join(_osm_clauses(lo, la) for lo, la in part) + ");out tags geom;"
        body = _get(OVERPASS, urllib.parse.urlencode({"data": q}).encode(), pause=2.0)
        for el in json.loads(body).get("elements") or []:
            k = (el["type"], el["id"])
            if k in seen:
                continue
            seen.add(k)
            pts = _el_pts(el)
            if pts:
                _OSM.append((el, pts))
        print(f"overpass {min(i + chunk, len(coords))}/{len(coords)} points, {len(_OSM)} features", flush=True)


def overpass(lon: float, lat: float, r: int = 1200) -> list[dict]:
    """The prefetched OSM features with a vertex (or segment) within `r` m of the point."""
    x, y = _xy(lon, lat)
    return [el for el, pts in _OSM if _d_line((x, y), pts) <= r]


def wfs(layer: str, x: float, y: float, r: float, extra: str = "") -> list[dict]:
    cql = (f"DWITHIN(GEOMETRY,POINT({x:.0f} {y:.0f}),{r:.0f},meters)" if r else "") \
        + (f" AND {extra}" if extra and r else extra)
    url = (f"{WFS}?service=WFS&version=2.0.0&request=GetFeature&typeName={layer}&outputFormat=json"
           f"&srsName=EPSG:3005&count=500&CQL_FILTER={urllib.parse.quote(cql)}")
    return json.loads(_get(url)).get("features") or []


# --------------------------------------------------------------------------- geometry

def _xy(lon, lat):
    return _TO_ALB(lon, lat)


def _seg_d(p, a, b):
    ax, ay = a
    bx, by = b
    dx, dy = bx - ax, by - ay
    L = dx * dx + dy * dy
    t = 0.0 if L == 0 else max(0.0, min(1.0, ((p[0] - ax) * dx + (p[1] - ay) * dy) / L))
    return math.hypot(p[0] - ax - t * dx, p[1] - ay - t * dy)


def _d_line(p, pts):
    if len(pts) == 1:
        return math.hypot(p[0] - pts[0][0], p[1] - pts[0][1])
    return min(_seg_d(p, pts[i], pts[i + 1]) for i in range(len(pts) - 1))


def _el_pts(el) -> list[tuple[float, float]]:
    if el.get("type") == "node":
        return [_xy(el["lon"], el["lat"])]
    if el.get("geometry"):
        return [_xy(g["lon"], g["lat"]) for g in el["geometry"] if g]
    if el.get("members"):
        return [_xy(g["lon"], g["lat"]) for m in el["members"] for g in (m.get("geometry") or []) if g]
    b = el.get("bounds")
    if b:
        return [_xy((b["minlon"] + b["maxlon"]) / 2, (b["minlat"] + b["maxlat"]) / 2)]
    return []


def _feat_pts(f) -> list[tuple[float, float]]:
    g = f.get("geometry") or {}
    c = g.get("coordinates")
    if g.get("type") == "Point":
        return [tuple(c)]
    if g.get("type") == "LineString":
        return [tuple(p) for p in c]
    if g.get("type") == "MultiLineString":
        return [tuple(p) for ln in c for p in ln]
    return []


# --------------------------------------------------------------------------- words

def _tags_text(el) -> str:
    t = el.get("tags") or {}
    return _norm(" ".join(str(t.get(k, "")) for k in
                          ("name", "ref", "bridge:name", "alt_name", "old_name", "official_name",
                           "operator", "railway", "description")))


def _keys(desc: str, own: set[str]) -> tuple[set[str], set[str]]:
    """(numbers, distinctive words) the landmark is named by — what a candidate feature must carry."""
    toks = [t for t in tokens(desc) if not re.fullmatch(r"\d+m(-up|-down)?", t)]
    nums = {t for t in toks if re.fullmatch(r"\d+[a-z]?", t)}
    words = {t for t in toks if t.isalpha() and len(t) > 2 and t not in GENERIC and t not in TYPE_WORDS
             and t not in own and t not in {"downstream", "upstream", "above", "below", "near", "fishing",
                                            "signs", "crossing", "mile", "new", "lower", "middle", "located",
                                            "main", "approx", "approximately", "street", "avenue", "highway",
                                            "subdivision", "city"}}
    return nums, words


def _named(el_text: str, nums: set[str], words: set[str]) -> bool:
    toks = set(re.findall(r"[a-z0-9]+", el_text))
    return bool((nums & toks) or (words & toks))


RAIL = {"cpr", "cnr", "cn", "railway", "trestle", "en", "rail"}


# --------------------------------------------------------------------------- the point

class Ctx:
    def __init__(self):
        self.tidal = {}
        if TIDAL.exists():
            for c in json.loads(TIDAL.read_text())["candidates"]:
                self.tidal[c["blk"]] = c["proposed"]["coord"]
        self._lakes = {}

    def lakes_near(self, x, y, r=400.0):
        import pyogrio
        k = (round(x), round(y))
        if k not in self._lakes:
            df = pyogrio.read_dataframe(str(GPKG), layer="lakes", bbox=(x - r, y - r, x + r, y + r),
                                        columns=["GNIS_NAME_1", "GNIS_NAME_2"])
            self._lakes[k] = df
        return self._lakes[k]


def _res(status, method, delta=None, tol=None, why="", evidence=()):
    return {"status": status, "method": method, "delta_m": None if delta is None else round(delta, 1),
            "tolerance_m": tol, "why": why, "evidence": list(evidence)}


OFFSET_RE = re.compile(r"(\d+)m-(up|down)")


def _offset(desc: str) -> Optional[tuple[float, str]]:
    """An offset FROM a named anchor ("75 m below the weir"); a bare "(125 m downstream)" is not one."""
    toks = tokens(desc)
    for i, t in enumerate(toks):
        m = OFFSET_RE.fullmatch(t)
        if m and any(w.isalpha() and w not in GENERIC and w not in {"downstream", "upstream", "above", "below"}
                     for w in toks[i + 1:i + 6]):
            return float(m.group(1)), m.group(2)
    m = re.search(r"_(u|d)(\d+)m?$", desc.split()[0] if desc else "")
    if m:
        return float(m.group(2)), "up" if m.group(1) == "u" else "down"
    return None


def _cls(desc: str) -> str:
    d = _norm(desc)
    if "tidal" in d:
        return "tidal"
    if "→" in desc or "_into_" in desc or re.search(r"\bconfluence\b|\bmouth of\b", d):
        return "confluence"
    if re.search(r"\bbridge|trestle|overpass|abutment|\b\d+ (street|avenue)\b|\bcariboo road\b|\bstreet\b|\bavenue\b", d):
        return "bridge"
    if re.search(r"\bfalls\b", d):
        return "falls"
    if re.search(r"\bpowerline", d):
        return "powerline"
    if re.search(r"\bweir|\bdam\b|\bdams\b|fence|hatchery|tailrace|outfall|fishway|dyke|gates", d):
        return "structure"
    if re.search(r"\blake\b", d):
        return "lake"
    if re.search(r"\bmouth\b", d):
        return "mouth"
    if re.search(r"\bpark|lodge|resort|canyon|lagoon|station\b", d):
        return "place"
    return "sign"


def _family(first: str) -> str:
    f = first.lower()
    if "openstreetmap" in f or "osm" in f:
        return "osm"
    if "mot_road_structure" in f or "moti" in f:
        return "moti"
    if "flows straight into" in f or "fwa" in f:
        return "fwa"
    if "notice's coordinate" in f:
        return "coord"
    if "gns_geographical" in f or "gnis" in f or "bcgnws" in f or "geographical names" in f:
        return "gnis"
    return "curated"


def locate(cls: str, desc: str, x: float, y: float, fam: str, own: set[str], ctx: Ctx,
           gnis_id: Optional[int] = None) -> Optional[dict]:
    """Find the landmark `desc` names near (x, y) by a method not of family `fam`:
    {"xy", "d", "method", "evidence", "tol"} or None."""
    lon, lat = _TO_LL(x, y)
    nums, words = _keys(desc, own)

    def best(cands, method, tol, named=True):
        if not cands:
            return None
        d, el_xy, ev = min(cands, key=lambda c: c[0])
        return {"d": d, "xy": el_xy, "method": method, "evidence": [ev], "tol": tol, "named": named}

    def osm(filt, method, tol, need_name=True):
        els = [e for e in overpass(lon, lat) if filt(e)]
        cands = []
        for e in els:
            pts = _el_pts(e)
            if not pts:
                continue
            named = _named(_tags_text(e), nums, words)
            if need_name and (nums or words) and not named:
                continue
            d = _d_line((x, y), pts)
            near = min(pts, key=lambda p: math.hypot(p[0] - x, p[1] - y))
            cands.append((d, near, f"https://www.openstreetmap.org/{e['type']}/{e['id']}"))
        return best(cands, method, tol, named=bool(need_name and (nums or words)))

    def gnis(kind_re, method, tol, r=6000.0, by_id=None):
        if by_id:
            fs = wfs(GNIS, x, y, 0, f"FEATURE_ID={int(by_id)}")
        else:
            fs = wfs(GNIS, x, y, r)
        cands = []
        for f in fs:
            p = f["properties"]
            nm = _norm(p.get("GEOGRAPHICAL_NAME") or "")
            if not by_id:
                if not re.search(kind_re, (p.get("FEATURE_TYPE") or "") + " " + nm, re.I):
                    continue
                if (nums or words) and not _named(nm, nums, words):
                    continue
            pts = _feat_pts(f)
            if pts:
                cands.append((_d_line((x, y), pts), pts[0],
                              f"BC GNIS {p.get('GEOGRAPHICAL_NAME')} ({p.get('FEATURE_TYPE')}, {p.get('POSITION_TYPE')}) id {p.get('FEATURE_ID')}"))
        return best(cands, method, tol)

    if cls == "tidal":
        return None                                       # handled by the caller (needs the blk)
    if cls == "confluence" and fam == "gnis":
        return fwa_mouth(words, x, y)
    if cls == "confluence":
        return gnis(r"creek|river|slough|brook|stream", "BC GNIS mouth point of the named tributary",
                    TOL_CONFLUENCE, r=8000, by_id=gnis_id)
    if cls == "bridge":
        rail = bool(RAIL & set(tokens(desc)))
        flt = (lambda e: "railway" in (e.get("tags") or {})) if rail else \
              (lambda e: "bridge" in (e.get("tags") or {}) and "power" not in (e.get("tags") or {}))
        if fam != "osm":
            got = osm(flt, "OSM bridge (Overpass)", TOL_POINT)
            if not got and (rail or not (nums or words)):
                got = osm(flt, "OSM bridge (Overpass, nearest)", TOL_POINT, need_name=False)
            if not got and (nums or words) and not rail:
                # the only bridge(s) at this spot carry no name: accept the unnamed one as weak evidence
                got = osm(lambda e: flt(e) and not (e.get("tags") or {}).get("name"),
                          "OSM unnamed bridge (Overpass)", TOL_POINT, need_name=False)
            if got:
                return got
        if fam != "moti":
            fs = wfs(MOTI, x, y, 800)
            cands = []
            for f in fs:
                p = f["properties"]
                if (p.get("BMIS_STRUCTURE_TYPE") or "") != "BRIDGE":
                    continue
                nm = _norm(p.get("BMIS_STRUCTURE_NAME") or "")
                if re.search(r"\b[uo]/p\b|overpass|underpass|interchange", nm):
                    continue                               # a road over a road, not the river crossing
                if (nums or words) and not (_named(nm, nums, words) or (p.get("HIGHWAY_NUMBER") or "").lower() in nums):
                    continue
                pts = _feat_pts(f)
                if pts:
                    cands.append((_d_line((x, y), pts), pts[0],
                                  f"BC MoTI structure {p.get('BMIS_STRUCTURE_NAME')} #{p.get('BMIS_STRUCTURE_NUMBER')}"))
            got = best(cands, "BC MoTI road structure (openmaps WFS)", TOL_POINT)
            if got:
                return got
        return None
    if cls == "falls":
        if fam != "osm":
            got = osm(lambda e: (e.get("tags") or {}).get("natural") == "waterfall"
                      or (e.get("tags") or {}).get("waterway") == "waterfall"
                      or bool(re.search(r"\bfalls$", ((e.get("tags") or {}).get("name") or "").lower())),
                      "OSM waterfall (Overpass)",
                      TOL_POINT)
            if got:
                return got
        return gnis(r"falls", "BC GNIS falls", TOL_POINT * 2)
    if cls == "powerline":
        if fam == "osm":
            return None
        return osm(lambda e: (e.get("tags") or {}).get("power") == "line", "OSM power line (Overpass)", TOL_POINT,
                   need_name=False)
    if cls == "structure":
        d = _norm(desc)
        # only a weir / dam is a feature OSM draws ACROSS the river; a fence, outfall or tailrace is not
        if fam != "osm" and re.search(r"\bweir|\bdam\b|\bdams\b|gates", d):
            got = osm(lambda e: any((e.get("tags") or {}).get(k) in ("weir", "dam") for k in ("waterway", "man_made")),
                      "OSM weir / dam (Overpass)", TOL_POINT, need_name=False)
            if got:
                return got
        if re.search(r"\bdam\b", d):
            return gnis(r"dam", "BC GNIS dam", TOL_POINT * 2)
        return None
    if cls == "lake":
        m = re.search(r"([a-z][a-z' ]+?) lake", _norm(desc))
        name = (m.group(1).split()[-2:] if m else [])
        df = ctx.lakes_near(x, y)
        cands = []
        for _, r in df.iterrows():
            nm = _norm(f"{r.GNIS_NAME_1 or ''} {r.GNIS_NAME_2 or ''}")
            if name and not all(w in nm for w in name[-1:]):
                continue
            d = r.geometry.distance(__import__("shapely").geometry.Point(x, y))   # 0 inside: a lake cut
            cands.append((d, (x, y), f"FWA lake polygon {r.GNIS_NAME_1} (bc_fisheries_data.gpkg lakes)"))
        return best(cands, "FWA lake polygon by name (gpkg)", TOL_POINT)
    if cls in ("place", "sign"):
        if fam == "osm" or not words:
            return None
        return osm(lambda e: True, "OSM named place (Overpass)", TOL_PLACE)
    return None


def fwa_mouth(words: set[str], x: float, y: float, r: float = 8000.0) -> Optional[dict]:
    """The FWA stream named by `words` near (x, y): the downstream end of its main line."""
    import pyogrio
    from shapely import force_2d
    if not words:
        return None
    df = pyogrio.read_dataframe(str(GPKG), layer="streams", bbox=(x - r, y - r, x + r, y + r),
                                columns=["GNIS_NAME", "BLUE_LINE_KEY", "WATERSHED_KEY", "DOWNSTREAM_ROUTE_MEASURE"])
    df = df[df.GNIS_NAME.fillna("").map(lambda n: bool(words & set(re.findall(r"[a-z]+", _norm(n)))))]
    df = df[df.BLUE_LINE_KEY == df.WATERSHED_KEY]
    if df.empty:
        return None
    best = None
    for blk, g in df.groupby("BLUE_LINE_KEY"):
        row = g.loc[g.DOWNSTREAM_ROUTE_MEASURE.idxmin()]
        geom = force_2d(row.geometry)
        first = geom.geoms[0].coords[0] if geom.geom_type == "MultiLineString" else geom.coords[0]
        d = math.hypot(first[0] - x, first[1] - y)
        if best is None or d < best["d"]:
            best = {"d": d, "xy": first, "method": "FWA stream line's own mouth by name (gpkg)",
                    "evidence": [f"FWA {row.GNIS_NAME} blk {int(blk)}"], "tol": TOL_CONFLUENCE, "named": True}
    return best


def check_point(pt: dict, loc: dict, ctx: Ctx, own: set[str], water_mouth) -> dict:
    """One point: located again by a second method, the distance held against the tolerance."""
    if not pt.get("coord"):
        return _res("unchecked", "-", why=f"{pt['label']}: no coordinate to check")
    if (pt.get("split_id") or "").startswith("gauge__"):
        return _res("unchecked", "-", why=f"{pt['label']}: a gauge cut stands in for the notice's place — "
                                         "the owner decides whether the gauge is that place")
    x, y = _xy(*pt["coord"])
    fam = _family(pt["first"])
    desc = pt["desc"]
    # 1. the notice's own coordinate, when the first method did not use it
    cs = coords_in(loc["text"])
    if cs and fam != "coord":
        d = min(math.hypot(x - _xy(*c)[0], y - _xy(*c)[1]) for c in cs)
        if d <= TOL_POINT:          # a notice coordinate further off may be another landmark's
            return _res("pre-verified", "the notice's own coordinate", d, TOL_POINT,
                        f"{pt['label']}: {d:.0f} m from the notice's coordinate", ["DFO notice text"])
    # a parenthesis that only says where the landmark is ("base of X Falls (450 m below the bridge)") is
    # secondary: the landmark named before it is checked first
    outside = re.sub(r"\([^)]*\)?", " ", desc)
    off = None if _cls(outside) not in ("sign", "place") and not _offset(outside) else _offset(desc)
    if off is None and _offset(outside):
        off = _offset(outside)
    if off is None:
        desc = outside if _cls(outside) not in ("sign", "place") else desc
    anchor_desc = desc
    if off:
        m = re.search(r"\d+m\s*(?:\(\w+\)\s*)?(?:upstream|downstream|above|below)\s+(?:of\s+|from\s+)?(?:the\s+)?"
                      r"([^();,]{3,60})", _norm(desc))
        anchor_desc = m.group(1) if m else re.sub(r"\d+m\b", " ", _norm(desc))
    cls = _cls(anchor_desc)
    if off and cls in ("sign", "place") and re.search(r"\b(river|creek|slough)\b", _norm(anchor_desc)):
        cls = "confluence"                                 # "300 m above <a stream>": its mouth
    if cls == "tidal":
        tc = ctx.tidal.get(str(pt.get("blk") or ""))
        if not tc:
            return _res("unchecked", "tidal polygon crossing", why=f"{pt['label']}: no tidal candidate on its line")
        tx, ty = _xy(*tc)
        d = math.hypot(x - tx, y - ty)
        ok = d <= TOL_CONFLUENCE
        return _res("pre-verified" if ok else "disagree", "DFO tidal polygon crossing (tidal-candidates)", d,
                    TOL_CONFLUENCE, f"{pt['label']}: {d:.0f} m from the tidal polygon's crossing",
                    ["tidal-candidates.json"])
    if cls == "mouth" and water_mouth:
        mx, my = water_mouth
        d = math.hypot(x - mx, y - my)
        ok = d <= TOL_PLACE
        return _res("pre-verified" if ok else "disagree", "BC GNIS mouth point of the water", d, TOL_PLACE,
                    f"{pt['label']}: {d:.0f} m from the water's GNIS mouth ('near the mouth')", ["BC GNIS"])
    if cls == "sign" and not off:
        return _res("unchecked", "-", why=f"{pt['label']}: a sign named by no public landmark")
    got = None
    try:
        got = locate(cls, anchor_desc, x, y, fam, own, ctx, gnis_id=pt.get("trib_gnis"))
    except RuntimeError as e:
        return _res("unchecked", "-", why=f"{pt['label']}: second source unreachable ({e})")
    if not got:
        return _res("unchecked", "-", why=f"{pt['label']}: no second source names this {cls} "
                                         f"(first: {fam})")
    if off:
        n, _ = off
        tol = max(75.0, n * 0.5)
        if not got.get("named") or got["tol"] > TOL_CONFLUENCE:
            return _res("unchecked", got["method"] + " + the offset",
                        why=f"{pt['label']}: the anchor is found only as an unnamed / imprecise feature "
                            f"({math.hypot(x - got['xy'][0], y - got['xy'][1]):.0f} m away; the words say {n:.0f} m)",
                        evidence=got["evidence"])
        dist = math.hypot(x - got["xy"][0], y - got["xy"][1]) if cls != "lake" else got["d"]
        delta = abs(dist - n)
        ok = delta <= tol
        return _res("pre-verified" if ok else "disagree", got["method"] + " + the offset", delta, tol,
                    f"{pt['label']}: {dist:.0f} m from the anchor, the words say {n:.0f} m", got["evidence"])
    ok = got["d"] <= got["tol"]
    if not ok and not got.get("named"):
        return _res("unchecked", got["method"], got["d"], got["tol"],
                    f"{pt['label']}: the nearest unnamed {cls} is {got['d']:.0f} m away — may be another one",
                    got["evidence"])
    return _res("pre-verified" if ok else "disagree", got["method"], got["d"], got["tol"],
                f"{pt['label']}: {got['d']:.0f} m from the {cls} the second source places", got["evidence"])


# --------------------------------------------------------------------------- per location

def _detail(loc_id: str) -> dict:
    url = f"{API}/api/dfo/locations/{urllib.parse.quote(loc_id, safe='')}"
    f = CACHE / ("detail-" + hashlib.sha1(loc_id.encode()).hexdigest())
    if f.exists():
        return json.loads(f.read_text())
    body = urllib.request.urlopen(url, timeout=120).read()
    CACHE.mkdir(parents=True, exist_ok=True)
    f.write_bytes(body)
    return json.loads(body)


def points_of(row: dict, detail: dict) -> list[dict]:
    s = row.get("suggestion") or {}
    ns = {n["ref"]: n for n in s.get("new_splits") or []}
    feats = {f["properties"]["split_id"]: f for f in (detail.get("splits") or {}).get("features") or []}
    hunt = {p["ref"]: p for p in ((row.get("hunt") or {}).get("proposals") or [])}
    eps = row.get("endpoints") or []
    out = []
    for p in s.get("points") or []:
        sid = p.get("split_id")
        ep = next((e for e in eps if (e.get("fix") or {}).get("split_id") == sid), None)
        if p.get("kind") == "split":
            f = feats.get(sid)
            label = (f["properties"]["label"] if f else _split_label(sid))
            desc = f"{sid} {label} {(ep or {}).get('phrase') or ''}"
            out.append({"label": label, "coord": f["geometry"]["coordinates"] if f else _split_coord(sid),
                        "desc": desc, "first": (ep or {}).get("reason") or "curated cut by name",
                        "blk": None, "trib_gnis": None, "split_id": sid})
        else:
            n = ns.get(sid) or {}
            h = hunt.get(sid)
            first = (ep or {}).get("reason") or ""
            if h:
                first = "split hunt: " + " ".join(h.get("sources") or []) + " " + (h.get("notes") or "")
            m = re.search(r"\(gnis:(\d+)\) flows straight into", first)
            name = n.get("name") or sid
            desc = f"{name} {n.get('quote') or ''}"
            if (m or re.search(r"\b(creek|river|ck\.?)$", name.strip().lower())) and "confluence" not in desc:
                desc += " confluence"
            if n.get("anchor") == "split" and n.get("split_id") and not n.get("coord"):
                # an offset of a curated cut by the notice's own distance: verify the cut it hangs from
                f = feats.get(n["split_id"])
                alab = f["properties"]["label"] if f else _split_label(n["split_id"])
                out.append({"label": f"{name} (= {alab} {n.get('offset_m'):+.0f} m)",
                            "coord": f["geometry"]["coordinates"] if f else _split_coord(n["split_id"]),
                            "desc": f"{n['split_id']} {alab}", "first": "curated cut by name",
                            "blk": None, "trib_gnis": None, "split_id": n["split_id"],
                            "offset_of": n["split_id"]})
                continue
            mo = re.search(r"placed (\d+) m (upstream|downstream) of the anchor", n.get("note") or "")
            o = n.get("offset_m") or (float(mo.group(1)) * (1 if mo.group(2) == "upstream" else -1) if mo else None)
            if o and n.get("anchor") != "split":
                o = float(o)
                desc += f" {abs(o):.0f} m {'upstream' if o > 0 else 'downstream'} of {name}"
            out.append({"label": name, "coord": n.get("coord"), "desc": desc, "first": first,
                        "blk": n.get("blk"), "trib_gnis": int(m.group(1)) if m else None, "split_id": sid,
                        "anchor": n.get("anchor")})
    return out


_REG: dict[str, list[str]] = {}


def _names(item_ids: list[str]) -> list[str]:
    """Every name (and variant) of these registry items (the served registry, read once)."""
    if not _REG:
        reg = json.loads((ROOT / "data/generated/atlas/full/registry.json").read_text())["items"]
        for it in reg:
            _REG[it["id"]] = [it.get("name") or ""] + [v if isinstance(v, str) else (v or {}).get("name", "")
                                                       for v in it.get("variants") or []]
    return [n for i in item_ids for n in _REG.get(i, [])]


_RESOLVED: dict[str, dict] = {}
_LABELS: dict[str, str] = {}


def _split_label(sid: str) -> str:
    if not _LABELS:
        doc = json.loads((ROOT / "data/curated/waters/splits.json").read_text())
        for wb in (doc.get("waterbodies") or {}).values() if isinstance(doc.get("waterbodies"), dict) else doc.get("waterbodies") or []:
            for sp in (wb.get("splits") or []) if isinstance(wb, dict) else []:
                if isinstance(sp, dict) and sp.get("id"):
                    _LABELS[sp["id"]] = sp.get("label") or sp["id"]
    return _LABELS.get(sid, sid.split("__")[-1].replace("_", " "))


def _split_coord(sid: str) -> Optional[list[float]]:
    """A curated cut the location's detail does not draw: its served route measure on the FWA line."""
    if not _RESOLVED:
        for r in json.loads((ROOT / "data/generated/atlas/full/splits.resolved.json").read_text()):
            _RESOLVED[r["split_id"]] = r
    r = _RESOLVED.get(sid)
    if not r or not r.get("blk") or r.get("route_measure") is None:
        return None
    import sqlite3
    import pyogrio
    from shapely import force_2d
    from shapely.ops import linemerge
    con = sqlite3.connect(f"file:{GPKG}?mode=ro", uri=True)
    rows = con.execute("select fid, DOWNSTREAM_ROUTE_MEASURE from streams where BLUE_LINE_KEY = ? and "
                       "DOWNSTREAM_ROUTE_MEASURE <= ? order by DOWNSTREAM_ROUTE_MEASURE desc limit 1",
                       (int(r["blk"]), float(r["route_measure"]))).fetchall()
    con.close()
    if not rows:
        return None
    g = force_2d(pyogrio.read_dataframe(str(GPKG), layer="streams", fids=[rows[0][0]]).geometry.iloc[0])
    if g.geom_type == "MultiLineString":
        g = linemerge(g)
    pt = g.interpolate(min(max(float(r["route_measure"]) - rows[0][1], 0.0), g.length))
    return list(_TO_LL(pt.x, pt.y))


WATER_RE = re.compile(r"\b((?:[A-Z][A-Za-z'’\-]*\s+){1,3}(?:River|Creek|Slough|Lake|Lakes))\b")


def whole_check(row: dict, detail: dict) -> dict:
    # the words after these name what is LEFT OUT or a cross-reference, not the extent
    text = re.split(r"\(?\bsee\b|\bother than\b|\bexcept\b|\bexcluding\b|\bnote\b|\bunless\b|\banglers may\b",
                    row["text"], maxsplit=1, flags=re.I)[0]
    if re.search(r"\bwithin a \d+\s*m radius\b", row["text"], re.I):
        text += " " + re.search(r"within a \d+\s*m radius", row["text"], re.I).group(0)
    toks = tokens(text)
    item_names = {_norm(i.get("name") or "") for i in detail.get("items") or []}
    item_names |= {_norm(n) for n in _names(row.get("items") or [])}
    place = sorted({t for t in toks if t in TYPE_WORDS and t not in {"lake"}}
                   | {t for t in toks if re.fullmatch(r"\d+m(-up|-down)?", t)})
    if coords_in(text):
        place.append("a coordinate")
    named = [_norm(w) for w in WATER_RE.findall(text)]
    missing = [w for w in named if not any(w.strip() in n or n in w for n in item_names)]
    if re.search(r"\b(upstream|downstream|above|below|from|between)\b", text, re.I) and not place:
        place.append("a direction word")
    if place:
        return _res("disagree", "the notice's words read for place names",
                    why=f"whole water suggested, but the notice names {', '.join(place)}",
                    evidence=["DFO notice text"])
    if missing:
        return _res("disagree", "the notice's words read for place names",
                    why=f"whole water suggested, but {', '.join(missing)} is not among its items",
                    evidence=["DFO notice text"])
    return _res("pre-verified", "the notice's words name no place on the water; named waters = items",
                why="no landmark, offset or coordinate in the notice; every water it names is an item",
                evidence=["DFO notice text", "registry items"])


def water_mouth(detail: dict) -> Optional[tuple[float, float]]:
    for it in detail.get("items") or []:
        iid = it.get("id") or ""
        if iid.startswith("gnis:"):
            try:
                fs = wfs(GNIS, 0, 0, 0, f"FEATURE_ID={int(iid.split(':')[1])}")
            except RuntimeError:
                return None
            for f in fs:
                pts = _feat_pts(f)
                if pts:
                    return pts[0]
    return None


def check(row: dict, ctx: Ctx) -> dict:
    first_kind = row["outcome"]
    if row["outcome"] == "unresolvable":
        return {**_res("unchecked", "-", why="unresolvable — no suggestion to check: "
                       + ((row.get("hunt") or {}).get("why") or row.get("why") or "")), "first": "none"}
    detail = _detail(row["location_id"])
    if row["outcome"] == "whole":
        r = whole_check(row, detail)
        return {**r, "first": "the matcher read the whole water"}
    own = set()
    for i in detail.get("items") or []:
        own |= {t for t in tokens(i.get("name") or "") if t.isalpha()}
    pts = points_of(row, detail)
    if not pts:
        return {**_res("unchecked", "-", why="no points"), "first": first_kind}
    mouth = water_mouth(detail) if any(_cls(p["desc"]) == "mouth" for p in pts) else None
    res = [check_point(p, row, ctx, own, mouth) for p in pts]
    sts = [r["status"] for r in res]
    status = "disagree" if "disagree" in sts else "pre-verified" if all(s == "pre-verified" for s in sts) \
        else "unchecked"
    deltas = [r["delta_m"] for r in res if r["delta_m"] is not None]
    tols = [r["tolerance_m"] for r in res if r["tolerance_m"] is not None]
    firsts = sorted({_family(p["first"]) for p in pts})
    return {"status": status, "method": "; ".join(dict.fromkeys(r["method"] for r in res)),
            "first": ", ".join({"curated": "curated cut by name", "osm": "split hunt / OSM",
                                "moti": "split hunt / MoTI", "fwa": "FWA tributary mouth",
                                "coord": "notice coordinate", "gnis": "BC GNIS"}[f] for f in firsts),
            "delta_m": max(deltas) if deltas else None, "tolerance_m": min(tols) if tols else None,
            "why": " | ".join(r["why"] for r in res),
            "evidence": [e for r in res for e in r["evidence"]], "points": res}


# --------------------------------------------------------------------------- duplicates

def _sig(row: dict) -> Optional[tuple]:
    s = row.get("suggestion") or {}
    if not s:
        return None
    ns = {n["ref"]: n for n in s.get("new_splits") or []}
    pts = []
    for p in s.get("points") or []:
        if p.get("kind") == "split":
            pts.append(("s", p["split_id"]))
        else:
            c = (ns.get(p["split_id"]) or {}).get("coord")
            pts.append(("n", tuple(_xy(*c)) if c else p["split_id"]))
    sig = (s.get("op"), tuple(sorted(row.get("items") or [])), tuple(pts), bool(s.get("tributaries")))
    if not pts:
        # the whole water: the same only when the words leave out the same things too
        from pipeline.regs.dfo_salmon.coverage import place_words
        sig = sig + (frozenset(place_words(re.sub(r"\banglers may.*", "", row["text"], flags=re.I))),)
    return sig


def _same(a, b) -> bool:
    if a is None or b is None or a[:2] != b[:2] or a[3] != b[3] or len(a[2]) != len(b[2]) or a[4:] != b[4:]:
        return False
    for (ka, va), (kb, vb) in zip(a[2], b[2]):
        if ka != kb:
            return False
        if ka == "s" and va != vb:
            return False
        if ka == "n":
            if isinstance(va, tuple) and isinstance(vb, tuple):
                if math.hypot(va[0] - vb[0], va[1] - vb[1]) > DUP_M:
                    return False
            elif va != vb:
                return False
    return True


def duplicates(rows: dict) -> dict[str, str]:
    """location -> the canonical location the same decision covers."""
    ids = [k for k, r in rows.items() if r["outcome"] not in ("unresolvable",) and r.get("suggestion")]
    sig = {k: _sig(rows[k]) for k in ids}

    def rank(k):
        r = rows[k]
        cl = r.get("cluster") or {}
        return (0 if cl.get("role") == "canonical" else 1, 0 if r.get("status") == "active" else 1, len(k), k)

    out: dict[str, str] = {}
    seen: list[list[str]] = []
    for k in ids:
        for grp in seen:
            if _same(sig[k], sig[grp[0]]):
                grp.append(k)
                break
        else:
            seen.append([k])
    for grp in seen:
        if len(grp) < 2:
            continue
        canon = min(grp, key=rank)
        for k in grp:
            if k != canon:
                out[k] = canon
    # cluster variants the coverage pass already joined, same suggestion
    for k, r in rows.items():
        cl = r.get("cluster") or {}
        if k not in out and cl.get("role") not in (None, "canonical") and cl.get("canonical") \
                and cl["canonical"] != k and cl["canonical"] in rows \
                and _same(_sig(r), _sig(rows[cl["canonical"]])):
            out[k] = cl["canonical"]
    return out


# --------------------------------------------------------------------------- main

def run(out: Path = OUT) -> dict:
    doc = json.loads(SUGG.read_text())
    rows = doc["suggestions"]
    ctx = Ctx()
    coords = []
    for r in rows.values():
        if r["outcome"] in ("whole", "unresolvable"):
            continue
        try:
            coords += [tuple(p["coord"]) for p in points_of(r, _detail(r["location_id"])) if p.get("coord")]
        except Exception as e:                              # noqa: BLE001
            print("detail failed", r["location_id"], e)
    prefetch(sorted(set((round(a, 4), round(b, 4)) for a, b in coords)))
    items = {}
    for i, (k, r) in enumerate(rows.items()):
        try:
            res = check(r, ctx)
        except Exception as e:                              # noqa: BLE001 — one row never stops the pass
            res = {**_res("unchecked", "-", why=f"check failed: {e}"), "first": r["outcome"]}
        items[k] = {"status": res["status"], "method": res["method"], "first": res["first"],
                    "delta_m": res["delta_m"], "tolerance_m": res["tolerance_m"], "why": res["why"],
                    "duplicate_of": None, "evidence": list(dict.fromkeys(res["evidence"]))}
        print(f"{i + 1:3}/{len(rows)} {res['status']:12} {k}", flush=True)
    for k, canon in duplicates(rows).items():
        items[k]["duplicate_of"] = canon
    counts = Counter(v["status"] for v in items.values())
    body = {
        "_about": ("DFO suggestion PRE-VERIFY (owner 2026-10-27): every suggestion in dfo-suggestions.json re-checked "
                   "by a SECOND method independent of the one that produced it (OSM / BC MoTI / BC GNIS / FWA lake "
                   "polygons / the notice's coordinate / the tidal polygon). pre-verified = every point agrees within "
                   "tolerance; disagree = a second source places a landmark elsewhere (why says where); unchecked = "
                   "no second method reaches a point. duplicate_of = the same water and the same points as another "
                   "location (one decision covers both). Never applied — the owner's mark is the only write. "
                   "Regenerate: .venv/bin/python -m pipeline.regs.dfo_salmon.preverify"),
        "generated": "2026-10-27",
        "counts": {"pre-verified": counts.get("pre-verified", 0), "disagree": counts.get("disagree", 0),
                   "unchecked": counts.get("unchecked", 0),
                   "duplicates": sum(1 for v in items.values() if v["duplicate_of"])},
        "items": items,
    }
    out.write_text(json.dumps(body, indent=1, ensure_ascii=False) + "\n")
    return body


if __name__ == "__main__":
    b = run()
    print(json.dumps(b["counts"]))
