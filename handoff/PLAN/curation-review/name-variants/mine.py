"""T224 NAME VARIANTS: mine every local source for names on FWA lines / lakes that lack one or are named differently.

READ ONLY — writes only candidates.json / summary numbers beside this file. No curated file is touched.

Sources (each candidate carries its REAL source, never a borrowed label):
    municipal:<city>   the four municipal stream layers (data/generated/municipal/cleaned, from data/source/municipal)
    osm                OSM named waterways in the app basemap (bc.pmtiles z13, via osm_extract.py)
    eccc               ECCC / WSC hydrometric station names (data/curated/gauges/matches.json, bc_hydrometric_stations.json)
    dfo                DFO salmon regulation water names + aliases (data/curated/regulations/entries/dfo_salmon)
    synopsis           the synopsis entry name vs the item it is matched to (data/curated/regulations/entries/catalogue)
    bcgnis_cache       BC Geographical Names features cached by the DFO preverify (~/.cache/bcfish-preverify)

    NV_CACHE=<dir> python mine.py [--osm <osm_water.jsonl>]
"""
from __future__ import annotations

import difflib
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import shapely
from shapely.geometry import LineString, MultiLineString, Point, shape
from shapely.ops import substring

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import fwa_index as F  # noqa: E402

ROOT = F.ROOT
BUF = 25.0          # m: a source line "is" an FWA line where it runs within this of it
CHUNK = 3000.0      # m: long source lines are matched in pieces

SOURCE_LABEL = {
    "municipal:abbotsford": ("City of Abbotsford stream layer", "data/source/municipal/abbotsford_streams.geojson"),
    "municipal:burnaby": ("City of Burnaby waterways layer", "data/source/municipal/burnaby_waterways.geojson"),
    "municipal:port_moody": ("City of Port Moody ESA streams layer", "data/source/municipal/port_moody_esa_streams.geojson"),
    "municipal:squamish": ("District of Squamish watercourses layer", "data/source/municipal/squamish_watercourses.geojson"),
    "osm": ("OpenStreetMap (app basemap, OSM 2026-03-05)", "data/source/bc.pmtiles"),
    "eccc": ("ECCC hydrometric station name", "data/curated/gauges/matches.json"),
    "dfo": ("DFO salmon regulations", "data/curated/regulations/entries/dfo_salmon"),
    "synopsis": ("BC freshwater fishing synopsis 2025-27", "data/curated/regulations/entries/catalogue"),
    "bcgnis_cache": ("BC Geographical Names (cached WFS)", "~/.cache/bcfish-preverify"),
}
OFFICIAL = {"bcgnis_cache"}     # only a gazetted name is official; a regulation's name is authoritative, not gazetted

# ------------------------------------------------------------------------------------------------ names
GENERIC = {"creek", "river", "brook", "slough", "lake", "channel", "canal", "ditch", "stream", "pond", "cr", "ck",
           "side", "the", "of", "marsh", "reservoir", "lagoon", "lakes", "creeks"}
JUNK = re.compile(r"unnamed|^hide$|\btrib\b|trib\.|\btributary\b|\bditch\b|\bdrain\b|culvert|^\W*$|\d|^no name|"
                  r"^un-?named|storm|outfall|swale|^creek$|^river$|^stream$", re.I)


def key(s: str) -> str:
    s = (s or "").lower().replace("’", "").replace("'", "").replace('"', "")
    s = re.sub(r"\bmt\b\.?", "mount", s)
    s = re.sub(r"\bst\b\.?", "saint", s)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def base(s: str) -> str:
    return " ".join(w for w in key(s).split() if w not in GENERIC)


def tidy(s: str) -> str:
    """Display case for a SHOUTING source name ("KILGARD CREEK" -> "Kilgard Creek"); mixed case kept."""
    s = re.sub(r"\s+", " ", (s or "").strip())
    if s.isupper() or s.islower():
        s = " ".join(w[:1].upper() + w[1:].lower() if not w.startswith("(") else "(" + w[1:2].upper() + w[2:].lower()
                     for w in s.split(" "))
        s = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), s)
    return s


def relation(proposed: str, current: list[str]) -> str | None:
    """unnamed | same (None: not a candidate) | generic_differs | spelling | different."""
    cur = [c for c in current if c]
    if not cur:
        return "unnamed"
    k = key(proposed)
    ks = {key(c) for c in cur}
    if k in ks:
        return None
    b = base(proposed)
    for c in cur:
        if b and b == base(c):
            pg = set(key(proposed).split()) & GENERIC
            return None if not pg else "generic_differs"
    if max(difflib.SequenceMatcher(None, k, x).ratio() for x in ks) >= 0.85:
        return "spelling"
    return "different"


# ------------------------------------------------------------------------------------------------ current names
BLK = F.blk_table()
BY_BLK, BY_WBK, BY_GNIS = F.registry()
ITEMS = {it["id"]: it for it in json.loads(F.REGISTRY.read_text())["items"]}


def trim(wsc: str | None) -> str:
    return re.sub(r"(-000000)+$", "", wsc or "")


def _name_wscs() -> dict:
    """name key -> the trimmed watershed codes of every FWA line / registry stream that carries that name."""
    out = defaultdict(set)
    for _b, (gn, _l, w) in BLK.items():
        if gn and w:
            out[key(gn)].add(trim(w))
    for it in ITEMS.values():
        for r in it.get("ref_ids") or ():
            if r.startswith("wsc:"):
                for n in [it["name"], *it.get("variants", [])]:
                    out[key(n)].add(trim(r[4:]))
    return out


NAME_WSCS = _name_wscs()


def same_name_ancestor(name: str, wsc: str | None) -> str | None:
    """The watershed code of a water ALREADY carrying `name` that this line drains into (a source labelling a
    whole network by its main creek's name) — or None."""
    t = trim(wsc)
    for w in NAME_WSCS.get(key(name), ()):
        if t != w and t.startswith(w + "-"):
            return w
    return None


def blk_names(blk: str) -> tuple[list[str], list[str]]:
    gnis, _ln, _wsc = BLK.get(blk, (None, 0, None))
    names, items = ([gnis] if gnis else []), []
    for iid, n, vs in BY_BLK.get(blk, ()):
        items.append(iid)
        names += [n, *vs]
    return list(dict.fromkeys(names)), items


def wbk_names(wbk: str, fwa: list[str] | None = None) -> tuple[list[str], list[str]]:
    names, items = list(fwa or []), []
    for iid, n, vs in BY_WBK.get(wbk, ()):
        items.append(iid)
        names += [n, *vs]
    return list(dict.fromkeys(names)), items


# ------------------------------------------------------------------------------------------------ line matcher
def _chunks(g):
    for ln in (g.geoms if hasattr(g, "geoms") else [g]):
        L = ln.length
        if L <= CHUNK:
            yield ln
        else:
            k = int(L // CHUNK) + 1
            for i in range(k):
                yield substring(ln, i * L / k, (i + 1) * L / k)


def match_lines(g) -> tuple[float, dict]:
    """Per FWA blk near `g` (EPSG:3005 lines): near_m = FWA length within BUF of g; src_m = g length within BUF of it."""
    acc = defaultdict(lambda: [0.0, 0.0])
    total = 0.0
    for ch in _chunks(g):
        if ch.length < 1:
            continue
        total += ch.length
        rows = F.lines_near(ch, pad=BUF + 5)
        if not rows:
            continue
        chb = ch.buffer(BUF)
        byblk = defaultdict(list)
        for blk, _n, geom, _et in rows:
            byblk[blk].append(geom)
        for blk, gs in byblk.items():
            loc = shapely.union_all(gs)
            near = loc.intersection(chb).length
            if near < 5:
                continue
            acc[blk][0] += near
            acc[blk][1] += ch.intersection(loc.buffer(BUF)).length
    return total, acc


def confidence(blk_cov: float, src_cov: float, near: float) -> str | None:
    if blk_cov >= 0.8 and src_cov >= 0.8:
        return "high"
    if (blk_cov >= 0.5 and src_cov >= 0.3) or (src_cov >= 0.8 and near >= 150):
        return "medium"
    if blk_cov >= 0.3 or (src_cov >= 0.3 and near >= 100):
        return "low"
    return None


def line_candidates(source: str, name: str, g, extra: dict) -> list[dict]:
    total, acc = match_lines(g)
    if total < 20:
        return []
    out, same_on = [], []
    for blk, (near, src) in acc.items():
        _gn, blen, wsc = BLK.get(blk, (None, 0, None))
        blk_cov = min(1.0, near / blen) if blen else 0.0
        src_cov = min(1.0, src / total)
        conf = confidence(blk_cov, src_cov, near)
        if conf is None:
            continue
        cur, items = blk_names(blk)
        rel = relation(name, cur)
        if rel is None:
            same_on.append(blk)
            continue
        out.append({"target": {"kind": "stream", "blk": blk, "wsc": wsc, "length_m": round(blen)},
                    "current_names": cur, "registry_items": items, "proposed": name, "relation": rel,
                    "source": source, "evidence": {"blk_overlap": round(blk_cov, 3), "source_overlap": round(src_cov, 3),
                                                   "near_m": round(near), "source_len_m": round(total), **extra},
                    "confidence": conf, "flags": []})
    proposed_blks = [c for c in out if c["confidence"] in ("high", "medium")]
    for c in out:
        anc = same_name_ancestor(name, c["target"]["wsc"])
        if anc:         # drains into a water already called this: the source labels the network, not this line
            c["flags"].append(f"tributary_of_same_name:{anc}")
            c["confidence"] = "low"
        if same_on:     # the name is already on a neighbour: this piece is likely its branch / tributary
            c["flags"].append(f"name_already_on_blk:{','.join(same_on[:3])}")
            c["confidence"] = "low"
        if c["evidence"]["blk_overlap"] < 0.5:
            c["flags"].append("partial_blk")
        if len(proposed_blks) > 1:
            c["flags"].append(f"one_name_many_blks:{len(proposed_blks)}")
    return out


def cluster(lines, gap: float = 100.0):
    """Group lines (EPSG:3005) that come within `gap` m of each other."""
    if not lines:
        return []
    tree = shapely.STRtree(lines)
    parent = list(range(len(lines)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    for i, ln in enumerate(lines):
        for j in tree.query(ln, predicate="dwithin", distance=gap):
            a, b = find(i), find(int(j))
            if a != b:
                parent[a] = b
    groups = defaultdict(list)
    for i in range(len(lines)):
        groups[find(i)].append(lines[i])
    return [shapely.line_merge(shapely.union_all(gs)) for gs in groups.values()]


# ------------------------------------------------------------------------------------------------ sources
def municipal() -> list[dict]:
    out = []
    for city in ("abbotsford", "burnaby", "port_moody", "squamish"):
        fc = json.loads((ROOT / f"data/generated/municipal/cleaned/{city}.geojson").read_text())
        by = defaultdict(list)
        for f in fc["features"]:
            nm = (f["properties"].get("name") or "").strip()
            if not nm or not f.get("geometry"):
                continue
            g = F.to_albers(shape(f["geometry"]))
            by[tidy(nm)].append((g, f["properties"].get("src_id")))
        skipped = 0
        for nm, parts in by.items():
            if JUNK.search(nm):
                skipped += 1
                continue
            lines = [p for g, _ in parts for p in (g.geoms if hasattr(g, "geoms") else [g])]
            ids = [i for _, i in parts]
            for cg in cluster(lines):
                pt = cg.interpolate(0.5, normalized=True)
                out += line_candidates(f"municipal:{city}", nm, cg,
                                       {"source_ids": ids[:20], "lonlat": F.lonlat(pt.x, pt.y)})
        print(f"  municipal:{city}: {len(by)} names ({skipped} labels skipped)", flush=True)
    return out


def osm(path: Path) -> list[dict]:
    by = defaultdict(list)
    for ln in path.open():
        r = json.loads(ln)
        if "lines" in r and r.get("kind") in ("river", "stream", "canal", "drain", "ditch"):
            for c in r["lines"]:
                by[(tidy(r["name"]), r["kind"])].append(LineString(c))
    out, n = [], 0
    for (nm, kind), lines in by.items():
        if JUNK.search(nm):
            continue
        g = F.to_albers(MultiLineString(lines))
        for cg in cluster(list(g.geoms), gap=60.0):
            n += 1
            pt = cg.interpolate(0.5, normalized=True)
            out += line_candidates("osm", nm, cg, {"osm_kind": kind, "lonlat": F.lonlat(pt.x, pt.y)})
            if n % 2000 == 0:
                print(f"  osm: {n} channels, {len(out)} candidates", flush=True)
    print(f"  osm: {len(by)} names, {n} channels", flush=True)
    return out


STATION_SPLIT = re.compile(r"\s+(?:AT|NEAR|ABOVE|BELOW|DOWNSTREAM|UPSTREAM|ENTERING|"
                           r"BETWEEN|FROM|TO|IN|NR|ABV|BLW|@)\b.*$")


def station_water(name: str) -> tuple[str, str]:
    w = name.upper()
    paren = " ".join(re.findall(r"\(([^)]*)\)", w))
    w = STATION_SPLIT.sub("", re.sub(r"\s*\([^)]*\)", "", w)).strip()   # a diversion / outlet stays in the name
    return tidy(w), paren


def eccc() -> list[dict]:
    m = json.loads((ROOT / "data/curated/gauges/matches.json").read_text())["stations"]
    allst = {s["station"]: s for s in json.loads((ROOT / "data/source/bc_hydrometric_stations.json").read_text())}
    out = []
    for sid, st in allst.items():
        row = m.get(sid) or {}
        water, paren = station_water(st["name"])
        if not water or JUNK.search(water):
            continue
        blk = row.get("blk") or ""
        if not blk:
            mm = re.search(r"blk (\d+)", row.get("note") or row.get("reason") or "")
            blk = mm.group(1) if mm else ""
        wbks = row.get("wbk") or []
        trust = row.get("trust") or "unmatched"
        p = F.to_albers(Point(st["lon"], st["lat"]))
        dist = None
        if not blk and not wbks:      # an unmatched station: the nearest FWA line within 150 m
            near = [(geom.distance(p), b) for b, _n, geom, _e in F.lines_near(p.buffer(150), pad=0)]
            if not near:
                continue
            dist, blk = min(near)
            if dist > 150:
                continue
        ev = {"station": sid, "station_name": st["name"], "trust": trust,
              "distance_m": round(dist if dist is not None else (row.get("distance_m") or 0) or 0, 1),
              "lonlat": [round(st["lon"], 5), round(st["lat"], 5)], **({"qualifier": paren} if paren else {})}
        targets = ([("stream", blk, *blk_names(blk))] if blk else []) + \
                  [("lake", w, *wbk_names(w)) for w in wbks]
        for kind, tid, cur, items in targets:
            last = key(water).split()[-1:] or [""]
            if (kind == "lake" and last[0] in ("creek", "river", "brook")) or \
                    (kind == "stream" and last[0] in ("lake", "reservoir", "lakes")):
                continue                  # the station is on the other kind of water (Paq Lake Creek vs Lily Lake)
            rel = relation(water, cur)
            if rel is None:
                continue
            if trust in ("bound", "reviewed"):
                # a station named for the river at a point on a differently named channel is not a variant of it
                conf = "high" if rel == "unnamed" else ("medium" if rel in ("spelling", "generic_differs") else "low")
            elif trust == "auto":
                conf = "medium" if rel in ("spelling", "generic_differs") else "low"
            else:
                conf = "low"
            tgt = {"kind": kind, "blk": tid} if kind == "stream" else {"kind": kind, "wbk": tid}
            out.append({"target": tgt, "current_names": cur, "registry_items": items, "proposed": water,
                        "relation": rel, "source": "eccc", "evidence": ev, "confidence": conf,
                        "flags": (["station_name_is_a_point_on_the_water"] if rel == "different" else [])})
    print(f"  eccc: {len(allst)} stations", flush=True)
    return out


def _item_target(iid: str) -> dict:
    a, _, b = iid.partition(":")
    return {"kind": ITEMS.get(iid, {}).get("kind"), "item_id": iid, **({a: b} if a in ("blk", "wbk", "gnis") else {})}


def _item_names(iid: str) -> list[str]:
    it = ITEMS.get(iid)
    return list(dict.fromkeys([it["name"], *it.get("variants", [])])) if it else []


def dfo() -> list[dict]:
    out = []
    for p in sorted(glob.glob(str(ROOT / "data/curated/regulations/entries/dfo_salmon/region-*.json"))):
        for w in json.loads(Path(p).read_text())["waters"]:
            via = (w.get("match") or {}).get("via")
            others = {key(n) for i in (w.get("item_ids") or []) for n in _item_names(i)}
            for nm in [w["name"], *(w.get("aliases") or [])]:
                for iid in w.get("item_ids") or []:
                    if iid not in ITEMS:
                        continue
                    if len(w["item_ids"]) > 1 and key(nm) in others:
                        continue          # one regulation over several waters (Fraser + its channels): coverage
                    rel = relation(nm, _item_names(iid))
                    if rel is None:
                        continue
                    conf = "high" if via == "override" and w.get("locked") else ("medium" if via == "override" else "low")
                    out.append({"target": _item_target(iid), "current_names": _item_names(iid), "registry_items": [iid],
                                "proposed": tidy(nm), "relation": rel, "source": "dfo",
                                "evidence": {"water_id": w["water_id"], "match_via": via, "region": w["region"],
                                             "alias": nm != w["name"]},
                                "confidence": conf, "flags": []})
    print(f"  dfo: {len(out)} candidates", flush=True)
    return out


def synopsis() -> list[dict]:
    out = []
    for p in sorted(glob.glob(str(ROOT / "data/curated/regulations/entries/catalogue/region-*.json"))):
        for e in json.loads(Path(p).read_text())["entries"]:
            matched = e.get("matched") or []
            nm = re.sub(r"\s*\([^)]*\)", "", e.get("display_name") or e.get("name") or "").strip()
            nm = re.sub(r"[\"“”]", "", nm)
            if not nm or JUNK.search(nm) or "'s tributaries" in nm.lower() or "tributar" in nm.lower():
                continue
            others = {key(n) for i in matched for n in _item_names(i)}
            for iid in matched:
                if iid not in ITEMS:
                    continue
                if len(matched) > 1 and key(nm) in others:
                    continue              # the entry covers several waters and this name is another one's
                rel = relation(nm, _item_names(iid))
                if rel is None:
                    continue
                conf = "medium" if len(matched) == 1 else "low"
                out.append({"target": _item_target(iid), "current_names": _item_names(iid), "registry_items": [iid],
                            "proposed": tidy(nm), "relation": rel, "source": "synopsis",
                            "evidence": {"entry_id": e["entry_id"], "entry_name": e.get("name"), "matched": matched,
                                         "pages": e.get("source_pages")},
                            "confidence": conf,
                            "flags": (["entry_covers_several_waters"] if len(matched) > 1 else [])})
    print(f"  synopsis: {len(out)} candidates", flush=True)
    return out


def bcgnis_cache() -> list[dict]:
    cache = Path.home() / ".cache/bcfish-preverify"
    feats = {}
    for f in cache.glob("*"):
        b = f.read_bytes()
        if b"GEOGRAPHICAL_NAME" not in b[:4000]:
            continue
        try:
            d = json.loads(b)
        except ValueError:
            continue
        for ft in d.get("features", []):
            pr = ft["properties"]
            if pr.get("FEATURE_CLASS") == "Water Features" and pr.get("FEATURE_CATEGORY") in (
                    "Flowing Freshwater", "Standing Freshwater"):
                feats[pr["FEATURE_ID"]] = ft
    out = []
    for fid, ft in feats.items():
        pr = ft["properties"]
        nm = pr["GEOGRAPHICAL_NAME"]
        if str(fid) in BY_GNIS:     # FWA / the registry already carries this gazetted feature
            continue
        p = Point(ft["geometry"]["coordinates"])     # BC Albers already
        flowing = pr["FEATURE_CATEGORY"] == "Flowing Freshwater"
        if flowing:
            near = [(geom.distance(p), b) for b, _n, geom, _e in F.lines_near(p.buffer(200), pad=0)]
            if not near:
                continue
            d, blk = min(near)
            if d > 200:
                continue
            cur, items = blk_names(blk)
            tgt = {"kind": "stream", "blk": blk}
        else:
            near = [(poly.distance(p), w, nms) for _l, w, nms, _a, poly in F.polys_near(p.buffer(200), pad=0)]
            if not near:
                continue
            d, wbk, fwa = min(near)
            if d > 200:
                continue
            cur, items = wbk_names(wbk, fwa)
            tgt = {"kind": "lake", "wbk": wbk}
        rel = relation(nm, cur)
        if rel is None:
            continue
        conf = "medium" if d <= 50 and rel == "unnamed" else "low"
        out.append({"target": tgt, "current_names": cur, "registry_items": items, "proposed": nm, "relation": rel,
                    "source": "bcgnis_cache",
                    "evidence": {"feature_id": fid, "feature_type": pr["FEATURE_TYPE"], "position": pr.get("POSITION_TYPE"),
                                 "distance_m": round(d, 1), "relative_location": pr.get("RELATIVE_LOCATION")},
                    "confidence": conf, "flags": ["point_position_only"]})
    print(f"  bcgnis_cache: {len(feats)} water features, {len(out)} candidates", flush=True)
    return out


# ------------------------------------------------------------------------------------------------ existing variants
#: human labels for the pipeline's NameSource codes (pipeline/common/models/enums.py) — proposed display map
EXISTING_LABEL = {
    "gazette": ("BC Geographical Names (official, via FWA)", True),
    "side-channel": ("Main channel's official name (inherited by a side channel)", False),
    "override": ("Project curation (owner decision)", False),
    "gauge": ("ECCC hydrometric station name", False),
    "regulation": ("Freshwater Fishing Synopsis", False),
    "synopsis": ("Freshwater Fishing Synopsis (verbatim)", False),
    "stocking": ("Freshwater Fisheries Society stocking records", False),
    "bathymetry": ("BC lake bathymetry map", False),
    "marker": ("BC lake bathymetry map label", False),
    "vessel_restriction": ("Vessel Operation Restriction Regulations (SOR/2008-120)", False),
    "lake_survey": ("BC lake survey table", False),
    "dfo": ("DFO salmon regulations", False),
    "alias": ("Alternate name (source not recorded)", False),
}
CITY = {"burnaby": "municipal:burnaby", "port_moody": "municipal:port_moody", "squamish": "municipal:squamish",
        "abbotsford": "municipal:abbotsford"}


def audit_existing() -> dict:
    """Every name variant the build applies today, its source code, the label it should show, and where the code
    mislabels or drops the real source. Read-only."""
    rows = []
    for e in json.loads((ROOT / "data/curated/waters/name_variants.json").read_text()):
        for n in e.get("names", []):
            src, note = n.get("source", "regulation"), (n.get("note") or "")
            real = src
            issue = None
            low = note.lower()
            if src == "alias" and "city of burnaby" in low:
                real, issue = "municipal:burnaby", "municipal name stored as generic 'alias' (city lost)"
            elif src == "alias" and "fwa gazetted name" in low:
                real, issue = "gazette", "the FWA gazetted (official) name stored as 'alias'"
            elif src == "override" and not re.search(r"owner|decisions\.md", low):
                issue = "'override' with no recorded origin of the name itself"
            rows.append({"file": "data/curated/waters/name_variants.json", "name": n["name"], "target": e["target"],
                         "source_code": src, "real_source": real, "display": bool(n.get("display")),
                         "label": SOURCE_LABEL[real][0] if real in SOURCE_LABEL else EXISTING_LABEL[real][0],
                         "official": real == "gazette", "issue": issue})
    added = json.loads((ROOT / "data/curated/waters/added_streams.json").read_text())
    city_of_blk = {str(s["blk"]): s.get("source") for s in added["streams"]}
    for v in added["name_variants"]:
        city = city_of_blk.get(str(v["target_blk"]))
        if v["kind"] != "added":    # the FWA blk it names: the city is the nearest minted stream's (not stored)
            city = None
        rows.append({"file": "data/curated/waters/added_streams.json", "name": v["name"],
                     "target": {"blk": v["target_blk"]}, "source_code": "alias",
                     "real_source": CITY.get(city, "municipal:?"), "display": None, "official": False,
                     "label": SOURCE_LABEL[CITY[city]][0] if city in CITY else "a municipal stream layer (city not recorded)",
                     "issue": f"municipal name ({v['kind']}) applied as source 'alias', note 'added:{v['kind']}' — the city is "
                              + ("recoverable from the minted stream" if city else "NOT recorded")})
    unnamed_display = [r for r in rows if r["file"].endswith("added_streams.json") and r["target"]["blk"]
                       and not str(r["target"]["blk"]).startswith("-") and not BLK.get(str(r["target"]["blk"]), (None,))[0]]
    return {"label_map": {k: {"label": v[0], "official": v[1]} for k, v in EXISTING_LABEL.items()},
            "counts": {"variants": len(rows), "issues": sum(1 for r in rows if r["issue"]),
                       "by_source_code": dict(Counter(r["source_code"] for r in rows)),
                       "municipal_on_fwa_unnamed_lines": len(unnamed_display)},
            "issues": [r for r in rows if r["issue"]]}


# ------------------------------------------------------------------------------------------------ merge
RANK = {"high": 0, "medium": 1, "low": 2}
WATER_WORD = re.compile(r"\b(creek|river|brook|slough|canal|channel|ditch|stream|run|fork|rill|burn|drain|lake|lakes|"
                        r"pond|reservoir|cut|branch|springs?|falls|gulch|gully|diversion|headpond|lagoon|marsh|"
                        r"rivulet|backwater|cove|bay|inlet|outlet)\b", re.I)


def tkey(t: dict) -> str:
    return t.get("item_id") or (f"blk:{t['blk']}" if t.get("blk") else f"wbk:{t.get('wbk')}")


#: raw wsc PREFIXES, exactly as the atlas build removes them (`_apply_fwa_exclude`: startswith)
EXCLUDED = tuple(json.loads((ROOT / "data/curated/waters/added_streams.json").read_text()).get("fwa_exclude", []))


def superseded(t: dict) -> bool:
    """An FWA line the added-streams batch REMOVED (its municipal stream replaced it, name and all)."""
    if not t.get("blk") or t["blk"].startswith("-"):
        return False
    w = BLK.get(t["blk"], (None, 0, None))[2] or ""
    return any(w.startswith(e) for e in EXCLUDED)


def merge(rows: list[dict]) -> list[dict]:
    """One candidate per (target, proposed name key); every source kept with its own evidence."""
    by = {}
    n0 = len(rows)
    rows = [r for r in rows if not superseded(r["target"])]
    print(f"  dropped {n0 - len(rows)} rows on FWA lines the added streams replaced", flush=True)
    for r in rows:
        k = (tkey(r["target"]), key(r["proposed"]))
        label, ref = SOURCE_LABEL[r["source"]]
        rec = {"source": r["source"], "source_label": label, "source_ref": ref, "official": r["source"] in OFFICIAL,
               "evidence": r["evidence"], "confidence": r["confidence"], "flags": r["flags"]}
        if k not in by:
            by[k] = {"target": r["target"], "current_names": r["current_names"], "registry_items": r["registry_items"],
                     "proposed": r["proposed"], "relation": r["relation"], "sources": [rec]}
        else:
            c = by[k]
            if not any(s["source"] == r["source"] and s["evidence"] == r["evidence"] for s in c["sources"]):
                c["sources"].append(rec)
    out = []
    for c in by.values():
        best = min(RANK[s["confidence"]] for s in c["sources"])
        srcs = {s["source"].split(":")[0] for s in c["sources"]}
        conf = ["high", "medium", "low"][best]
        if len(srcs) >= 2 and conf == "medium":
            conf = "high"                    # corroborated by two independent sources
        if not WATER_WORD.search(c["proposed"]):     # "Arundell Spur", "Cat Stream"-like oddities: a person looks
            c.setdefault("name_flags", []).append("no_water_word")
            if conf == "high":
                conf = "medium"
        c["confidence"] = conf
        c["corroborated_by"] = sorted(srcs) if len(srcs) >= 2 else []
        out.append(c)
    # rivals: other names proposed for the same target
    by_t = defaultdict(list)
    for c in out:
        by_t[tkey(c["target"])].append(c["proposed"])
    for c in out:
        rivals = sorted({n for n in by_t[tkey(c["target"])] if key(n) != key(c["proposed"])})
        c["rival_names"] = rivals
        line = c["target"]["kind"] == "stream" and "blk" in c["target"]
        single = not rivals and not any(f.startswith(("one_name_many_blks", "name_already_on"))
                                        for s in c["sources"] for f in s["flags"])
        ov = max((s["evidence"].get("blk_overlap", 0) for s in c["sources"]), default=0)
        c["auto_accept_eligible"] = bool(c["confidence"] == "high" and single and c["relation"] == "unnamed"
                                         and (not line or ov >= 0.8 or any(s["source"] == "eccc" for s in c["sources"])))
    out.sort(key=lambda c: (RANK[c["confidence"]], c["relation"] != "unnamed", c["proposed"]))
    return out


if __name__ == "__main__":
    rows = []
    rows += municipal()
    rows += eccc()
    rows += dfo()
    rows += synopsis()
    rows += bcgnis_cache()
    if "--osm" in sys.argv:
        rows += osm(Path(sys.argv[sys.argv.index("--osm") + 1]))
    cands = merge(rows)
    raw = Counter((r["source"], r["confidence"]) for r in rows)
    doc = {"generated": "2026-10-28", "tool": "handoff/PLAN/curation-review/name-variants/mine.py (read-only)",
           "about": "Name-variant CANDIDATES for FWA lines / lakes / registry items: one row per (target, proposed name); "
                    "every source keeps its own label, ref and evidence. Nothing applied.",
           "counts": {"candidates": len(cands),
                      "by_confidence": dict(Counter(c["confidence"] for c in cands)),
                      "by_relation": dict(Counter(c["relation"] for c in cands)),
                      "auto_accept_eligible": sum(c["auto_accept_eligible"] for c in cands),
                      "raw_by_source_confidence": {f"{s}|{c}": n for (s, c), n in sorted(raw.items())}},
           "existing_variants_audit": audit_existing(),
           "candidates": cands}
    (HERE / "candidates.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False))
    print(json.dumps(doc["counts"], indent=1))
