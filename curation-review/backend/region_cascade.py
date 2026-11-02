"""EVERY REGION'S DFO CASCADE (owner 2026-11-02), read only.

What a DFO page says holds BEFORE any table row — its region-wide scope sentences (`cascade.page_statements`) and,
in Region 6, its lettered section banners — laid out as an ordered tree, set against what the catalogue encodes
(the region / section / tidal-Area default locations and their extents) and what the bundle answers for a water
with no row (`deliver.salmon.NOT_LISTED`: closed). A difference is a PROPOSAL quoting DFO's own words; nothing here
writes a curated file.

    GET /api/dfo/cascade-regions                every region: tree, unlisted default, code, proposals, odd rules
    GET /api/dfo/cascade-regions/{slug}/map     the region outline + each scope's coverage + the Section E check

"FLOWS INTO AREA N". A water's tidal Area is its BASIN's mouth's nearest PFMA Area (`atlas.splits.pfma_drainage`).
The atlas mints `area:drains_to:pfma_N` only for Region 6's Areas 3-6; the province-wide basin -> Area table is
cached at `data/generated/review/pfma_basin_areas.json` (`python region_cascade.py --basins`, ~40 s, run it under
scripts/heavy.sh) so Region 2's Areas 13/15/16/28/29 can be told apart here without an atlas rebuild.
"""
from __future__ import annotations

import gzip
import json
import re
import sqlite3
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

import reuse
from pipeline.regs.review import dfo as D

SLUGS = ["1", "2", "3", "4", "5a", "5b", "6", "7", "8"]
BASINS = Path(reuse.REACH_DIR).parents[1] / "review" / "pfma_basin_areas.json"
TIERS = {0: "region default", 1: "section default", 2: "tidal-area default", 3: "named water"}

#: the registry area each page's region is (5a / 5b split region 5 by management unit)
REGION_AREA = {"1": "area:region:1", "2": "area:region:2", "3": "area:region:3", "4": "area:region:4",
               "5a": "area:region:5", "5b": "area:mu_group:management_units_5_6_to_5_11", "6": "area:region:6",
               "7": ("area:region:7a", "area:region:7b"), "8": "area:region:8"}
#: the outline: WMUs by id
WMU_PREFIX = {"5a": [f"5-{n}" for n in (1, 2, 3, 4, 5, 12, 13, 14, 15, 16)], "5b": [f"5-{n}" for n in range(6, 12)]}

#: Region 6 scope ids -> the ACTIVE location carrying it, narrowest first (the bundle's chain order)
R6_ORDER = [("E:areas-5", "6:e:area:areas-5~2"), ("E:areas-6", "6:e:area:areas-6~2"),
            ("E:areas-3-4-5-6", "6:e:area:areas-3-4-5-6~2"), ("B(i)", "6:b(i):section"), ("B(ii)", "6:b(ii):section"),
            ("C", "6:c:section"), ("D", "6:d:section"), ("F", "6:f:closure"), ("E", "6:e:section")]
R6_SIBLINGS = {"B": "6:b:section", "C": "6:c:section", "D": "6:d:section", "F": "6:f:closure"}
R6_SIBLING_PREFIX = {"100-": "Fraser (F)", "400-": "Skeena (B)", "500-": "Nass (C)"}


CACHE = Path(__file__).resolve().parents[2] / "cache" / "dfo_salmon"


def _raw(slug: str) -> str:
    return (CACHE / "raw" / f"region{slug}-eng.html").read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def _basins(_m: int) -> dict:
    return json.loads(BASINS.read_text()) if BASINS.exists() else {}


def basins() -> dict:
    return _basins(BASINS.stat().st_mtime_ns if BASINS.exists() else 0)


def area_of(wsc: str) -> int | None:
    from pipeline.atlas.splits import pfma_drainage as P
    return P.area_of(wsc or "", basins())


# ── the tree ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def _ext(e: dict) -> str:
    s = f"{e.get('op')} {e.get('area_id') or e.get('item_id') or ''}"
    if e.get("splits"):
        s += f" at {', '.join(e['splits'])}"
    if e.get("within_area"):
        s += f" ∩ {e['within_area']}"
    if e.get("outside_areas"):
        s += f" − {{{', '.join(e['outside_areas'])}}}"
    if e.get("feature_types"):
        s += f" [{'/'.join(e['feature_types'])} only]"
    return s.strip()


def _code_default(loc: dict) -> dict:
    rows, _rules = D.rule_rows(loc)
    st = loc.get("source_text") or {}
    return {"id": loc["location_id"], "status": loc.get("status"), "precedence": loc.get("precedence", 0),
            "tier": TIERS.get(loc.get("precedence", 0)), "scope": st.get("specific_area") or st.get("waters") or "",
            "extents": [_ext(e) for e in (loc.get("binding") or {}).get("extents") or []], "rows": rows}


def _region(slug: str, locs: dict) -> dict:
    from pipeline.deliver import salmon as S
    from pipeline.regs.dfo_salmon import cascade as C
    from pipeline.regs.dfo_salmon.parse import parse_cached
    html = _raw(slug)
    stmts = C.page_statements(html)
    parsed = parse_cached(slug, CACHE)
    mine = {lid: loc for lid, (s, loc, _w) in locs.items() if s == slug}
    defaults = [_code_default(l) for l in mine.values() if l.get("precedence", 3) < 3 or not l.get("water_id")]
    waters_by_sec = Counter((l.get("section") or "") for l in mine.values() if l.get("water_id"))
    code_unlisted = S.NOT_LISTED["t"]

    def defaults_for(sec: str | None, areas=()) -> list:
        out = []
        for d in defaults:
            loc = mine[d["id"]]
            if tuple((loc.get("source_text") or {}).get("areas") or ()) != tuple(areas):
                continue
            if (loc.get("section") or "region") == (sec or "region"):
                out.append(d)
        return out

    proposals, odd = [], []
    root = {"id": "region", "label": f"Region {slug.upper()} — {parsed.region_name}", "kind": "region",
            "quote": None, "children": [], "code": [], "n_waters": waters_by_sec.get("", 0)}
    whole = [s for s in stmts if not s["areas"]]
    if whole:
        root["quote"] = whole[0]["t"]
        root["unlisted"] = whole[0]["unlisted"]
    tree_unlisted = {"page": [{"scope": s["areas"] or "whole region", "says": s["unlisted"], "t": s["t"]} for s in stmts],
                     "code": code_unlisted}

    # region-wide sentences limited to tidal Areas (Region 2)
    for s in stmts:
        if not s["areas"]:
            continue
        node = {"id": "areas-" + "-".join(map(str, s["areas"])), "kind": "tidal_areas", "areas": s["areas"],
                "label": f"waters flowing into Areas {', '.join(map(str, s['areas']))}", "quote": s["t"],
                "unlisted": s["unlisted"], "children": [], "code": defaults_for("region", s["areas"])}
        if not node["code"]:
            proposals.append({
                "id": f"{slug}:areas-{'-'.join(map(str, s['areas']))}", "scope": node["label"], "quote": s["t"],
                "code_now": f"nothing — an unlisted water here gets the bundle's '{code_unlisted}'",
                "proposal": (f"a tidal-area default (precedence 2) over Region {slug}'s waters draining to Areas "
                             f"{s['areas']}, answering '{s['unlisted']}' where no row does")})
        root["children"].append(node)

    # section banners (Region 6) as their containment tree
    if parsed.sections:
        by = {}
        for sec in parsed.sections:
            parent = None if sec.key == "A" else (sec.key.split("(")[0] if "(" in sec.key else "A")
            by[sec.key] = {"id": sec.key, "kind": "section", "label": sec.title, "quote": sec.title,
                           "falls_back_to_a": sec.falls_back_to_a, "children": [], "parent": parent,
                           "code": defaults_for(sec.key), "n_waters": waters_by_sec.get(sec.key, 0)}
        # the tidal-Area rows sit under the section that printed them
        for d in defaults:
            loc = mine[d["id"]]
            areas = (loc.get("source_text") or {}).get("areas") or []
            if areas and loc.get("section") in by:
                sid = f"{loc['section']}:areas-{'-'.join(map(str, areas))}"
                kid = next((k for k in by[loc["section"]]["children"] if k["id"] == sid), None)
                if kid is None:
                    kid = {"id": sid, "kind": "tidal_areas", "areas": areas, "label": d["scope"], "quote": d["scope"],
                           "children": [], "code": []}
                    by[loc["section"]]["children"].append(kid)
                kid["code"].append(d)
                if d["status"] == "active":
                    kid["label"] = kid["quote"] = d["scope"]
        for k, n in by.items():
            if n["parent"]:
                by[n["parent"]]["children"].append(n)
        root["children"].append(by["A"])
        for n in by.values():
            n["children"].sort(key=lambda c: (c["kind"] != "tidal_areas", c["id"]))
    elif defaults:
        for d in defaults:
            if not (mine[d["id"]].get("source_text") or {}).get("areas"):
                root["children"].append({"id": d["id"], "kind": "table_default", "label": d["scope"],
                                         "quote": d["scope"], "children": [], "code": [d]})

    # ── comparisons the page's own words decide ──
    for s in stmts:
        if s["unlisted"] == "release" and slug != "6":      # 6: section A lists every species all year
            proposals.append({
                "id": f"{slug}:unlisted{'-areas-' + '-'.join(map(str, s['areas'])) if s['areas'] else ''}",
                "scope": "a water with no row" + (f" (Areas {s['areas']})" if s["areas"] else ""),
                "quote": s["t"], "code_now": f"'{code_unlisted}' (deliver.salmon.NOT_LISTED)",
                "proposal": "'daily limit 0' — fishing allowed, no retention (release), not 'closed'"})
    if slug == "4":
        tbl = next((d for d in defaults if d["rows"]), None)
        if tbl:
            odd.append(f"Header: \"{stmts[0]['t']}\" but the table's only row reads \"{tbl['rows'][0]}\" — "
                       "non-retention is not 'no fishing'.")
    if slug in ("5a", "7"):
        odd.append(f"\"{stmts[0]['t']}\" — yet the table lists in-season openings ({waters_by_sec.get('', 0)} rows on "
                   "named waters); the closure is the default, the rows are the exceptions.")
    if slug == "5b":
        odd.append("Two definitions of 5B on one page: \"Management Units 5-6 to 5-11\" and \"(i.e. Region 5 not "
                   "including the Fraser River watershed)\". They differ on the ground (MU lines do not follow the "
                   "Fraser divide); the code binds the MU list.")
        odd.append("\"All lakes or streams or part thereof in the aggregate (combined total)\" — a quota shared across "
                   "waters, which the catalogue has no field for.")
    if slug == "2":
        odd.append("Region 2's scopes are keyed by the TIDAL AREA a water drains to, not by watershed or MU; "
                   "`header.parse_header` stops at these sentences, so nothing read them before this view.")
        odd.append("\"Squamish watershed: the aggregate daily limit of hatchery-marked coho in the entire Squamish "
                   "non-tidal systems combined is one (1).\" — an aggregate bucket over a whole watershed.")
    if slug == "6":
        odd += [
            "E is a residual: \"Other Mainland Watersheds, except for the Fraser River watershed\" — region 6 minus "
            "B, C, D, F. It includes the transboundary salmon rivers (Stikine, Taku, Alsek/Tatshenshini, Unuk) "
            "whose mouths are in Alaska, and the Arctic drainages (Liard, Dease, Turnagain…).",
            "E's tidal rows nest by specificity, not page order: Area 5 (streams) and Area 6 (streams) answer before "
            "Areas 3-4-5-6 (streams AND lakes) — a lake draining to Area 5 skips the Area 5 row.",
            "B is a pure container: the page prints no B-wide row (the B section default is dormant); B(i)/B(ii) "
            "split the Skeena watershed at one point, the CNR bridge at Terrace.",
            "F is a closure written in its banner: \"There is no fishing for salmon in the portions of the Fraser "
            "River watershed in Region 6.\"",
            "B(ii)'s \"No fishing for eulachon\" row is not a salmon rule.",
            "The header's \"daily limit … is zero\" is unreachable in practice: A (\"All Region 6 waters\") lists every "
            "species all year.",
        ]
        e = mine.get("6:e:section") or {}
        for ex in (e.get("binding") or {}).get("extents") or []:
            if ex.get("area_id") and ex.get("area_id") in (ex.get("outside_areas") or []):
                proposals.append({"id": "6:e:noop-extent", "scope": "E (code)",
                                  "quote": by["E"]["quote"] if parsed.sections else "",
                                  "code_now": f"extent `{_ext(ex)}` selects nothing",
                                  "proposal": "drop the no-op second extent from 6:e:section"})
        dup = Counter(re.sub(r"~\d+$", "", d["id"]) for d in defaults)
        for base, n in dup.items():
            if n > 1:
                odd.append(f"{base}: {n} locations for one scope (older page wordings kept dormant).")
    return {"slug": slug, "name": parsed.region_name, "url": parsed.url, "statements": stmts, "tree": root,
            "unlisted": tree_unlisted, "defaults": defaults, "n_waters": sum(waters_by_sec.values()),
            "proposals": proposals, "odd": odd}


def regions() -> dict:
    locs = D.locations()
    return reuse._json_safe({"regions": [_region(s, locs) for s in SLUGS], "basins_cached": BASINS.exists()})


# ── the map ──────────────────────────────────────────────────────────────────────────────────────────────────────────

def _outline(slug: str):
    from shapely import wkb as _wkb
    from shapely.ops import unary_union
    src = reuse.SOURCE / "bc_fisheries_data.gpkg"
    ids = WMU_PREFIX.get(slug)
    pre = slug.rstrip("ab") + "-"
    out = []
    db = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        for mu, blob in db.execute("SELECT WILDLIFE_MGMT_UNIT_ID, geom FROM wmu"):
            if not blob or not mu or (mu not in ids if ids else not str(mu).startswith(pre)):
                continue
            env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get((blob[3] >> 1) & 0x07, 0)
            out.append(_wkb.loads(bytes(blob[8 + env:])))
    finally:
        db.close()
    return unary_union(out).buffer(0) if out else None


def _reach_rows(ids: set) -> dict:
    with gzip.open(Path(reuse.REACH_DIR) / "dfo_salmon.jsonl.gz", "rt", encoding="utf-8") as fh:
        return {r["location_id"]: set(r.get("sections") or ()) for r in map(json.loads, fh) if r["location_id"] in ids}


def _items(aid) -> set:
    reg = reuse._registry()
    out = set()
    for a in (aid if isinstance(aid, tuple) else (aid,)):
        out |= set(getattr(reg.get(a), "section_ids", ()) or ())
    return out


SCOPE_STYLE = {
    "E:areas-5": ("E · Area 5 streams", "#be185d"), "E:areas-6": ("E · Area 6 streams", "#7c3aed"),
    "E:areas-3-4-5-6": ("E · Areas 3/4/5/6 streams + lakes", "#db2777"), "B(i)": ("B(i) Skeena above CNR bridge", "#a16207"),
    "B(ii)": ("B(ii) Skeena below CNR bridge", "#ca8a04"), "C": ("C Nass", "#0e7490"), "D": ("D Haida Gwaii", "#4d7c0f"),
    "F": ("F Fraser (closed)", "#6b7280"), "E": ("E · other: drains to Area 7/8", "#f97316"),
    "E:transboundary": ("E · other: no BC tidal Area (Alaska / Yukon / Arctic)", "#fdba74"),
    "gap": ("in Region 6 but in no section", "#dc2626"),
    "areas-28-29": ("Areas 28/29 — no fishing except listed", "#9ca3af"),
    "areas-13-15-16": ("Areas 13/15/16 — daily limit 0", "#2563eb"),
    "elsewhere": ("drains to no BC tidal Area (via the US)", "#dc2626"),
    "nocode": ("lake with no FWA watershed code (Area unknown)", "#a3a3a3"),
    "region": ("the whole region", "#0f766e"),
}


@lru_cache(maxsize=16)
def _region_map(slug: str, min_order: int, tol: float, _stamp: tuple) -> dict:
    from shapely.geometry import MultiLineString, mapping
    from shapely.ops import linemerge, transform
    nodes = reuse._graph().nodes
    geoms = reuse._geoms()
    to_ll = reuse._to_lonlat()
    assign: dict[str, str] = {}
    check: dict = {}
    flagged: dict[str, str] = {}

    def wsc(s):
        n = nodes.get(s)
        return (getattr(n, "wsc", "") or "") if n else ""

    if slug == "6":
        rows = _reach_rows({lid for _k, lid in R6_ORDER} | set(R6_SIBLINGS.values()) | {"6:a:region"})
        for k, lid in R6_ORDER:
            for s in rows.get(lid, ()):
                assign.setdefault(s, k)
        for s in rows.get("6:a:region", set()) | _items("area:region:6"):
            assign.setdefault(s, "gap")             # the registry's region too: the reach clips straddlers
        E = rows.get("6:e:section", set())
        by_area = Counter()
        for s in E:
            a = area_of(wsc(s))
            by_area[str(a)] += 1
            if assign.get(s) == "E" and a is None:
                assign[s] = "E:transboundary" if wsc(s) else "nocode"
            pre = wsc(s)[:4]
            if pre in R6_SIBLING_PREFIX:
                flagged[s] = f"E section on {R6_SIBLING_PREFIX[pre]} code"
        overlap = {k: len(E & rows.get(lid, set())) for k, lid in R6_SIBLINGS.items()}
        for k, lid in R6_SIBLINGS.items():
            for s in E & rows.get(lid, set()):
                flagged[s] = f"E overlaps {k}"
        gaps = [s for s, k in assign.items() if k == "gap"]
        gap_rows = []
        for s in sorted(gaps):
            n = nodes.get(s)
            a = area_of(wsc(s))
            flagged[s] = f"in no section (drains to Area {a})" if a else "in no section"
            gap_rows.append({"section": s, "name": getattr(n, "display_name", "") if n else "", "wsc": wsc(s)[:24],
                             "area": a, "order": getattr(n, "stream_order", None) if n else None})
        coast_gaps = [g for g in gap_rows if g["area"] in (3, 4, 5, 6)]
        check = {
            "E_sections": len(E), "E_by_tidal_area": dict(by_area.most_common()),
            "E_overlap_siblings": overlap,
            "E_on_sibling_codes": sum(1 for v in flagged.values() if v.startswith("E section on")),
            "gaps": gap_rows, "coast_gaps": len(coast_gaps),
            "E_named_outside_areas_3_6": _named(E, nodes, lambda s: area_of(wsc(s)) not in (3, 4, 5, 6)),
            "verdict": ("E holds no Fraser/Skeena/Nass/Haida Gwaii section and overlaps no sibling"
                        if not any(overlap.values()) and not any(v.startswith("E section on") for v in flagged.values())
                        else "E reaches into a sibling — see flagged"),
        }
    elif slug == "2":
        r2 = _items(REGION_AREA["2"])
        by = Counter()
        out = defaultdict(set)
        for s in r2:
            a = area_of(wsc(s))
            k = ("areas-28-29" if a in (28, 29) else "areas-13-15-16" if a in (13, 15, 16)
                 else "elsewhere" if wsc(s) else "nocode")
            assign[s] = k
            by[str(a)] += 1
            out[k].add(s)
        check = {"R2_sections": len(r2), "by_tidal_area": dict(by.most_common()), "basins_cached": BASINS.exists(),
                 "elsewhere_named": _named(out["elsewhere"], nodes, lambda s: True),
                 "verdict": (f"{len(out['areas-28-29']) + len(out['areas-13-15-16']):,} of {len(r2):,} sections drain to Areas "
                             f"13/15/16/28/29; {len(out['elsewhere']):,} drain to no BC tidal Area (via the US); "
                             f"{len(out['nocode']):,} lakes carry no watershed code")}
    else:
        aid = REGION_AREA[slug]
        secs = _items(aid)
        if slug == "5a":
            secs -= _items(REGION_AREA["5b"])
        for s in secs:
            assign[s] = "region"

    lines: dict[str, list] = defaultdict(list)
    counts = Counter(assign.values())
    for s, k in assign.items():
        if s.startswith("lake:"):
            continue
        n = nodes.get(s)
        o = getattr(n, "stream_order", None) if n else None
        if s not in flagged and min_order > 1 and (o or 0) < min_order:
            continue
        g = geoms.get(s)
        if g is None or g.is_empty:
            continue
        lines[k].extend(getattr(g, "geoms", [g]))
    feats = []
    for k, ls in lines.items():
        g = linemerge(MultiLineString(ls)).simplify(tol, preserve_topology=False)
        if not g.is_empty:
            feats.append({"type": "Feature", "properties": {"scope": k, "kind": "line"},
                          "geometry": mapping(transform(to_ll, g))})
    for s, why in flagged.items():
        g = geoms.get(s)
        if g is not None and not g.is_empty:
            feats.append({"type": "Feature", "properties": {"scope": "flag", "kind": "flag", "why": why, "section": s},
                          "geometry": mapping(transform(to_ll, g))})
    ol = _outline(slug)
    if ol is not None:
        feats.insert(0, {"type": "Feature", "properties": {"kind": "outline"},
                         "geometry": mapping(transform(to_ll, ol.simplify(200)))})
    legend = [{"scope": k, "label": SCOPE_STYLE.get(k, (k, "#000"))[0], "color": SCOPE_STYLE.get(k, (k, "#000"))[1],
               "n": counts[k]} for k in [*dict.fromkeys([k for k, _ in R6_ORDER] + list(SCOPE_STYLE))] if counts.get(k)]
    return {"slug": slug, "min_order": min_order, "legend": legend, "check": check,
            "geojson": {"type": "FeatureCollection", "features": feats}}


def _named(secs, nodes, keep) -> list:
    c = Counter()
    for s in secs:
        if keep(s):
            n = nodes.get(s)
            nm = getattr(n, "display_name", "") if n else ""
            if nm:
                c[nm] += 1
    return c.most_common(30)


def region_map(slug: str, min_order: int = 4, tol: float = 60.0) -> dict:
    if slug not in SLUGS:
        raise KeyError(slug)
    stamp = (str(BASINS), BASINS.stat().st_mtime_ns if BASINS.exists() else 0,
             (Path(reuse.REACH_DIR) / "dfo_salmon.jsonl.gz").stat().st_mtime_ns)
    return reuse._json_safe(_region_map(slug, int(min_order), float(tol), stamp))


if __name__ == "__main__":
    import sys
    if "--basins" in sys.argv:
        from pipeline.atlas.splits import pfma_drainage as P
        b = P.basin_areas(areas=range(1, 200))
        BASINS.parent.mkdir(parents=True, exist_ok=True)
        BASINS.write_text(json.dumps(b))
        print(f"{len(b):,} basins -> {BASINS}")
