"""pfma_drainage — which Pacific Fishery Management Area a water drains into.

DFO Region 6 section E scopes three rules by tidal Area: *"All streams flowing into tidal water
Area 5"*, Area 6, and Areas 3/4/5/6. That is a drainage relation, not containment — the streams
are freshwater ABOVE the marine area — so testing sections against a PFMA polygon selects the few
estuary slivers that touch salt water, or nothing.

**THE WHOLE THING IS A STRING PREFIX.** The FWA watershed code is a path from the ocean inward, so
a tributary's code nests under its parent's:

    400-000000-000000-...   Skeena River          a MAJOR watershed's mainstem
    400-179139-000000-...   a Skeena tributary
    400-179139-522840-...   a tributary of that
    910-665062-000000-...   Kitimat River         a non-major tidal stream
    915-669295-093269-...   a stream on coastline trunk 669295

So a basin is a prefix — `400-` is the whole Skeena, `910-665062-` the whole Kitimat — and
membership is a `startswith`, not a graph walk. Verified against the tributary walk: of the
84,624 sections `build_reach` reaches from the Skeena, **84,617 carry prefix `400` — 99.99%**.

That gives three steps, and no step needs the flow graph:

    1. BASIN     the code's first group for a major watershed, its first two for a `9XX` coastal
                 stream. 3,497 distinct basins in the Areas 3-6 envelope.
    2. ITS MOUTH among that basin's segments at DOWNSTREAM_ROUTE_MEASURE 0, the one nearest any
                 Area. A river's braids all share its prefix and each has a local measure 0 — the
                 Skeena has 486 such blue lines, one of them 110 km inland — and taking the
                 nearest picks the true mouth without needing to identify the principal line.
    3. ITS AREA  the Area that mouth is nearest. Measured: median 0 m, p95 0 m, max 6.0 km.
                 NEAREST rather than containment, because a mouth digitised a few metres inland of
                 the polygon must still resolve; nearest rather than a BUFFER, because a buffer
                 lets two neighbouring Areas claim one stream while nearest returns exactly one.

Then every section whose code starts with that basin's prefix carries that Area — mainstem,
tributaries and braids alike, in one lookup.

**A CITY CREEK WITH ITS OWN TIDAL MOUTH (T1246, owner 2026-11-27 "Fix it properly now").** An added city line that
drains straight to the sea has a code of its own (`added_streams.coastal`), so no FWA line marks its basin's mouth and
step 2 found none: such creeks of Burnaby, Port Moody and Squamish drained to no Area, and DFO's "waters of
Region 2 flowing into Areas 28 and 29" missed them. Its own mouth (the line's first point, `city_tidal_mouths`) is a
candidate in step 2 beside the FWA mouths (`OWN_MOUTH_TAKES_ITS_AREA`). A city creek that drains into a river or
another creek has a code under its receiver's, so it takes its receiver's Area through the prefix, as an FWA
tributary does; it needs no mouth of its own.

**A MOUTH ON AN AREA LINE.** "The Area its mouth lies in" is the NEAREST Area polygon (distance 0 inside it). A mouth
on the line between two Areas is at distance 0 from both: it takes the LOWER Area number (`TIE_TAKES_LOWER_AREA`),
so the answer never depends on row order. The same rule picks among a basin's mouths: the nearest to any Area, then
the lower Area number.

**The layer is `pfma_areas`** (`WHSE_ADMIN_BOUNDARIES.DFO_PFMA_STAT_AREA_BDRY_SP`), fetched by
`data.fetch_data --layers pfma_areas`. Section E names Areas, so Areas are the right granularity,
and the subareas layer is not on the public WFS anyway.

⚠️ **120 of that layer's 171 polygons carry `MANAGEMENT_AREA == 0`** — land and filler. Drop them
or every stream in BC lands in "Area 0".
"""

from __future__ import annotations

import collections
import logging
from typing import Dict, Iterable, Optional

from pipeline.common.curated import SOURCE
from pipeline.common.utils import wsc as _wsc

logger = logging.getLogger(__name__)

_GPKG = str(SOURCE.fwa_gpkg)

LAYER = "pfma_areas"
AREA_FIELD = "MANAGEMENT_AREA"
#: The Areas DFO Region 6 section E names. Others exist; these are the ones a rule asks for.
SECTION_E_AREAS = (3, 4, 5, 6)
#: (T349, owner 2026-11-02) The Areas DFO Region 2's page keys its two unlisted-water scopes on: "waters of Region 2
#: flowing into Areas 28 and 29" (no fishing except listed) and "… flowing into Areas 13, 15, and 16" (daily limit 0).
REGION_2_AREAS = (13, 15, 16, 28, 29)
#: Every Area the registry mints an `area:drains_to:pfma_N` for (`registry.build`).
MINTED_AREAS = SECTION_E_AREAS + REGION_2_AREAS

#: FWA's 9XX major-watershed groups — the coastal drainages, which is what a basin prefix's first
#: group means. Kept here because it turns an opaque number into a place, and because three of
#: them are NOT British Columbia.
COASTAL_GROUPS = {
    "900": "South Coast Rivers — south of Cape Caution",
    "905": "South Coast Islands — south of Cape Caution",
    "910": "North Coast Rivers — north of Cape Caution",
    "915": "North Coast Islands — north of Cape Caution",
    "920": "Vancouver Island East Rivers — Church Point to Cape Scott",
    "925": "Vancouver Island East — Gulf Islands",
    "930": "Vancouver Island West Rivers — Church Point to Cape Scott",
    "935": "Vancouver Island West Islands",
    "940": "Graham Island Rivers",
    "945": "Graham Island — offshore islands",
    "950": "Moresby Island Rivers",
    "955": "Moresby Island — offshore islands",
    "960": "Alaska Rivers — Hyder AK to south of the 60th parallel",
    "970": "Washington Coast Rivers — north of 49°N draining to the Columbia in WA",
    "990": "Alsek River drainage",
}

#: **NOT BRITISH COLUMBIA.** FWA maps these because they cross the line, but a DFO Pacific Region
#: rule does not reach them, and geometry alone will not exclude them: Hyder sits at the head of
#: Portland Canal, so a `960` stream can be metres from Area 3. Measured, these are 250 of the 254
#: basins the distance guard was catching — 237 Alsek, 13 Alaska — so excluding them by CODE is
#: both exact and self-explaining where a distance was neither.
OUT_OF_PROVINCE_GROUPS = {"960", "970", "990"}

#: How far a basin's mouth may sit from the Area it drains into. This catches what the code table
#: cannot: INLAND MAJOR watersheds, which carry no 9XX group at all. Measured, exactly four reach
#: this far — Nazcha Creek (`800-`, 341 km), Wolverine Creek (`700-`, 254 km), Hoy Creek (`200-`,
#: 171 km) and Olatine Creek (`600-`, 76 km) — all draining to the Arctic, the Peace or the Liard
#: rather than the Pacific. The data leaves a wide gap to sit in: across all 15,720 basins p97 is
#: 0 m and 15,464 are within 10 km, then the rest jump to hundreds of kilometres. 10 km is chosen
#: because the worst OBSERVED coastal mouth offset is 6.0 km.
MAX_MOUTH_OFFSET_M = 10_000

#: (T1246) A city creek with its own tidal mouth takes the Area that mouth lies in (`city_tidal_mouths`). Off, the
#: city creeks with codes of their own drain to no Area and DFO's Area scopes skip them (before 2026-11-27).
OWN_MOUTH_TAKES_ITS_AREA = True
#: (T1246) A mouth equally near two Areas (on the line between them) takes the lower Area number; FWA's basins too.
TIE_TAKES_LOWER_AREA = True

#: `basin_areas` reads the whole province, so memoise it: a second call in the same process (the
#: report, a test, a build step) costs nothing.
_CACHE: Dict[str, dict] = {}   # {gpkg|all: {"fwa": {basin: [(d, area)]}, "areas": polygons}}


def area_id(area: int) -> str:
    """The registry id for a drainage area.

    Deliberately NOT `area:pfma:5` — that would name the MARINE polygon, and a rule bound to it
    selects the estuary slivers that touch salt water and nothing else. This names the land
    draining to it. (The resolver treats the id as an opaque key either way; the distinction is
    for whoever reads the rule. The middle segment is real, though: `area_kind: "drains_to"`
    unions every one of these.)
    """
    return f"area:drains_to:pfma_{area}"


def basin_of(watershed_code: str) -> Optional[str]:
    """The prefix identifying the tidal water this code drains to, or None.

    A major watershed is its first group (`400-` is the whole Skeena). A `9XX` coastal stream is
    its first two (`910-665062-` is the whole Kitimat). `999-` is FWA's unknown.
    """
    if not isinstance(watershed_code, str) or not watershed_code:
        return None
    groups = _wsc.groups(watershed_code)
    if not groups or _wsc.is_unknown(watershed_code):
        return None
    if _wsc.in_nine_hundreds(watershed_code):
        return _wsc.head(watershed_code, 2) + "-" if len(groups) > 1 else None
    return groups[0] + "-"


def city_tidal_mouths(streams: Iterable[dict]) -> list[tuple[str, float, float]]:
    """(basin prefix, x, y) in EPSG:3005 of every added city line that drains straight to the sea (`receiver_kind`
    "tidal"): its mouth is its line's first point (the added streams are stored mouth first). The input to
    `basin_areas(city_mouths=...)`; a line whose receiver is a river or another creek is not one (it inherits)."""
    out = []
    for s in streams:
        if s.get("receiver_kind") != "tidal":
            continue
        basin = basin_of(s.get("wsc") or "")
        segs = s.get("segments") or []
        if not basin or not segs or not segs[0].get("coords3005"):
            raise ValueError(f"added stream {s.get('blk')} ({s.get('name')}) drains to the sea but has no code or no "
                             "line: its Area cannot be found")
        x, y = segs[0]["coords3005"][0][:2]
        out.append((basin, float(x), float(y)))
    return sorted(out)


def _pick(cands: Iterable[tuple[float, int]]) -> tuple[float, int]:
    """The (distance, Area) a basin takes among its candidate mouths' nearest Areas: the nearest, then (on a tie,
    `TIE_TAKES_LOWER_AREA`) the lower Area number."""
    return min(cands) if TIE_TAKES_LOWER_AREA else min(cands, key=lambda c: c[0])


def basin_areas(areas: Iterable[int] = SECTION_E_AREAS, gpkg: str = _GPKG, *,
                city_mouths: Iterable[tuple[str, float, float]]) -> Dict[str, int]:
    """`{basin prefix -> area number}` for every basin draining into one of `areas`.

    A section belongs to a basin iff its watershed code starts with the prefix, so this map is the
    whole membership answer: no graph, no walk, no per-section geometry.

    **The search is province-wide and `areas` only filters the RESULT.** Restricting it earlier is
    a bug that bites twice: a basin's mouth may sit outside the Areas being asked about (ask only
    for Area 5 and the Skeena's mouth is out of view, so its tributaries in the Ecstall get filed
    under 5 instead of 4), and a neighbouring Area that was not offered cannot win.

    ``city_mouths`` (`city_tidal_mouths`, required: a build with no added streams passes ``()``): the city creeks'
    own mouths, candidates beside the FWA mouths (T1246).
    """
    import geopandas as gpd
    from shapely.geometry import Point

    want = set(areas)
    key = f"{gpkg}|all"
    if key not in _CACHE:
        every = gpd.read_file(gpkg, layer=LAYER, engine="pyogrio")
        every = every[every[AREA_FIELD] > 0]
        if every.empty:
            raise ValueError(f"{LAYER} carries no real Area polygons — only MANAGEMENT_AREA 0?")

        # Only the mouths: one row per blue line, province-wide, ~1.6M rows in a few seconds.
        mouths = gpd.read_file(gpkg, layer="streams", where="DOWNSTREAM_ROUTE_MEASURE = 0",
                               columns=["FWA_WATERSHED_CODE"], engine="pyogrio")
        mouths["_basin"] = mouths["FWA_WATERSHED_CODE"].map(basin_of)
        mouths = mouths[mouths["_basin"].notna()]
        # Out of province by FWA's own definition — drop before any geometry is consulted.
        mouths = mouths[~mouths["_basin"].map(_wsc.root).isin(OUT_OF_PROVINCE_GROUPS)]
        if mouths.empty:
            raise ValueError("no basin mouths found — is the streams layer built?")

        def _mouth_point(geom):
            g = geom.geoms[0] if hasattr(geom, "geoms") else geom
            return Point(g.coords[0])    # FWA measures FROM the mouth, so coord 0 IS the mouth

        pts = gpd.GeoDataFrame(mouths[["_basin"]],
                               geometry=mouths.geometry.apply(_mouth_point), crs=mouths.crs)
        near = gpd.sjoin_nearest(pts, every[[AREA_FIELD, "geometry"]],
                                 how="inner", distance_col="_d")
        # One mouth per basin: the candidate closest to the sea. This absorbs a river's braids —
        # they share the prefix, and the inland ones are simply further away. A mouth on an Area line is
        # nearest to both (two rows): `_pick` decides, never the row order.
        fwa: Dict[str, list] = collections.defaultdict(list)
        for b, d, a in zip(near["_basin"], near["_d"], near[AREA_FIELD]):
            fwa[b].append((float(d), int(a)))
        _CACHE[key] = {"fwa": dict(fwa), "areas": every[[AREA_FIELD, "geometry"]]}

    cands = {b: list(v) for b, v in _CACHE[key]["fwa"].items()}
    if OWN_MOUTH_TAKES_ITS_AREA:
        from shapely.geometry import Point
        polys = _CACHE[key]["areas"]
        for basin, x, y in city_mouths:
            dist = polys.geometry.distance(Point(x, y))
            dmin = float(dist.min())
            cands.setdefault(basin, []).extend((dmin, int(a)) for a in polys.loc[dist == dmin, AREA_FIELD])
    best = {b: _pick(v) for b, v in cands.items()}
    reached = {b: a for b, (d, a) in best.items() if d <= MAX_MOUTH_OFFSET_M}
    logger.info("%d basins; %d drain to a Pacific tidal Area, %d do not (inland majors — Arctic, Peace, Liard)",
                len(best), len(reached), len(best) - len(reached))

    missing = want - set(reached.values()) - set(range(1, 200))
    if missing:                                   # an Area number that exists in no polygon
        raise ValueError(f"{LAYER} carries no polygon for Area(s) {sorted(missing)}")
    return {b: a for b, a in reached.items() if a in want}


def area_of(watershed_code: str, basins: Dict[str, int]) -> Optional[int]:
    """The Area a single section drains to — the lookup every consumer needs."""
    basin = basin_of(watershed_code)
    return basins.get(basin) if basin else None


def waters_in_area(area: int, gpkg: str = _GPKG, named_only: bool = True):
    """Every water draining into one Area — the reverse of `basin_areas`.

    This is the question a curator actually asks ("what does section E's Area 5 rule reach?"), and
    a rule that binds thousands of sections nobody can list is exactly what AGENTS' report-don't-
    decide rule exists to prevent. Returns a DataFrame of GNIS_NAME / BLUE_LINE_KEY / basin.
    """
    import geopandas as gpd

    basins = basin_areas((area,), gpkg, city_mouths=())   # FWA's waters (the reverse lookup lists FWA names)
    every = gpd.read_file(gpkg, layer=LAYER, engine="pyogrio")
    every = every[every[AREA_FIELD] == area]
    if every.empty:
        raise ValueError(f"{LAYER} carries no polygon for Area {area}")
    st = gpd.read_file(gpkg, layer="streams", bbox=tuple(every.total_bounds), engine="pyogrio")
    st = st.assign(basin=st["FWA_WATERSHED_CODE"].map(basin_of))
    hit = st[st["basin"].isin(basins)]
    if named_only:
        hit = hit.dropna(subset=["GNIS_NAME"])
    cols = ["GNIS_NAME", "BLUE_LINE_KEY", "basin", "FWA_WATERSHED_CODE"]
    return (hit[cols].drop_duplicates(subset=["GNIS_NAME", "basin"])
            .sort_values("GNIS_NAME").reset_index(drop=True))


def report(areas: Iterable[int] = SECTION_E_AREAS, gpkg: str = _GPKG) -> str:
    """What a curator reads before this membership is trusted.

    AGENTS: the builder never decides where a regulation applies — it reports, and the curator
    decides. A wrong Area on a NAMED river is the failure a human can actually catch, so the named
    basins are printed per Area.
    """
    import geopandas as gpd

    basins = basin_areas(areas, gpkg, city_mouths=())     # FWA's named basins only
    counts = collections.Counter(basins.values())
    lines = [f"{len(basins):,} basins drain into Areas {sorted(counts)}"]
    for a in sorted(counts):
        lines.append(f"  Area {a}: {counts[a]:>6,} basins  ->  {area_id(a)}")

    every = gpd.read_file(gpkg, layer=LAYER, engine="pyogrio")
    every = every[every[AREA_FIELD].isin(set(areas))]
    st = gpd.read_file(gpkg, layer="streams", bbox=tuple(every.total_bounds), engine="pyogrio")
    named = st.dropna(subset=["GNIS_NAME"]).copy()
    named["_a"] = named["FWA_WATERSHED_CODE"].map(lambda c: area_of(c, basins))
    lines += ["", "named waters, by the Area their basin drains into:"]
    for a in sorted(counts):
        names = sorted(set(named.loc[named["_a"] == a, "GNIS_NAME"]))
        shown = ", ".join(names[:12]) or "—"
        lines.append(f"  Area {a}: {shown}"
                     + (f" … +{len(names) - 12} more" if len(names) > 12 else ""))
    return "\n".join(lines)


#: The one thing still unverified, kept visible rather than smoothed over.
LIMITATION = """Step 2 uses nearest-Area, which always returns something.

Measured, that is tight — median 0 m, p95 0 m — but the worst basin's mouth sits 6.0 km from the
Area it was given, and ~39 sit beyond 100 m, clustered near the Area 6/7 boundary by Princess
Royal Island. A mouth kilometres from its nearest Area is a basin whose true Area may lie outside
the set. `report()` is where that must reach a curator before this is minted into the registry.
"""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(report())
