"""SECTION ACCESS (T54): which land a section runs through that the public may not enter, or may enter only with leave.

Access is not regulation. A section's open / restricted / closed answer never changes because of it; the app draws a
section MOSTLY inside no-access land in a fourth colour and says so on its page, and notes gentler restrictions
(private land, a reserve, permit-only land). Those are the app's calls (@app/core `access.ts`); this records the FACT
they are made from, once, in the bundle:

    access_area     ix, tier, area_id, name — every polygon a section touches, named for a reader
    section_access  sid, area, permille     — how much of the section lies inside it (1..1000)
    section_length  sid, m                  — (T387) each named section's length along the water, metres
    section_no_access sid, permille         — (T561) how much of the section lies inside the UNION of the no-access
                                              polygons: what "mostly inside" is decided from

THE UNION, NOT THE SUM (T561). The no-access polygons overlap (OSM nests them: "Slesse Creek Demolition Training Areas"
inside "Slesse RTA", 19 overlapping pairs): `section_access` keeps one row per area so the page can NAME each, but a
section's share of no-access land is measured once against their union (`section_no_access`), so it never reads over
1000 permille and an overlap never counts twice. `mostly_inside` reads it. The app (and `paint_sync`, which reads the
shipped rows as the app does) still SUMS the per-area rows: `write` refuses a bundle where the two rules would colour a
section differently, naming it, until the app reads the union too.

SECTION LENGTH (T387, owner 2026-11-04: "in the one section not all clicks are equal"): the app's Regulations page is a
PART's, so a part's access is its sections' pooled — each area with the share of the whole part it covers, the sections
weighed by their length along the water. Measured here from the same tile geometry as the shares: a section with a line
feature, its line length; a section drawn only as polygons (a lake, a river drawn as a polygon), half its perimeter (a
long river polygon's half perimeter is its length; a lake's is a little more than its width). Metres: the scaled
lon/lat below times `METRES_PER_DEGREE`. Only the named sections (`item_section`) are written: a part is a named
water's; the derived step copies each into `part_section.m`, which the release ships in the water file.

THE POLYGONS ARE THE TILES' OWN. Read from the tile layers' geojsonl (`GENERATED.tiles / "layers"`): `no_access`
(kind `closed` → tier `no_access`, kind `permit` → tier `permit`), `indigenous_land` (tier `reserve`) and `parcel`
(the one dissolved private-title polygon, tier `private`). So the boundary a reader sees on the map is by construction
the boundary this measured against — one vintage, never a second copy — and `write` refuses a tile set cut from another
atlas (its `atlas.meta.json` section handles must be the bundle's).

THE SECTIONS ARE THE TILES' OWN TOO: the `stream`, `lake` and `wetland` layers, keyed by `section_id` (the section
handle, the bundle's `sid`). A section drawn as several features is summed. A line's share is by LENGTH, a polygon's
(a lake, a river drawn as a polygon) by AREA; a section with any line feature is measured as a line.

THE SHARE, NOT A YES/NO (the straddling-sections note: mark, don't cut). `restricted_land_access` is cut first entry /
last exit (areas.json), so a section can still run partly inside; the permit land, reserves and private land are not
cut at all. The app decides what "mostly" is from the share (`MOSTLY_INSIDE` in @app/core `access.ts`).

MEASURED IN THE TILES' OWN LON/LAT, SCALED PER SECTION: the intersection is cut in the coordinates the map draws (a
polygon edge is straight in lon/lat there), then a line's longitudes are scaled by the cosine of its section's mid
latitude, so a degree of x is a degree of y wherever the section is. A share is a ratio within one section; this keeps
an east-west reach from reading longer than a north-south one at 55°N. An area ratio needs no scale.

Deterministic: areas sorted (tier, name, area_id), rows sorted (sid, area); no clocks.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple

#: The tiers, in the order the app's wording ranks them. `no_access` is the strong one (Tier A: the fourth colour);
#: the other three are possible restrictions (Tier B: a note, no colour).
TIERS: Tuple[str, ...] = ("no_access", "permit", "reserve", "private")

SCHEMA = """
CREATE TABLE access_area (ix      INTEGER PRIMARY KEY,
                          tier    TEXT NOT NULL CHECK (tier IN ('no_access', 'permit', 'reserve', 'private')),
                          area_id TEXT NOT NULL,
                          name    TEXT NOT NULL);
CREATE TABLE section_access (sid      INTEGER NOT NULL,
                             area     INTEGER NOT NULL REFERENCES access_area (ix),
                             permille INTEGER NOT NULL CHECK (permille BETWEEN 1 AND 1000),
                             PRIMARY KEY (sid, area)) WITHOUT ROWID;
CREATE TABLE section_length (sid INTEGER PRIMARY KEY,
                             m   INTEGER NOT NULL CHECK (m >= 0)) WITHOUT ROWID;
CREATE TABLE section_no_access (sid      INTEGER PRIMARY KEY,
                                permille INTEGER NOT NULL CHECK (permille BETWEEN 1 AND 1000)) WITHOUT ROWID;
"""

#: (T387) Metres in one degree of latitude (a degree of longitude scaled by the cosine of its latitude is the same).
METRES_PER_DEGREE = 111_320.0

#: The tile layers holding the polygons, and those holding the sections.
AREA_LAYERS = ("no_access", "indigenous_land", "parcel")
SECTION_LAYERS = ("stream", "lake", "wetland")

def tile_area_name(layer: str, p: dict) -> str:
    """An access polygon's name as the TILES carry it (T679 #4): the one cleaned name
    (`place_names.area_display`, applied by the tile export), read as is — never cleaned again here. A
    reserve polygon with no id and no name is nameless (""). An area with an id and no name, or a name
    still carrying its OpenStreetMap id or a cut-off word (tiles older than the one cleaner), is refused."""
    from pipeline.deliver.bundle.place_names import area_name_uncleaned
    name, aid = p.get("name") or "", p.get("area_id") or ""
    if aid and not name:
        raise ValueError(f"access: {layer} area {aid!r} has no name on the tile — the tile export writes "
                         "`place_names.area_display` of every area (rebuild the tiles)")
    if area_name_uncleaned(name):
        raise ValueError(f"access: {layer} area {aid!r} carries the uncleaned name {name!r} — the tiles must "
                         "carry `place_names.area_display` of it (T679 #4: rebuild the tiles)")
    return name


def tier_of(layer: str, kind: str | None) -> str:
    if layer == "no_access":
        if kind == "closed":
            return "no_access"
        if kind == "permit":
            return "permit"
        raise ValueError(f"access: no_access kind {kind!r} is neither 'closed' nor 'permit' — "
                         "pipeline/deliver/tiles/layers.py says which two there are")
    if layer == "indigenous_land":
        return "reserve"
    if layer == "parcel":
        return "private"
    raise ValueError(f"access: layer {layer!r} holds no access polygons")


@dataclass(frozen=True)
class Area:
    tier: str
    area_id: str
    name: str


def _features(path: Path) -> Iterator[dict]:
    try:
        import orjson
        loads = orjson.loads
    except ImportError:                       # pragma: no cover - orjson is in the venv
        loads = json.loads
    with path.open("rb") as fh:
        for line in fh:
            if line.strip():
                yield loads(line)


def _scaled_length(geoms, k):
    """Each line's length with its longitudes scaled by `k[i]`, the cosine of ITS section's latitude (module
    docstring): a constant per section, so the section and the part of it inside an area are measured alike."""
    import numpy as np
    import shapely

    _, idx = shapely.get_coordinates(geoms, return_index=True)
    fac = np.asarray(k, dtype=float)[idx]

    def f(c):
        return np.column_stack([c[:, 0] * fac, c[:, 1]])

    return shapely.length(shapely.transform(geoms, f))


def _require(layers: Path, name: str) -> Path:
    p = layers / f"{name}.geojsonl"
    if not p.exists():
        raise SystemExit(f"access: {p} not found — the tile export writes it "
                         "(python -m pipeline.deliver.tiles); section access is measured against the tiles' polygons")
    return p


def load_areas(layers: Path):
    """The access polygons, as the tiles draw them (lon/lat), as (areas, parts, part_area): `areas` sorted (tier, name, area_id);
    each polygon exploded into its parts for the index, `part_area[i]` naming the area part `i` belongs to."""
    import numpy as np
    import shapely
    from shapely.geometry import shape

    found: Dict[Area, list] = {}
    for layer in AREA_LAYERS:
        for f in _features(_require(layers, layer)):
            p = f.get("properties") or {}
            tier = tier_of(layer, p.get("kind"))
            # the dissolved private polygon has no attributes: it is one area, "parcel"
            aid = "parcel" if layer == "parcel" else str(p.get("area_id") or "")
            a = Area(tier, aid, "" if layer == "parcel" else tile_area_name(layer, p))
            found.setdefault(a, []).append(shape(f["geometry"]))
    areas = sorted(found, key=lambda a: (TIERS.index(a.tier), a.name, a.area_id))
    parts: list = []
    part_area: List[int] = []
    for i, a in enumerate(areas):
        for g in found[a]:
            ps = shapely.get_parts(g)
            parts.extend(ps)
            part_area.extend([i] * len(ps))
    arr = np.array(parts, dtype=object)
    bad = ~shapely.is_valid(arr)
    if bad.any():
        arr[bad] = shapely.make_valid(arr[bad])
    return areas, arr, np.array(part_area, dtype=np.int64)


def _sections(layers: Path, batch: int) -> Iterator[Tuple[List[int], list]]:
    from shapely.geometry import shape

    for layer in SECTION_LAYERS:
        sids: List[int] = []
        geoms: list = []
        for f in _features(_require(layers, layer)):
            sid = (f.get("properties") or {}).get("section_id")
            if sid is None or f.get("geometry") is None:
                continue
            sids.append(int(sid))
            geoms.append(shape(f["geometry"]))
            if len(sids) >= batch:
                yield sids, geoms
                sids, geoms = [], []
        if sids:
            yield sids, geoms


def compute(layers: Path, *, batch: int = 20_000) -> Tuple[List[Area], List[Tuple[int, int, int]]]:
    """(areas, rows): every (sid, area index, permille) with at least one permille of the section inside."""
    areas, rows, _, _ = measure(layers, batch=batch)
    return areas, rows


def _no_access_union(areas: Sequence[Area], parts, part_area):
    """(T561) The no-access tier's polygons as ONE cover: their union, exploded into disjoint parts."""
    import numpy as np
    import shapely

    na = np.array([areas[i].tier == "no_access" for i in part_area], dtype=bool)
    if not na.any():
        return np.array([], dtype=object)
    return np.array(shapely.get_parts(shapely.union_all(parts[na])), dtype=object)


def _permille(acc: List[float], total: List[float]):
    """A section's share inside, in permille (by length for a section with any line, else by area), or None."""
    line, poly, _ = total
    if line > 0:
        share = acc[0] / line
    elif poly > 0:
        share = acc[1] / poly
    else:
        return None
    pm = int(round(min(1.0, share) * 1000))
    return pm if pm >= 1 else None


def measure(layers: Path, *, batch: int = 20_000) -> Tuple[List[Area], List[Tuple[int, int, int]], Dict[int, int],
                                                           List[Tuple[int, int]]]:
    """(areas, rows, lengths, no_access): `compute`'s two, (T387) every section's length along the water in whole metres
    (a line's length; a section drawn only as polygons, half its perimeter — module docstring), and (T561) every
    (sid, permille) of the section inside the UNION of the no-access polygons."""
    import numpy as np
    import shapely
    from shapely.strtree import STRtree

    areas, parts, part_area = load_areas(layers)
    tree = STRtree(parts)
    union = _no_access_union(areas, parts, part_area)
    utree = STRtree(union) if len(union) else None
    inside: Dict[Tuple[int, int], List[float]] = {}     # (sid, area) -> [line length in, polygon area in]
    inside_u: Dict[int, List[float]] = {}               # sid -> the same, inside the no-access union
    totals: Dict[int, List[float]] = {}                 # sid -> [line length, polygon area, polygon perimeter], summed

    for sids, geoms in _sections(layers, batch):
        g = np.array(geoms, dtype=object)
        dim = shapely.get_dimensions(g)
        lo = shapely.bounds(g)
        cosl = np.cos(np.radians((lo[:, 1] + lo[:, 3]) / 2))
        length = _scaled_length(g, cosl)
        area = shapely.area(g)                          # a ratio of areas within one section: no scale needed
        for k, sid in enumerate(sids):
            t = totals.setdefault(sid, [0.0, 0.0, 0.0])
            if dim[k] == 1:
                t[0] += float(length[k])
            elif dim[k] == 2:
                t[1] += float(area[k])
                t[2] += float(length[k])                 # a polygon's length is its perimeter
        for t, polys, acc_of in ((tree, parts, lambda sid, d: inside.setdefault((sid, int(part_area[d])), [0.0, 0.0])),
                                 (utree, union, lambda sid, d: inside_u.setdefault(sid, [0.0, 0.0]))):
            if t is None:
                continue
            src, dst = t.query(g, predicate="intersects")
            if not len(src):
                continue
            cut = shapely.intersection(g[src], polys[dst])
            cl, ca = _scaled_length(cut, cosl[src]), shapely.area(cut)
            for j in range(len(src)):
                k = int(src[j])
                acc = acc_of(sids[k], dst[j])
                if dim[k] == 1:
                    acc[0] += float(cl[j])
                else:
                    acc[1] += float(ca[j])
    rows: List[Tuple[int, int, int]] = []
    for (sid, a), acc in inside.items():
        pm = _permille(acc, totals[sid])
        if pm is not None:
            rows.append((sid, a, pm))
    rows.sort()
    no_access = sorted((sid, pm) for sid, acc in inside_u.items() if (pm := _permille(acc, totals[sid])) is not None)
    lengths = {sid: int(round((line if line > 0 else per / 2) * METRES_PER_DEGREE))
               for sid, (line, _, per) in totals.items()}
    return areas, rows, lengths, no_access


def write(db: sqlite3.Connection, tiles: Path, *, handles: str, cov=None) -> int:
    """Measure and write `access_area` / `section_access`, and (T387) `section_length` for every named section
    (`item_section`, which must be written first). `tiles` is the tile directory (`atlas.meta.json` + `layers/`); it must
    be cut from the bundle's atlas (`handles`, the bundle's section handles digest)."""
    meta = tiles / "atlas.meta.json"
    if not meta.exists():
        raise SystemExit(f"access: {meta} not found — section access is measured against the tiles' polygons")
    theirs = json.loads(meta.read_text()).get("section_handles")
    if theirs != handles:
        raise SystemExit(f"access: the tiles at {tiles} carry section handles {theirs}, not the bundle's {handles} — "
                         "they were cut from another atlas; rebuild the tiles, or build the bundle from theirs")
    areas, rows, lengths, no_access = measure(tiles / "layers")
    split = summed_rule_disagrees(areas, rows, no_access)
    if split:
        raise SystemExit(f"access: {len(split)} section(s) are mostly inside no-access land by the summed per-area rows "
                         f"(the app's rule, paint_sync's) but not by the union of the polygons (or the reverse): "
                         f"{split[:10]} — the app must read the union share (T561) before this bundle can ship")
    named = sorted({s for (s,) in db.execute("SELECT sid FROM item_section")})
    db.executemany("INSERT INTO access_area (ix, tier, area_id, name) VALUES (?,?,?,?)",
                   [(i, a.tier, a.area_id, a.name) for i, a in enumerate(areas)])
    db.executemany("INSERT INTO section_access (sid, area, permille) VALUES (?,?,?)", rows)
    db.executemany("INSERT INTO section_length (sid, m) VALUES (?,?)", [(s, lengths[s]) for s in named if s in lengths])
    db.executemany("INSERT INTO section_no_access (sid, permille) VALUES (?,?)", no_access)
    if cov is not None:
        cov.filled("access_area", len(areas))
        cov.filled("section_access", len(rows))
        cov.filled("section_length", sum(1 for s in named if s in lengths))
        cov.filled("section_no_access", len(no_access))
    return len(rows)


#: MOSTLY INSIDE, in permille: the app's `MOSTLY_INSIDE` (0.5, @app/core `access.ts`) — a section whose no-access land
#: covers MORE than half of it is drawn in the fourth colour. `pipeline/tests/test_section_access.py` pins the two.
MOSTLY_INSIDE_PERMILLE = 500


def summed_rule_disagrees(areas: Sequence[Area], rows: Iterable[Tuple[int, int, int]],
                          no_access: Iterable[Tuple[int, int]]) -> List[int]:
    """(T561) The sections the SUMMED per-area no-access rows (the app's rule today) and the UNION share decide
    differently about "mostly inside" — the ones an overlap of no-access polygons would mis-colour. Sorted sids."""
    summed: Dict[int, int] = {}
    for sid, a, pm in rows:
        if areas[a].tier == "no_access":
            summed[sid] = summed.get(sid, 0) + pm
    union = dict(no_access)
    return sorted(s for s in set(summed) | set(union)
                  if (summed.get(s, 0) > MOSTLY_INSIDE_PERMILLE) != (union.get(s, 0) > MOSTLY_INSIDE_PERMILLE))


def mostly_inside(db: sqlite3.Connection) -> set:
    """The sections MOSTLY inside no-access land (the app's fourth colour), read from `section_no_access` — the share
    inside the UNION of the no-access polygons, so nested polygons never count twice (T561). The part grouping reads
    this (T226): a section inside never shares a part with one outside. Empty for a bundle with no access rows (a
    hand-built fixture)."""
    return {s for (s,) in db.execute("SELECT sid FROM section_no_access WHERE permille > ?", (MOSTLY_INSIDE_PERMILLE,))}


def summary(areas: Sequence[Area], rows: Iterable[Tuple[int, int, int]]) -> dict:
    """Counts for a measurement run: sections and rows per tier, and the no-access share distribution."""
    by_tier: Dict[str, int] = {t: 0 for t in TIERS}
    secs: Dict[str, set] = {t: set() for t in TIERS}
    hist = [0] * 11
    for sid, a, pm in rows:
        t = areas[a].tier
        by_tier[t] += 1
        secs[t].add(sid)
        if t == "no_access":
            hist[pm // 100] += 1
    return {"rows": by_tier, "sections": {t: len(s) for t, s in secs.items()},
            "no_access_permille_deciles": hist}


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m pipeline.deliver.bundle.access --out FILE` — MEASURE ONLY: reads the tile layers, writes the rows as
    JSON lines (sid, tier, name, permille) and prints the summary. Touches no bundle."""
    import argparse

    from pipeline.common.curated import GENERATED

    ap = argparse.ArgumentParser(description=main.__doc__)
    ap.add_argument("--tiles", type=Path, default=None)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    tiles = a.tiles or GENERATED.tiles
    areas, rows = compute(tiles / "layers")
    with a.out.open("w") as fh:
        for sid, ix, pm in rows:
            fh.write(json.dumps({"sid": sid, "tier": areas[ix].tier, "name": areas[ix].name,
                                 "area_id": areas[ix].area_id, "permille": pm}) + "\n")
    print(json.dumps({"areas": len(areas), **summary(areas, rows)}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
