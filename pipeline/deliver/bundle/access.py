"""SECTION ACCESS (T54): which land a section runs through that the public may not enter, or may enter only with leave.

Access is not regulation. A section's open / restricted / closed answer never changes because of it; the app draws a
section MOSTLY inside no-access land in a fourth colour and says so on its page, and notes gentler restrictions
(private land, a reserve, permit-only land). Those are the app's calls (@app/core `access.ts`); this records the FACT
they are made from, once, in the bundle:

    access_area     ix, tier, area_id, name — every polygon a section touches, named for a reader
    section_access  sid, area, permille     — how much of the section lies inside it (1..1000)

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
import re
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
"""

#: The tile layers holding the polygons, and those holding the sections.
AREA_LAYERS = ("no_access", "indigenous_land", "parcel")
SECTION_LAYERS = ("stream", "lake", "wetland")

_OSM_SUFFIX = re.compile(r"\s*\[\d+\]\s*$")


def display_name(name: str | None) -> str:
    """A polygon's name for a reader: the OSM id suffix dropped ("Coquitlam Watershed [13480215]" → "Coquitlam
    Watershed"), trimmed. A nameless polygon is ""."""
    return _OSM_SUFFIX.sub("", (name or "").strip()).strip()


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
            a = Area(tier, aid, "" if layer == "parcel" else display_name(p.get("name") or p.get("area_id")))
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
    import numpy as np
    import shapely
    from shapely.strtree import STRtree

    areas, parts, part_area = load_areas(layers)
    tree = STRtree(parts)
    inside: Dict[Tuple[int, int], List[float]] = {}     # (sid, area) -> [line length in, polygon area in]
    totals: Dict[int, List[float]] = {}                 # sid -> [line length, polygon area], summed over its features

    for sids, geoms in _sections(layers, batch):
        g = np.array(geoms, dtype=object)
        dim = shapely.get_dimensions(g)
        lo = shapely.bounds(g)
        cosl = np.cos(np.radians((lo[:, 1] + lo[:, 3]) / 2))
        length = _scaled_length(g, cosl)
        area = shapely.area(g)                          # a ratio of areas within one section: no scale needed
        for k, sid in enumerate(sids):
            t = totals.setdefault(sid, [0.0, 0.0])
            if dim[k] == 1:
                t[0] += float(length[k])
            elif dim[k] == 2:
                t[1] += float(area[k])
        src, dst = tree.query(g, predicate="intersects")
        if not len(src):
            continue
        cut = shapely.intersection(g[src], parts[dst])
        cl, ca = _scaled_length(cut, cosl[src]), shapely.area(cut)
        for j in range(len(src)):
            k = int(src[j])
            sid = sids[k]
            key = (sid, int(part_area[dst[j]]))
            acc = inside.setdefault(key, [0.0, 0.0])
            if dim[k] == 1:
                acc[0] += float(cl[j])
            else:
                acc[1] += float(ca[j])
    rows: List[Tuple[int, int, int]] = []
    for (sid, a), (li, pi) in inside.items():
        line, poly = totals[sid]
        if line > 0:
            share = li / line
        elif poly > 0:
            share = pi / poly
        else:
            continue
        pm = int(round(min(1.0, share) * 1000))
        if pm >= 1:
            rows.append((sid, a, pm))
    rows.sort()
    return areas, rows


def write(db: sqlite3.Connection, tiles: Path, *, handles: str, cov=None) -> int:
    """Measure and write `access_area` / `section_access`. `tiles` is the tile directory (`atlas.meta.json` +
    `layers/`); it must be cut from the bundle's atlas (`handles`, the bundle's section handles digest)."""
    meta = tiles / "atlas.meta.json"
    if not meta.exists():
        raise SystemExit(f"access: {meta} not found — section access is measured against the tiles' polygons")
    theirs = json.loads(meta.read_text()).get("section_handles")
    if theirs != handles:
        raise SystemExit(f"access: the tiles at {tiles} carry section handles {theirs}, not the bundle's {handles} — "
                         "they were cut from another atlas; rebuild the tiles, or build the bundle from theirs")
    areas, rows = compute(tiles / "layers")
    db.executemany("INSERT INTO access_area (ix, tier, area_id, name) VALUES (?,?,?,?)",
                   [(i, a.tier, a.area_id, a.name) for i, a in enumerate(areas)])
    db.executemany("INSERT INTO section_access (sid, area, permille) VALUES (?,?,?)", rows)
    if cov is not None:
        cov.filled("access_area", len(areas))
        cov.filled("section_access", len(rows))
    return len(rows)


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
