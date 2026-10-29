"""T173 — THE FISH OBSTACLES LAYER: falls, cascades, canyons, velocity barriers, culverts and dams, as small marks.

Two sources, both BC Data Catalogue, both **Open Government Licence – British Columbia** (checked 2026-10-28):

* **FISS** — Provincial Obstacles to Fish Passage, `WHSE_FISH.FISS_OBSTACLES_PNT_SP` (catalogue
  35bbac7c-2e2f-4587-9108-f4aa1e862809): 24,832 surveyed points, each an obstacle type, a height where measured and a
  survey date. `GAZETTED_NAME` is the WATER's name (the stream the obstacle is on), not the obstacle's.
* **WRIS** — BC Dams, `WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW` (catalogue bd632217-35f9-4d01-8e57-a6dbc454f236):
  2,490 crest LINES; a mark is the crest's midpoint.

* **PSCIS** — Provincial Stream Crossing Inventory System, `WHSE_FISH.PSCIS_ASSESSMENT_SVW` (catalogue
  7ecfafa6-5e18-48cd-8d9b-eae5b5ea2881 "PSCIS Assessments"): 19,905 assessed road crossings, one row per crossing, each
  with its CURRENT barrier result. The catalogue shows "Access Only"; the owner treats it as OGL-BC (2026-10-28, Q165,
  decisions.md) — it is in the BC Data Catalogue on the open WFS.

What is kept (`fiss_type`, `KEEP_DAM`):

* FISS: falls, cascades (and chutes), canyons, velocity barriers, culverts and every kind of built dam (hydro,
  irrigation, storage, private, weir, unknown origin). Dropped: beaver dams, log jams, rocks, debris, landslides,
  bridges, pumps, wedges and "other" — transient, or not an obstacle a reader can find again (most were surveyed in the
  1990s).
* WRIS: a STANDING dam (Active, Deactivated) on its water (Main, Conversion, Outlet). Dropped: saddle dams and
  spillways (a saddle closes a low point of a reservoir's rim; a spillway is part of the main dam) and every dam not
  built, removed, breached or decommissioned.

Deduped (`dedupe`):

* FISS points of the same type within `SAME_POINT_M` of each other are one obstacle (3,422 pairs sit within 0.5 m:
  the same record surveyed twice). The one kept has a height, then the latest survey, then the lowest id.
* A FISS dam within `DAM_MATCH_M` of a kept WRIS crest line IS that dam: the WRIS record is the authority (named, with
  its height). 200 m because FISS points were placed on 1:50,000 maps.

* PSCIS (`pscis_marks`): only closed-bottom structures (culverts: round, box, pipe-arch, oval, wood box) whose current
  result is in `PSCIS_SHOW` (barrier, potential barrier, not rated). Bridges and other open-bottom crossings are passable by
  definition; fords and "other" crossings are not culverts and carry no assessment (unknown); passable culverts are
  noise on a map of obstacles (T173 measured each option, see the ticket).
* A FISS culvert within `PSCIS_MATCH_M` of ANY PSCIS crossing is that crossing (PSCIS is the newer, scored
  assessment): the FISS record goes, whether or not the PSCIS one is shown (a crossing PSCIS rates passable, or a
  culvert since replaced by a bridge, is not a barrier any more). Where PSCIS has no stream name, the FISS record's
  gazetted name fills it.

The slim schema (`layers.obstacle`): `obstacle_id` (`fiss:<point id>` / `wris:<dam file number>`), `type`
(`OBSTACLE_TYPES`), `name` (the dam's name; FISS obstacles have none), `water` (FISS: the gazetted name of the water it
is on), `height_m` (where known: FISS heights of 0, below 0 or over 500 m are survey placeholders and are dropped),
`source` (`fiss` | `wris` | `pscis`), `year` (FISS: surveyed; WRIS: commissioned; PSCIS: assessed), and on PSCIS
crossings only `barrier` (barrier | potential | passable | unknown), `habitat` (high | medium | low: the assessor's
upstream habitat value), `road` (the road's name) and `date` (assessed, ISO). `obstacle_id` is `pscis:<stream crossing
id>`. No dam owner: WRIS names private people as owners.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path

from pipeline.common.curated import SOURCE

#: The fetched file (`python data/fetch_data.py --layers fish_obstacles_fiss fish_obstacles_dams fish_obstacles_pscis`).
GPKG = "fish_obstacles/fish_obstacles.gpkg"

#: Every type a mark can have. The app's words (`@app/core` obstacles.ts `OBSTACLE_TYPES`) are held equal to this.
OBSTACLE_TYPES = ("falls", "cascade", "canyon", "velocity", "culvert", "dam")

#: FISS `OBSTACLE_NAME` (lower-cased) → type. Anything not here is dropped (see the module docstring). Mapped by NAME,
#: not code: the code `PD` means both "Private Dam" and "Persistent debris".
FISS_TYPES = {
    "falls": "falls",
    "cascade": "cascade", "cascade or chute": "cascade",
    "canyon": "canyon",
    "velocity barrier": "velocity",
    "culvert": "culvert",
    "dam": "dam", "dam - unknown origin": "dam", "hydro dam": "dam", "irrigation district dam": "dam",
    "private dam": "dam", "water management storage dam": "dam", "fisheries management dam": "dam",
    "regional district dam": "dam", "weir": "dam",
}

#: WRIS dams kept: a standing structure on its water.
KEEP_DAM = {"operation": ("Active", "Deactivated"), "function": ("Main", "Conversion", "Outlet")}

#: FISS points of one type closer than this are one obstacle.
SAME_POINT_M = 10.0
#: A FISS dam this close to a kept WRIS crest line is that dam.
DAM_MATCH_M = 200.0
#: PSCIS `CURRENT_BARRIER_RESULT_CODE` → the tile's `barrier`.
PSCIS_BARRIER = {"BARRIER": "barrier", "POTENTIAL": "potential", "PASSABLE": "passable", "UNKNOWN": "unknown"}
#: The PSCIS results a mark is drawn for (T173, measured in the ticket): barriers, potential barriers, and culverts not
#: yet rated (195; +41 kB). Passable culverts (874) are noise on a map of obstacles.
PSCIS_SHOW = ("barrier", "potential", "unknown")
#: PSCIS crossing types drawn: CBS = closed-bottom structure, i.e. a culvert. OBS (bridges, open-bottom) and OTHER (fords).
PSCIS_TYPES = ("CBS",)
#: PSCIS `HABITAT_VALUE_CODE` → the tile's `habitat`.
PSCIS_HABITAT = {"HIGH": "high", "MEDIUM": "medium", "LOW": "low"}
#: A FISS culvert this close to a PSCIS crossing is that crossing. Small: 3,480 PSCIS crossings have another within
#: 100 m (one road over a run of tributaries), so a wide radius would merge neighbours.
PSCIS_MATCH_M = 50.0
#: Names a surveyor wrote for "no name" — left out, never shown.
_PLACEHOLDER = re.compile(r"^(unnamed|un-named|unknown|n/?a|none|nil|no name|-+|\?+)$", re.I)

#: A FISS height outside (0, MAX_HEIGHT_M] is a placeholder (-1000, 0, 9999), not a measurement. Della Falls is 440 m.
MAX_HEIGHT_M = 500.0


def fiss_type(obstacle_name) -> str | None:
    """The mark's type for a FISS `OBSTACLE_NAME`, or None (dropped)."""
    if not isinstance(obstacle_name, str):
        return None
    return FISS_TYPES.get(obstacle_name.strip().lower())


def height(v) -> float | None:
    """A height in metres, or None where the source holds a placeholder."""
    try:
        h = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(h) or h <= 0 or h > MAX_HEIGHT_M:
        return None
    return round(h, 1)


def year_of(v) -> int | None:
    """A year from a date (Timestamp / ISO text) or a number, or None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return None if isinstance(v, float) and math.isnan(v) else int(v)
    y = getattr(v, "year", None)
    if isinstance(y, int):
        return y
    s = str(v)
    return int(s[:4]) if len(s) >= 4 and s[:4].isdigit() else None


def _text(v) -> str | None:
    if not isinstance(v, str):
        return None
    s = " ".join(v.split())
    return s or None


def _name(v) -> str | None:
    """A surveyor's free-text name, or None for a placeholder ("Unnamed", "n/a", "unknown")."""
    s = _text(v)
    return None if s is None or _PLACEHOLDER.match(s) else s


def date_of(v) -> str | None:
    """An ISO date (YYYY-MM-DD) from a Timestamp / ISO text, or None."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except ValueError:                       # NaT
            return None
    s = str(v)
    return s[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", s) else None


def pscis_record(r) -> dict | None:
    """One PSCIS crossing (a row of `PSCIS_ASSESSMENT_SVW`) as a mark, EVERY crossing (the shown filter is
    `merge_pscis`'s): {…, barrier, habitat, road, date, crossing_type, x, y}. None without a point."""
    g = r.geometry
    if g is None or g.is_empty:
        return None
    d = date_of(r.ASSESSMENT_DATE)
    return {"obstacle_id": f"pscis:{int(r.STREAM_CROSSING_ID)}", "type": "culvert", "name": None,
            "water": _name(r.STREAM_NAME), "height_m": None, "source": "pscis", "year": int(d[:4]) if d else None,
            "barrier": PSCIS_BARRIER.get(r.CURRENT_BARRIER_RESULT_CODE, "unknown"),
            "habitat": PSCIS_HABITAT.get(r.HABITAT_VALUE_CODE), "road": _name(r.ROAD_NAME), "date": d,
            "crossing_type": r.CURRENT_CROSSING_TYPE_CODE, "x": g.x, "y": g.y}


def merge_pscis(kept: list[dict], pscis: list[dict], *, show=PSCIS_SHOW, types=PSCIS_TYPES,
                match_m: float = PSCIS_MATCH_M) -> list[dict]:
    """The deduped FISS marks (`kept`) with PSCIS folded in: every FISS culvert within `match_m` of ANY PSCIS crossing
    goes (PSCIS is the newer assessment of that crossing), and the PSCIS crossings of `types` whose `barrier` is in
    `show` are added, a missing stream name filled from the nearest FISS culvert it replaced."""
    from scipy.spatial import cKDTree

    if not pscis:
        return kept
    tree = cKDTree([(p["x"], p["y"]) for p in pscis])
    took: dict[int, dict] = {}                    # pscis index -> the nearest FISS culvert it replaced
    out = []
    for o in kept:
        if o["type"] == "culvert":
            d, i = tree.query((o["x"], o["y"]))
            if d <= match_m:
                if i not in took or d < took[i][0]:
                    took[int(i)] = (d, o)
                continue
        out.append(o)
    for i, p in enumerate(pscis):
        if p["crossing_type"] not in types or p["barrier"] not in show:
            continue
        p = dict(p)
        if p["water"] is None and i in took:
            p["water"] = took[i][1].get("water")
        out.append(p)
    return out


def _rank(o: dict) -> tuple:
    """Which of several same-place records is kept: a height, then the latest survey, then the lowest id."""
    return (o["height_m"] is None, -(o["year"] or 0), o["_n"])


def dedupe(fiss: list[dict], dam_lines: list, *, same_m: float = SAME_POINT_M, dam_m: float = DAM_MATCH_M) -> list[dict]:
    """FISS records (each with `x`, `y` in EPSG:3005 metres), less the same obstacle twice and the dams WRIS holds.

    `dam_lines`: the kept WRIS crests (shapely geometries, EPSG:3005). Deterministic: clusters are joined by union-find
    over every pair within `same_m`, and each cluster keeps its `_rank` first."""
    from scipy.spatial import cKDTree

    if not fiss:
        return []
    xy = [(o["x"], o["y"]) for o in fiss]
    parent = list(range(len(fiss)))

    def root(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i, j in sorted(cKDTree(xy).query_pairs(same_m)):
        if fiss[i]["type"] == fiss[j]["type"]:
            a, b = root(i), root(j)
            if a != b:
                parent[max(a, b)] = min(a, b)
    groups: dict[int, list[dict]] = {}
    for i, o in enumerate(fiss):
        groups.setdefault(root(i), []).append(o)
    kept = [min(g, key=_rank) for g in groups.values()]

    if dam_lines:
        from shapely import STRtree
        from shapely.geometry import Point
        tree = STRtree(dam_lines)
        kept = [o for o in kept
                if o["type"] != "dam" or not len(tree.query(Point(o["x"], o["y"]), predicate="dwithin", distance=dam_m))]
    return kept


def build(gpkg: Path, *, show=PSCIS_SHOW, types=PSCIS_TYPES) -> list[dict]:
    """Every mark, as {obstacle_id, type, name, water, height_m, source, year, lon, lat} (+ barrier, habitat, road,
    date on PSCIS), sorted by id. `show` / `types`: the PSCIS results and crossing types drawn (a census varies them)."""
    import geopandas as gpd
    from pyproj import Transformer

    if not Path(gpkg).exists():
        raise SystemExit(f"{gpkg}: no fish obstacles — fetch them first:\n"
                         "    PYTHONPATH=\"$PWD\" .venv/bin/python data/fetch_data.py "
                         "--layers fish_obstacles_fiss fish_obstacles_dams fish_obstacles_pscis")
    f = gpd.read_file(gpkg, layer="fiss", engine="pyogrio")
    d = gpd.read_file(gpkg, layer="dams", engine="pyogrio")
    for g in (f, d):
        if g.crs is None or g.crs.to_epsg() != 3005:
            raise SystemExit(f"{gpkg}: expected EPSG:3005, got {g.crs}")

    fiss: list[dict] = []
    for n, r in enumerate(f.sort_values("FISH_OBSTACLE_POINT_ID").itertuples(index=False)):
        t = fiss_type(r.OBSTACLE_NAME)
        if t is None or r.geometry is None or r.geometry.is_empty:
            continue
        fiss.append({"obstacle_id": f"fiss:{int(r.FISH_OBSTACLE_POINT_ID)}", "type": t, "name": None,
                     "water": _text(r.GAZETTED_NAME), "height_m": height(r.HEIGHT), "source": "fiss",
                     "year": year_of(r.SURVEY_DATE), "x": r.geometry.x, "y": r.geometry.y, "_n": n})

    import fiona
    if "pscis" not in set(fiona.listlayers(gpkg)):
        raise SystemExit(f"{gpkg}: no `pscis` layer — fetch it:\n"
                         "    PYTHONPATH=\"$PWD\" .venv/bin/python data/fetch_data.py --layers fish_obstacles_pscis")
    pg = gpd.read_file(gpkg, layer="pscis", engine="pyogrio")
    if pg.crs is None or pg.crs.to_epsg() != 3005:
        raise SystemExit(f"{gpkg}: expected EPSG:3005, got {pg.crs}")
    pscis = [m for m in (pscis_record(r) for r in pg.sort_values("STREAM_CROSSING_ID").itertuples(index=False)) if m]

    dams = d[d.DAM_OPERATION_CODE.isin(KEEP_DAM["operation"]) & d.DAM_FUNCTION.isin(KEEP_DAM["function"])]
    dams = dams.sort_values(["DAM_FILE_NUMBER", "WRIS_DP_SYSID"])
    wris: list[dict] = []
    lines = []
    for r in dams.itertuples(index=False):
        if r.geometry is None or r.geometry.is_empty:
            continue
        lines.append(r.geometry)
        p = r.geometry.interpolate(0.5, normalized=True)
        ident = _text(r.DAM_FILE_NUMBER) or str(int(r.WRIS_DP_SYSID))
        wris.append({"obstacle_id": f"wris:{ident}", "type": "dam", "name": _text(r.DAM_NAME), "water": None,
                     "height_m": height(r.DAM_HEIGHT), "source": "wris", "year": year_of(r.COMMISSIONED_YEAR),
                     "x": p.x, "y": p.y})

    out = merge_pscis(dedupe(fiss, lines), pscis, show=show, types=types) + wris
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    seen: set[str] = set()
    marks = []
    for o in out:
        if o["obstacle_id"] in seen:            # two WRIS crests of one dam file: one mark
            continue
        seen.add(o["obstacle_id"])
        lon, lat = tf.transform(o["x"], o["y"])
        marks.append({k: v for k, v in o.items() if k not in ("x", "y", "_n", "crossing_type")}
                     | {"lon": round(lon, 6), "lat": round(lat, 6)})
    return sorted(marks, key=lambda m: m["obstacle_id"])


def export_obstacles(out_dir: Path, gpkg: Path | None = None) -> dict:
    """Write `obstacle.geojsonl` into the tile layer directory. Returns {"obstacle": count}."""
    from pipeline.deliver.tiles.export import _writer
    from pipeline.deliver.tiles.layers import BY_NAME
    from pipeline.deliver.tiles.names import display

    spec = BY_NAME["obstacle"]
    write, close = _writer(Path(out_dir), spec)
    for m in build(Path(gpkg) if gpkg else SOURCE / GPKG):
        props = {k: m.get(k) for k in spec.attrs}
        for k in ("name", "water", "road"):
            if props[k]:
                props[k] = display(props[k])
        write({"type": "Point", "coordinates": [m["lon"], m["lat"]]}, props, spec.minzoom)
    return {spec.name: close()}


if __name__ == "__main__":                       # a census, no files written
    import collections
    import sys

    got = build(Path(sys.argv[1]) if len(sys.argv) > 1 else SOURCE / GPKG)
    print(json.dumps({"marks": len(got),
                      "by_source_type": dict(sorted(collections.Counter(f"{m['source']}:{m['type']}" for m in got).items())),
                      "pscis_barrier": dict(sorted(collections.Counter(m["barrier"] for m in got if m["source"] == "pscis").items())),
                      "with_height": sum(m["height_m"] is not None for m in got),
                      "with_name": sum(bool(m["name"]) for m in got),
                      "with_water": sum(bool(m["water"]) for m in got)}, indent=1))
