"""THE MINI RELEASE (T667 slice 8, owner D2 2026-11-11): one small INVENTED release built by the REAL pipeline.

    python -m pipeline.tools.mini_release --build DIR            # inputs -> atlas -> reach -> ... into DIR (scratch)
    python -m pipeline.tools.mini_release --build DIR --twice    # ... twice, and refuse unless both digests agree
    python -m pipeline.tools.mini_release --inputs DIR           # only the FWA-shaped GeoPackages (seconds)

WHAT GOES IN (committed, `pipeline/tests/fixtures/mini/`): `world.json` — a dozen invented waters as lines and boxes
in a local frame — and `curated/`, a whole curated tree in the layout of `data/curated` holding only invented rows.
Nothing here reads the live FWA files or the live curated tree:

  * the FWA GeoPackage and the coast pair are WRITTEN from `world.json` (`write_inputs`), in FWA's own layer names,
    columns and types (the layers `pipeline.atlas` reads: streams, lakes, wetlands, manmade, watersheds, wmu,
    tidal_boundary, pfma_areas; coast: land, rivers), so the atlas build runs over them UNCHANGED;
  * every stage runs as its own production command (`pipeline.atlas.build`, `pipeline.atlas.reach.cli`, ...) with
    `CURATED_ROOT` pointed at the mini tree (T422: the one switch that moves the whole curated tree, never a file) and
    `--gpkg` / `--coast-gpkg` / `--out` at the scratch files. `pipeline build` itself is NOT the driver: its preflight
    checks the real source files and the real curated tree, and a side run links the live basemap (step 1 measured).

WHAT COMES OUT: everything under `--build DIR` and nothing anywhere else. `digests(DIR)` is the determinism proof
(AGENTS 19): one sha256 per output file that carries no clock, over a normalised form where a file legitimately holds
one (the atlas meta's build time, the gpkg stamp of a scratch path). Two runs from the same inputs must agree.

STEPS (ticket T667 slice 8): (1) the input tree + the atlas, (2) the reach run and deliver + the digest proof, (3)
the app logic tests move onto the committed release, (4) the pipeline side. Built: atlas -> reach -> tile layers
(`--no-archive`) -> bundle -> verdicts -> status index, every fetched and fed input invented (`write_source`), and a
watch that refuses a build that changed a live directory (`watched_dirs`), then the export, answers, card gate,
store and release, the export held to the mini corpus's own claims. `--check` rebuilds in a temp dir and compares
the release with the committed copy (`--release-dir` names another); `--write` replaces it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from pipeline.common.curated import REPO_ROOT, SOURCE
from pipeline.common.utils import wsc as W
from pipeline.gauges.liveness import may_speak, speaks_now      # the one reader of a station's liveness (T122)

#: the committed inputs (a fixture of the tests; the one place this path is spelled)
FIXTURE = REPO_ROOT / "pipeline" / "tests" / "fixtures" / "mini"
WORLD = FIXTURE / "world.json"
CURATED_TREE = FIXTURE / "curated"
CLAIMS = FIXTURE / "claims.json"
FORMAT = "mini-world/1"

#: FWA's full watershed code is 21 groups: the root then 20 six-digit groups, zero-padded
_WSC_GROUPS = 21
_NO_CODE = W.join(W.UNKNOWN_ROOT, *["999999"] * (_WSC_GROUPS - 1))
EPSG = 3005


# ---------------------------------------------------------------------------------------------------------- the world

def load_world(path: Path = WORLD) -> dict:
    """`world.json`, checked: its format, unique ids, every stream's breaks inside it and every waterbody run on a
    break. A wrong world fails here, naming the water, never three stages later."""
    w = json.loads(Path(path).read_text(encoding="utf-8"))
    if w.get("format") != FORMAT:
        raise ValueError(f"{path}: format {w.get('format')!r}, expected {FORMAT!r}")
    seen: set = set()
    for s in w["streams"]:
        if s["blk"] in seen:
            raise ValueError(f"{path}: blue line {s['blk']} twice")
        seen.add(s["blk"])
        length = _length(s["vertices"])
        cuts = [0.0, *map(float, s["breaks"]), length]
        if cuts != sorted(set(cuts)):
            raise ValueError(f"{path}: {s['blk']} breaks must rise strictly inside (0, {length:g})")
        for run in s.get("waterbodies", ()):
            if float(run["from"]) not in cuts or float(run["to"]) not in cuts:
                raise ValueError(f"{path}: {s['blk']} waterbody run {run['wbk']} must start and end on a break")
    wbks = [b["wbk"] for b in w["waterbodies"]]
    if len(wbks) != len(set(wbks)):
        raise ValueError(f"{path}: a waterbody key twice")
    return w


def full_wsc(world: dict, rel: str | None, root: str | None = None) -> str:
    """The 21-group FWA code of a code written relative to the world's root (`""` = the root itself; None = FWA's
    no-code sentinel, a water off the network). `root`: a stream's own root in another drainage (T1250: Northward
    Creek drains to the Arctic, FWA 200, so the world keeps one "possible" steelhead stream under RULINGS S14)."""
    if rel is None:
        return _NO_CODE
    groups = W.groups(root or world["wsc_root"]) + W.groups(rel)
    if len(groups) > _WSC_GROUPS:
        raise ValueError(f"watershed code {rel!r} is deeper than FWA's {_WSC_GROUPS} groups")
    return W.join(*groups, *["000000"] * (_WSC_GROUPS - len(groups)))


def _length(vertices) -> float:
    return sum(((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5 for (x0, y0), (x1, y1) in zip(vertices, vertices[1:]))


def _shift(world: dict, x: float, y: float) -> tuple[float, float]:
    ox, oy = world["origin"]
    return float(ox + x), float(oy + y)


def _box(world: dict, b):
    from shapely.geometry import MultiPolygon, box
    x0, y0 = _shift(world, b[0], b[1])
    x1, y1 = _shift(world, b[2], b[3])
    return MultiPolygon([box(x0, y0, x1, y1)])


def _features(world: dict, s: dict) -> list[dict]:
    """A stream's FWA features: its line cut at its breaks, each with its route measures, z, waterbody and edge."""
    from shapely.geometry import LineString, MultiLineString
    from shapely.ops import substring
    pts = [_shift(world, x, y) for x, y in s["vertices"]]
    line = LineString(pts)
    length = line.length
    z0, z1 = map(float, s["z"])
    cuts = [0.0, *map(float, s["breaks"]), length]
    runs = {(float(r["from"]), float(r["to"])): r for r in s.get("waterbodies", ())}
    out = []
    for lo, hi in zip(cuts, cuts[1:]):
        part = substring(line, lo, hi)
        # z rises evenly from mouth to source (FWA lines are 3D; nothing here reads z but the tiles' elevation)
        coords = [(x, y, round(z0 + (z1 - z0) * line.project(_pt(x, y)) / length, 3)) for x, y in part.coords]
        run = next((r for (a, b), r in runs.items() if a <= lo and hi <= b), None)
        out.append({"lo": lo, "hi": hi, "geometry": MultiLineString([coords]),
                    "wbk": run["wbk"] if run else None,
                    "edge_type": int(run.get("edge_type", 1000)) if run else int(s.get("edge_type", 1000))})
    return out


def _pt(x, y):
    from shapely.geometry import Point
    return Point(x, y)


# ------------------------------------------------------------------------------------------------- the GeoPackages

def write_inputs(out_dir: Path, world: dict | None = None) -> tuple[Path, Path]:
    """Write `fwa.gpkg` and `coast.gpkg` from the world into `out_dir` (created; existing files replaced). The layers,
    columns and dtypes are FWA's (`pipeline.atlas.fwa` normalises them on read exactly as it does the real file)."""
    import geopandas as gpd
    import pandas as pd

    world = world or load_world()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fwa, coast = out_dir / "fwa.gpkg", out_dir / "coast.gpkg"
    for p in (fwa, coast, out_dir / "obstacles.gpkg"):
        if p.exists():
            p.unlink()
    nan = float("nan")

    def write(path, layer, rows, geoms, dtypes, geometry_type=None):
        df = pd.DataFrame(rows, columns=list(dtypes))
        for col, dt in dtypes.items():
            df[col] = df[col].astype(dt)
        kw = {"geometry_type": geometry_type} if geometry_type else {}
        gpd.GeoDataFrame(df, geometry=list(geoms), crs=f"EPSG:{EPSG}").to_file(
            path, layer=layer, driver="GPKG", engine="pyogrio", **kw)

    # --- streams
    stream_cols = {
        "LINEAR_FEATURE_ID": "float64", "WATERSHED_GROUP_ID": "float64", "EDGE_TYPE": "int32",
        "BLUE_LINE_KEY": "int32", "WATERSHED_KEY": "int32", "FWA_WATERSHED_CODE": "object",
        "LOCAL_WATERSHED_CODE": "object", "WATERSHED_GROUP_CODE": "object", "DOWNSTREAM_ROUTE_MEASURE": "float64",
        "LENGTH_METRE": "float64", "FEATURE_SOURCE": "object", "GNIS_ID": "float64", "GNIS_NAME": "object",
        "LEFT_RIGHT_TRIBUTARY": "object", "STREAM_ORDER": "int32", "STREAM_MAGNITUDE": "int32",
        "WATERBODY_KEY": "float64", "BLUE_LINE_KEY_50K": "float64", "WATERSHED_CODE_50K": "object",
        "WATERSHED_KEY_50K": "float64", "WATERSHED_GROUP_CODE_50K": "object", "GRADIENT": "float64",
        "FEATURE_CODE": "object", "UPSTREAM_ROUTE_MEASURE": "float64", "GEOMETRY_Length": "float64"}
    rows, geoms, fid = [], [], 910000000
    for s in world["streams"]:
        code = full_wsc(world, s["wsc"], s.get("wsc_root"))
        for f in _features(world, s):
            fid += 1
            rows.append([float(fid), 990.0, f["edge_type"], s["blk"], s.get("watershed_key", s["blk"]), code, code,
                         "MINI", f["lo"], f["hi"] - f["lo"], "linear stream -",
                         float(s["gnis_id"]) if s.get("gnis_id") else nan, s.get("name"),
                         s.get("side"), s["order"], s["magnitude"], float(f["wbk"]) if f["wbk"] else nan,
                         nan, None, nan, None, nan, "GA24850000", f["hi"], f["hi"] - f["lo"]])
            geoms.append(f["geometry"])
    write(fwa, "streams", rows, geoms, stream_cols)

    # --- waterbody polygons (lakes / wetlands / manmade): one schema, FWA's
    wb_cols = {
        "WATERBODY_POLY_ID": "float64", "WATERSHED_GROUP_ID": "float64", "WATERBODY_TYPE": "object",
        "WATERBODY_KEY": "int32", "AREA_HA": "float64", "GNIS_ID_1": "float64", "GNIS_NAME_1": "object",
        "GNIS_ID_2": "float64", "GNIS_NAME_2": "object", "GNIS_ID_3": "float64", "GNIS_NAME_3": "object",
        "BLUE_LINE_KEY": "int32", "WATERSHED_KEY": "int32", "FWA_WATERSHED_CODE": "object",
        "LOCAL_WATERSHED_CODE": "object", "WATERSHED_GROUP_CODE": "object", "LEFT_RIGHT_TRIBUTARY": "object",
        "WATERBODY_KEY_50K": "float64", "WATERSHED_GROUP_CODE_50K": "object",
        "WATERBODY_KEY_GROUP_CODE_50K": "object", "WATERSHED_CODE_50K": "object", "FEATURE_CODE": "object",
        "GEOMETRY_Length": "float64", "GEOMETRY_Area": "float64"}
    kinds = {"lakes": ("L", "GB15300000"), "wetlands": ("W", "WA24111110"), "manmade": ("X", "GB15300000")}
    poly_id = 920000000
    for layer, (wtype, fcode) in kinds.items():
        rows, geoms = [], []
        for b in (b for b in world["waterbodies"] if b["layer"] == layer):
            g = _box(world, b["box"])
            poly_id += 1
            code = full_wsc(world, b.get("wsc"))
            rows.append([float(poly_id), 990.0, wtype, b["wbk"], round(g.area / 10_000, 6),
                         float(b["gnis_id"]) if b.get("gnis_id") else nan, b.get("name"), nan, None, nan, None,
                         int(b.get("blk") or 0), int(b.get("blk") or 0), code, None, "MINI", None,
                         nan, None, None, None, fcode, g.length, g.area])
            geoms.append(g)
        if not rows:
            raise ValueError(f"world.json: no {layer} (every layer the atlas reads is written, never empty)")
        write(fwa, layer, rows, geoms, wb_cols)

    # --- named watersheds
    ws_cols = {"NAMED_WATERSHED_ID": "float64", "GNIS_ID": "int32", "GNIS_NAME": "object", "BLUE_LINE_KEY": "int32",
               "WATERSHED_KEY": "int32", "FWA_WATERSHED_CODE": "object", "STREAM_ORDER": "int32",
               "STREAM_MAGNITUDE": "int32", "AREA_HA": "float64", "FEATURE_CODE": "object",
               "GEOMETRY_Length": "float64", "GEOMETRY_Area": "float64"}
    rows, geoms = [], []
    for i, ws in enumerate(world["watersheds"], 1):
        g = _box(world, ws["box"])
        rows.append([float(930000 + i), ws["gnis_id"], ws["name"], ws["blk"], ws["blk"], full_wsc(world, ws["wsc"]),
                     ws["order"], ws["magnitude"], round(g.area / 10_000, 6), "WA25100000", g.length, g.area])
        geoms.append(g)
    write(fwa, "watersheds", rows, geoms, ws_cols)

    # --- wildlife management units: the B.C. outline is their union (AGENTS 59), the regions dissolve from them
    wmu_cols = {"id": "object", "WILDLIFE_MGMT_UNIT_ID": "object", "FEATURE_CODE": "object",
                "REGION_RESPONSIBLE": "int32", "REGION_RESPONSIBLE_ID": "object", "REGION_RESPONSIBLE_NAME": "object",
                "GAME_MANAGEMENT_ZONE_ID": "object", "GAME_MANAGEMENT_ZONE_NAME": "object",
                "FEATURE_AREA_SQM": "float64", "FEATURE_LENGTH_M": "float64", "OBJECTID": "int32",
                "SE_ANNO_CAD_DATA": "object"}
    from shapely.geometry import Polygon
    rows, geoms = [], []
    for i, r in enumerate(world["regions"], 1):
        g = _box(world, r["box"]).geoms[0]           # FWA's wmu layer is Polygon, not MultiPolygon
        rows.append([f"MINI.WMU.{i}", r["mu"], "WI02800110", int(r["region"]), r["region"], r["region_name"],
                     r["zone"], f"{r['region_name']} zone", g.area, g.length, i, None])
        geoms.append(Polygon(g.exterior.coords))
    write(fwa, "wmu", rows, geoms, wmu_cols)

    # --- the sea: the tidal boundary polygon (no attributes, as FWA's) and the fishery management areas
    write(fwa, "tidal_boundary", [[]], [_box(world, world["sea"])], {})
    pf_cols = {"id": "object", "DFO_PFMA_SAB_ID": "int32", "FEATURE_CODE": "object", "MANAGEMENT_AREA": "int32",
               "OBJECTID": "int32", "SE_ANNO_CAD_DATA": "object", "FEATURE_AREA_SQM": "float64",
               "FEATURE_LENGTH_M": "float64"}
    rows, geoms = [], []
    for i, a in enumerate(world["pfma"], 1):
        g = _box(world, a["box"])
        rows.append([f"MINI.PFMA.{i}", i, "FM90000010", a["area"], i, None, g.area, g.length])
        geoms.append(g)
    write(fwa, "pfma_areas", rows, geoms, pf_cols)

    # --- the OpenStreetMap access layers the area defs select from (`restricted_land_access`, `aboriginal_lands`)
    la_cols = {"osm_id": "object", "osm_type": "object", "name": "object", "access": "object", "landuse": "object",
               "restriction_level": "object"}
    write(fwa, "land_access", [[a["osm_id"], a["osm_id"].split("/")[0], a["name"], "no", a.get("landuse"),
                                 a["restriction_level"]] for a in world["land_access"]],
          [_box(world, a["box"]) for a in world["land_access"]], la_cols)
    al_cols = {"osm_id": "object", "name": "object", "boundary": "object", "type": "object"}
    write(fwa, "aboriginal_lands", [[a["osm_id"], a["name"], "aboriginal_lands", "boundary"]
                                    for a in world["aboriginal_lands"]],
          [_box(world, a["box"]) for a in world["aboriginal_lands"]], al_cols)

    # --- the watershed groups (the gauge field's catchments: `deliver.tiles.basins`, the bundle's basin_member)
    wg_cols = {"WATERSHED_GROUP_ID": "float64", "WATERSHED_GROUP_CODE": "object", "WATERSHED_GROUP_NAME": "object"}
    write(fwa, "watershed_groups", [[float(g["id"]), g["code"], g["name"]] for g in world["watershed_groups"]],
          [_box(world, g["box"]) for g in world["watershed_groups"]], wg_cols)

    # --- the dissolved title fabric (the tile export draws its Private class as `parcel`; access measures it, T54)
    parcel_cols = {"OWNER_TYPE": "object", "FEATURE_AREA_SQM": "float64", "FEATURE_LENGTH_M": "float64"}
    rows, geoms = [], []
    for p in world["parcels"]:
        g = _box(world, p["box"])
        rows.append([p["owner"], g.area, g.length])
        geoms.append(g)
    write(fwa, "land_parcels_crown", rows, geoms, parcel_cols)

    # --- the fish obstacles, in the fetched file's three layers (`deliver.tiles.obstacles` reads it whole)
    obstacles = out_dir / "obstacles.gpkg"
    fiss_cols = {"FISH_OBSTACLE_POINT_ID": "int32", "OBSTACLE_NAME": "object", "GAZETTED_NAME": "object",
                 "HEIGHT": "float64", "SURVEY_DATE": "datetime64[ms]"}
    rows, geoms = [], []
    for o in world["obstacles"]:
        rows.append([o["id"], o["name"], o.get("water"), float(o["height_m"]), pd.Timestamp(o["surveyed"])])
        geoms.append(_pt(*_shift(world, *o["at"])))
    write(obstacles, "fiss", rows, geoms, fiss_cols)
    dam_cols = {"WRIS_DP_SYSID": "int32", "DAM_FILE_NUMBER": "object", "DAM_NAME": "object",
                "COMMISSIONED_YEAR": "float64", "DAM_FUNCTION": "object", "DAM_OPERATION_CODE": "object",
                "DAM_HEIGHT": "float64"}
    write(obstacles, "dams", [], [], dam_cols, geometry_type="MultiLineString")
    pscis_cols = {"STREAM_CROSSING_ID": "int32", "ASSESSMENT_DATE": "datetime64[ms]", "STREAM_NAME": "object",
                  "ROAD_NAME": "object", "CURRENT_CROSSING_TYPE_CODE": "object",
                  "CURRENT_BARRIER_RESULT_CODE": "object", "HABITAT_VALUE_CODE": "object"}
    write(obstacles, "pscis", [], [], pscis_cols, geometry_type="Point")

    # --- the coast pair: the land mass and the FWA river polygons by blue line
    land_cols = {"WATERSHED_FEATURE_ID": "float64", "WATERSHED_TYPE": "object", "FWA_WATERSHED_CODE": "object",
                 "AREA_HA": "float64", "FEATURE_CODE": "object"}
    rows, geoms = [], []
    for i, b in enumerate(world["land"], 1):
        g = _box(world, b)
        rows.append([float(940000 + i), "L", full_wsc(world, ""), round(g.area / 10_000, 6), "WA21100000"])
        geoms.append(g)
    write(coast, "land", rows, geoms, land_cols)
    rows, geoms = [], []
    for i, r in enumerate(world["coast_rivers"], 1):
        g = _box(world, r["box"])
        poly_id += 1
        rows.append([float(poly_id), 990.0, "R", r["wbk"], round(g.area / 10_000, 6), nan, None, nan, None, nan, None,
                     r["blk"], r["blk"], full_wsc(world, ""), None, "MINI", None, nan, None, None, None,
                     "GA02850000", g.length, g.area])
        geoms.append(g)
    write(coast, "rivers", rows, geoms, wb_cols)
    return fwa, coast


# ------------------------------------------------------------------------------------- the fetched and fed inputs

def lonlat(world: dict, x: float, y: float) -> tuple[float, float]:
    """A world point as the fetched files carry it (lon, lat, 6 decimals)."""
    from pyproj import Transformer
    lon, lat = Transformer.from_crs(EPSG, 4326, always_xy=True).transform(*_shift(world, x, y))
    return round(lon, 6), round(lat, 6)


#: the running footer the page map is read off (`synopsis_pages._FOOTER_PAGE`)
FOOTER = "2025-2027 BC Freshwater Fishing Regulations Synopsis"


def tiny_pdf(pages: list[tuple[list[str], str]]) -> bytes:
    """A PDF of `pages`, each (lines from the top, a footer line at the foot; Helvetica): the synopsis the bundle reads its period off
    (`synopsis_pages.synopsis_period`) and the export its printed-page map (`printed_to_pdf`). Written byte for byte
    here, so it is the same on every machine."""
    def esc(t: str) -> str:
        return t.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    n = len(pages)
    font = 3 + 2 * n                                    # objects: 1 catalog, 2 pages, (page, content) per page, font
    objs = [b"<< /Type /Catalog /Pages 2 0 R >>",
            ("<< /Type /Pages /Kids [" + " ".join(f"{3 + 2 * i} 0 R" for i in range(n)) + f"] /Count {n} >>").encode()]
    for i, (lines, foot) in enumerate(pages):
        body = " ".join([*(f"BT /F1 10 Tf 40 {700 - 14 * k} Td ({esc(t)}) Tj ET" for k, t in enumerate(lines)),
                         *([f"BT /F1 8 Tf 40 14 Td ({esc(foot)}) Tj ET"] if foot else [])])
        stream = body.encode("latin-1")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents {4 + 2 * i} 0 R "
                    f"/Resources << /Font << /F1 {font} 0 R >> >> >>".encode())
        objs.append(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
    objs.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for i, o in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + o + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offsets)
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def synopsis_pages(world: dict) -> list[tuple[list[str], str]]:
    """The mini synopsis: every page from 1 to the last one the corpus cites, each printing its own number in the
    book's running footer (so printed = PDF page), the cover (page 1) stating the period."""
    syn = world["synopsis"]
    return [([syn["cover"]] if n == 1 else [f"Invented page {n} of the mini synopsis."], f"{n} {FOOTER}")
            for n in range(1, max(syn["printed_pages"]) + 1)]


def write_source(out_dir: Path, fwa: Path, world: dict | None = None) -> dict[str, Path]:
    """The FETCHED and FED inputs the deliver steps read, invented, into `out_dir` (`bundle --source / --clim /
    --dfo-typed / --dfo-cache`, `release --source / --dfo-rules`): the source tree (`source/`: the station roster,
    the places, the B.C. outline, the synopsis, HYDAT, the FWA gpkg), the gauge envelope and an empty DFO salmon
    feed. Returns {name: path}. Every value comes from `world.json`; nothing is copied from the live tree."""
    import shutil as _sh
    import sqlite3

    from shapely.geometry import mapping
    from shapely.ops import transform, unary_union
    from pyproj import Transformer

    world = world or load_world()
    out_dir = Path(out_dir)
    src = out_dir / "source"
    src.mkdir(parents=True, exist_ok=True)
    _sh.copyfile(fwa, SOURCE.under(src).fwa_gpkg)
    roster = [{"station": g["station"], "name": g["name"], "lon": lonlat(world, *g["at"])[0],
               "lat": lonlat(world, *g["at"])[1], "realtime": speaks_now(g), "active": may_speak(g),
               "area_km2": g["area_km2"]} for g in world["gauges"]]
    (src / "bc_hydrometric_stations.json").write_text(json.dumps(roster, indent=1) + "\n", encoding="utf-8")
    places = [{"name": p["name"], "lat": lonlat(world, *p["at"])[1], "lon": lonlat(world, *p["at"])[0],
               "place": p["place"], "rank": p["rank"], "pop": p["pop"], "alt": []} for p in world["places"]]
    (src / "bc_places.json").write_text(json.dumps(
        {"_licence": {"spdx": "none", "notice": "INVENTED for the mini release (T667): no source"},
         "places": places}, indent=1) + "\n", encoding="utf-8")
    tf = Transformer.from_crs(EPSG, 4326, always_xy=True).transform
    # the B.C. outline is the union of the management units (AGENTS 59), coastal water included
    outline = transform(tf, unary_union([_box(world, r["box"]) for r in world["regions"]]))
    (src / "bc_boundary.geojson").write_text(json.dumps(
        {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"name": "Mini Province"},
                                                    "geometry": mapping(outline)}]}) + "\n", encoding="utf-8")
    pdf = SOURCE.under(src).synopsis_pdf
    pdf.write_bytes(tiny_pdf(synopsis_pages(world)))
    # the row boxes cut from that PDF (the highlight a rule's page opens at): none — the mini book prints no table rows
    import hashlib as _h
    (src / "row_boxes.json").write_text(json.dumps(
        {"pdf_sha256": _h.sha256(pdf.read_bytes()).hexdigest(), "rows": {}}, indent=1) + "\n", encoding="utf-8")
    # what the corpus claims (`pipeline.deliver.export.claims`): the committed declaration, its files these
    _sh.copyfile(CLAIMS, out_dir / "claims.json")
    (src / "hydat.release").write_text(world["hydat_release"] + "\n", encoding="utf-8")
    hydat = src / "hydat.sqlite3"
    if hydat.exists():
        hydat.unlink()
    con = sqlite3.connect(hydat)
    con.executescript("""
        CREATE TABLE STATIONS (STATION_NUMBER TEXT PRIMARY KEY, STATION_NAME TEXT, PROV_TERR_STATE_LOC TEXT,
                               HYD_STATUS TEXT, LATITUDE REAL, LONGITUDE REAL, DRAINAGE_AREA_GROSS REAL,
                               REAL_TIME INTEGER);
        CREATE TABLE STN_REGULATION (STATION_NUMBER TEXT, YEAR_FROM INTEGER, YEAR_TO INTEGER, REGULATED INTEGER);
        CREATE TABLE STN_REMARKS (STATION_NUMBER TEXT, REMARK_TYPE_CODE INTEGER, YEAR INTEGER, REMARK_EN TEXT,
                                  REMARK_FR TEXT);""")
    for g, r in zip(world["gauges"], roster):
        con.execute("INSERT INTO STATIONS VALUES (?,?,?,?,?,?,?,?)",
                    (g["station"], g["name"], "BC", "A" if may_speak(g) else "D", r["lat"], r["lon"], g["area_km2"],
                     int(speaks_now(g))))
        con.execute("INSERT INTO STN_REGULATION VALUES (?,?,?,?)", (g["station"], 1990, 2024, int(g["regulated"])))
    con.commit()
    con.close()
    feeds = out_dir / "feeds"
    feeds.mkdir(parents=True, exist_ok=True)
    clim = {"release": world["hydat_release"], "percentiles": [10, 25, 50, 75, 90], "pentads": 73,
            "stations": {g["station"]: g["pentads"] for g in world["gauges"]},
            "stats": {g["station"]: g["record"] for g in world["gauges"]},
            "recentYears": {}, "recentYearCount": 2, "ratings": {}, "ratingPadM": 0.5, "levelShifts": {}}
    (feeds / "clim.json").write_text(json.dumps(clim, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    # THE DFO SALMON FEED, EMPTY: every region page the fetch reads, typed with no rows and no notes — the mini world
    # has no federal salmon water (a page that says nothing, in today's typed format)
    from pipeline.regs.dfo_salmon import header as H
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    from pipeline.regs.dfo_salmon.typed import FORMAT as TYPED_FORMAT
    typed, cache, rules = (feeds / "dfo_salmon" / x for x in ("typed", "cache", "rules"))
    for d in (typed, cache, rules):
        d.mkdir(parents=True, exist_ok=True)
    for s in (s for s in ALL_SLUGS if not PAGES[s].is_stub):
        (typed / f"region-{s}.json").write_text(json.dumps(
            {"region": s, "date_modified": None, "typed_format": TYPED_FORMAT, "page": {"sha256": None,
             "date_modified": None}, "notes": [], "header_format": H.FORMAT, "preamble": [], "locators": []},
            indent=1) + "\n", encoding="utf-8")
    return {"source": src, "clim": feeds / "clim.json", "dfo_typed": typed, "dfo_cache": cache,
            "claims": out_dir / "claims.json"}


# ----------------------------------------------------------------------------------------------------------- stages


def _env() -> dict:
    env = dict(os.environ)
    env["CURATED_ROOT"] = str(CURATED_TREE)
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("PYTHONHASHSEED", "0")
    return env


def stage_commands(out: Path, fwa: Path, coast: Path) -> list[tuple[str, list[str]]]:
    """Every stage, as the production command it is, pointed at `out` (ORDER IS THE BUILD). The deliver steps are
    `pipeline.deliver all`'s own, in its order, each named its inputs (`write_source`); `all` itself is not the driver:
    it promotes into canonical homes, keeps a step cache beside its out-dir and reads the released tiles + basemap
    (route (a), T667 slice 8: this release ships no tiles and no basemap). The tile step writes only its layer files
    (`--no-archive`): the bundle measures section access against them (T54).
    The export and the answers are held to the mini corpus's own claims (`fixtures/mini/claims.json`,
    `pipeline.deliver.export.claims`); the release ships no basemap and no synopsis. The answers hold the trout section
    (required since the T810 cutover, ticket L): the card gate runs its proof (J) here, as fast-fail's `--check` does.
    """
    py = [sys.executable, "-m"]
    atlas, reaches, tiles, d, fed = out / "atlas", out / "reaches", out / "tiles", out / "deliver", out / "fed"
    b = str(d / "bundle.sqlite")
    dlv = py + ["pipeline.deliver"]
    return [
        ("atlas", py + ["pipeline.atlas.build", "--gpkg", str(fwa), "--coast-gpkg", str(coast), "--full",
                        "--no-gauge-splits", "--out", str(atlas)]),
        # after the atlas, against ITS registry: without `--registry` the check reads the promoted live atlas's
        ("check_curated", py + ["pipeline.tools.audit.check_curated", "--waters", "--registry",
                                str(atlas / "registry.json")]),
        ("reach", py + ["pipeline.atlas.reach.cli", "--build", str(atlas), "--out", str(reaches)]),
        ("tiles", py + ["pipeline.deliver.tiles", "--build", str(atlas), "--gpkg", str(fwa), "--out", str(tiles),
                        "--no-archive", "--no-release", "--obstacles", str(fwa.parent / "obstacles.gpkg")]),
        ("bundle", dlv + ["bundle", "--build", str(atlas), "--reaches", str(reaches), "--tiles", str(tiles),
                          "--out", b, "--source", str(fed / "source"), "--clim", str(fed / "feeds" / "clim.json"),
                          "--dfo-typed", str(fed / "feeds" / "dfo_salmon" / "typed"),
                          "--dfo-cache", str(fed / "feeds" / "dfo_salmon" / "cache")]),
        ("verdicts", dlv + ["verdicts", "--bundle", b, "--out", str(d / "verdicts.sqlite"), "--workers", "1"]),
        ("status_index", dlv + ["status_index", "--bundle", b, "--out", str(d / "status_index.bin")]),
        ("export", dlv + ["export", "--bundle", b, "--out", str(d / "ui-rules-export.json"),
                          "--claims", str(fed / "claims.json")]),
        ("answers", dlv + ["answers", "--bundle", b, "--export-dir", str(d), "--workers", "1",
                           "--claims", str(fed / "claims.json")]),
        ("card_check", dlv + ["card_check", "--bundle", b, "--export-dir", str(d)]),
        ("store", dlv + ["store", "--bundle", b, "--export-dir", str(d), "--out", str(d / "regs.sqlite")]),
        ("release", dlv + ["release", "--bundle", b, "--store", str(d / "regs.sqlite"), "--status-index",
                           str(d / "status_index.bin"), "--export-dir", str(d), "--out-root", str(out / "release"),
                           "--tiles", str(tiles), "--tiles-release-root", str(out / "release"), "--no-basemap",
                           "--no-synopsis", "--source", str(fed / "source"),
                           "--dfo-rules", str(fed / "feeds" / "dfo_salmon" / "rules")]),
    ]


def build(out: Path, stop_after: str | None = None, trace: Path | None = None) -> dict:
    """Inputs then every stage into `out` (emptied first). Each stage's log is `out/logs/<stage>.log`; a failed stage
    stops the build and raises naming it and its log's last lines. Returns {stage: seconds}. (T1229) `trace`: each stage
    runs under the recorder (`PIPELINE_TRACE=<trace>/<stage>`), which the stage-key guard reads
    (`test_stage_content_keys.py`)."""
    out = Path(out).resolve()
    _refuse_inside_repo_data(out)
    before = listing(watched_dirs())
    if out.exists():
        shutil.rmtree(out)
    (out / "logs").mkdir(parents=True)
    t0 = time.perf_counter()
    fwa, coast = write_inputs(out / "inputs")
    write_source(out / "fed", fwa)
    (out / "deliver").mkdir()
    # the atlas build makes its --out only when it has a curated cut (`cut_curated`); its coast record is written
    # either way, so with no cut it died writing coast_owner.json. A writer may create its directory (AGENTS 41).
    (out / "atlas").mkdir()
    took = {"inputs": round(time.perf_counter() - t0, 1)}
    for name, cmd in stage_commands(out, fwa, coast):
        log = out / "logs" / f"{name}.log"
        t = time.perf_counter()
        with log.open("w", encoding="utf-8") as fh:
            fh.write("$ " + " ".join(cmd) + "\n")
            fh.flush()
            env = _env() if trace is None else dict(_env(), PIPELINE_TRACE=str(Path(trace).resolve() / name))
            rc = subprocess.run(cmd, cwd=REPO_ROOT, env=env, stdout=fh, stderr=subprocess.STDOUT).returncode
        took[name] = round(time.perf_counter() - t, 1)
        print(f"mini release: {name:14} {'ok' if rc == 0 else f'FAILED rc={rc}':12} {took[name]:7.1f}s  {log}",
              flush=True)
        if rc:
            tail = log.read_text(encoding="utf-8").splitlines()[-25:]
            raise SystemExit(f"mini release: stage {name} failed (rc {rc}); last lines of {log}:\n  "
                             + "\n  ".join(tail))
        if name == stop_after:
            break
    after = listing(watched_dirs())
    moved = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    if moved:
        raise SystemExit(f"mini release: the build changed {len(moved)} live path(s) — it must write only under "
                         f"{out}:\n  " + "\n  ".join(moved[:20]))
    (out / "timings.json").write_text(json.dumps(took, indent=1) + "\n", encoding="utf-8")
    return took


def watched_dirs() -> list[Path]:
    """The live directories a deliver step writes when no flag moves it (step 1 measured, T667 slice 8): the bundle,
    the tiles and the released tile folders, the release root, the run-timing base, the synopsis cache, the deliver
    step cache, the gauge feeds, the DFO salmon feed and its page cache, the fetched map assets — and the curated
    tree. A mini build must leave every one exactly as it found it (`build` refuses otherwise)."""
    from pipeline.common.curated import GENERATED, ROOTS, SOURCE, load_repo
    from pipeline.regs.dfo_salmon.fetch import DEFAULT_CACHE
    return [Path(GENERATED.bundle), Path(GENERATED.tiles), Path(ROOTS.release), Path(GENERATED.runtiming),
            Path(GENERATED.synopsis), Path(ROOTS.generated) / "deliver-cache", Path(GENERATED.gauges.feeds),
            Path(GENERATED.regs.dfo_salmon),
            Path(GENERATED.reaches), Path(SOURCE) / "map-assets", REPO_ROOT / DEFAULT_CACHE, Path(load_repo().base)]


def listing(dirs: list[Path]) -> dict[str, tuple]:
    """{path: (size, mtime_ns)} of every file and directory under `dirs` (a missing dir lists as absent)."""
    got: dict[str, tuple] = {}
    for d in dirs:
        if not d.exists():
            got[str(d)] = ("absent",)
            continue
        for root, subdirs, files in os.walk(d):
            for n in [*subdirs, *files]:
                st = os.lstat(os.path.join(root, n))
                got[os.path.join(root, n)] = (st.st_size if n in files else "dir", st.st_mtime_ns)
    return got


def _refuse_inside_repo_data(out: Path) -> None:
    """The mini release never writes into the repo's data tree or the fixture (scratch or a test tmp dir only)."""
    for root in (REPO_ROOT / "data", FIXTURE):
        root = root.resolve()
        if out == root or root in out.parents:
            raise SystemExit(f"mini release: refusing --build {out}: it is inside {root} (scratch only)")


# ---------------------------------------------------------------------------------------------------------- digests

#: files that carry a clock or a scratch path by design, and how each is normalised before it is hashed
_SKIP = {"logs", "inputs", "fed", "timings.json"}


def _normalised(rel: str, data: bytes, out: Path) -> bytes | None:
    """The bytes a file is compared by, or None to leave it out (named in `digests`' report)."""
    name = rel.rsplit("/", 1)[-1]
    if name.endswith(".pkl"):
        return None                                  # pickles: compared by what they decode to (`_decoded`)
    if name.endswith(".gpkg"):
        return _gpkg_content(out / rel)              # sqlite pages carry write times: compared by their features
    if name.endswith(".sqlite"):
        return _sqlite_content(out / rel, out)       # the bundle's meta names its atlas by path: by rows, path dropped
    text = data.decode("utf-8", errors="strict") if name.endswith((".json", ".jsonl", ".txt")) else None
    if text is None:
        return data
    text = text.replace(str(out), "<OUT>")
    if name == "summary.txt":                        # the atlas's profile: timings by design
        text = text.split("\n\ntimings, by cost:")[0]
    if name.endswith(".meta.json") or name == "report.json" or name == "COMPLETE.json":
        doc = json.loads(text)
        # the clocks: the atlas meta's build time, COMPLETE.json's `at`, the reach report's seconds
        clocks = ("build_ts", "built_at", "elapsed_s", "timings", "seconds", "wall_s", "ts")
        for k in clocks + (("at",) if name == "COMPLETE.json" else ()):
            _drop(doc, k)
        _drop_stamps(doc)
        text = json.dumps(doc, sort_keys=True)
    return text.encode("utf-8")


def _gpkg_content(path: Path) -> bytes:
    """A GeoPackage by what it holds: per layer (sorted), its columns and every feature's attributes + WKB."""
    import pyogrio
    parts = []
    for layer in sorted(str(n) for n, _ in pyogrio.list_layers(path)):
        df = pyogrio.read_dataframe(path, layer=layer)
        cols = [c for c in df.columns if c != "geometry"]
        attrs = df[cols].astype(str).values.tolist() if cols else [[] for _ in range(len(df))]
        rows = sorted(json.dumps(a) + "|" + (g.wkb_hex if g is not None else "")
                      for a, g in zip(attrs, df.geometry))
        parts.append(json.dumps([layer, cols, rows]))
    return "\n".join(parts).encode("utf-8")


def _sqlite_content(path: Path, out: Path) -> bytes:
    """A SQLite file by what it holds: per table (sorted), its schema and its rows sorted, the scratch root `out`
    written `<OUT>` (the bundle's `meta.build` names the atlas by path when it lies outside the repo)."""
    import sqlite3
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        parts = []
        for name, sql in con.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name"):
            rows = sorted(repr(tuple(v.hex() if isinstance(v, bytes) else v for v in r))
                          for r in con.execute(f'SELECT * FROM "{name}"'))
            parts.append(json.dumps([name, sql, rows]))
    finally:
        con.close()
    return "\n".join(parts).replace(str(out), "<OUT>").encode("utf-8")


def _drop(doc, key):
    if isinstance(doc, dict):
        doc.pop(key, None)
        for v in doc.values():
            _drop(v, key)
    elif isinstance(doc, list):
        for v in doc:
            _drop(v, key)


def _drop_stamps(doc):
    """The two source digests of the atlas record (`atlas.inputs.SOURCE_KEYS`: `sha256:<hex>` since T1223, before it
    `stamp:<size>:<mtime>`): the scratch gpkg is rewritten each run, and its bytes with it. Only those two keys: every
    curated `sha256:` digest stays compared."""
    from pipeline.atlas.inputs import SOURCE_KEYS
    if isinstance(doc, dict):
        for k, v in list(doc.items()):
            if isinstance(v, str) and (k in SOURCE_KEYS or v.startswith("stamp:")):
                doc[k] = "<scratch source>"
            else:
                _drop_stamps(v)
    elif isinstance(doc, list):
        for v in doc:
            _drop_stamps(v)


def _decoded(path: Path) -> bytes:
    """A pickle compared by a canonical dump of what it holds (sets sorted): pickle bytes follow set order."""
    import pickle
    with path.open("rb") as fh:
        obj = pickle.load(fh)
    return _canon(obj).encode("utf-8")


def _canon(o, depth=0) -> str:
    if depth > 60:
        return repr(o)
    if isinstance(o, dict):
        return "{" + ",".join(f"{_canon(k, depth + 1)}:{_canon(v, depth + 1)}" for k, v in
                              sorted(o.items(), key=lambda kv: repr(kv[0]))) + "}"
    if isinstance(o, (set, frozenset)):
        return "set(" + ",".join(sorted(_canon(v, depth + 1) for v in o)) + ")"
    if isinstance(o, (list, tuple)):
        return "[" + ",".join(_canon(v, depth + 1) for v in o) + "]"
    if hasattr(o, "wkb_hex"):
        return "geom:" + o.wkb_hex
    if hasattr(o, "__dataclass_fields__"):
        return type(o).__name__ + _canon({k: getattr(o, k) for k in o.__dataclass_fields__}, depth + 1)
    if hasattr(o, "__dict__") and not isinstance(o, type):
        return type(o).__name__ + _canon(vars(o), depth + 1)
    if hasattr(o, "__slots__"):
        return type(o).__name__ + _canon({k: getattr(o, k, None) for k in o.__slots__}, depth + 1)
    return repr(o)


def digests(out: Path) -> dict[str, str]:
    """{relative path: sha256} of every output under `out` — pickles by their decoded content, json by a form with no
    clock (`_normalised`). Sorted; the same inputs must give the same map (AGENTS 19)."""
    out = Path(out).resolve()
    got: dict[str, str] = {}
    for p in sorted(out.rglob("*")):
        rel = p.relative_to(out).as_posix()
        if not p.is_file() or rel.split("/", 1)[0] in _SKIP or p.name.endswith(".wkb"):
            continue
        if p.suffix == ".pkl":
            data = _decoded(p)
        else:
            data = _normalised(rel, p.read_bytes(), out)
            if data is None:
                continue
        got[rel] = hashlib.sha256(data).hexdigest()
    return got


def total(d: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------------------- the committed release copy

#: where the committed copy lives (T667 slice 8 plan); `--release-dir` names another (a scratch copy)
COMMITTED = REPO_ROOT / "app" / "packages" / "data" / "dev" / "mini-release"


def release_files(root: Path) -> dict[str, str]:
    """{path under `root`: sha256} of a release root holding ONE release (`r-*/` + `LATEST`); anything else is
    refused, so a stale second folder can never pass a check."""
    from pipeline.deliver.release.manifest import latest, releases      # the one lister + the one pointer reader
    root = Path(root)
    rels = sorted(p.name for p in releases(root))
    if len(rels) != 1 or not (root / "LATEST").is_file() or latest(root).name != rels[0]:
        raise SystemExit(f"mini release: {root} must hold exactly one r-* folder and LATEST (found {rels})")
    extra = sorted(p.name for p in root.iterdir() if p.name not in {rels[0], "LATEST"})
    if extra:
        raise SystemExit(f"mini release: {root} holds {extra} beside the release")
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


#: the manifest fields that follow a file's BYTES (a SQLite or zlib build writes the same rows in other bytes): the
#: release is compared by what each file holds, so a CI runner's libraries cannot fail a copy cut on this machine
_BYTE_FIELDS = ("sha256", "url", "gz", "bytes")


def release_content(root: Path) -> dict[str, str]:
    """{logical file: sha256 of what it holds} of the one release under `root`: each file the manifest names, gunzipped,
    a SQLite file by its rows (`_sqlite_content`), the manifest with its byte-following fields and the release id
    dropped. Two releases with the same content compare equal whatever library wrote their bytes."""
    import gzip
    import tempfile
    release_files(root)
    from pipeline.deliver.release.manifest import latest
    rel = latest(Path(root))
    man = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
    out = {}
    with tempfile.TemporaryDirectory(prefix="mini-content-") as t:
        for name, f in sorted(man["files"].items()):
            raw = gzip.decompress((Path(root) / f["url"]).read_bytes())
            if f["url"].endswith(".sqlite.gz"):
                db = Path(t) / f"{name}.sqlite"
                db.write_bytes(raw)
                raw = _sqlite_content(db, Path(t))
            out[name] = hashlib.sha256(raw).hexdigest()
    # (T1030) the per-water digest list the manifest names beside the files, by what it holds (gunzipped)
    # — and its url dropped from the manifest as every file's is: it names the release id, which follows the files' bytes
    # (CI's SQLite and zlib cut the same rows into another id; run 45410159283)
    wd = man.get("water_digests")
    if wd is not None:
        out["water_digests"] = hashlib.sha256(gzip.decompress((Path(root) / wd["url"]).read_bytes())).hexdigest()
        wd.pop("url")
    man.pop("data_release", None)
    for f in man["files"].values():
        for k in _BYTE_FIELDS:
            f.pop(k, None)
    out["manifest.json"] = hashlib.sha256(json.dumps(man, sort_keys=True).encode()).hexdigest()
    return out


def _fresh_release(scratch: Path) -> Path:
    took = build(scratch)
    print(f"mini release: rebuilt in {sum(took.values()):.1f}s")
    return scratch / "release"


def write(dest: Path, scratch: Path) -> int:
    """Rebuild in `scratch` and replace the copy at `dest` with it, whole (a reviewed diff, never hand surgery)."""
    dest = Path(dest)
    new = _fresh_release(scratch)
    release_files(new)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(new, dest)
    print(f"mini release: wrote {dest} ({len(release_files(dest))} files)")
    return 0


def check(dest: Path, scratch: Path) -> int:
    """Rebuild in `scratch` and compare, file by file BY CONTENT (`release_content`), with the copy at `dest`; 1 and the
    differences when any."""
    have = release_content(dest)
    got = release_content(_fresh_release(scratch))
    diff = sorted(k for k in set(have) | set(got) if have.get(k) != got.get(k))
    if diff:
        print(f"mini release: {dest} is STALE — {len(diff)} file(s) differ from a rebuild:")
        for k in diff[:40]:
            print(f"  {k}: committed {have.get(k, '(absent)')[:12]}, rebuilt {got.get(k, '(absent)')[:12]}")
        print(f"  re-cut it: python -m pipeline.tools.mini_release --write"
              + ("" if Path(dest).resolve() == COMMITTED.resolve() else f" --release-dir {dest}"))
        return 1
    print(f"mini release: {dest} matches a rebuild ({len(have)} files)")
    return 0


# -------------------------------------------------------------------------------------------------------------- cli

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.mini_release", description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, help="build the mini release into this scratch directory")
    ap.add_argument("--twice", action="store_true", help="build twice (DIR/a, DIR/b) and refuse unless digests agree")
    ap.add_argument("--stop-after", help="stop after this stage (atlas, check_curated, reach, tiles, bundle, verdicts, status_index)")
    ap.add_argument("--inputs", type=Path, help="only write the FWA-shaped GeoPackages into this directory")
    ap.add_argument("--compare", nargs=2, type=Path, metavar=("A", "B"),
                    help="digest two finished builds and refuse unless they agree (reads only)")
    ap.add_argument("--check", action="store_true",
                    help="rebuild in a temp dir and compare with the committed copy (or --release-dir); exit 1 on drift")
    ap.add_argument("--write", action="store_true",
                    help="rebuild in a temp dir and replace the committed copy (or --release-dir) with it")
    ap.add_argument("--release-dir", type=Path, default=COMMITTED,
                    help="the release copy --check / --write compare / replace (default: the committed one)")
    a = ap.parse_args(argv)
    if a.check or a.write:
        import tempfile
        with tempfile.TemporaryDirectory(prefix="mini-release-") as t:
            return (check if a.check else write)(a.release_dir, Path(t) / "build")
    if a.compare:
        return _compare(*(digests(d) for d in a.compare))
    if a.inputs:
        fwa, coast = write_inputs(a.inputs)
        print(f"wrote {fwa} ({fwa.stat().st_size:,} B) and {coast} ({coast.stat().st_size:,} B)")
        return 0
    if not a.build:
        ap.error("give --build DIR (scratch) or --inputs DIR")
    runs = [a.build / "a", a.build / "b"] if a.twice else [a.build]
    results = []
    for r in runs:
        took = build(r, a.stop_after)
        d = digests(r)
        (r.parent / f"{r.name}.digests.json").write_text(json.dumps(d, indent=1, sort_keys=True) + "\n")
        results.append((r, took, d))
        print(f"mini release: {r}: {len(d)} files, digest {total(d)}  ({sum(took.values()):.1f}s)")
    if a.twice:
        return _compare(results[0][2], results[1][2])
    return 0


def _compare(da: dict[str, str], db: dict[str, str]) -> int:
    diff = sorted(k for k in set(da) | set(db) if da.get(k) != db.get(k))
    if diff:
        print(f"mini release: NOT DETERMINISTIC — {len(diff)} file(s) differ between the two runs:")
        for k in diff[:40]:
            print(f"  {k}: {da.get(k, '(absent)')[:12]} vs {db.get(k, '(absent)')[:12]}")
        return 1
    print(f"mini release: deterministic — both runs {total(da)} over {len(da)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
