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
the app logic tests move onto the committed release, (4) the pipeline side. `--check` / `--write` against a committed
copy come with step 3, when there is a release to commit.
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

from pipeline.common.curated import REPO_ROOT

#: the committed inputs (a fixture of the tests; the one place this path is spelled)
FIXTURE = REPO_ROOT / "pipeline" / "tests" / "fixtures" / "mini"
WORLD = FIXTURE / "world.json"
CURATED_TREE = FIXTURE / "curated"
FORMAT = "mini-world/1"

#: FWA's full watershed code is 21 groups: the root then 20 six-digit groups, zero-padded
_WSC_GROUPS = 21
_NO_CODE = "999-" + "-".join(["999999"] * (_WSC_GROUPS - 1))
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


def full_wsc(world: dict, rel: str | None) -> str:
    """The 21-group FWA code of a code written relative to the world's root (`""` = the root itself; None = FWA's
    no-code sentinel, a water off the network)."""
    if rel is None:
        return _NO_CODE
    groups = world["wsc_root"].split("-") + ([g for g in rel.split("-")] if rel else [])
    if len(groups) > _WSC_GROUPS:
        raise ValueError(f"watershed code {rel!r} is deeper than FWA's {_WSC_GROUPS} groups")
    return "-".join(groups + ["000000"] * (_WSC_GROUPS - len(groups)))


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
    for p in (fwa, coast):
        if p.exists():
            p.unlink()
    nan = float("nan")

    def write(path, layer, rows, geoms, dtypes):
        df = pd.DataFrame(rows, columns=list(dtypes))
        for col, dt in dtypes.items():
            df[col] = df[col].astype(dt)
        gpd.GeoDataFrame(df, geometry=list(geoms), crs=f"EPSG:{EPSG}").to_file(
            path, layer=layer, driver="GPKG", engine="pyogrio")

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
        code = full_wsc(world, s["wsc"])
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
                     3, 4, round(g.area / 10_000, 6), "WA25100000", g.length, g.area])
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


# ----------------------------------------------------------------------------------------------------------- stages

def _env() -> dict:
    env = dict(os.environ)
    env["CURATED_ROOT"] = str(CURATED_TREE)
    env["PYTHONPATH"] = str(REPO_ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("PYTHONHASHSEED", "0")
    return env


def stage_commands(out: Path, fwa: Path, coast: Path) -> list[tuple[str, list[str]]]:
    """Every stage, as the production command it is, pointed at `out` (ORDER IS THE BUILD)."""
    py = [sys.executable, "-m"]
    atlas, reaches = out / "atlas", out / "reaches"
    return [
        ("check_curated", py + ["pipeline.tools.audit.check_curated", "--waters"]),
        ("atlas", py + ["pipeline.atlas.build", "--gpkg", str(fwa), "--coast-gpkg", str(coast), "--full",
                        "--no-gauge-splits", "--out", str(atlas)]),
        ("reach", py + ["pipeline.atlas.reach.cli", "--build", str(atlas), "--out", str(reaches)]),
    ]


def build(out: Path, stop_after: str | None = None) -> dict:
    """Inputs then every stage into `out` (emptied first). Each stage's log is `out/logs/<stage>.log`; a failed stage
    stops the build and raises naming it and its log's last lines. Returns {stage: seconds}."""
    out = Path(out).resolve()
    _refuse_inside_repo_data(out)
    if out.exists():
        shutil.rmtree(out)
    (out / "logs").mkdir(parents=True)
    t0 = time.perf_counter()
    fwa, coast = write_inputs(out / "inputs")
    took = {"inputs": round(time.perf_counter() - t0, 1)}
    for name, cmd in stage_commands(out, fwa, coast):
        log = out / "logs" / f"{name}.log"
        t = time.perf_counter()
        with log.open("w", encoding="utf-8") as fh:
            fh.write("$ " + " ".join(cmd) + "\n")
            fh.flush()
            rc = subprocess.run(cmd, cwd=REPO_ROOT, env=_env(), stdout=fh, stderr=subprocess.STDOUT).returncode
        took[name] = round(time.perf_counter() - t, 1)
        print(f"mini release: {name:14} {'ok' if rc == 0 else f'FAILED rc={rc}':12} {took[name]:7.1f}s  {log}",
              flush=True)
        if rc:
            tail = log.read_text(encoding="utf-8").splitlines()[-25:]
            raise SystemExit(f"mini release: stage {name} failed (rc {rc}); last lines of {log}:\n  "
                             + "\n  ".join(tail))
        if name == stop_after:
            break
    (out / "timings.json").write_text(json.dumps(took, indent=1) + "\n", encoding="utf-8")
    return took


def _refuse_inside_repo_data(out: Path) -> None:
    """The mini release never writes into the repo's data tree or the fixture (scratch or a test tmp dir only)."""
    for root in (REPO_ROOT / "data", FIXTURE):
        root = root.resolve()
        if out == root or root in out.parents:
            raise SystemExit(f"mini release: refusing --build {out}: it is inside {root} (scratch only)")


# ---------------------------------------------------------------------------------------------------------- digests

#: files that carry a clock or a scratch path by design, and how each is normalised before it is hashed
_SKIP = {"logs", "inputs", "timings.json"}


def _normalised(rel: str, data: bytes, out: Path) -> bytes | None:
    """The bytes a file is compared by, or None to leave it out (named in `digests`' report)."""
    name = rel.rsplit("/", 1)[-1]
    if name.endswith(".pkl") or name.endswith(".gpkg"):
        return None                                  # pickles / sqlite pages: compared by what they decode to
    text = data.decode("utf-8", errors="strict") if name.endswith((".json", ".jsonl", ".txt")) else None
    if text is None:
        return data
    text = text.replace(str(out), "<OUT>")
    if name == "summary.txt":                        # the atlas's profile: timings by design
        text = text.split("\n\ntimings, by cost:")[0]
    if name.endswith(".meta.json") or name == "report.json" or name == "COMPLETE.json":
        doc = json.loads(text)
        for k in ("build_ts", "built_at", "elapsed_s", "timings", "seconds", "wall_s", "ts"):
            _drop(doc, k)
        _drop_stamps(doc)
        text = json.dumps(doc, sort_keys=True)
    return text.encode("utf-8")


def _drop(doc, key):
    if isinstance(doc, dict):
        doc.pop(key, None)
        for v in doc.values():
            _drop(v, key)
    elif isinstance(doc, list):
        for v in doc:
            _drop(v, key)


def _drop_stamps(doc):
    """`stamp:<size>:<mtime>` (the gpkg digests, `atlas.inputs`): the scratch gpkg is rewritten each run."""
    if isinstance(doc, dict):
        for k, v in list(doc.items()):
            if isinstance(v, str) and v.startswith("stamp:"):
                doc[k] = "stamp:<scratch>"
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


# -------------------------------------------------------------------------------------------------------------- cli

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.mini_release", description=__doc__.split("\n\n")[0])
    ap.add_argument("--build", type=Path, help="build the mini release into this scratch directory")
    ap.add_argument("--twice", action="store_true", help="build twice (DIR/a, DIR/b) and refuse unless digests agree")
    ap.add_argument("--stop-after", help="stop after this stage (inputs, check_curated, atlas, reach)")
    ap.add_argument("--inputs", type=Path, help="only write the FWA-shaped GeoPackages into this directory")
    a = ap.parse_args(argv)
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
        (ra, _, da), (rb, _, db) = results
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
