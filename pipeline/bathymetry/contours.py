"""Placed depth contours (`WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP`, "Bathymetric Maps of Surveyed Lakes") -> lake
PROPOSALS (T634 third pass, PROTOTYPE; nothing accepted, nothing published).

LICENCE: catalogue "Access Only"; owner says OGL (2026-11-11, link owed). Every row and file carries LICENCE_FLAG.
Confirm before any production publish.

The layer is already on the earth (EPSG:3005) and names its lake three ways: `LAKE_FWA_WATERBODY_KEY` (our durable
lake id), `LAKE_WSA_WATERBODY_IDENTIFIER` (the 50K code) and `LAKE_GAZETTED_NAME`. The key proposes; the code (must be
on that FWA polygon), the name (core / word order / an independent curated variant, never a name inside a name) and the
0 m polygon's overlap with the FWA outline (IoU) confirm.

    python -m pipeline.bathymetry.contours --fetch     # one public WFS request to ~/data-offgit/bc-bathymetry-contours/
    python -m pipeline.bathymetry.contours             # -> GENERATED.base/bathymetry/contour_proposals.json
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from pipeline.deliver.lake_facts import core

LICENCE_FLAG = "owner says OGL, catalogue says Access Only, unconfirmed"
WFS = ("https://openmaps.gov.bc.ca/geo/pub/wfs?service=WFS&version=2.0.0&request=GetFeature"
       "&typeName=pub:WHSE_FISH.BATH_LAKE_BATHYMETRIC_SP&outputFormat=json&srsName=EPSG:3005")
IOU_MIN = 0.8
SENTINEL_DEPTH = 99999          # the layer's "deepest area" polygon, not a depth
DEPTH_FIELDS = ("LAKE_MEAN_DEPTH_M", "LAKE_MAX_DEPTH_M", "LAKE_SURFACE_AREA_SQM", "LAKE_VOLUME_CUBM",
                "LAKE_LITTORAL_AREA_SQM", "LAKE_SURFACE_ELEVATION_M", "LAKE_SURVEY_DATE", "LAKE_MAP_SCALE")


def default_dir() -> Path:
    return Path.home() / "data-offgit" / "bc-bathymetry-contours"


def fwa_lakes(where: str) -> List[dict]:
    """FWA lake polygons (lakes + manmade layers) matching an SQL `where`, read by index (seconds, not the full scan)."""
    import pyogrio
    from project_config import get_config
    gpkg = get_config().get_path("data", "fetch", "output_gpkg")
    out = []
    for layer in ("lakes", "manmade"):
        df = pyogrio.read_dataframe(gpkg, layer=layer, where=where,
                                    columns=["WATERBODY_KEY", "WATERBODY_KEY_GROUP_CODE_50K", "GNIS_ID_1", "GNIS_NAME_1",
                                             "GNIS_NAME_2", "GNIS_NAME_3", "AREA_HA"])
        for r in df.itertuples():
            code = r.WATERBODY_KEY_GROUP_CODE_50K           # pandas gives NaN (a float) for an empty cell
            out.append({"layer": layer, "wbk": int(r.WATERBODY_KEY), "code": code.strip() if isinstance(code, str) else "",
                        "names": [n for n in (r.GNIS_NAME_1, r.GNIS_NAME_2, r.GNIS_NAME_3) if isinstance(n, str) and n],
                        "gnis_id": int(r.GNIS_ID_1) if r.GNIS_ID_1 == r.GNIS_ID_1 and r.GNIS_ID_1 else None,
                        "geometry": r.geometry})
    return out


def name_agrees(title: str, names: Sequence[str], variants: Sequence[tuple]) -> Optional[str]:
    """equal | word_order | variant (independent source) | None. No name-inside-a-name match (owner, T634)."""
    from pipeline.bathymetry.propose import CIRCULAR_SOURCES, word_order
    if not title or not core(title):
        return None
    if any(core(n) == core(title) for n in names):
        return "equal"
    if any(word_order(n) == word_order(title) for n in names):
        return "word_order"
    if any(s not in CIRCULAR_SOURCES and (core(v) == core(title) or word_order(v) == word_order(title))
           for v, s in variants):
        return "variant"
    if any(core(v) == core(title) for v, _ in variants):
        return "variant_circular"
    return None


def propose(features: Sequence[Mapping], lakes_by_wbk: Mapping[int, List[dict]], var_wbk, var_gnis,
            app: Mapping[str, Mapping]) -> dict:
    from shapely.geometry import shape
    from shapely.ops import unary_union
    by_lake: Dict[int, List[Mapping]] = defaultdict(list)
    for f in features:
        by_lake[int(f["properties"]["LAKE_FWA_WATERBODY_KEY"])].append(f)
    rows = []
    for wbk in sorted(by_lake):
        fs = by_lake[wbk]
        p0 = fs[0]["properties"]
        depths = sorted({f["properties"]["CONTOUR_DEPTH_M"] for f in fs})
        row = {"wbk": f"wbk:{wbk}", "code": p0["LAKE_WSA_WATERBODY_IDENTIFIER"], "title": p0["LAKE_GAZETTED_NAME"],
               "contour_depths_m": [d for d in depths if d != SENTINEL_DEPTH],
               "has_deepest_polygon": SENTINEL_DEPTH in depths, "polygons": len(fs),
               "facts": {k: p0.get(k) for k in DEPTH_FIELDS}, "licence": LICENCE_FLAG, "evidence": {}}
        ev = row["evidence"]
        polys = lakes_by_wbk.get(wbk, [])
        if not polys:
            rows.append(_done(row, "no_lake"))
            continue
        names = sorted({n for p in polys for n in p["names"]})
        codes = sorted({p["code"] for p in polys if p["code"]})
        ev.update(lake_names=names, lake_codes=codes, in_app=f"wbk:{wbk}" in app)
        variants = list(var_wbk.get(wbk, [])) + [v for p in polys if p["gnis_id"] for v in var_gnis.get(p["gnis_id"], [])]
        ev["name_evidence"] = name_agrees(row["title"], names, variants)
        ev["code_agrees"] = row["code"] in codes
        zero = [shape(f["geometry"]) for f in fs if f["properties"]["CONTOUR_DEPTH_M"] == 0]
        lake = unary_union([p["geometry"] for p in polys])
        if zero:
            z = unary_union(zero)
            inter = z.intersection(lake).area
            ev["iou_0m"] = round(inter / (z.area + lake.area - inter), 4)
        if not ev["code_agrees"]:
            rule = "code_disagrees"
        elif ev["name_evidence"] in (None, "variant_circular"):
            rule = "name_disagrees" if ev["name_evidence"] is None else "variant_circular"
        elif ev.get("iou_0m") is None or ev["iou_0m"] < IOU_MIN:
            rule = "outline_disagrees"
        elif not ev["in_app"]:
            rule = "not_in_app"
        else:
            rule = "clean"
        rows.append(_done(row, rule))
    return {"_about": "Placed-contour lake -> FWA lake PROPOSALS (pipeline.bathymetry.contours, T634). Nothing accepted; "
                      "`python -m pipeline.bathymetry.accept --set contours` is the only writer of the frozen file.",
            "licence": LICENCE_FLAG, "rules": {"iou_min": IOU_MIN},
            "counts": dict(sorted(Counter(r["rule"] for r in rows).items())), "proposals": rows}


def _done(row: dict, rule: str) -> dict:
    row["rule"] = rule
    row["verdict"] = {"clean": "clean", "not_in_app": "held"}.get(rule, "review")
    row["chart_id"] = row["wbk"]          # the accept key
    row["wbks"] = [row["wbk"]]
    row["titles"] = [row["title"]]
    return row


def main(argv=None) -> int:
    from pipeline.bathymetry.propose import read_app, variants_by_lake
    from pipeline.common.curated import CURATED, GENERATED
    ap = argparse.ArgumentParser(description="placed contours -> lake proposals (T634 prototype)")
    ap.add_argument("--dir", type=Path, default=default_dir())
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--release", type=Path)
    a = ap.parse_args(argv)
    src = a.dir.expanduser() / "contours.geojson"
    if a.fetch:
        src.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(urllib.request.Request(WFS, headers={"User-Agent": "canifishthis.ca research (T634)"}),
                                    timeout=300) as r:
            data = json.loads(r.read())
        data["licence"] = LICENCE_FLAG
        src.write_text(json.dumps(data))
        print(f"wrote {src} ({len(data['features'])} polygons)")
        return 0
    feats = json.loads(src.read_text())["features"]
    wbks = sorted({int(f["properties"]["LAKE_FWA_WATERBODY_KEY"]) for f in feats})
    lakes: Dict[int, List[dict]] = defaultdict(list)
    for l in fwa_lakes(f"WATERBODY_KEY IN ({','.join(map(str, wbks))})"):
        lakes[l["wbk"]].append(l)
    var_wbk, var_gnis = variants_by_lake(json.loads(Path(CURATED.waters.name_variants).read_text()))
    out = propose(feats, lakes, var_wbk, var_gnis, _app(a.release, read_app))
    dest = GENERATED.base / "bathymetry" / "contour_proposals.json"
    tmp = dest.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False))
    os.replace(tmp, dest)
    print(json.dumps(out["counts"]), f"lakes {len(out['proposals'])}; wrote {dest}")
    return 0


def _app(release: Optional[Path], read_app) -> Dict[str, dict]:
    """The release's lake items (core sqlite), as propose.main reads them."""
    import gzip
    import shutil
    import tempfile
    from pipeline.common.curated import GENERATED
    from pipeline.deliver.release.manifest import live
    rel = release or live(GENERATED.release)
    cores = sorted(p for p in Path(rel).glob("core-*.sqlite.gz") if p.name.count("-") == 1)
    if len(cores) != 1:
        raise SystemExit(f"{rel}: want one core-<hash>.sqlite.gz, found {[p.name for p in cores]}")
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "core.sqlite"
        with gzip.open(cores[0]) as s, open(db, "wb") as d:
            shutil.copyfileobj(s, d)
        return read_app(db)


if __name__ == "__main__":
    raise SystemExit(main())
