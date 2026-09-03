"""Standalone end-to-end proof of the added-streams flow — WITHOUT touching build.py.

Loads real FWA fids for a small bbox (auto-derived from the added streams' extent), ingests a curated
added_streams GeoJSON, builds a graph over FWA ∪ added fids, attaches connector edges, and reports:
merged mainstem = one node/blk/wsc, tributaries nest under it, added nodes are ancestors of the FWA
stream they join. Optionally writes output/added_demo.gpkg for eyeballing in QGIS.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.atlas.waters.added_streams.harness \
        --geojson pipeline/atlas/waters/added_streams/tests/data/added_streams.sample.geojson

Local only (no network, no credits): it reads the FWA gpkg and the curated GeoJSON.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyproj import Transformer

from pipeline.atlas.fwa import FWADataAccessor
from pipeline.atlas.waters.added_streams.ingest import attach_connectors, ingest, load_features
from pipeline.atlas.build import get_lake_names, get_lake_wbk_kind
from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.atlas.graph.graph import ancestors, build_section_geometries, build_stream_graph
from pipeline.common.models import NodeKind

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)


def _bbox_3005(features: list[dict], pad: float) -> tuple[float, float, float, float]:
    xs, ys = [], []
    for f in features:
        for lon, lat in f["geometry"]["coordinates"]:
            x, y = _TO_ALBERS.transform(lon, lat)
            xs.append(x)
            ys.append(y)
    return (min(xs) - pad, min(ys) - pad, max(xs) + pad, max(ys) + pad)


def run(geojson: str, gpkg: str, bbox=None, pad: float = 3000.0, out: str | None = None) -> dict:
    features = load_features(geojson)
    bbox = bbox or _bbox_3005(features, pad)
    print(f"== added-streams harness ==\n  geojson: {geojson}\n  bbox(3005): "
          f"({bbox[0]:.0f},{bbox[1]:.0f},{bbox[2]:.0f},{bbox[3]:.0f})")

    fwa = FWADataAccessor(gpkg)
    fwa_fids = load_stream_fids(gpkg, bbox=bbox)
    lake_kind = get_lake_wbk_kind(fwa, bbox=bbox)
    lake_names = get_lake_names(fwa, bbox=bbox)
    fwa_chains = build_blk_chains(fwa_fids, lake_kind)
    print(f"  FWA: {len(fwa_fids)} fids, {len(fwa_chains)} chains")

    add_fids, add_chains, specs = ingest(features, fwa_chains)
    graph = build_stream_graph(fwa_chains + add_chains, fwa_fids + add_fids, lake_kind, lake_names)
    geoms = build_section_geometries(fwa_chains + add_chains, fwa_fids + add_fids, lake_kind)
    report = attach_connectors(graph, geoms, specs)

    # FWA roots the added streams ultimately hang off (for the ancestor check)
    fwa_roots = {s.to_blk for s in specs if s.to_fwa}
    fwa_ancestors: set[str] = set()
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and str(n.blk) in fwa_roots:
            fwa_ancestors |= ancestors(graph, nid)

    print(f"\n  attached: added={report['added']} skipped={report['skipped']}")
    print(f"  {'added blk':>10} | {'wsc':<34} | len(m) | ord | mag | ∈anc(FWA)")
    for ch in add_chains:
        nid = f"{ch.blk}:0"
        node = graph.nodes.get(nid)
        in_anc = nid in fwa_ancestors
        print(f"  {ch.blk:>10} | {ch.fwa_watershed_code:<34} | {ch.length_m:6.0f} | "
              f"{ch.stream_order or 0:>3} | {ch.stream_magnitude or 0:>3} | "
              f"{'yes' if in_anc else 'NO' if node else 'MISSING'}")
    for s in specs:
        print(f"    edge {s.from_node} -> blk {s.to_blk} ({'FWA' if s.to_fwa else 'added'}) "
              f"@ {s.at_measure:.0f}m  [{s.kind}]")

    if out:
        _write_gpkg(graph, geoms, add_chains, specs, out)
        print(f"\n  wrote {out}")
    return {"report": report, "chains": len(add_chains),
            "all_tributaries": all(f"{c.blk}:0" in fwa_ancestors for c in add_chains)}


def _write_gpkg(graph, geoms, add_chains, specs, out: str) -> None:
    import geopandas as gpd

    rows = []
    for ch in add_chains:
        g = geoms.get(f"{ch.blk}:0")
        if g is not None:
            rows.append({"kind": "added_stream", "id": f"{ch.blk}:0",
                         "wsc": ch.fwa_watershed_code, "geometry": g})
    for k, g in geoms.items():
        if k.startswith("connector:"):
            rows.append({"kind": "connector", "id": k, "wsc": "", "geometry": g})
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:3005").to_file(out, driver="GPKG")


def main() -> None:
    from project_config import get_config
    ap = argparse.ArgumentParser(description="Standalone added-streams end-to-end demo.")
    ap.add_argument("--geojson", default="pipeline/atlas/waters/added_streams/tests/data/added_streams.sample.geojson")
    ap.add_argument("--gpkg", default=None, help="FWA gpkg (default: project config)")
    ap.add_argument("--bbox", nargs=4, type=float, default=None,
                    metavar=("MINX", "MINY", "MAXX", "MAXY"), help="EPSG:3005 bbox (default: auto)")
    ap.add_argument("--pad", type=float, default=3000.0, help="auto-bbox padding metres")
    ap.add_argument("--out", default=None, help="output gpkg (default: config output.added_streams/added_demo.gpkg)")
    args = ap.parse_args()
    gpkg = args.gpkg or get_config().fwa_data_gpkg
    out = args.out or str(get_config().added_streams_dir / "added_demo.gpkg")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    result = run(args.geojson, gpkg, bbox=args.bbox, pad=args.pad, out=out)
    print(f"\n  RESULT: all added streams are FWA tributaries = {result['all_tributaries']}")


if __name__ == "__main__":
    main()
