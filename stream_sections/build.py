"""Build + validate the section graph. Run with the project venv:

    .venv/bin/python -m stream_sections.build --gnis "Adams River" --out output/v2/adams
    .venv/bin/python -m stream_sections.build --bbox 1282000 476000 1305000 508000 --out output/v2/chehalis
    .venv/bin/python -m stream_sections.build --full --out output/v2/full   # whole province (heavy)

Produces (under --out): blk_chains.pkl, topology.pkl, segments.geojson, nodes.geojson, and a
summary.txt. GeoJSON (WGS84) can be dropped into geojson.io / QGIS for manual validation.
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from data.data_extractor import FWADataAccessor

from .blk_chains import build_blk_chains, load_stream_fids
from .export_gpkg import export_graph_gpkg
from .names import resolve_names
from .serialize import export_nodes_geojson, export_segments_geojson, write_artifact
from .topology import build_topology

_DEFAULT_GPKG = "data/bc_fisheries_data.gpkg"


def get_lake_wbk_kind(fwa: FWADataAccessor, bbox=None) -> dict[str, str]:
    kind: dict[str, str] = {}
    for layer, k in (("lakes", "lake"), ("manmade", "manmade")):
        if layer in fwa.layer_names:
            gdf = fwa.get_layer(layer, columns=["WATERBODY_KEY"], bbox=bbox)
            for wbk in gdf["WATERBODY_KEY"]:
                if wbk:
                    kind.setdefault(str(wbk), k)
    return kind


def bbox_from_gnis(fwa: FWADataAccessor, names: list[str], pad: float = 3000.0):
    gdf = fwa.get_features_by_attribute("streams", "GNIS_NAME", names)
    if gdf.empty:
        raise SystemExit(f"No streams matched GNIS_NAME in {names}")
    minx, miny, maxx, maxy = gdf.total_bounds
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def summarize(chains, topo, fids, lake_kind) -> str:
    node_kinds = Counter(n.kind.value for n in topo.nodes.values())
    segs_per_blk = Counter()
    for s in topo.segments.values():
        segs_per_blk[s.blk] += 1
    dist = Counter(segs_per_blk.values())
    named = sum(1 for c in chains if c.name_tuples)

    # Self-validating coverage: every open-channel fid must land in exactly one segment.
    open_fids = {f.fid for f in fids if f.wbk not in lake_kind}
    member: Counter = Counter()
    for s in topo.segments.values():
        member.update(s.member_fids)
    member_set = set(member)
    missing = len(open_fids - member_set)
    dup = sum(1 for _, n in member.items() if n > 1)
    extra = len(member_set - open_fids)
    coverage_ok = (missing == 0 and dup == 0 and extra == 0)

    lines = [
        f"chains (BLKs):            {len(chains)}",
        f"  with a name:            {named}",
        f"segments (topology edges):{len(topo.segments)}",
        f"nodes:                    {len(topo.nodes)}",
        f"  by kind:                {dict(node_kinds)}",
        f"segments-per-BLK dist:    {dict(sorted(dist.items()))}",
        "",
        f"COVERAGE CHECK: open_fids={len(open_fids)} in_segments={len(member_set)} "
        f"missing={missing} duplicated={dup} extra={extra} -> {'OK' if coverage_ok else 'FAIL'}",
        "",
        "sample named chains (blk -> name_tuples):",
    ]
    for c in [c for c in chains if c.name_tuples][:15]:
        lines.append(f"  {c.blk}  wsc={c.fwa_watershed_code}  "
                     + "; ".join(f"{t.name}[{t.source.value}]" for t in c.name_tuples))
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", default=_DEFAULT_GPKG)
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("MINX", "MINY", "MAXX", "MAXY"))
    ap.add_argument("--gnis", help="comma-separated GNIS_NAME(s); bbox derived from them")
    ap.add_argument("--full", action="store_true", help="whole province (no bbox; heavy)")
    ap.add_argument("--out", default="output/v2/validate")
    args = ap.parse_args()

    fwa = FWADataAccessor(args.gpkg)
    if args.full:
        bbox = None
    elif args.gnis:
        bbox = bbox_from_gnis(fwa, [n.strip() for n in args.gnis.split(",")])
        print(f"bbox from GNIS {args.gnis!r}: {bbox}")
    elif args.bbox:
        bbox = tuple(args.bbox)
    else:
        raise SystemExit("provide --gnis, --bbox, or --full")

    print("loading lake/manmade waterbody keys ...")
    lake_kind = get_lake_wbk_kind(fwa, bbox)
    print(f"  {len(lake_kind)} lake/manmade wbks")

    print("loading stream fids ...")
    fids = load_stream_fids(args.gpkg, bbox=bbox)
    print(f"  {len(fids)} fids")

    print("building blk chains + names ...")
    chains = resolve_names(build_blk_chains(fids, lake_kind))

    print("building topology ...")
    topo = build_topology(fids, lake_kind)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(topo, str(out / "topology.pkl"))
    export_segments_geojson(topo, str(out / "segments.geojson"))
    export_nodes_geojson(topo, str(out / "nodes.geojson"))
    export_graph_gpkg(chains, topo, str(out / "graph.gpkg"))   # temporary QGIS inspection layer

    summary = summarize(chains, topo, fids, lake_kind)
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + geojson to {out}/")


if __name__ == "__main__":
    main()
