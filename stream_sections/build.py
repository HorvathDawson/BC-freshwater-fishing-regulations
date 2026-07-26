"""Build + validate the stream graph. Run with the project venv:

    .venv/bin/python -m stream_sections.build --gnis "Adams River" --out output/v2/adams
    .venv/bin/python -m stream_sections.build --bbox 1282000 476000 1305000 508000 --out output/v2/chehalis
    .venv/bin/python -m stream_sections.build --full --out output/v2/full   # whole province (heavy)

Produces (under --out): blk_chains.pkl, graph.pkl, graph.gpkg (streams / confluences /
anchors layers for QGIS), and summary.txt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from data.data_extractor import FWADataAccessor

from .blk_chains import build_blk_chains, load_stream_fids
from .export_gpkg import export_graph_gpkg
from .graph import build_stream_graph
from .names import resolve_names
from .serialize import write_artifact
from .splits import load_split_defs

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


def summarize(chains, graph, fids) -> str:
    named = sum(1 for n in graph.nodes.values() if n.name_tuples)
    roots = [nid for nid in graph.nodes if not graph.down_adj.get(nid)]
    in_deg = {nid: len(graph.up_adj.get(nid, [])) for nid in graph.nodes}
    with_tribs = sum(1 for v in in_deg.values() if v)
    top = sorted(graph.nodes.values(), key=lambda n: in_deg[n.node_id], reverse=True)[:5]

    # Sanity: every edge references existing nodes; every fid's BLK is a node.
    bad_edge = sum(1 for e in graph.edges if e.from_node not in graph.nodes or e.to_node not in graph.nodes)
    fid_blks = {f.blk for f in fids}
    missing_blk_nodes = len(fid_blks - set(graph.nodes) - {""})

    lines = [
        f"stream nodes (BLKs):      {len(graph.nodes)}",
        f"  with a name:            {named}",
        f"flow edges (confluences): {len(graph.edges)}",
        f"  nodes with tributaries: {with_tribs}",
        f"  roots (drain out/clip): {len(roots)}",
        "",
        f"INTEGRITY: edges w/ missing node={bad_edge}  fid-BLKs w/o a node={missing_blk_nodes} "
        f"-> {'OK' if bad_edge == 0 and missing_blk_nodes == 0 else 'FAIL'}",
        "",
        "biggest confluences (node -> #tributaries in):",
    ]
    for n in top:
        lines.append(f"  {in_deg[n.node_id]:4d}  {n.blk}  {n.display_name or '(unnamed)'}")
    lines.append("")
    lines.append("sample named streams (blk -> name_tuples):")
    for n in [n for n in graph.nodes.values() if n.name_tuples][:15]:
        lines.append(f"  {n.blk}  wsc={n.wsc}  "
                     + "; ".join(f"{t.name}[{t.source.value}]" for t in n.name_tuples))
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", default=_DEFAULT_GPKG)
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("MINX", "MINY", "MAXX", "MAXY"))
    ap.add_argument("--gnis", help="comma-separated GNIS_NAME(s); bbox derived from them")
    ap.add_argument("--full", action="store_true", help="whole province (no bbox; heavy)")
    ap.add_argument("--splits", help="path to a splits.json to overlay as an anchors layer")
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

    print("building stream graph ...")
    graph = build_stream_graph(chains, fids)

    splits = load_split_defs(args.splits) if args.splits else None

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(graph, str(out / "graph.pkl"))
    export_graph_gpkg(graph, str(out / "graph.gpkg"), splits=splits)

    summary = summarize(chains, graph, fids)
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + graph.gpkg to {out}/")


if __name__ == "__main__":
    main()
