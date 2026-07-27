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
from .export_gpkg import export_graph_gpkg, export_lake_io, export_tributaries
from .graph import build_section_geometries, build_stream_graph
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


def get_lake_names(fwa: FWADataAccessor, bbox=None) -> dict[str, tuple]:
    """wbk -> tuple of the lake's gazette names (GNIS_NAME_1/2/3, non-null). Usually empty
    (~96.7% of lakes are unnamed -> display falls back to a threading river name). GNIS_NAME_1/2
    are accessor-normalized (null -> ""); GNIS_NAME_3 is NOT in the prod STRING_COLUMNS (only 4
    non-null province-wide), so it may arrive as float NaN in a bbox subset -> cleaned locally."""
    cols = ["WATERBODY_KEY", "GNIS_NAME_1", "GNIS_NAME_2", "GNIS_NAME_3"]

    def _clean(n) -> str:
        if n is None:
            return ""
        s = str(n).strip()
        return "" if s.lower() in ("", "nan", "none") else s

    # A wbk can span several polygon rows (e.g. Nechako Reservoir's reaches), each with
    # different GNIS names — UNION them so every gazette name of the waterbody is captured.
    acc: dict[str, list] = {}
    for layer in ("lakes", "manmade"):
        if layer in fwa.layer_names:
            gdf = fwa.get_layer(layer, columns=cols, bbox=bbox)
            for row in gdf.itertuples():
                wbk = str(row.WATERBODY_KEY) if row.WATERBODY_KEY else ""
                if not wbk:
                    continue
                seen = acc.setdefault(wbk, [])
                for v in (_clean(row.GNIS_NAME_1), _clean(row.GNIS_NAME_2), _clean(row.GNIS_NAME_3)):
                    if v and v not in seen:
                        seen.append(v)
    return {w: tuple(v) for w, v in acc.items() if v}


def get_mu_polys(fwa: FWADataAccessor) -> dict:
    """WILDLIFE_MGMT_UNIT_ID -> full MU polygon (the fishing region-MU scheme reuses these).
    Loaded whole (only ~225 rows) so mu_boundary anchors get unclipped polygons for adjacency."""
    if "wmu" not in fwa.layer_names:
        return {}
    gdf = fwa.get_layer("wmu", columns=["WILDLIFE_MGMT_UNIT_ID"])
    return {str(r.WILDLIFE_MGMT_UNIT_ID): r.geometry for r in gdf.itertuples()
            if r.WILDLIFE_MGMT_UNIT_ID and r.geometry is not None}


def bbox_from_gnis(fwa: FWADataAccessor, names: list[str], pad: float = 3000.0):
    gdf = fwa.get_features_by_attribute("streams", "GNIS_NAME", names)
    if gdf.empty:
        raise SystemExit(f"No streams matched GNIS_NAME in {names}")
    minx, miny, maxx, maxy = gdf.total_bounds
    return (minx - pad, miny - pad, maxx + pad, maxy + pad)


def resolve_node(graph, key: str):
    """Resolve a --tributaries-of key to a node_id: exact blk, then exact name, then substring.

    On a name match, prefer the mainstem (largest magnitude, then longest) so
    ``--tributaries-of "Adams River"`` lands on the whole river, not a short same-named reach.
    """
    if key in graph.nodes:
        return key
    lk = key.lower()
    exact = [n for n in graph.nodes.values() if (n.display_name or "").lower() == lk]
    partial = [n for n in graph.nodes.values() if lk in (n.display_name or "").lower()]
    cands = exact or partial
    if not cands:
        return None
    return max(cands, key=lambda n: (n.stream_magnitude or 0, n.length_m)).node_id


def summarize(chains, graph, fids) -> str:
    from .models import NodeKind
    streams = [n for n in graph.nodes.values() if n.kind == NodeKind.stream]
    lakes = [n for n in graph.nodes.values() if n.kind == NodeKind.lake]
    named = sum(1 for n in graph.nodes.values() if n.name_tuples)
    roots = [nid for nid in graph.nodes if not graph.down_adj.get(nid)]
    in_deg = {nid: len(graph.up_adj.get(nid, [])) for nid in graph.nodes}
    with_tribs = sum(1 for v in in_deg.values() if v)
    top = sorted(graph.nodes.values(), key=lambda n: in_deg[n.node_id], reverse=True)[:5]

    # Sanity: every edge references existing nodes; every fid maps to some node.
    bad_edge = sum(1 for e in graph.edges if e.from_node not in graph.nodes or e.to_node not in graph.nodes)
    covered = set()
    for n in graph.nodes.values():
        covered.update(n.member_fids)
    missing_fids = len({f.fid for f in fids} - covered)
    multi_outlet = sum(1 for n in lakes if len(graph.down_adj.get(n.node_id, [])) > 1)

    lines = [
        f"nodes: {len(graph.nodes)}   stream pieces={len(streams)}  lakes={len(lakes)}",
        f"  with a name:            {named}",
        f"flow edges:               {len(graph.edges)}",
        f"  nodes with tributaries: {with_tribs}",
        f"  roots (drain out/clip): {len(roots)}",
        f"  multi-outlet lakes:     {multi_outlet}",
        "",
        f"INTEGRITY: edges w/ missing node={bad_edge}  fids w/o a node={missing_fids} "
        f"-> {'OK' if bad_edge == 0 and missing_fids == 0 else 'FAIL'}",
        "",
        "biggest confluences (node -> #tributaries in):",
    ]
    for n in top:
        lines.append(f"  {in_deg[n.node_id]:4d}  {n.node_id}  {n.display_name or '(unnamed)'}")
    lines.append("")
    lines.append("sample lake nodes (wbk -> name [through rivers]):")
    for n in lakes[:8]:
        lines.append(f"  {n.wbk}  {n.display_name or '(unnamed)'}  through={list(n.through_names)[:3]}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", default=_DEFAULT_GPKG)
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("MINX", "MINY", "MAXX", "MAXY"))
    ap.add_argument("--gnis", help="comma-separated GNIS_NAME(s); bbox derived from them")
    ap.add_argument("--full", action="store_true", help="whole province (no bbox; heavy)")
    ap.add_argument("--splits", help="path to a splits.json to overlay as an anchors layer")
    ap.add_argument("--tributaries-of", metavar="NAME|BLK",
                    help="export the upstream tributary walk of this node as a 'tributaries' layer")
    ap.add_argument("--lakes", action="store_true",
                    help="export lake inlet/outlet points as a 'lake_io' layer")
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
    lake_names = get_lake_names(fwa, bbox)
    print(f"  {len(lake_kind)} lake/manmade wbks ({len(lake_names)} named)")

    print("loading stream fids ...")
    fids = load_stream_fids(args.gpkg, bbox=bbox)
    print(f"  {len(fids)} fids")

    print("building blk chains + names ...")
    chains = resolve_names(build_blk_chains(fids, lake_kind))

    print("building stream graph (lakes as nodes) ...")
    graph = build_stream_graph(chains, fids, lake_kind, lake_names)
    print("building geometry sidecar ...")
    geoms = build_section_geometries(chains, fids, lake_kind)

    splits = load_split_defs(args.splits) if args.splits else None
    if splits:
        from .anchors import resolve_split_defs
        from .models import AnchorType
        from .sectionizer import split_graph_at
        mu_polys = (get_mu_polys(fwa) if any(s.anchor.type == AnchorType.mu_boundary for s in splits)
                    else None)
        pts = resolve_split_defs(splits, chains, mu_polys=mu_polys)
        fid_index = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}
        split_graph_at(graph, geoms, pts, fid_index)   # curated sections BEFORE any tributary walk
        print(f"  resolved {len(pts)} curated split point(s) -> {len(graph.nodes)} nodes")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(graph, str(out / "graph.pkl"))
    write_artifact(geoms, str(out / "geometries.pkl"))
    gpkg_path = str(out / "graph.gpkg")
    export_graph_gpkg(graph, geoms, gpkg_path, splits=splits)

    if args.tributaries_of:
        nid = resolve_node(graph, args.tributaries_of)
        if nid is None:
            print(f"  tributaries: no node matched {args.tributaries_of!r}")
        else:
            n = export_tributaries(graph, geoms, nid, gpkg_path)
            tgt = graph.nodes[nid]
            print(f"  tributaries of {tgt.display_name or nid} ({nid}): "
                  f"{n - 1} tributary nodes -> 'tributaries' layer")

    if args.lakes:
        n = export_lake_io(graph, geoms, gpkg_path)
        print(f"  lake inlet/outlet edges: {n} -> 'lake_io' layer")

    summary = summarize(chains, graph, fids)
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + graph.gpkg to {out}/")


if __name__ == "__main__":
    main()
