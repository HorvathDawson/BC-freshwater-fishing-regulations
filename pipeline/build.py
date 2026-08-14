"""Build + validate the stream graph. Run with the project venv:

    .venv/bin/python -m pipeline.build --gnis "Adams River" --out output/v2/adams
    .venv/bin/python -m pipeline.build --bbox 1282000 476000 1305000 508000 --out output/v2/chehalis
    .venv/bin/python -m pipeline.build --full --out output/v2/full   # whole province (heavy)

Produces (under --out): blk_chains.pkl, graph.pkl, graph.gpkg (streams / confluences /
anchors layers for QGIS), and summary.txt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from data.data_extractor import FWADataAccessor

from pipeline.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.io.export_gpkg import export_graph_gpkg, export_lake_io, export_tributaries
from pipeline.graph.graph import build_section_geometries, build_stream_graph
from pipeline.graph.names import resolve_names
from pipeline.io.serialize import write_artifact
from pipeline.splits.splits import load_split_defs

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


def get_area_polys(fwa: FWADataAccessor, area_specs, bbox=None) -> dict:
    """Load the admin/park polygon for each area_boundary split: ``area_name -> (Multi)Polygon``.
    ``area_specs`` = iterable of (layer, name_field, name_value); one ``unary_union`` per matched
    name. Loaded within ``bbox`` (the build extent) — a park larger than the bbox is returned with
    full geometry for the features that intersect it, which is all the in-frame streams can cross."""
    from shapely.ops import unary_union
    out: dict = {}
    for layer, field, value in area_specs:
        if layer not in fwa.layer_names or value in out:
            continue
        gdf = fwa.get_layer(layer, columns=[field], bbox=bbox)
        polys = [g for v, g in zip(gdf[field], gdf.geometry)
                 if str(v) == value and g is not None and not g.is_empty]
        if polys:
            out[value] = unary_union(polys)
    return out


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
    from pipeline.models import NodeKind
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
    ap.add_argument("--border", action="store_true",
                    help="split cross-border BLKs at the BC outline + flag out-of-BC pieces "
                         "(auto-on with --full; off for small inland bboxes to stay fast)")
    ap.add_argument("--name-variants", help="path to a compiled name_variants.json (docs/13)")
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

    import time as _time
    _clock = _time.perf_counter
    _t0 = _prev = _clock()
    timings: list[tuple[str, float]] = []

    def _tick(label: str) -> None:
        nonlocal _prev
        now = _clock()
        timings.append((label, now - _prev))
        _prev = now
        print(f"    [{label}: {timings[-1][1]:.1f}s]")

    print("loading lake/manmade waterbody keys ...")
    lake_kind = get_lake_wbk_kind(fwa, bbox)
    lake_names = get_lake_names(fwa, bbox)
    print(f"  {len(lake_kind)} lake/manmade wbks ({len(lake_names)} named)")

    print("loading stream fids ...")
    fids = load_stream_fids(args.gpkg, bbox=bbox)
    print(f"  {len(fids)} fids")
    _tick("load fids + lakes")

    print("building blk chains + names ...")
    chains = resolve_names(build_blk_chains(fids, lake_kind))

    print("building stream graph (lakes as nodes) ...")
    graph = build_stream_graph(chains, fids, lake_kind, lake_names)
    print("building geometry sidecar ...")
    geoms = build_section_geometries(chains, fids, lake_kind)
    _tick("blk-chains + graph + geometry")

    fid_index = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}

    # Border pass FIRST (like lakes, but via splits) so curated points can pick up border splits.
    if args.border or args.full:
        from pipeline.splits.border import apply_border
        print("applying BC border splits (cross-border BLKs) ...")
        n_bsplits, n_flagged = apply_border(fwa, graph, geoms, chains, fid_index)
        print(f"  {n_bsplits} border split(s); {n_flagged} out-of-BC piece(s) flagged "
              f"-> {len(graph.nodes)} nodes")
        _tick("border")

    splits = load_split_defs(args.splits) if args.splits else None
    applied_splits: list = []
    if splits:
        from pipeline.splits.anchors import resolve_split_defs
        from pipeline.models import AnchorType
        from pipeline.splits.sectionizer import split_graph_at
        mu_polys = (get_mu_polys(fwa) if any(s.anchor.type == AnchorType.mu_boundary for s in splits)
                    else None)
        area_specs = [(s.anchor.area_layer, s.anchor.area_name_field, s.anchor.area_name)
                      for s in splits if s.anchor.type == AnchorType.area_boundary]
        area_polys = get_area_polys(fwa, area_specs, bbox=bbox) if area_specs else None
        pts = resolve_split_defs(splits, chains, mu_polys=mu_polys, area_polys=area_polys)
        # Area cuts are the PRIMARY boundary of a closure (never a relabel of an existing one), so
        # they run with proximity-pickup OFF and separately from the pickup-enabled point/line/lake
        # cuts. Everything still runs BEFORE any tributary walk so each piece is a first-class node.
        area_pts = [p for p in pts if p.anchor_type == AnchorType.area_boundary]
        other_pts = [p for p in pts if p.anchor_type != AnchorType.area_boundary]
        split_graph_at(graph, geoms, other_pts, fid_index, proximity_pickup=True, applied=applied_splits)
        if area_pts:
            from pipeline.splits.anchors import _target_blks
            from pipeline.splits.border import mark_inside_area
            split_graph_at(graph, geoms, area_pts, fid_index, proximity_pickup=False,
                           applied=applied_splits)
            for s in splits:
                poly = (area_polys or {}).get(s.anchor.area_name) if s.anchor.type == AnchorType.area_boundary else None
                if poly is not None:
                    blks = set(_target_blks(s, chains, descendants=s.anchor.wsc_descendants))
                    marked = mark_inside_area(graph, geoms, poly, s.label, blks=blks)
                    print(f"  area '{s.label}': cut {sum(1 for p in area_pts if p.split_id == s.id)} "
                          f"crossing(s), flagged {marked} inside piece(s)")
        n_pick = sum(1 for s in applied_splits if s.picked_up)
        print(f"  resolved {len(pts)} curated split point(s) ({n_pick} picked up existing "
              f"boundaries) -> {len(graph.nodes)} nodes")
        _tick("curated splits")

    # Attach compiled name variations (docs/13) to nodes — AFTER splits so reach targets hit pieces.
    from pipeline.graph.names import apply_name_variants, load_name_variants
    nv_path = args.name_variants or (Path(__file__).resolve().parent / "name_variants.json")
    nv = load_name_variants(nv_path)
    if nv:
        n = apply_name_variants(graph, nv)
        print(f"  applied {len(nv)} name-variant entries -> {n} node attachments")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(graph, str(out / "graph.pkl"))
    write_artifact(geoms, str(out / "geometries.pkl"))
    if applied_splits:
        from pipeline.splits.splits import write_resolved
        write_resolved(applied_splits, str(out / "splits.resolved.json"))
    obstacles = None
    if "obstacles" in fwa.layer_names:
        obstacles = fwa.get_layer(
            "obstacles", bbox=bbox,
            columns=["FISH_OBSTACLE_POINT_ID", "OBSTACLE_NAME", "GAZETTED_NAME",
                     "WATERSHED_CODE_50K", "HEIGHT", "geometry"])
        print(f"  loaded {len(obstacles)} fish-passage obstacle(s) for the obstacles layer")
    gpkg_path = str(out / "graph.gpkg")
    export_graph_gpkg(graph, geoms, gpkg_path, splits=splits, split_points=applied_splits,
                      obstacles=obstacles, area_polys=area_polys if splits else None)
    _tick("write artifacts + gpkg")

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

    timings.append(("TOTAL", _clock() - _t0))
    timing_str = "timings:\n" + "\n".join(f"  {label:32} {secs:8.1f}s" for label, secs in timings)

    summary = summarize(chains, graph, fids) + "\n\n" + timing_str
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + graph.gpkg to {out}/")


if __name__ == "__main__":
    main()
