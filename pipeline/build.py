"""Build + validate the stream graph. Run with the project venv:

    .venv/bin/python -m pipeline.build --gnis "Adams River" --out output/v2/adams
    .venv/bin/python -m pipeline.build --bbox 1282000 476000 1305000 508000 --out output/v2/chehalis
    .venv/bin/python -m pipeline.build --full --out output/v2/full   # whole province (heavy)

Produces (under --out): blk_chains.pkl, graph.pkl, graph.gpkg (streams / confluences /
anchors layers for QGIS), and summary.txt.
"""

from __future__ import annotations

import argparse
import json
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


def _gnis_name_pairs(fwa: FWADataAccessor, layers, bbox=None) -> dict[str, tuple]:
    """wbk -> tuple of a waterbody's gazette ``(name, gnis_id)`` pairs across ``layers`` (GNIS_NAME/
    ID_1/2/3, non-null, positionally paired so NAME_i keeps its ID_i). GNIS_NAME_1/2 are accessor-
    normalized (null -> ""); GNIS_NAME_3/ID_3 are NOT in the prod STRING_COLUMNS (only 4 non-null
    province-wide) so may arrive as float NaN in a bbox subset -> cleaned locally. Only named
    waterbodies are returned. Shared by lakes/manmade (node names) and wetlands (registry names)."""
    cols = ["WATERBODY_KEY", "GNIS_NAME_1", "GNIS_NAME_2", "GNIS_NAME_3",
            "GNIS_ID_1", "GNIS_ID_2", "GNIS_ID_3"]

    def _clean(n) -> str:
        if n is None:
            return ""
        s = str(n).strip()
        return "" if s.lower() in ("", "nan", "none") else s

    def _cid(n) -> str:
        s = _clean(n)
        return str(int(float(s))) if s and s.replace(".", "").isdigit() else s

    # A wbk can span several polygon rows (e.g. Nechako Reservoir's reaches), each with
    # different GNIS names — UNION them so every gazette name of the waterbody is captured.
    acc: dict[str, list] = {}
    for layer in layers:
        if layer in fwa.layer_names:
            gdf = fwa.get_layer(layer, columns=cols, bbox=bbox)
            for row in gdf.itertuples():
                wbk = str(row.WATERBODY_KEY) if row.WATERBODY_KEY else ""
                if not wbk:
                    continue
                seen = acc.setdefault(wbk, [])
                for nm, gid in ((_clean(row.GNIS_NAME_1), _cid(row.GNIS_ID_1)),
                                (_clean(row.GNIS_NAME_2), _cid(row.GNIS_ID_2)),
                                (_clean(row.GNIS_NAME_3), _cid(row.GNIS_ID_3))):
                    if nm and (nm, gid) not in seen:
                        seen.append((nm, gid))
    return {w: tuple(v) for w, v in acc.items() if v}


def get_lake_names(fwa: FWADataAccessor, bbox=None) -> dict[str, tuple]:
    """wbk -> gazette ``(name, gnis_id)`` pairs for lakes/manmade (usually empty; ~96.7% of lakes are
    unnamed -> display falls back to a threading river name). The paired gnis id lets a lake node
    carry its gnis (like a stream) so gnis-keyed name variants / overrides resolve onto it."""
    return _gnis_name_pairs(fwa, ("lakes", "manmade"), bbox)


def get_wetland_names(fwa: FWADataAccessor, bbox=None) -> dict[str, tuple]:
    """wbk -> gazette ``(name, gnis_id)`` pairs for NAMED wetlands (marshes/ponds/sloughs). Wetlands
    are NOT graph nodes (streams overlay them), but named ones are regulated waterbodies a reg can
    target (e.g. Minnekhada Marsh, Jerry Sulina Park Pond) — so they enter the registry as
    ``kind='wetland'`` items keyed by wbk. The vast majority of the ~375k wetlands are unnamed and
    excluded here."""
    return _gnis_name_pairs(fwa, ("wetlands",), bbox)


def get_waterbody_polys(fwa: FWADataAccessor, wbks: set[str], bbox=None) -> dict:
    """wbk -> its (unioned) FWA polygon, for the given ``wbks`` across the lake/manmade/wetland layers.
    Isolated named lakes/reservoirs and wetlands never become graph nodes, so they carry no section
    geometry — this loads their OWN polygon so the registry can compute the MUs they intersect (a wbk
    can span several reaches; they're unioned). Only wbks in ``wbks`` are kept."""
    from shapely.ops import unary_union
    if not wbks:
        return {}
    parts: dict[str, list] = {}
    for layer in ("lakes", "manmade", "wetlands"):
        if layer not in fwa.layer_names:
            continue
        gdf = fwa.get_layer(layer, columns=["WATERBODY_KEY"], bbox=bbox)
        for row in gdf.itertuples():
            wbk = str(row.WATERBODY_KEY) if row.WATERBODY_KEY else ""
            if wbk in wbks and row.geometry is not None and not row.geometry.is_empty:
                parts.setdefault(wbk, []).append(row.geometry)
    return {w: (g[0] if len(g) == 1 else unary_union(g)) for w, g in parts.items()}


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


def summarize(chains, graph, fids, pruned_fids=None) -> str:
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
    # a DELIBERATELY pruned braid loop is not a missing fid — the check exists to catch fids that
    # silently failed to reach the graph, not ones we chose to drop.
    missing_fids = len({f.fid for f in fids} - covered - set(pruned_fids or ()))
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


_ADDED_STREAMS_JSON = Path(__file__).resolve().parent / "added_streams.build.json"


def _apply_fwa_exclude(fids: list, prefixes: list[str]) -> tuple[list, int]:
    """Drop FWA fids whose (already trimmed) WSC starts with any `fwa_exclude` prefix — the coarse FWA
    tributaries the municipal added-streams network supersedes. Returns (kept fids, dropped count)."""
    if not prefixes:
        return fids, 0
    kept = [f for f in fids if not any(f.wsc.startswith(p) for p in prefixes)]
    return kept, len(fids) - len(kept)


def _streams_in_bbox(streams: list[dict], bbox) -> list[dict]:
    """Keep added streams with a vertex inside ``bbox`` (EPSG:3005) PLUS their added-receiver ancestors, so a
    bbox-limited build doesn't add another region's streams as orphans, yet never severs an added->added chain
    at the bbox edge (a kept tributary always keeps the mainstem it drains into). ``bbox`` None keeps all."""
    if bbox is None:
        return streams
    minx, miny, maxx, maxy = bbox
    by_blk = {str(s["blk"]): s for s in streams}
    keep: dict[str, dict] = {}
    stack = [s for s in streams
             if any(minx <= x <= maxx and miny <= y <= maxy
                    for seg in s["segments"] for x, y in seg["coords3005"])]
    while stack:
        s = stack.pop()
        if str(s["blk"]) in keep:
            continue
        keep[str(s["blk"])] = s
        if s.get("receiver_kind") == "added":            # follow the mainstem it drains into
            r = by_blk.get(str(s["receiver_blk"]))
            if r is not None:
                stack.append(r)
    return list(keep.values())


def _added_name_variant_entries(variants: list[dict]) -> list[dict]:
    """Convert the artifact's flat name variants ({kind,name,target_blk,target_gnis}) to apply_name_variants
    entries (a municipal/FWA name aliased onto the blk it belongs to)."""
    out = []
    for v in variants:
        if not v.get("name"):
            continue
        out.append({"target": {"blk": str(v["target_blk"]), "gnis_id": str(v.get("target_gnis") or "")},
                    "names": [{"name": v["name"], "source": "alias", "note": f"added:{v.get('kind', '')}"}]})
    return out


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
    ap.add_argument("--no-border", action="store_true",
                    help="force-skip the border stage even under --full (the bc_outline WMU union + "
                         "cross-border split is slow; irrelevant to registry item names/MUs)")
    ap.add_argument("--name-variants", help="path to a compiled name_variants.json (docs/13)")
    ap.add_argument("--added-streams", default=None,
                    help="path to a frozen added_streams.build.json (default: the packaged one)")
    ap.add_argument("--no-added-streams", action="store_true",
                    help="skip merging the minted municipal added-streams dataset (on by default)")
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

    # Merge the frozen, vetted municipal added-streams dataset (on by default): remove the FWA blue lines it
    # supersedes (fwa_exclude), then add its synthetic fids so the SAME blk-chain / graph / geometry passes below
    # ingest them as first-class streams. Connectors (added stream -> receiver) and name variants are applied
    # after the graph is built. See pipeline/hack/added_streams.
    add_specs: list = []
    add_nv: list[dict] = []
    if not args.no_added_streams:
        from pipeline.hack.added_streams.build_dataset import to_graph_inputs
        asp = Path(args.added_streams) if args.added_streams else _ADDED_STREAMS_JSON
        data = json.loads(asp.read_text(encoding="utf-8"))
        add_streams = _streams_in_bbox(data["streams"], bbox)
        add_fids, add_specs = to_graph_inputs(add_streams)
        fids, n_excl = _apply_fwa_exclude(fids, data.get("fwa_exclude", []))
        fids += add_fids
        add_nv = data.get("name_variants", [])
        print(f"  added streams: -{n_excl} superseded FWA fids, +{len(add_fids)} added fids, "
              f"{len(add_specs)} connectors, {len(add_nv)} name variants")
    _tick("load fids + lakes")

    print("building blk chains + names ...")
    chains = resolve_names(build_blk_chains(fids, lake_kind))

    print("building stream graph (lakes as nodes) ...")
    graph = build_stream_graph(chains, fids, lake_kind, lake_names)
    print("building geometry sidecar ...")
    geoms = build_section_geometries(chains, fids, lake_kind)
    if add_specs:                                   # wire each added stream to its receiver at the confluence
        from pipeline.hack.added_streams.ingest import attach_connectors
        cr = attach_connectors(graph, geoms, add_specs)
        print(f"  added-stream connectors: {cr['added']} edges ({cr['skipped']} skipped)")
    _tick("blk-chains + graph + geometry")

    # Braid loops carry no tributary, no name and no possible regulation; they only make a reach
    # ambiguous ("is this channel above or below the cut" has no answer when it is attached at both
    # ends). Pruned BEFORE the border/split stages so nothing is ever cut onto a piece we then drop.
    from pipeline.graph.prune import prune_mainstem_loops
    print("pruning pure braid loops off mainstems ...")
    graph, n_pruned, pruned_fids = prune_mainstem_loops(graph, geoms)
    print(f"  {n_pruned} loop piece(s) removed -> {len(graph.nodes)} nodes")
    _tick("prune braid loops")

    fid_index = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}

    # Border pass FIRST (like lakes, but via splits) so curated points can pick up border splits.
    if (args.border or args.full) and not args.no_border:
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
        aliased_splits: list = []
        split_graph_at(graph, geoms, other_pts, fid_index, proximity_pickup=True,
                       applied=applied_splits, aliased=aliased_splits)
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
        if aliased_splits:
            # Could not be cut because the measure sits inside a LAKE RUN — a dam or weir at the
            # outlet, whose coordinate projects a little way into the water. Recorded as another name
            # for that lake's boundary, so the binding still resolves. The distance from the lake edge
            # is printed: a large one means the authored point is not really at the outlet.
            print(f"  {len(aliased_splits)} split(s) landed in a lake and were aliased onto it:")
            for sid, onto, dist in aliased_splits[:20]:
                print(f"      {sid}  ->  {onto}  ({dist:.0f} m from the lake edge)")
            if len(aliased_splits) > 20:
                print(f"      … and {len(aliased_splits) - 20} more")
        _tick("curated splits")

    # Blanket area closures (areas.json) — cut ALL streams crossing each admin polygon
    # (national parks, ecological reserves, the Chilkoot trail) at first-enter/last-exit + flag
    # inside reaches. Runs BEFORE name variants (so cut pieces get named); a no-op where the bbox
    # hits no such area.
    from pipeline.splits.area_splits import load_area_split_defs, load_area_polys, resolve_area_splits
    area_defs = load_area_split_defs()
    catalog_polys: dict[str, dict] = {}                # {area_def id: {name: polygon}} for the lazy catalog
    if area_defs:
        from pipeline.splits.sectionizer import split_graph_at
        for ad in area_defs:
            polys = load_area_polys(fwa, ad, bbox=bbox)
            if not polys:
                continue
            catalog_polys[ad["id"]] = polys
            if ad.get("cut", True):                    # `cut` flag is the SOLE cut trigger (default on)
                apts = resolve_area_splits(polys, chains)
                split_graph_at(graph, geoms, apts, fid_index, proximity_pickup=False, applied=applied_splits)
                print(f"  area '{ad['id']}': {len(polys)} polygon(s), {len(apts)} transition cut(s)")
            else:
                print(f"  area '{ad['id']}': {len(polys)} polygon(s), membership-only (no cut)")
        # LAZY membership (DECISION 2026-08-16): NO eager `mark_inside_areas` pass. Membership
        # (intersects + feature_types) is computed at RESOLVE time for the few areas a reg references.
        # Cutting above stays at build (geometry). The catalog (polygons only) is written below.
        _tick("blanket area splits (cut only)")

    # Attach compiled name variations (docs/13) to nodes — AFTER splits so reach targets hit pieces.
    from pipeline.graph.names import apply_name_variants, load_name_variants
    nv_path = args.name_variants or (Path(__file__).resolve().parent / "name_variants.json")
    nv = load_name_variants(nv_path) + _added_name_variant_entries(add_nv)   # + municipal aliases from the artifact
    if nv:
        n = apply_name_variants(graph, nv)
        print(f"  applied {len(nv)} name-variant entries -> {n} node attachments")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(graph, str(out / "graph.pkl"))
    write_artifact(geoms, str(out / "geometries.pkl"))

    # Registry (parser truth) — build from the finalized graph + persist, so the parser tools
    # (matcher / batch_exporter / ingest) never need to rebuild the graph from the ~10GB FWA data.
    from pipeline.registry import add_curated_wbk_items, add_mu_sets, add_waterbody_items, build_registry
    from pipeline.registry import write_registry
    registry = build_registry(graph)
    print(f"  registry: {len(registry)} named items")
    # Named waterbodies the graph alone misses: isolated named lakes/reservoirs with no through-stream
    # (never noded, e.g. Frazer Lake) and wetlands (never noded). Add them from the FWA layers so a reg
    # can target them by name or a curated wbk/gnis pin.
    n0 = len(registry)
    registry = add_waterbody_items(registry, lake_names, "lake")
    n_lakes = len(registry) - n0
    registry = add_waterbody_items(registry, get_wetland_names(fwa, bbox), "wetland")
    print(f"  + {n_lakes} isolated named lake item(s) + {len(registry) - n0 - n_lakes} named wetland item(s)")
    # Waterbodies named ONLY by curation (name_variants wbk target on an isolated FWA-unnamed lake) —
    # add them so the curated name is matchable (e.g. Redstart Lake's 2nd polygon).
    n1 = len(registry)
    registry = add_curated_wbk_items(registry, nv)
    print(f"  + {len(registry) - n1} curated-only wbk item(s) (named via name_variants)")
    _tick("build_registry")
    # Isolated lakes/wetlands are minted from FWA layers with no graph geometry; load their own wbk
    # polygons so add_mu_sets can compute the MUs they intersect (every named item gets real MUs).
    nogeom_wbks = {iid.split(":", 1)[1] for iid, it in registry.items()
                   if it.kind in ("lake", "wetland") and not it.section_ids and iid.startswith("wbk:")}
    wbk_polys = get_waterbody_polys(fwa, nogeom_wbks, bbox)
    print(f"  loaded {len(wbk_polys)} isolated waterbody polygon(s) for MU calc ({len(nogeom_wbks)} needed)")
    registry = add_mu_sets(registry, geoms, get_mu_polys(fwa), wbk_polys)
    _tick("add_mu_sets")
    write_registry(registry, out / "registry.json")
    print(f"  registry -> {out / 'registry.json'}")
    # Lazy area catalog (polygons only; membership computed at resolve time) — see DECISION 2026-08-16.
    if catalog_polys:
        from pipeline.splits.area_catalog import catalog_entries, write_area_catalog
        cat = catalog_entries(area_defs, catalog_polys)
        write_area_catalog(cat, out / "area_catalog.gpkg")
        print(f"  area catalog: {len(cat)} referenceable area(s) -> {out / 'area_catalog.gpkg'}")
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

    summary = summarize(chains, graph, fids, pruned_fids) + "\n\n" + timing_str
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + graph.gpkg to {out}/")


if __name__ == "__main__":
    main()
