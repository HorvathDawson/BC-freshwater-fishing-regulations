"""Build + validate the stream graph. Run with the project venv:

    .venv/bin/python -m pipeline.atlas.build --gnis "Adams River" --out data/generated/atlas/adams
    .venv/bin/python -m pipeline.atlas.build --bbox 1282000 476000 1305000 508000 --out data/generated/atlas/chehalis
    .venv/bin/python -m pipeline.atlas.build --full --out data/generated/atlas/full   # whole province (heavy)

Produces (under --out): blk_chains.pkl, graph.pkl, graph.gpkg (streams / confluences /
anchors layers for QGIS), and summary.txt.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from project_config import get_config
from pipeline.atlas.fwa import FWADataAccessor

from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
from pipeline.common.io.export_gpkg import export_graph_gpkg, export_lake_io, export_tributaries
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.atlas.graph.names import resolve_names
from pipeline.common.io.serialize import write_artifact
from pipeline.atlas.splits.splits import load_split_defs
from pipeline.common.curated import CURATED, GENERATED, SOURCE

_DEFAULT_GPKG = str(get_config().fwa_data_gpkg)


def get_wetland_wbks(fwa: FWADataAccessor, bbox=None) -> set[str]:
    """Every wetland WATERBODY_KEY, named or not. `get_wetland_names` returns only the GAZETTED ones,
    but curation names wetlands the gazetteer does not: Cheam Lake and Minnekhada Marsh are wetland
    polygons with all three GNIS_NAME fields null, named only by a `name_variants` entry. Minting was
    gated on the lake/manmade key set, so those never became nodes and their registry items resolved
    to nothing — the exact failure minting exists to prevent."""
    out: set[str] = set()
    if "wetlands" in fwa.layer_names:
        # ATTRIBUTES ONLY. `columns=` narrows the attributes and says nothing about the
        # shapes, so this used to deserialise all 375,178 wetland polygons to read one key.
        gdf = fwa.get_layer("wetlands", columns=["WATERBODY_KEY"], bbox=bbox, geometry=False)
        out = {str(w) for w in gdf["WATERBODY_KEY"] if w}
    return out


def get_all_waterbody_wbks(fwa: FWADataAccessor, bbox=None) -> set[str]:
    """Every WATERBODY_KEY in the province, across all three waterbody layers.

    Named or not, noded or not. A waterbody becomes a node when a stream is routed through it,
    and a wetland only when something names it — so 417,111 of them have no node, and a node is
    the only thing that carries `mus`. A zone regulation targets water by where it is, so those
    417,111 would be invisible to every zone rule. This is the set that has to be minted.
    """
    out: set[str] = set()
    for layer in ("lakes", "manmade", "wetlands"):
        if layer not in fwa.layer_names:
            continue
        gdf = fwa.get_layer(layer, columns=["WATERBODY_KEY"], bbox=bbox, geometry=False)
        out |= {str(w) for w in gdf["WATERBODY_KEY"] if w}
    return out


def get_lake_wbk_kind(fwa: FWADataAccessor, bbox=None) -> dict[str, str]:
    kind: dict[str, str] = {}
    for layer, k in (("lakes", "lake"), ("manmade", "manmade")):
        if layer in fwa.layer_names:
            gdf = fwa.get_layer(layer, columns=["WATERBODY_KEY"], bbox=bbox, geometry=False)
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
            gdf = fwa.get_layer(layer, columns=cols, bbox=bbox, geometry=False)
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
    from pipeline.common.models import WATERBODY_KINDS, NodeKind
    streams = [n for n in graph.nodes.values() if n.kind == NodeKind.stream]
    lakes = [n for n in graph.nodes.values() if n.kind in WATERBODY_KINDS]
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


_ADDED_STREAMS_JSON = CURATED.waters.added_streams
_ADDED_LAKES_GEOJSON = CURATED.waters.added_lakes
_DEFAULT_SPLITS = CURATED.waters.splits


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
    # ON by default: an unnamed anabranch is not a water any regulation names, and leaving the loops
    # in is what makes a big braided river a hairball of 330 channels all displaying as one name.
    # A NAMED channel and a dead-end channel are never removed either way.
    ap.add_argument("--no-simplify-braids", dest="simplify_braids", action="store_false",
                    help="keep braid loops that a TRIBUTARY flows into. By default they are removed "
                         "and the tributary's mouth is re-homed onto the loop's downstream exit, "
                         "trading ~16k anabranch pieces for ~10k approximate confluences.")
    ap.set_defaults(simplify_braids=True)
    ap.add_argument("--bbox", nargs=4, type=float, metavar=("MINX", "MINY", "MAXX", "MAXY"))
    ap.add_argument("--gnis", help="comma-separated GNIS_NAME(s); bbox derived from them")
    ap.add_argument("--full", action="store_true", help="whole province (no bbox; heavy)")
    ap.add_argument("--splits", help="path to a splits.json to overlay as an anchors layer "
                    f"(default: the checked-in {_DEFAULT_SPLITS.name})")
    ap.add_argument("--no-splits", action="store_true",
                    help="build with NO curated splits. Almost never what you want — every "
                         "'downstream of the bridge' regulation stops resolving.")
    ap.add_argument("--border", action="store_true",
                    help="split cross-border BLKs at the BC outline + flag out-of-BC pieces "
                         "(auto-on with --full; off for small inland bboxes to stay fast)")
    ap.add_argument("--no-border", action="store_true",
                    help="force-skip the border stage even under --full (the bc_outline WMU union + "
                         "cross-border split is slow; irrelevant to registry item names/MUs)")
    ap.add_argument("--name-variants", help="path to a compiled name_variants.json (docs/13)")
    ap.add_argument("--added-streams", default=None,
                    help="path to a frozen added_streams.build.json (default: the packaged one)")
    ap.add_argument("--write-gpkg", action="store_true",
                    help="also write graph.gpkg (4.7 GB, ~40%% of the build). OFF BY "
                         "DEFAULT: it is a CURATION artifact — QGIS, the dossier tool, the "
                         "review backend — and nothing downstream reads it. Pass it when "
                         "you are about to curate.")
    ap.add_argument("--no-gauge-splits", action="store_true",
                    help="do not section rivers at their hydrometric stations")
    ap.add_argument("--max-section-km", type=float, default=25.0, metavar="KM",
                    help="cap section length: any section longer than this is cut at its "
                         "interior tributary confluences until it fits (default 25). A cut "
                         "lands where the river's drainage actually changes, so each piece "
                         "gets its own catchment, donor panel and colour — see "
                         "pipeline/atlas/splits/length_splits.py for the measured cost of "
                         "each setting. 0 disables the stage.")
    ap.add_argument("--added-lakes", help="path to an added_lakes.geojson (default: the packaged one)")
    ap.add_argument("--no-added-lakes", action="store_true",
                    help="skip the curated non-FWA lake polygons (see pipeline/atlas/waters/added_lakes)")
    ap.add_argument("--no-added-streams", action="store_true",
                    help="skip merging the minted municipal added-streams dataset (on by default)")
    ap.add_argument("--tributaries-of", metavar="NAME|BLK",
                    help="export the upstream tributary walk of this node as a 'tributaries' layer")
    ap.add_argument("--lakes", action="store_true",
                    help="export lake inlet/outlet points as a 'lake_io' layer")
    ap.add_argument("--no-leaf-prune", action="store_true",
                    help="keep unnamed headwater capillaries. The leaf prune removes ~9%% of "
                         "stream vertices and is the switch to reach for first if water is "
                         "missing from the map; see pipeline/atlas/graph/leaf_prune.py.")
    ap.add_argument("--out", default=str(GENERATED.build("validate")))
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

    # Curated lake polygons the FWA waterbody layer is missing (Redsand Lake). MUST run before
    # build_blk_chains: the whole mechanism is re-stamping FidRow.wbk inside each polygon, and the
    # chain/graph passes below are already what read that field — a fid carrying a lake wbk is given
    # to the lake node and BREAKS the stream run there, which is what cuts the stream and mints the
    # `lake:{wbk}` boundary a regulation binds to. See pipeline/atlas/waters/added_lakes/README.md.
    added_lake_polys: dict = {}
    if not args.no_added_lakes:
        from pipeline.atlas.waters.added_lakes.ingest import merge as _merge_lakes
        _alp = Path(args.added_lakes) if args.added_lakes else _ADDED_LAKES_GEOJSON
        _rep = _merge_lakes(fids, lake_kind, lake_names, added_lake_polys, _alp)
        if _rep["lakes"]:
            print(f"  + {_rep['lakes']} curated lake polygon(s) from {_alp.name}: "
                  + ", ".join(f"{n!r} (wbk {w}, {len(_rep['claimed'].get(w, []))} fid(s) claimed)"
                              for w, n in _rep["names"].items()))

    # Merge the frozen, vetted municipal added-streams dataset (on by default): remove the FWA blue lines it
    # supersedes (fwa_exclude), then add its synthetic fids so the SAME blk-chain / graph / geometry passes below
    # ingest them as first-class streams. Connectors (added stream -> receiver) and name variants are applied
    # after the graph is built. See pipeline/atlas/waters/added_streams.
    add_specs: list = []
    add_nv: list[dict] = []
    if not args.no_added_streams:
        from pipeline.atlas.waters.added_streams.build_dataset import to_graph_inputs
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
        from pipeline.atlas.waters.added_streams.ingest import attach_connectors
        cr = attach_connectors(graph, geoms, add_specs)
        print(f"  added-stream connectors: {cr['added']} edges ({cr['skipped']} skipped)")
    _tick("blk-chains + graph + geometry")

    # Braid loops carry no tributary, no name and no possible regulation; they only make a reach
    # ambiguous ("is this channel above or below the cut" has no answer when it is attached at both
    # ends). Pruned BEFORE the border/split stages so nothing is ever cut onto a piece we then drop.
    # NAME FIRST, then prune. The prune decides what to keep by asking whether a channel carries a
    # name of its own, so it has to be asked AFTER the names exist. Running the variants late meant it
    # was asked too early: a curated channel still looked anonymous and was deleted before it could be
    # named — which is how all four blue lines of Seabird Island North Side Channel once vanished. The
    # fix for that was `protected_blks`, a list of blks the prune must not touch: a patch over the
    # ordering rather than the ordering.
    #
    # Only 7 of 3,663 variants actually need the splits (they name a REACH by measure and must land on
    # a section boundary). The other 3,656 name a whole blk, waterbody, gnis or watershed code and can
    # be applied the moment the graph exists. So the pass is split in two, and `protected_blks` goes.
    from pipeline.atlas.graph.names import apply_name_variants, load_name_variants
    _nv_path = args.name_variants or CURATED.waters.name_variants
    _nv_all = load_name_variants(_nv_path) + _added_name_variant_entries(add_nv)
    _nv_reach = [e for e in _nv_all if e.get("reach") or (e.get("target") or {}).get("reach")]
    _nv_now = [e for e in _nv_all if e not in _nv_reach]
    if _nv_now:
        n = apply_name_variants(graph, _nv_now)
        print(f"  named {len(_nv_now)} variant entries before the prune -> {n} node attachments")
    # A waterbody becomes a graph node because stream fids pass THROUGH it. Two kinds of named,
    # regulated water therefore never got one: ISOLATED waters with no stream connection at all
    # (Frazer Lake, Hall Road Pond, Kinglet Lake) and OVERLAID ones where a stream crosses the polygon
    # but the polygon is a wetland/marsh, so the fids record it in `member_wbks` and the waterbody
    # itself is never noded (Cheam Lake, Minnekhada Marsh). Both left their registry item with an empty
    # `section_ids`, so `op=whole` resolved against an empty universe and the rule silently bound
    # nothing — on waters people fish, several of them closures.
    #
    # Mint here, once the curated names are known (they are what makes an FWA-unnamed water matchable
    # at all). Gazetted lakes/manmade + gazetted wetlands + curation-only names, in that precedence:
    # a wbk already noded, or already minted by an earlier source, is skipped.
    from pipeline.atlas.graph.names import mint_waterbody_nodes
    from pipeline.common.models import NameSource
    wetland_names = get_wetland_names(fwa, bbox)
    # Curation-only names, split by which layer the polygon lives in so each gets the right node kind.
    _wet_wbks = get_wetland_wbks(fwa, bbox)
    _curated, _curated_wet = {}, {}
    for _e in _nv_all:
        _nm = next((n.get("name") for n in (_e.get("names") or []) if n.get("name")), "")
        if not _nm:
            continue
        for _w in ((_e.get("target") or {}).get("wbks") or []):
            _w = str(_w)
            if _w in lake_kind:
                _curated.setdefault(_w, ((_nm, ""),))
            elif _w in _wet_wbks:
                _curated_wet.setdefault(_w, ((_nm, ""),))
    from pipeline.common.models import NodeKind as _NK
    _iso = (mint_waterbody_nodes(graph, lake_names, NameSource.gazette)
            + mint_waterbody_nodes(graph, wetland_names, NameSource.gazette, _NK.wetland)
            + mint_waterbody_nodes(graph, _curated, NameSource.override)
            + mint_waterbody_nodes(graph, _curated_wet, NameSource.override, _NK.wetland))
    if _iso:
        print(f"  minted {_iso} named waterbody node(s) — no stream runs through them")

    # EVERY REMAINING WATERBODY. A zone regulation targets water by WHERE IT IS, not by what it
    # is called, so an unnamed pond in a management unit with a spring closure is closed. Only a
    # node carries `mus`, so an unnamed waterbody with no node is invisible to every zone rule —
    # 417,111 of them province-wide, which is most of the small water people actually fish.
    # Minted edgeless (nothing flows through them) and given their FWA polygon as sidecar
    # geometry, so the membership passes below and the tile exporter both read ONE source.
    _all_wbks = get_all_waterbody_wbks(fwa, bbox)
    # Load every waterbody polygon ONCE. It is needed three times over: to mint the missing
    # nodes, to give the membership passes something to test, and as the shape the tiles draw.
    wb_polys = get_waterbody_polys(fwa, _all_wbks, bbox)
    # AND THE CURATED ONES, HERE, so there is genuinely one source.
    #
    # `added_lake_polys` was merged into a LOCAL dict for the area-membership pass and
    # nowhere else, under a comment reading "a curated lake draws + gets MUs like any
    # other". It got the MUs. It never drew: `waterbody_polys.pkl` is written from
    # `wb_polys` alone, so the six curated lakes reached the bundle as items you could
    # search and tap, and the lake layer had no polygon for any of them. A water that is
    # findable everywhere except on the map is the hardest kind of missing to notice.
    wb_polys.update(added_lake_polys)
    _todo = {w for w in _all_wbks if f"lake:{w}" not in graph.nodes}
    if _todo:
        _wet = get_wetland_wbks(fwa, bbox)
        _n = 0
        for _w in _todo:
            _pl = wb_polys.get(_w)
            if _pl is None or _pl.is_empty:
                continue
            _kind = _NK.wetland if _w in _wet else _NK.lake
            mint_waterbody_nodes(graph, {_w: ()}, NameSource.gazette, _kind, allow_unnamed=True)
            _n += 1
        print(f"  minted {_n} unnamed waterbody node(s) so zone rules can reach them")
    _tick("name variants (whole-feature)")

    _moved_tribs: list = []
    _nests: list = []
    from pipeline.atlas.graph.prune import prune_mainstem_loops
    print("pruning pure braid loops off mainstems ...")
    graph, n_pruned, pruned_fids = prune_mainstem_loops(
        graph, geoms, reconnect_tributaries=args.simplify_braids,
        moved=_moved_tribs, kept_out=_nests)
    print(f"  {n_pruned} loop piece(s) removed -> {len(graph.nodes)} nodes")
    if _moved_tribs:
        # A re-homed mouth that moves a long way means the "braid" was not the small anabranch this
        # assumes. The MEDIAN is not the number to watch — most mouths do not move at all, because the
        # braid and its exit share an endpoint — so print the tail, where a bad case would hide.
        worst = sorted(_moved_tribs, key=lambda t: -t[3])
        d = sorted(t[3] for t in _moved_tribs)
        far = sum(1 for x in d if x > 500)
        print(f"  re-homed {len(_moved_tribs)} confluence(s) off removed braids "
              f"(median {d[len(d) // 2]:.0f} m, p99 {d[int(len(d) * .99)]:.0f} m, "
              f"max {d[-1]:.0f} m; {far} over 500 m = {100 * far / len(d):.2f}%)")
        for src, old_t, new_t, dist in worst[:10]:
            print(f"      {src} : {old_t} -> {new_t}  ({dist:.0f} m)")
    if _nests:
        # Each nest is reduced to the channels that carry something, not kept or dropped whole.
        was = sum(a for a, _b, _c in _nests)
        now = sum(b for _a, b, _c in _nests)
        spare = sum(c for _a, _b, c in _nests)
        print(f"  {len(_nests)} braid nest(s): {was} channels -> {now} "
              f"({spare} route(s) already reachable another way)")
    _orphans = sum(1 for nid in graph.nodes if not graph.down_adj.get(nid)
                   and graph.up_adj.get(nid))
    print(f"  fed-but-no-outlet nodes after prune: {_orphans}")
    _tick("prune braid loops")

    # ---- LEAF PRUNE: unnamed headwater capillaries -------------------------------------
    #
    # A separate pass, and nothing to do with the braid prune above. That one reduces nests
    # attached at both ends and has to re-home what flowed through them; this removes leaves,
    # which orphan nothing. See graph/leaf_prune.py.
    #
    # HERE, not in the tile export, so the registry, the bundle, the reach builder and the
    # tiles all see the same water. Pruning at export time would have left the bundle binding
    # rules to sections the map does not draw — and because this runs BEFORE the tributary
    # sweep, no rule ever claims them in the first place.
    #
    # AFTER the naming above, and that is what makes a `protected_blks` list unnecessary
    # here: the braid prune needed one because it ran before anything was named, and the
    # naming pass was split in two precisely so it no longer does. A curated channel already
    # carries its own `display_name` by this point, so it is not a candidate at all.
    from pipeline.atlas.graph.leaf_prune import DEFAULT_RULE
    LEAF_PRUNE = None if args.no_leaf_prune else DEFAULT_RULE
    if LEAF_PRUNE is not None:
        from pipeline.atlas.graph.leaf_prune import prune_leaves
        print(f"pruning unnamed headwater leaves ({LEAF_PRUNE.describe()}) ...")
        _before = len(graph.nodes)
        graph, _leaves = prune_leaves(graph, LEAF_PRUNE)
        print(f"  {len(_leaves):,} leaf section(s) removed -> {len(graph.nodes):,} nodes "
              f"({100 * len(_leaves) / max(_before, 1):.1f}% of the graph)")
        _tick("prune unnamed leaves")

    fid_index = {f.fid: (f.down_m, f.up_m, f.stream_order, f.stream_magnitude) for f in fids}

    # Border pass FIRST (like lakes, but via splits) so curated points can pick up border splits.
    if (args.border or args.full) and not args.no_border:
        from pipeline.atlas.splits.border import apply_border
        print("applying BC border splits (cross-border BLKs) ...")
        n_bsplits, n_flagged = apply_border(fwa, graph, geoms, chains, fid_index)
        print(f"  {n_bsplits} border split(s); {n_flagged} out-of-BC piece(s) flagged "
              f"-> {len(graph.nodes)} nodes")
        _tick("border")

    # CURATED SPLITS DEFAULT ON. `--splits` used to have no default, so omitting it built a whole
    # province with ZERO curated cuts and said nothing: 376 confluence/point/lake splits silently
    # gone, 373 registry boundaries with them (Fraser 29->13, Kokish 6->0, Stamp 7->2), and every
    # "downstream of X" regulation left unresolvable. The build still passed its tests and the
    # registry still looked healthy, because only the AREA splits — which come from a different
    # source — survived. A build that quietly drops the curated geometry must not be reachable by
    # forgetting a flag; opting out is now explicit.
    if args.no_splits:
        splits = None
        print("!! --no-splits: building with NO curated splits; 'downstream of X' rules will not resolve")
    else:
        _sp = Path(args.splits) if args.splits else _DEFAULT_SPLITS
        if not _sp.exists():
            raise SystemExit(f"splits file not found: {_sp}  (pass --splits, or --no-splits to skip)")
        splits = load_split_defs(str(_sp))
        print(f"curated splits: {len(splits)} from {_sp}")
        # GAUGE CUTS RIDE IN AS ORDINARY SPLIT DEFS, SECOND.
        #
        # A section takes ONE station: the one that most nearly is that water. On a river of
        # 20 sections with 18 stations that means the reading at Hope is claimed for water at
        # Lillooet, because one section runs between them. The answer is not a better ranking
        # but a shorter reach — a gauge is the boundary between two measurements.
        #
        # They are `gauge` point anchors scoped by WSC, derived here from
        # `pipeline/gauge_match.json` — the one frozen record of where BC's gauges are,
        # written by `python -m pipeline.gauges.generate.match --build <a completed build>` and read
        # by this and by `pipeline.deliver.bundle`. `pipeline/gauges/consume/cuts.py` draws the flow.
        #
        # AFTER the curated ones, and that order is load-bearing rather than tidy: proximity
        # pickup means a station near a hand-authored boundary REUSES it instead of cutting a
        # near-duplicate a few metres away, and a split can only pick up a boundary that is
        # already there. Authored geometry is the primary boundary; a gauge defers to it.
        #
        # Appended rather than resolved separately so there is ONE resolver, one set of rules
        # about braids and offsets and proximity, and one place a curator reviews every cut in
        # the province — splits.resolved.json and the gpkg both.
        if not args.no_gauge_splits:
            # READ ONLY. `pipeline.gauges.matches` is the frozen record and its IO; the
            # MATCHER lives in `gauges.generate` and is deliberately not importable from
            # here — a build that could re-derive a match is a build that can silently
            # change one, and 206 of these were confirmed by a person against a map.
            from pipeline.gauges.matches import MATCH_FILE, read_match
            from pipeline.gauges.consume.cuts import split_defs as _gauge_defs
            from pipeline.gauges.consume.shed import load_stations
            from pipeline.gauges.review import load as _load_review
            from pipeline.common.models.splits import SplitDef
            _matches = read_match()
            if not _matches:
                # ABSENT IS LOUD, NOT A DEFAULT. A graph with no gauge boundaries looks
                # perfectly healthy and is wrong everywhere it matters: every long river
                # claims one station's reading for its whole length, because one section
                # runs between Hope and Lillooet. This is the `--splits` incident exactly —
                # an omission that produces a plausible result — so it stops the build.
                raise SystemExit(
                    f"no gauge matches at {MATCH_FILE}\n"
                    "  generate them:  python -m pipeline.gauges.generate.match "
                    "--build <a completed build>\n"
                    "  or build without cutting rivers at their gauges: --no-gauge-splits\n"
                    "  (a build without them is valid and wrong: a river of 20 sections "
                    "with 18 stations on it\n"
                    "   reports the reading at Hope for water at Lillooet)")
            _rows = _gauge_defs(_matches, load_stations(
                SOURCE / "bc_hydrometric_stations.json"))
            splits = list(splits) + [SplitDef.from_dict(r) for r in _rows]
            # THE TRUST MIX, not just the count. A build cutting at 1,864 unreviewed
            # guesses is in a different state from one cutting at 206 human-confirmed
            # matches, and the finished graph cannot tell you afterwards which it was.
            _rev = _load_review().stations
            _by = {}
            for _r in _rows:
                _d = _rev.get(_r["station"])
                _by[_d.verdict if _d else "auto"] = _by.get(_d.verdict if _d else "auto", 0) + 1
            _mix = ", ".join(f"{n} {k}" for k, n in sorted(_by.items(), key=lambda kv: -kv[1]))
            print(f"gauge cuts:     {len(_rows)} offered from {MATCH_FILE.name}  ({_mix})")
    applied_splits: list = []
    if splits:
        from pipeline.atlas.splits.anchors import resolve_split_defs
        from pipeline.common.models import AnchorType
        from pipeline.atlas.splits.sectionizer import split_graph_at
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
            from pipeline.atlas.splits.anchors import _target_blks
            from pipeline.atlas.splits.border import mark_inside_area
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
    from pipeline.atlas.splits.area_splits import load_area_split_defs, load_area_polys, resolve_area_splits
    area_defs = load_area_split_defs()
    catalog_polys: dict[str, dict] = {}                # {area_def id: {name: polygon}} for the lazy catalog
    if area_defs:
        from pipeline.atlas.splits.sectionizer import split_graph_at
        for ad in area_defs:
            polys = load_area_polys(fwa, ad, bbox=bbox)
            if not polys:
                continue
            catalog_polys[ad["id"]] = polys
            if ad.get("cut", True):                    # `cut` flag is the SOLE cut trigger (default on)
                # `label_term` turns a cut's label from the polygon's own name into what it
                # SEPARATES — "Region 2 – Region 3 boundary" rather than "2". An area whose
                # name already reads as a place ("Garibaldi Provincial Park") sets no term
                # and keeps its name.
                apts = resolve_area_splits(polys, chains, term=ad.get("label_term"))
                split_graph_at(graph, geoms, apts, fid_index, proximity_pickup=False, applied=applied_splits)
                print(f"  area '{ad['id']}': {len(polys)} polygon(s), {len(apts)} transition cut(s)")
            else:
                print(f"  area '{ad['id']}': {len(polys)} polygon(s), membership-only (no cut)")
        _tick("blanket area splits (cut only)")

    # LENGTH CAP — the last cut, and the only one that is not about a named feature.
    #
    # Everything above cuts where somebody drew a line: a lake, a border, a closure, a
    # hand-authored point, a gauge. What is left over is a section that no line happened to
    # cross and that is simply too long to describe with one number — the Fraser between two
    # stations, a 44 km named reach with a dozen tributaries inside it. This cuts those at
    # their own interior confluences, which is where their drainage actually changes.
    #
    # LAST, so it only spends a cut where nothing else reached. BEFORE the membership passes
    # below, because `_split_one` copies the parent's attributes onto the new piece and a
    # piece split after them would inherit MU and area flags across boundaries it crosses.
    if args.max_section_km and args.max_section_km > 0:
        from pipeline.common.models import NodeKind as _LenNK
        from pipeline.atlas.splits.length_splits import junction_cuts
        from pipeline.atlas.splits.sectionizer import split_graph_at
        _cap_m = args.max_section_km * 1000.0
        # `fid_index` snaps each cut onto the FWA segment boundary at the confluence, so
        # the pieces repartition cleanly and each gets its OWN order and magnitude.
        _len_pts = junction_cuts(graph, cap_m=_cap_m, fid_index=fid_index)
        if _len_pts:
            _before = len(graph.nodes)
            split_graph_at(graph, geoms, _len_pts, fid_index, proximity_pickup=False,
                           applied=applied_splits)
            _named = sum(1 for p in _len_pts if p.label)
            # The share landing at a NAMED tributary is the reviewable number here: those
            # cuts produce a section a reader can locate ("downstream of Sloquet Creek"),
            # the rest produce an honest but unlabelled break.
            print(f"  length cap {args.max_section_km:g} km: {len(_len_pts)} confluence cut(s), "
                  f"{_named} at a named tributary -> {len(graph.nodes) - _before} new section(s)")
            # A section with NO interior confluence cannot be cut and stays long. Almost all
            # of them are out-of-BC reaches, where FWA carries no tributaries at all — there
            # is nothing known to change along them, so this is a fact, not a failure.
            _still = [n for n in graph.nodes.values()
                      if n.kind == _LenNK.stream and n.length_m > _cap_m]
            if _still:
                _oob = sum(1 for n in _still if n.out_of_bc)
                print(f"    {len(_still)} section(s) still over the cap "
                      f"({_oob} outside BC, where there are no tributaries to cut at)")
        _tick("length cap splits")

    # AREA MEMBERSHIP (supersedes the 2026-08-16 "lazy at resolve time" decision). Membership is
    # computed HERE, for every catalog area, because resolve time cannot afford it: testing a polygon
    # against sections needs the 2 GB geometry sidecar, which the review app does not load and should
    # not have to. Doing it once at build costs one STRtree pass and turns every area into an `area:`
    # registry item carrying its sections — after which `within(area)` is a set intersection needing no
    # geometry at all.
    #
    # It also collapses two divergent paths into one. A rule-scoped `area_boundary` split (splits.json,
    # scoped to one water by `applies_to`) already flagged `in_areas` eagerly; blanket areas from
    # areas.json were cut but never flagged, so a `within(ecological reserve)` rule had no item to
    # resolve against and simply failed. Both now produce the same kind of item, and the resolver
    # applies one rule to both (see resolve_extent: intersect with the rule's items, or take the whole
    # area when the rule names no water).
    # ONE POLYGON SOURCE, and `wb_polys` is it — curated lakes included, merged where it is
    # loaded. This used to be built here as `{**wb_polys, **added_lake_polys}`, which meant
    # the curated lakes existed for the membership and MU passes and for nothing else; the
    # tile geometry is pickled from `wb_polys` and never saw them.
    wbk_polys: dict = dict(wb_polys)
    if catalog_polys:
        from pipeline.common.models import WATERBODY_KINDS
        from pipeline.atlas.splits.area_catalog import area_id as _area_id
        from pipeline.atlas.splits.border import mark_inside_areas
        _polys = {_area_id(ad.get("kind", ad["id"]), nm): pl
                  for ad in area_defs for nm, pl in (catalog_polys.get(ad["id"], {}) or {}).items()}
        # A minted waterbody (isolated lake, marsh, reservoir) has no line geometry, so the membership
        # test has nothing to measure — and those are exactly the waters an area closure most often
        # names. Load their FWA polygons and key them by node id. Loaded ONCE here and reused for the
        # MU pass below, which needs the same polygons for the same reason.
        # Every waterbody, not just the ones with no line geometry: a NODED lake's sidecar
        # geometry is the under-lake route through it, which is the wrong shape to test a
        # polygon against. Its actual outline is here.
        _flags = mark_inside_areas(graph, geoms, _polys,
                                   extra={f"lake:{w}": pl for w, pl in wbk_polys.items()})
        print(f"  area membership: {_flags} flag(s) across {len(_polys)} area(s) "
              f"({len(wbk_polys)} minted waterbody polygon(s) included)")
        _tick("area membership")

    # MANAGEMENT UNITS. Separate pass from area membership, and unconditional, because the two
    # are different kinds of fact: `in_areas` is membership of a REGULATED area and exists only
    # where someone wrote a rule; `mus` is administrative geography that is true everywhere.
    # Zone regulations ("in MU 4-5, no bait") resolve through `mus` and nothing else — before
    # this pass, MU came only from the synopsis entry, so unnamed streams had no MU and no zone
    # rule could reach them. Measured: 100% coverage, 99.14% of sections in exactly one MU.
    from pipeline.atlas.splits.border import load_mu_polys, mark_mus
    _mu_polys = load_mu_polys(args.gpkg, bbox)
    if _mu_polys:
        _mu_flags = mark_mus(graph, geoms, _mu_polys,
                             extra={f"lake:{w}": pl for w, pl in wbk_polys.items()})
        _with_mu = sum(1 for n in graph.nodes.values() if n.mus)
        print(f"  management units: {_mu_flags} flag(s) across {len(_mu_polys)} MU(s); "
              f"{_with_mu}/{len(graph.nodes)} sections stamped")
        _tick("management units")

    # The REACH-qualified variants only: these name a measure range and must land on a section
    # boundary, so they wait for the splits. Everything else was applied before the prune, above.
    if _nv_reach:
        n = apply_name_variants(graph, _nv_reach)
        print(f"  applied {len(_nv_reach)} reach-qualified variant entries -> {n} node attachments")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if wb_polys:
        # A waterbody has TWO geometries and they answer different questions: `geometries.pkl`
        # holds the under-lake ROUTE a river takes through it (a line, for topology), this holds
        # its SHAPE (a polygon, for drawing). One file per fact; neither is derivable from the
        # other, and drawing the route as the shape is what renders a lake as a spiky asterisk.
        import pickle as _pk
        with (out / "waterbody_polys.pkl").open("wb") as _fh:
            _pk.dump({f"lake:{w}": pl for w, pl in wb_polys.items()}, _fh, protocol=5)
        print(f"  wrote waterbody_polys.pkl ({len(wb_polys):,} outlines)")
    write_artifact(chains, str(out / "blk_chains.pkl"))
    write_artifact(graph, str(out / "graph.pkl"))
    write_artifact(geoms, str(out / "geometries.pkl"))

    # Registry (parser truth) — build from the finalized graph + persist, so the parser tools
    # (matcher / batch_exporter / ingest) never need to rebuild the graph from the ~10GB FWA data.
    from pipeline.atlas.registry import add_curated_wbk_items, add_mu_sets, add_waterbody_items, build_registry
    from pipeline.atlas.registry import write_registry
    # `build_registry` reads overrides.json itself for the waterbody keys a curator pinned by
    # hand, so an FWA-unnamed water a regulation names still becomes an item. See
    # `pipeline.atlas.registry.build.pinned_by_override`.
    registry = build_registry(graph)
    print(f"  registry: {len(registry)} named items")
    # Named waterbodies the graph alone misses: isolated named lakes/reservoirs with no through-stream
    # (never noded, e.g. Frazer Lake) and wetlands (never noded). Add them from the FWA layers so a reg
    # can target them by name or a curated wbk/gnis pin.
    n0 = len(registry)
    registry = add_waterbody_items(registry, lake_names, "lake")
    n_lakes = len(registry) - n0
    registry = add_waterbody_items(registry, wetland_names, "wetland")
    print(f"  + {n_lakes} isolated named lake item(s) + {len(registry) - n0 - n_lakes} named wetland item(s)")
    # Waterbodies named ONLY by curation (name_variants wbk target on an isolated FWA-unnamed lake) —
    # add them so the curated name is matchable (e.g. Redstart Lake's 2nd polygon).
    n1 = len(registry)
    registry = add_curated_wbk_items(registry, _nv_all)
    print(f"  + {len(registry) - n1} curated-only wbk item(s) (named via name_variants)")
    _tick("build_registry")
    # Isolated/overlaid waterbodies have a node but NO sidecar geometry (the client draws them from the
    # FWA polygon layer), so load their own wbk polygon for add_mu_sets. Select on missing GEOMETRY,
    # not on missing sections: since these waters are minted as nodes their items do have a section,
    # and keying off `not section_ids` would silently leave every one of them with no MUs.
    nogeom_wbks = {iid.split(":", 1)[1] for iid, it in registry.items()
                   if it.kind in ("lake", "wetland") and iid.startswith("wbk:")
                   and not any(geoms.get(nid) is not None for nid in it.section_ids)}
    _missing = nogeom_wbks - set(wbk_polys)          # already loaded for the area pass; top up any rest
    if _missing:
        wbk_polys = {**wbk_polys, **get_waterbody_polys(fwa, _missing, bbox)}
    print(f"  {len(wbk_polys)} isolated waterbody polygon(s) for MU calc ({len(nogeom_wbks)} needed, "
          f"{len(_missing)} newly loaded)")
    registry = add_mu_sets(registry, geoms, get_mu_polys(fwa), wbk_polys)
    _tick("add_mu_sets")
    write_registry(registry, out / "registry.json")
    print(f"  registry -> {out / 'registry.json'}")
    # THE SECTION HANDLE TABLE, written here because this is where the set of sections is
    # finally known and because exactly one place may decide it. The tile and the bundle both
    # read it; neither invents an order. See pipeline/common/section_handles.
    from pipeline.common.section_handles import FILENAME as _HANDLES, write as _write_handles

    _hd = _write_handles(graph.nodes.keys(), out)
    print(f"  {len(graph.nodes):,} section handles -> {out / _HANDLES}  (digest {_hd})")
    # Lazy area catalog (polygons only; membership computed at resolve time) — see DECISION 2026-08-16.
    if catalog_polys:
        from pipeline.atlas.splits.area_catalog import catalog_entries, write_area_catalog
        cat = catalog_entries(area_defs, catalog_polys)
        write_area_catalog(cat, out / "area_catalog.gpkg")
        print(f"  area catalog: {len(cat)} referenceable area(s) -> {out / 'area_catalog.gpkg'}")
    if applied_splits:
        from pipeline.atlas.splits.splits import write_resolved
        write_resolved(applied_splits, str(out / "splits.resolved.json"))
    obstacles = None
    if "obstacles" in fwa.layer_names:
        obstacles = fwa.get_layer(
            "obstacles", bbox=bbox,
            columns=["FISH_OBSTACLE_POINT_ID", "OBSTACLE_NAME", "GAZETTED_NAME",
                     "WATERSHED_CODE_50K", "HEIGHT", "geometry"])
        print(f"  loaded {len(obstacles)} fish-passage obstacle(s) for the obstacles layer")
    # ITEM PINS: item_id -> (lon, lat), a representative point per registry item.
    #
    # ~21k rows and about a megabyte. It exists so that wanting a MAP PIN does not mean
    # keeping a 4.7 GB GeoPackage alive: `dfo_salmon.dossier` needs one coordinate per water
    # to print an OSM link, and used to open graph.gpkg with a WHERE clause to get it.
    #
    # A REPRESENTATIVE POINT, never a centroid — the centroid of a bent river lands on dry
    # ground, and a curator following that pin ends up looking at a hillside.
    _pins: dict[str, list[float]] = {}
    for _it in registry.values() if isinstance(registry, dict) else registry:
        _iid = getattr(_it, "id", None) or (_it.get("id") if isinstance(_it, dict) else None)
        _secs = getattr(_it, "section_ids", None)
        if _secs is None and isinstance(_it, dict):
            _secs = _it.get("section_ids") or ()
        _parts = [geoms[n] for n in (_secs or ()) if geoms.get(n) is not None
                  and not geoms[n].is_empty]
        if not _iid or not _parts:
            continue
        from shapely.ops import unary_union
        _g = _parts[0] if len(_parts) == 1 else unary_union(_parts)
        _pt = _g.representative_point()
        _pins[_iid] = [round(_pt.x, 2), round(_pt.y, 2)]
    import geopandas as _gpd
    if _pins:
        _ser = _gpd.GeoSeries(
            [__import__("shapely.geometry", fromlist=["Point"]).Point(*v) for v in _pins.values()],
            crs=3005).to_crs(4326)
        _pins = {k: [round(g.x, 5), round(g.y, 5)] for k, g in zip(_pins, _ser)}
    (out / "item_points.json").write_text(json.dumps(_pins, separators=(",", ":")))
    print(f"  item pins: {len(_pins):,} -> {out.name}/item_points.json")
    _tick("item pins")

    # graph.gpkg IS FOR HUMANS, and it is 41% of this build.
    #
    # 4.7 GB and 742 seconds of the 1,810 measured on the province — more than the splits
    # and the graph construction put together. Nothing downstream reads it: not the bundle,
    # not the tiles, not the app. Its readers are the CURATION tools — `dfo_salmon.dossier`
    # and the review backend — which want to open a river in QGIS.
    #
    # So a build made to bundle, to tile, or to check parity should not pay for it, and a
    # build made to curate asks for it. OFF BY DEFAULT: most builds are not curation builds,
    # and 742 seconds is most of the difference between a 30-minute build and an 18-minute
    # one. `dossier.py` and the review backend both name the file they need, so a curator
    # who forgets gets a clear missing-file error rather than a wrong answer.
    if not args.write_gpkg:
        print("graph.gpkg:     skipped — pass --write-gpkg when you are about to curate "
              "(4.7 GB, ~40% of the build)")
        _tick("write artifacts (no gpkg)")
    else:
        gpkg_path = str(out / "graph.gpkg")
        export_graph_gpkg(graph, geoms, gpkg_path, splits=splits, split_points=applied_splits,
                          obstacles=obstacles, area_polys=area_polys if splits else None,
                          wbk_polys=wbk_polys)
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

    _total = _clock() - _t0
    timings.append(("TOTAL", _total))
    # PROFILE, not just a log. Sorted by cost with a share column, because "which stage
    # would repay optimising" is the question this is read for, and a chronological list of
    # 30 numbers does not answer it. Anything under 1% is rolled up: a build has a long
    # tail of sub-second stages and listing them buries the four that matter.
    _ranked = sorted((t for t in timings if t[0] != "TOTAL"), key=lambda kv: -kv[1])
    _small = [t for t in _ranked if t[1] / _total < 0.01]
    _big = [t for t in _ranked if t[1] / _total >= 0.01]
    _bar = lambda f: "#" * max(1, round(f * 40))
    timing_str = (
        "timings, by cost:\n"
        + "\n".join(f"  {label:32} {secs:8.1f}s  {secs/_total:5.1%}  {_bar(secs/_total)}"
                    for label, secs in _big)
        + (f"\n  {'(' + str(len(_small)) + ' stages under 1%)':32} "
           f"{sum(t[1] for t in _small):8.1f}s  {sum(t[1] for t in _small)/_total:5.1%}"
           if _small else "")
        + f"\n  {'TOTAL':32} {_total:8.1f}s  ({_total/60:.1f} min)"
        + "\n\nchronological:\n"
        + "\n".join(f"  {label:32} {secs:8.1f}s" for label, secs in timings))

    summary = summarize(chains, graph, fids, pruned_fids) + "\n\n" + timing_str
    (out / "summary.txt").write_text(summary)
    print("\n" + summary)
    print(f"\nwrote artifacts + graph.gpkg to {out}/")


if __name__ == "__main__":
    main()
