"""TIDAL WATER ON THE MAP (T166 step 5; owner 2026-10-28: "tidal pieces COME OFF the map").

The stream below an accepted tidal boundary is not freshwater (RULINGS K7), so the tile export does not
draw it — the line, a river polygon, any piece the reach run makes tidal. Instead the DFO tidal-waters
polygon is drawn as one labelled area (`tidal_water`), so a tap there explains it.

ONE SET, READ. The pieces are the atlas's (T678): its one tidal record, and on an atlas built since plan 5 its graph
nodes (`StreamNode.tidal`, checked against the record: `atlas.splits.coast.read_tidal`, `on_the_graph`) — what the reach
run reads too, so the tiles and the bundle's `tidal` table cannot disagree (the bundle proves it: `bundle.undrawn`,
layer `tidal`). A row marked tidal (Nitinat Lake) is not a cut: its water stays drawn.

THE AREA. The DFO polygon follows the coast; where the owner put a cut ABOVE the polygon's edge (the words'
landmark: Capilano's CN bridge, Nimpkish's signs) the removed piece would sit in neither — so each removed
piece's footprint (a line buffered `PIECE_BUFFER_M`, a polygon as it is), less the polygon, joins the layer.
THE WHOLE POLYGON, FROM z9, NOT A COASTAL BAND (measured 2026-10-28, one-layer archives with the production
flags): the polygon whole is 3.73 MB at z9-13 (+0.09 MB for z6-8, dropped: z9 is where the streams show); clipped
to a 2 km coastal band it is 4.13 MB — its 601,582 vertices are nearly all shoreline, which the band keeps, and the
band's own inner edge adds a line to every coastal tile where the open sea's solid tiles had deduplicated. As a
GeoJSON overlay (like `bc_outside.geojson`) it would be 3.4 MB gzip / 13 MB raw whole (4.3 / 15 MB as the band),
parsed whole on every map load — 0.7 MB gzip only at a 50 m simplification, not crisp at z15.

Where the owner put a cut BELOW the polygon's edge (Cowichan: the mouth, the polygon 950 m up) the
freshwater stream is drawn over the tidal fill; a tap on the stream is the stream's (it is asked first).
"""
from __future__ import annotations

from pathlib import Path

#: a removed stream LINE's footprint in the tidal area: half a channel's width either side, so a tap on
#: where the line was lands in it (the app's tap box is 24 px)
PIECE_BUFFER_M = 25.0
#: the one meaning every `tidal_water` feature has (the app's sheet keys on it)
AREA_ID = "tidal"
NAME = "Tidal water"


def tidal_pieces(build_dir: Path) -> frozenset[str]:
    """The section (node) ids of this atlas's tidal water (its one tidal record, `coast.read_record`): the pieces below
    its tidal boundaries, its whole tidal waters, the lines tidal for their length, the pieces below a coast cut. An
    atlas cut before the coast rule is refused (T678: no second reading). On an atlas whose nodes carry the owner
    (plan 5) each exporter that loads the graph checks the record against them (`on_the_graph`)."""
    from pipeline.atlas.splits import coast as _coast
    _coast.require_coast_cut(build_dir)
    return frozenset(_coast.read_record(build_dir))


def on_the_graph(build_dir: Path, graph, pieces: frozenset) -> frozenset:
    """THE NODE IS THE FACT (T678 plan 5): on an atlas whose nodes carry their tidal owner, the set read off `graph`
    (the exporter's own graph.pkl), refused unless it is `pieces` (the record) exactly; else `pieces` as given."""
    from pipeline.atlas.splits import coast as _coast
    if not pieces and not (Path(build_dir) / _coast.TIDAL_RECORD).exists():
        return pieces                                           # a layer test's atlas: no tidal water asked about
    if not _coast.record_on_nodes(build_dir):
        return pieces
    got = frozenset(_coast.read_tidal(build_dir, graph))
    if got != pieces:
        raise SystemExit(f"tiles: the graph's tidal nodes are not the tidal pieces asked for "
                         f"({len(got ^ pieces)} differ) — the graph is not this atlas's")
    return got


def area_features(dfo, pieces) -> list:
    """THE TIDAL AREA's polygons (EPSG:3005): every part of the DFO polygon `dfo` (a (Multi)Polygon, or
    None), then each removed piece's footprint not already inside it (`pieces`: lines and polygons)."""
    import shapely
    from shapely.geometry import MultiPolygon, Polygon
    from shapely.strtree import STRtree

    out: list = []
    parts = []
    if dfo is not None and not dfo.is_empty:
        parts = list(dfo.geoms) if isinstance(dfo, MultiPolygon) else [dfo]
        out += [p for p in parts if not p.is_empty]
    tree = STRtree(parts) if parts else None
    for g in pieces:
        if g is None or g.is_empty:
            continue
        foot = g.buffer(PIECE_BUFFER_M) if g.geom_type in ("LineString", "MultiLineString") else g
        if tree is not None:
            near = [parts[i] for i in tree.query(foot)]
            if near:
                foot = foot.difference(shapely.union_all(near))
        if foot.is_empty or foot.area < 1.0:
            continue
        out += [p for p in (foot.geoms if hasattr(foot, "geoms") else [foot])
                if isinstance(p, Polygon) and not p.is_empty]
    return out


def read_dfo(gpkg: str):
    """The DFO tidal-waters polygon (`tidal_boundary`, one MultiPolygon, EPSG:3005). A gpkg without the
    layer is refused: the map would lose its tidal water silently (AGENTS 37)."""
    import geopandas as gpd
    import shapely
    gdf = gpd.read_file(gpkg, layer="tidal_boundary", engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    geoms = [g for g in gdf.geometry if g is not None and not g.is_empty]
    if not geoms:
        raise SystemExit(f"tidal: {gpkg} layer tidal_boundary holds no polygon")
    return shapely.union_all(geoms)


def export_tidal(gpkg: str, out_dir: Path, pieces, *, dfo=None) -> dict:
    """Write `tidal_water` (the DFO polygon + the removed pieces' footprints)."""
    from pyproj import Transformer
    from pipeline.deliver.tiles.export import _to4326, _writer
    from pipeline.deliver.tiles.layers import BY_NAME
    spec = BY_NAME["tidal_water"]
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    write, close = _writer(out_dir, spec)
    feats = area_features(read_dfo(gpkg) if dfo is None else dfo, list(pieces))
    for g in feats:
        write(_to4326(g, tf), {"area_id": AREA_ID, "name": NAME}, spec.minzoom)
    n = close()
    print(f"  tidal_water    {n:>9,}  (DFO polygon parts + removed pieces outside it)")
    return {"tidal_water": n}
