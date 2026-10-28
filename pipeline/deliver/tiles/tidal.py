"""TIDAL WATER ON THE MAP (T166 step 5; owner 2026-10-28: "tidal pieces COME OFF the map").

The stream below an accepted tidal boundary is not freshwater (RULINGS K7), so the tile export does not
draw it — the line, a river polygon, any piece the reach run makes tidal. Instead the DFO tidal-waters
polygon is drawn as one labelled area (`tidal_water`), so a tap there explains it.

ONE SET, ONE FUNCTION. The pieces are the reach run's: `reach.outside.tidal_cut_owner` over this atlas's
tidal splits (`tidal_splits`, from `splits.resolved.json`) — the same call the reach run makes, on the same
registry and graph, so the tiles and the bundle's `tidal` table cannot disagree (the bundle proves it:
`bundle.undrawn`, layer `tidal`). A row marked tidal (Nitinat Lake) is not a cut: its water stays drawn.

THE AREA. The DFO polygon follows the coast; where the owner put a cut ABOVE the polygon's edge (the words'
landmark: Capilano's CN bridge, Nimpkish's signs) the removed piece would sit in neither — so each removed
piece's footprint (a line buffered `PIECE_BUFFER_M`, a polygon as it is), less the polygon, joins the layer.
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


def tidal_pieces(build_dir: Path, graph=None, registry=None) -> frozenset[str]:
    """The section (node) ids below this atlas's tidal splits — the reach run's own set. Empty when the
    atlas cut no tidal boundary."""
    from pipeline.atlas.reach.outside import tidal_cut_owner, tidal_splits
    ids = tidal_splits(build_dir)
    if not ids:
        return frozenset()
    if registry is None:
        from pipeline.atlas.registry import load_registry
        registry = load_registry(str(Path(build_dir) / "registry.json"))
    if graph is None:
        from pipeline.common.io.serialize import read_artifact
        graph = read_artifact(str(Path(build_dir) / "graph.pkl"))
    return frozenset(tidal_cut_owner(registry, graph, ids))


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
