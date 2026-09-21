"""Build artifacts -> newline-delimited GeoJSON, one file per tile layer.

Everything here is a pure READ of a completed build. Nothing is derived that the build
could have derived — in particular MU and admin-area membership are read off
``StreamNode.mus`` and ``StreamNode.in_areas``, never recomputed here. Two places that
each work out which MU a stream is in are two places that can disagree, and the one the
app trusts would be whichever ran last.

If a build predates those passes the export fails loudly rather than silently shipping
tiles with no zone-regulation membership, which would look fine and answer wrong.

Coordinates go out as EPSG:4326 because that is what tippecanoe wants.
"""

from __future__ import annotations

import json
import math
import pickle
from collections import defaultdict
from pathlib import Path

from pipeline.deliver.tiles import ladder
from pipeline.deliver.tiles.layers import BY_NAME, LayerSpec
from pipeline.deliver.tiles.names import display, haystack
from pipeline.common.registry_kinds import waters

_ROUND = 6                      # ~11 cm; tippecanoe quantises to the tile grid anyway


def _writer(out_dir: Path, spec: LayerSpec):
    fh = (out_dir / f"{spec.name}.geojsonl").open("w")
    allowed = set(spec.attrs)
    n = 0

    def write(geom: dict, props: dict, minzoom: int) -> None:
        nonlocal n
        clean = {k: v for k, v in props.items()
                 if k in allowed and v is not None and v != ""}
        fh.write(json.dumps({
            "type": "Feature", "geometry": geom, "properties": clean,
            "tippecanoe": {"layer": spec.name,
                           "minzoom": max(spec.minzoom, min(minzoom, spec.maxzoom)),
                           "maxzoom": spec.maxzoom},
        }, separators=(",", ":")) + "\n")
        n += 1

    def close() -> int:
        fh.close()
        return n

    return write, close


def _to4326(geom, tf):
    """shapely geometry (EPSG:3005) -> GeoJSON dict in EPSG:4326."""
    def ring(coords):
        return [[round(x, _ROUND), round(y, _ROUND)]
                for x, y in (tf.transform(a, b) for a, b in coords)]
    t = geom.geom_type
    if t == "LineString":
        return {"type": "LineString", "coordinates": ring(geom.coords)}
    if t == "MultiLineString":
        return {"type": "MultiLineString", "coordinates": [ring(g.coords) for g in geom.geoms]}
    if t == "Polygon":
        return {"type": "Polygon",
                "coordinates": [ring(geom.exterior.coords)] +
                               [ring(i.coords) for i in geom.interiors]}
    if t == "MultiPolygon":
        return {"type": "MultiPolygon",
                "coordinates": [[ring(p.exterior.coords)] + [ring(i.coords) for i in p.interiors]
                                for p in geom.geoms]}
    if t == "Point":
        x, y = tf.transform(geom.x, geom.y)
        return {"type": "Point", "coordinates": [round(x, _ROUND), round(y, _ROUND)]}
    raise ValueError(f"unsupported geometry {t}")


# A route through a lake that is far longer than the lake is not a route through that lake.
#
# MEASURED over 80,000 routes against their own waterbody outline (route length / polygon
# bounding diagonal): median 0.85, p90 1.72, p99 4.13 — and a maximum of 293. The sane ones
# cluster around 1, because a line across a lake is about as long as the lake. The tail is
# a defect: those draw as long parallel streaks running clean off the far side of the water
# they supposedly cross, which is what the under-lake layer looked like on screen.
#
# 3x keeps the genuinely winding routes through irregular lakes and drops the rest.
_MAX_ROUTE_OVER_DIAGONAL = 3.0


def export_streams(build_dir: Path, out_dir: Path, *, limit: int | None = None) -> dict:
    """Flowing water, from the geometry sidecar. Lines only — waterbodies are polygons and
    come from ``export_waterbodies`` below."""
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    from pyproj import Transformer
    tf = Transformer.from_crs(3005, 4326, always_xy=True)

    print("  loading graph…")
    graph = pickle.load((build_dir / "graph.pkl").open("rb"))
    print("  loading geometries…")
    geoms = pickle.load((build_dir / "geometries.pkl").open("rb"))
    geoms = {k: v for k, v in geoms.items() if v is not None and not v.is_empty}
    if limit:
        geoms = dict(list(geoms.items())[:limit])

    item_of, variants = _identity(build_dir)
    _require_membership(graph)


    spec = BY_NAME["stream"]
    write, close = _writer(out_dir, spec)
    ul_spec = BY_NAME["under_lake"]
    ul_write, ul_close = _writer(out_dir, ul_spec)
    # The waterbody outlines, to sanity-check each route against the lake it crosses.
    wb_path = build_dir / "waterbody_polys.pkl"
    wb_polys = pickle.load(wb_path.open("rb")) if wb_path.exists() else {}
    dropped_routes = 0
    dropped_outside = 0
    for sec, g in geoms.items():
        node = graph.nodes.get(sec)
        if node is None:
            continue
        # OUTSIDE BC IS NOT DRAWN AT ALL.
        #
        # These reaches were kept and drawn dotted, on the reasoning that a river visibly
        # continuing into Washington is more honest than one stopping at a line. In practice
        # it is the opposite: the app answers a question — what may I fish, and what is the
        # water doing — and it has NO answer beyond the border. There are no regulations, no
        # gauges we read, and FWA carries no tributaries there, so those reaches also have
        # no drainage, no magnitude and no panel. They render as water the reader can tap
        # and be told nothing about.
        #
        # Dropping them at the tile is also the only place it sticks: the graph keeps them,
        # because the topology of a cross-border river is real and the border split depends
        # on it. This is a DRAWING decision, made where the drawing is produced.
        if node.out_of_bc:
            dropped_outside += 1
            continue
        if _kind(node) != "stream":
            # A lake node's sidecar geometry is the route THROUGH the lake, not the lake.
            # Drawn dotted so a chain of lakes still reads as one river; never as water
            # you can fish, which is why it is a separate layer with its own style.
            #
            # LAKES ONLY. Including wetlands put a dotted fragment through every marsh in
            # the province — 333,526 of them, mostly two to five points long — and the
            # result is a web of disconnected dashes over every low-lying valley. It is
            # also pointless: the reason this layer exists is so a CHAIN OF LAKES still
            # reads as one river. A stream crossing a marsh is already drawn as a stream.
            if _kind(node) == "lake":
                pl = wb_polys.get(sec) if wb_polys else None
                if pl is not None and not pl.is_empty:
                    minx, miny, maxx, maxy = pl.bounds
                    diag = math.hypot(maxx - minx, maxy - miny)
                    if diag > 0 and g.length > diag * _MAX_ROUTE_OVER_DIAGONAL:
                        dropped_routes += 1
                        continue
                # NOTHING AT ALL — the route has no identity and no size (see layers.py).
                # It is a construction line: it says the river continues through this lake,
                # and it is drawn at a small constant width so it cannot be read as river.
                ul_write(_to4326(g, tf), {},
                         ladder.zoom_for_area(g.length * g.length, ul_spec.minzoom))
            continue
        nm = display(node.display_name)
        write(_to4326(g, tf), {
            # THE HANDLE, not the string — the bundle keys every section table by it, and
            # this is the feature id the app sets state on, so the two must be the same
            # number. See pipeline/common/section_handles.
            "section_id": sid[sec],
            "item": item_of.get(sec),
            "name": nm,
            "alt": haystack(nm, sorted(variants.get(sec, ()))),
            # NO `mag`. It is the input to the zoom ladder, and the ladder has already run
            # by the time this feature is written — `zoom_for_magnitude` below turns it into
            # the per-feature minzoom tippecanoe actually uses. Shipping the magnitude too
            # sent the same fact twice, the second copy to a client that never read it:
            # 2.3% of the archive, on a property with no consumer in the app or the style.
            "ord": node.stream_order,
            "mus": ",".join(sorted(node.mus)) or None,
            "areas": ",".join(sorted(node.in_areas)) or None,
        }, ladder.zoom_for_magnitude(node.stream_magnitude, spec.minzoom))
    n, n_ul = close(), ul_close()
    print(f"  stream         {n:>9,}"
          + (f"  ({dropped_outside:,} outside BC, not drawn)" if dropped_outside else ""))
    print(f"  under_lake     {n_ul:>9,}"
          + (f"  ({dropped_routes:,} dropped: longer than "
             f"{_MAX_ROUTE_OVER_DIAGONAL:g}x their own waterbody)" if dropped_routes else ""))
    return {"stream": n, "under_lake": n_ul}


def export_waterbodies(build_dir: Path, gpkg: str, out_dir: Path) -> dict:
    """Standing water — from the graph, exactly like streams.

    Every waterbody in the province is now an edgeless node carrying its FWA polygon as sidecar
    geometry (build.py, "minted N unnamed waterbody node(s)"), so this reads ONE source. It used
    to stream the gpkg and merge in a separate membership artifact; that was two places that
    could disagree about which management unit a pond is in.

    A lake node has TWO geometries and they mean different things: the polygon is its shape and
    is what this draws; the under-lake fid run is the route a river takes through it and is
    drawn separately, dotted, by ``export_streams``.
    """
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    from pyproj import Transformer
    tf = Transformer.from_crs(3005, 4326, always_xy=True)

    graph = pickle.load((build_dir / "graph.pkl").open("rb"))
    poly_path = build_dir / "waterbody_polys.pkl"
    if not poly_path.exists():
        raise SystemExit(
            f"{poly_path} is missing, so no lake has a shape to draw. A lake node's sidecar\n"
            "geometry is the under-lake ROUTE, not its outline. Rebuild:\n"
            "    python -m pipeline.atlas.build --full --out <dir>")
    geoms = pickle.load(poly_path.open("rb"))
    item_of, variants = _identity(build_dir)

    writers: dict[str, tuple] = {}
    no_geom = 0
    for nid, node in graph.nodes.items():
        kind = _kind(node)
        if kind not in ("lake", "wetland"):
            continue
        g = geoms.get(nid)
        if g is None or g.is_empty:
            no_geom += 1
            continue
        lname = "wetland" if kind == "wetland" else "lake"
        spec = BY_NAME[lname]
        if lname not in writers:
            writers[lname] = _writer(out_dir, spec)
        write, _ = writers[lname]
        nm = display(node.display_name)
        write(_to4326(g, tf), {
            "section_id": sid[nid],
            "item": item_of.get(nid),
            "name": nm,
            "alt": haystack(nm, sorted(variants.get(nid, ()))),
            "area_m2": round(g.area),
            "mus": ",".join(sorted(node.mus)) or None,
            "areas": ",".join(sorted(node.in_areas)) or None,
        }, ladder.zoom_for_area(g.area, spec.minzoom))
    counts = {lname: close() for lname, (_, close) in writers.items()}
    for lname, n in counts.items():
        print(f"  {lname:<14} {n:>9,}")
    if no_geom:
        print(f"  ({no_geom:,} waterbody nodes had no polygon — see build.py minting)")
    return counts


def _kind(node) -> str:
    return node.kind.value if hasattr(node.kind, "value") else str(node.kind)


def _identity(build_dir: Path) -> tuple[dict, dict]:
    """{section -> item_id} and {section -> every registry name it answers to}."""
    # WATERS ONLY. An `area:` item's `section_ids` are the sections INSIDE the polygon, not
    # the sections that ARE it — and areas outnumber waters twelve to one and sort first, so
    # `setdefault` gave every feature inside a park the park's id and its slug as a search
    # name. See pipeline/common/registry_kinds. Areas reach the tile as the `areas`
    # attribute of each feature, which is the right place for containment.
    reg = waters(json.loads((build_dir / "registry.json").read_text())["items"])
    item_of: dict[str, str] = {}
    variants: dict[str, set[str]] = defaultdict(set)
    for it in reg:
        iid, nm = it["id"], it.get("name") or ""
        names = {v for v in (it.get("variants") or []) if v} | ({nm} if nm else set())
        for sec in it.get("section_ids") or ():
            item_of.setdefault(sec, iid)
            variants[sec] |= names
    return item_of, variants


def _require_membership(graph) -> None:
    n_mu = sum(1 for n in graph.nodes.values() if n.mus)
    n_area = sum(1 for n in graph.nodes.values() if n.in_areas)
    print(f"  membership from the build: {n_mu:,} with an MU, {n_area:,} inside an admin area")
    if n_mu == 0:
        raise SystemExit(
            "This build has no management units stamped, so every zone regulation would be\n"
            "invisible in the tiles. Rebuild with the MU pass:\n"
            "    python -m pipeline.atlas.build --full --out <dir>\n"
            "Refusing to ship tiles that look right and answer wrong.")


def export_admin(gpkg: str, out_dir: Path) -> dict:
    """Administrative geography, driven entirely by ``pipeline/areas.json``.

    The exporter states no selector of its own. Each area def already says which gpkg layer,
    which name field and which ``where`` clause makes that area, and the graph build uses the
    SAME def to stamp ``in_areas``; the def's ``tile_layer`` says which tile layer its polygons
    are drawn in. So a park is one definition, used twice, and the boundary you see is by
    construction the boundary a regulation was matched against.

    ``mu`` is the exception, and deliberately: management units are not a regulated area, so
    they are not in areas.json at all (see border.mark_mus).
    """
    import geopandas as gpd
    from pyproj import Transformer

    from pipeline.atlas.splits.area_splits import (load_area_attrs, load_area_polys,
                                                   load_area_split_defs)

    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    writers: dict[str, tuple] = {}

    def writer_for(lname: str):
        if lname not in writers:
            writers[lname] = _writer(out_dir, BY_NAME[lname])
        return writers[lname][0]

    for ad in load_area_split_defs():
        lname = ad.get("tile_layer")
        if not lname:
            # AN AREA MAY OPT OUT, BUT ONLY OUT LOUD. `not_drawn` carries the reason, so a
            # boundary that never ships is a decision somebody wrote down rather than a
            # missing field nobody noticed — which is the whole point of the guard below.
            # An area can still be CUT and STAMPED without being drawn: the MU groups are
            # unions of units whose own outlines already draw, and a zone rule resolves off
            # the `mus` attribute every feature already carries.
            if ad.get("not_drawn"):
                continue
            raise SystemExit(f"areas.json: '{ad['id']}' has no tile_layer — every area must say "
                             f"which tile layer draws it, or its boundary silently never ships. "
                             f"To leave it undrawn on purpose, set `not_drawn` to the reason.")
        if lname not in BY_NAME:
            raise SystemExit(f"areas.json: '{ad['id']}' names tile_layer '{lname}', which is not "
                             f"in pipeline/deliver/tiles/layers.py")
        spec = BY_NAME[lname]
        write = writer_for(lname)
        polys = load_area_polys(None, ad)
        # Per-feature extras the def asked to `carry` — `cuts` on a closure zone. `_writer`
        # drops anything not in `spec.attrs`, so a carried field that the layer does not
        # declare is discarded silently; the check below turns that into a failure instead.
        extra = load_area_attrs(ad)
        for f in (ad.get("carry") or ()):
            if f not in spec.attrs:
                raise SystemExit(f"areas.json: '{ad['id']}' carries '{f}', which tile layer "
                                 f"'{lname}' does not declare in attrs — it would be dropped "
                                 f"at export and the tile would ship without it.")
        for name, geom in polys.items():
            if geom is None or geom.is_empty:
                continue
            # `area_id`, NOT `id`. The contract declares area_id as the feature id for
            # every admin layer, and `_writer` drops anything not in `spec.attrs` — so
            # `id` was silently discarded and six layers shipped with NO FEATURE ID AT
            # ALL. setFeatureState then addresses nothing: a park closure could never be
            # coloured, and nothing anywhere errored. Exactly the failure the tile
            # contract was created to stop, one seam further along.
            props = {"area_id": name, "name": display(name), "kind": ad.get("tile_kind")}
            props.update({k: v for k, v in (extra.get(name) or {}).items() if v is not None})
            # A CLOSURE EARNS FEWER PIXELS BEFORE IT MUST BE SHOWN.
            #
            # The ladder draws a polygon once its side exceeds `visible_px` on screen, which
            # is the right rule for "is this worth the bytes" and the wrong one for "may I
            # fish here". A provincial park appearing late costs a reader nothing; a
            # national park, an ecological reserve or land with no public access appearing
            # late means they plan a trip into water that is closed. So the def may lower
            # its own threshold — same derivation, a policy in the number — and the effect
            # is that closures survive two or three zooms further out than open parks.
            vpx = ad.get("tile_visible_px")
            mz = (ladder.zoom_for_area(geom.area, spec.minzoom,
                                       **({"visible_px": vpx} if vpx else {}))
                  if spec.ladder == "area" else spec.minzoom)
            write(_to4326(geom, tf), props, mz)
        print(f"  {lname:<16} <- {ad['id']:<26} {len(polys):>6,}")

    # Management units: geography, not a regulated area, so not in areas.json.
    spec = BY_NAME["mu"]
    write = writer_for("mu")
    gdf = gpd.read_file(gpkg, layer="wmu", engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    for _, row in gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        # `mu_id` ONLY. `region` and `region_name` were 3.0% of the archive between them —
        # a region number and a string like "Region 2 - Lower Mainland" repeated into every
        # tile the unit touches, read by nothing. A management unit's region is a property
        # of the unit, and 225 of those fit in the bundle if anything ever needs it.
        write(_to4326(g, tf),
              {"mu_id": str(row.get("WILDLIFE_MGMT_UNIT_ID") or "").strip()},
              spec.minzoom)
    print(f"  {'mu':<16} <- {'wmu (geography)':<26} {len(gdf):>6,}")

    # And the same fabric dissolved one level up. Eight regions, unioned from the units, so
    # the low-zoom map has a shape a person can navigate by instead of 225 boundaries no
    # tile can draw legibly -- and so the region is stated ONCE rather than on every unit.
    spec = BY_NAME["region"]
    write = writer_for("region")
    rid = "REGION_RESPONSIBLE_ID"
    rnm = "REGION_RESPONSIBLE_NAME"
    n_reg = 0
    if rid in gdf.columns:
        for key, part in gdf.dissolve(by=rid).iterrows():
            g = part.geometry
            if g is None or g.is_empty:
                continue
            write(_to4326(g, tf),
                  {"region_id": str(key).strip(),
                   "name": display(str(part.get(rnm) or "").strip())},
                  spec.minzoom)
            n_reg += 1
    print(f"  {'region':<16} <- {'wmu dissolved by region':<26} {n_reg:>6,}")

    # Private land: same reasoning as `mu`. Nothing regulates fishing by who holds title, so
    # it is not an area — but you still have to cross the ground to reach the water.
    #
    # THE DISSOLVED LAYER, NOT THE PARCELS. `land_parcels_private` is 1,290,764 individual
    # lots and 297 MiB of raw geometry; `land_parcels_crown` is the same fabric already
    # unioned into one polygon per ownership class, which is the only form that can ship.
    #
    # AND ONLY THE PRIVATE ONE. Writing all nine classes produced a 308 MB layer file —
    # bigger than every park, unit and region put together — to say "Crown land" over most
    # of British Columbia, which is its default state and not news. See layers.py.
    spec = BY_NAME["parcel"]
    write = writer_for("parcel")
    gdf = gpd.read_file(gpkg, layer="land_parcels_crown", engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    n_priv = 0
    for _, row in gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        if str(row.get("OWNER_TYPE") or "").strip() != "Private":
            continue
        write(_to4326(g, tf), {}, spec.minzoom)
        n_priv += 1
    print(f"  {'parcel':<16} <- {'private title (dissolved)':<26} {n_priv:>6,}")

    return {lname: close() for lname, (_, close) in writers.items()}


def export_contours(gpkg: str, out_dir: Path) -> dict:
    """Digitised lake bathymetry. Absent until `fetch_data.py bathymetry_contours` runs —
    a missing layer is a skip, never a failure, because the rest of the map is still valid."""
    import fiona
    import geopandas as gpd
    from pyproj import Transformer
    if "bathymetry_contours" not in set(fiona.listlayers(gpkg)):
        print("  contour          <- not fetched yet, skipping")
        return {"contour": 0}
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    spec = BY_NAME["contour"]
    write, close = _writer(out_dir, spec)
    gdf = gpd.read_file(gpkg, layer="bathymetry_contours", engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    depth_col = next((c for c in gdf.columns if "DEPTH" in c.upper()), None)
    wb_col = next((c for c in gdf.columns if "WATERBODY" in c.upper()), None)
    for _, row in gdf.iterrows():
        g = row.geometry
        if g is None or g.is_empty:
            continue
        d = row.get(depth_col) if depth_col else None
        d = float(d) if d is not None else None
        write(_to4326(g.boundary if g.geom_type in ("Polygon", "MultiPolygon") else g, tf),
              {"item": row.get(wb_col), "depth_m": d,
               "index": 1 if (d and d % 10 == 0) else None},
              ladder.zoom_for_contour(d, floor=spec.minzoom))
    n = close()
    print(f"  contour          <- bathymetry_contours {n:>6,}")
    return {"contour": n}


# NO RANK TABLE HERE. `data/fetch_data.py` assigns the rank when it fetches the gazetteer
# and writes it into every record, over nine place kinds. This file used to recompute it
# from a four-key table, so 3,459 of 4,677 places got a rank the gazetteer disagreed with —
# every locality, hamlet, neighbourhood and quarter — and since minzoom is derived from it,
# 1,731 localities were drawn from z10 instead of z14.
_DEFAULT_RANK = 5


def export_outside(boundary: Path, out_dir: Path) -> dict:
    """The world minus British Columbia, as one polygon.

    A negative space, deliberately. The alternative — draw BC and dim everything else in
    the client — needs the client to own a copy of the provincial outline and to composite
    it correctly on two renderers. One polygon in the tile costs nothing and cannot drift.

    The rectangle is the province's own bounds plus a margin — NOT the world. A
    world-minus-BC polygon lands in every tile of the pyramid at every zoom, which took the
    archive from 0.83 GB to over 1.8 GB for one decorative shape. The tiles only cover BC's
    neighbourhood in the first place, so a mask that covers the same neighbourhood covers
    everything a reader can actually pan to.
    """
    import geopandas as gpd
    from shapely.geometry import box, mapping
    from shapely.ops import unary_union

    spec = BY_NAME["outside"]
    write, close = _writer(out_dir, spec)
    gdf = gpd.read_file(boundary).to_crs(4326)
    bc = unary_union(list(gdf.geometry))
    minx, miny, maxx, maxy = bc.bounds
    M = 6.0                                   # degrees of margin, ~450 km at this latitude
    extent = box(minx - M, miny - M, maxx + M, maxy + M)
    write(mapping(extent.difference(bc)), {"side": "outside"}, spec.minzoom)
    n = close()
    print(f"  outside          <- {boundary.name} {n:>6,}")
    return {"outside": n}


def export_places(places_json: Path, out_dir: Path) -> dict:
    """Settlement labels, ranked so the style thins by importance rather than at random."""
    spec = BY_NAME["place"]
    write, close = _writer(out_dir, spec)
    seen: set[tuple] = set()
    for p in json.loads(places_json.read_text()):
        rank = int(p.get("rank", _DEFAULT_RANK))
        key = (p["name"], round(p["lon"], 3), round(p["lat"], 3))
        if key in seen:            # the gazetteer lists some places twice
            continue
        seen.add(key)
        write({"type": "Point", "coordinates": [round(p["lon"], _ROUND), round(p["lat"], _ROUND)]},
              {"name": p["name"], "kind": p.get("place"), "rank": rank},
              max(spec.minzoom, 4 + rank * 2))
    n = close()
    print(f"  place            <- {places_json.name} {n:>6,}")
    return {"place": n}
