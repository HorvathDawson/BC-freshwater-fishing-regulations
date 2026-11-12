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
from pathlib import Path

from pipeline.deliver.tiles import ladder
from pipeline.deliver.tiles.layers import BY_NAME, LAKE_LINK, LayerSpec
from pipeline.deliver.bundle.place_names import area_display, area_name_uncleaned, is_shouting
from pipeline.deliver.tiles.names import display
from pipeline.common.fishing_water import not_fishing_kind

_ROUND = 6                      # ~11 cm; tippecanoe quantises to the tile grid anyway



def region_id_of(name) -> str:
    """One spelling of a region's id on every `region` feature: "1" … "8", "7A", "7B" (P18)."""
    return str(name).strip().upper()

def _writer(out_dir: Path, spec: LayerSpec, file: str | None = None):
    """Features of `spec`'s tile layer, written to `<file or spec.name>.geojsonl` (`file`: a layer file drawn INTO
    another tile layer, `layers.DRAWN_INTO`)."""
    fh = (out_dir / f"{file or spec.name}.geojsonl").open("w")
    allowed = set(spec.attrs)
    n = 0
    shouting: list[str] = []
    uncleaned: list[str] = []

    def write(geom: dict, props: dict, minzoom: int, maxzoom: int | None = None) -> None:
        nonlocal n
        clean = {k: v for k, v in props.items()
                 if k in allowed and v is not None and v != ""}
        # A MAP LABEL NEVER SHOUTS (T113): the same check the export runs on every shown name
        # (`place_names.is_shouting`, acronyms allowed). `display` makes it true by construction;
        # this is what fails the build the day a name reaches a tile some other way.
        if isinstance(clean.get("name"), str) and is_shouting(clean["name"]):
            shouting.append(clean["name"])
        # AN AREA'S LABEL IS ITS ONE DISPLAY NAME (T679 #4): an admin feature (it has `area_id`) carries
        # `place_names.area_display` of it — never the OpenStreetMap id or the source's cut-off word.
        if "area_id" in clean and isinstance(clean.get("name"), str) and area_name_uncleaned(clean["name"]):
            uncleaned.append(clean["name"])
        fh.write(json.dumps({
            "type": "Feature", "geometry": geom, "properties": clean,
            "tippecanoe": {"layer": spec.name,
                           "minzoom": max(spec.minzoom, min(minzoom, spec.maxzoom)),
                           "maxzoom": spec.maxzoom if maxzoom is None else maxzoom},
        }, separators=(",", ":")) + "\n")
        n += 1

    def close() -> int:
        fh.close()
        if shouting:
            raise SystemExit(f"{spec.name}: {len(shouting)} tile name(s) still in capitals, e.g. "
                             f"{sorted(set(shouting))[:5]} — every shown name goes through "
                             "`tiles.names.display` (T113)")
        if uncleaned:
            raise SystemExit(f"{spec.name}: {len(uncleaned)} area name(s) not cleaned, e.g. "
                             f"{sorted(set(uncleaned))[:5]} — an area's label is `place_names.area_display` "
                             "of it (T679 #4)")
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


class Owners:
    """WHAT THE REGISTRY SAYS OF EACH SECTION — its water's NAME and KIND, and the ghosts it does not
    draw — read once from `registry.json`, the one source the bundle's `item` table is written from.

    The tile used to take a feature's name from the graph node's label, and 86 sections disagreed
    with the water the bundle opens on a tap ("Two Forty-One Creek" labelled two sections of the
    Penticton Creek; 72 waters named only through a variant had no label at all). The registry
    already decided the name and the kind (FREV/dataflow DF-1, DF-2); the tile reads them.
    """

    def __init__(self, build_dir: Path):
        from pipeline.common.registry_kinds import is_water
        items = json.loads((build_dir / "registry.json").read_text(encoding="utf-8"))["items"]
        self.name: dict[str, str] = {}
        self.kind: dict[str, str] = {}
        #: (T493) every water item each section is a piece of — `fill_zooms` closes holes inside ONE water only
        self.items: dict[str, frozenset[str]] = {}
        parents = {i["part_of"] for i in items if i.get("part_of")}
        # A LAKE CUT INTO PARTS owns no section (user ruling 2026-10-03) — its whole polygon is a
        # ghost under its parts, and is NOT DRAWN: a feature nothing names and nothing colours.
        self.ghosts: set[str] = {f"lake:{i['id'].split(':', 1)[1]}" for i in items
                                 if i["id"] in parents and i["id"].startswith("wbk:")}
        for i in items:
            if not is_water(i):
                continue
            for sec in i.get("section_ids", []):
                self.name.setdefault(sec, i.get("name") or "")
                self.kind.setdefault(sec, i.get("kind") or "")
                self.items[sec] = self.items.get(sec, frozenset()) | {i["id"]}

    def label(self, sec: str, node) -> str:
        """The owner's name, else (a section of no named water) the node's own label."""
        return display(self.name.get(sec) or node.display_name)


def export_streams(build_dir: Path, out_dir: Path, *, limit: int | None = None,
                   tidal: frozenset = frozenset(), tidal_geoms: list | None = None) -> dict:
    """Flowing water, from the geometry sidecar: every stream LINE, and the route through a
    polygon that is a STREAM's own (`Owners.kind`: a river's wide reach, a slough — the registry
    folded them into their water, `registry.flowing`), written to the `stream` layer under the
    polygon's section id so the river has no hole at the zooms its polygon is not yet drawn at
    (FREV/sloughs F4: 65 of 141 such polygons first drew 1-7 zoom levels later than the line
    beside them). Lakes' routes go to `under_lake`; lake and wetland shapes come from
    ``export_waterbodies`` below."""
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

    _require_membership(graph)
    owners = Owners(build_dir)

    def neighbour_weight(sec: str) -> tuple[int | None, int | None]:
        """(order, magnitude) for a stream's polygon: the LARGEST of its node's own and the stream
        pieces it joins — the river is as big through its wide reach as beside it, and a polygon
        node's own magnitude (the fids inside it) can read smaller than the line's (measured:
        25 of the Rancheria's 42 drew 1-2 zooms later than their neighbours on the node's own)."""
        node = graph.nodes[sec]
        orders = [node.stream_order or 0]
        mags = [node.stream_magnitude or 0]
        for ei in list(graph.up_adj.get(sec, ())) + list(graph.down_adj.get(sec, ())):
            e = graph.edges[ei]
            o = graph.nodes.get(e.from_node if e.to_node == sec else e.to_node)
            if o is not None and _kind(o) == "stream":
                orders.append(o.stream_order or 0)
                mags.append(o.stream_magnitude or 0)
        return max(orders) or None, max(mags) or None

    spec = BY_NAME["stream"]
    _write, close = _writer(out_dir, spec)
    #: (T493) the first zoom each drawn stream-layer feature is drawn at, for the links through hidden lakes
    drawn_z: dict[str, int] = {}
    #: (T493 reopened) the stream-layer features in write order, (section, props), written only once every zoom is
    #: known and `fill_zooms` has closed the holes inside each water; and each stream's own polygon's first zoom
    pending: list[tuple[str, dict]] = []
    poly_z: dict[str, int] = {}

    def write(props: dict, minzoom: int, sec: str) -> None:
        drawn_z[sec] = max(spec.minzoom, min(minzoom, spec.maxzoom))
        pending.append((sec, props))

    ul_spec = BY_NAME["under_lake"]
    ul_write, ul_close = _writer(out_dir, ul_spec)
    lake_spec = BY_NAME["lake"]
    link_write, link_close = _writer(out_dir, spec, LAKE_LINK)
    #: (T493) every lake route kept, in order: (section, route, its under_lake zoom); and each one's lake's first zoom
    routes: list[tuple[str, object, int]] = []
    lake_z: dict[str, int] = {}
    # The waterbody outlines, to sanity-check each route against the lake it crosses.
    wb_path = build_dir / "waterbody_polys.pkl"
    if not wb_path.exists():                # P2: no missing-file fallbacks
        raise FileNotFoundError(f"{wb_path} not found — an atlas build writes it")
    wb_polys = pickle.load(wb_path.open("rb"))
    dropped_routes = 0
    dropped_outside = 0
    run_zoom = side_channel_run_zooms(
        graph, [s for s, g in geoms.items() if s in graph.nodes and not graph.nodes[s].out_of_bc],
        lambda s: ladder.zoom_for_magnitude(graph.nodes[s].stream_magnitude, spec.minzoom))
    lowered = sum(1 for s, z in run_zoom.items()
                  if z < ladder.zoom_for_magnitude(graph.nodes[s].stream_magnitude, spec.minzoom))
    print(f"  side channels: {len(run_zoom):,} section(s) in runs, {lowered:,} drawn earlier to match their run")
    dropped_tidal = 0
    for sec, g in geoms.items():
        node = graph.nodes.get(sec)
        if node is None:
            continue
        if sec in tidal:
            # TIDAL WATER IS NOT DRAWN (T166, owner 2026-10-28): the piece below a tidal boundary comes off
            # the map; its footprint joins the `tidal_water` area (`tiles/tidal.py`), and `undrawn.json`
            # names it (layer `tidal`) for the bundle's proof.
            dropped_tidal += 1
            if tidal_geoms is not None:
                tidal_geoms.append(g)
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
        if _kind(node) != "stream" and owners.kind.get(sec) == "stream":
            # A STREAM'S OWN POLYGON: its route is the river, under the polygon's section id (the
            # same id the polygon draws with in `lake` / `wetland`), at the river's own weight.
            ord_, mag = neighbour_weight(sec)
            # ...and NO LATER THAN a side-channel run it joins (T306): the run's lines come in
            # together from the run's biggest piece (`side_channel_run_zooms`), so a polygon
            # keyed on magnitude alone drew 3-5 zooms after the slough beside it (Maria Slough,
            # Hogranch Channel).
            run_z = [run_zoom[o] for ei in list(graph.up_adj.get(sec, ())) + list(graph.down_adj.get(sec, ()))
                     for e in (graph.edges[ei],)
                     for o in ((e.from_node if e.to_node == sec else e.to_node),) if o in run_zoom]
            write({"section_id": sid[sec], "name": owners.label(sec, node), "ord": ord_},
                  min([ladder.zoom_for_magnitude(mag, spec.minzoom)] + run_z), sec)
            # (T493 reopened) the polygon is the river where it is drawn: its first zoom exactly as
            # `export_waterbodies` draws it (a ghost, not-fishing water or an undrawn wetland is not drawn)
            pl = wb_polys.get(sec)
            p_spec = BY_NAME["wetland" if _kind(node) == "wetland" else "lake"]
            if (pl is not None and not pl.is_empty and sec not in owners.ghosts
                    and not getattr(node, "not_fishing", "")
                    and (_kind(node) != "wetland" or wetland_is_drawn(sec, node, owners))):
                poly_z[sec] = max(p_spec.minzoom, min(ladder.zoom_for_area(pl.area, p_spec.minzoom), p_spec.maxzoom))
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
                # Written after the loop (T493): where the lake is hidden the route is drawn as the creek instead.
                routes.append((sec, g, ladder.zoom_for_area(g.length * g.length, ul_spec.minzoom)))
                # (T493) the LAKE's first zoom, exactly as `export_waterbodies` draws it; only a lake that is drawn
                # with an identity (a polygon, not a ghost under its parts, not "not fishing water") gets a link
                if (pl is not None and not pl.is_empty and sec not in owners.ghosts
                        and not getattr(node, "not_fishing", "")):
                    lake_z[sec] = max(lake_spec.minzoom, min(ladder.zoom_for_area(pl.area, lake_spec.minzoom),
                                                             lake_spec.maxzoom))
            continue
        write({
            # THE HANDLE, not the string — the bundle keys every section table by it, and
            # this is the feature id the app sets state on, so the two must be the same
            # number. See pipeline/common/section_handles.
            "section_id": sid[sec],
            # THE REGISTRY'S NAME — the water the bundle opens on a tap (`Owners`).
            "name": owners.label(sec, node),
            # NO `mag`. It is the input to the zoom ladder, and the ladder has already run
            # by the time this feature is written — `zoom_for_magnitude` below turns it into
            # the per-feature minzoom tippecanoe actually uses. Shipping the magnitude too
            # sent the same fact twice, the second copy to a client that never read it:
            # 2.3% of the archive, on a property with no consumer in the app or the style.
            # NO `item`, `alt`, `mus`, `areas` either (2026-10-03): the app read none of them —
            # identity, search and containment are the bundle's (`item_section`, `alias`,
            # `area:` items) — and `_writer` drops what `layers.py` does not list.
            "ord": node.stream_order,
        }, run_zoom.get(sec) or ladder.zoom_for_magnitude(node.stream_magnitude, spec.minzoom), sec)
    # (T493 reopened) NO HOLE INSIDE ONE WATER: a piece hidden at a zoom its own water is drawn at on both sides of it
    # is drawn from that zoom (Nicomen Slough z7-z10, Camp Slough z11). Then every stream feature is written.
    filled = fill_zooms(graph, drawn_z, owners.items, poly_z)
    print(f"  holes filled   {sum(1 for s in drawn_z if filled[s] < drawn_z[s]):>9,}  (a piece of a water drawn "
          "on both sides of it, T493)")
    drawn_z = filled
    for sec, props in pending:
        _write(_to4326(geoms[sec], tf), props, drawn_z[sec])
    # (T493) THE CREEK THROUGH A HIDDEN LAKE: below the lake's first zoom its route is drawn in the STREAM layer under
    # the LAKE's section id, so the creek has no gap, the stretch wears the lake's status and a tap opens the lake.
    links = lake_link_zooms(graph, lake_z, drawn_z)
    geom_of = {sec: g for sec, g, _ in routes}
    for sec in sorted(links, key=lambda s: sid[s]):                     # DETERMINISM
        link_write(_to4326(geom_of[sec], tf), {"section_id": sid[sec], "ord": neighbour_weight(sec)[0]},
                   links[sec], lake_z[sec] - 1)
    for sec, g, z in routes:
        # the dotted thread through a lake is drawn only where the lake is (a link draws the route below it)
        ul_write(_to4326(g, tf), {}, max(z, lake_z[sec]) if sec in links else z)
    n, n_ul, n_link = close(), ul_close(), link_close()
    print(f"  lake links     {n_link:>9,}  (a creek through a lake hidden at its zoom, T493)")
    print(f"  stream         {n:>9,}"
          + (f"  ({dropped_outside:,} outside BC, not drawn)" if dropped_outside else "")
          + (f"  ({dropped_tidal:,} tidal, not drawn)" if dropped_tidal else ""))
    print(f"  under_lake     {n_ul:>9,}"
          + (f"  ({dropped_routes:,} dropped: longer than "
             f"{_MAX_ROUTE_OVER_DIAGONAL:g}x their own waterbody)" if dropped_routes else ""))
    return {"stream": n, "under_lake": n_ul, LAKE_LINK: n_link}


def fill_zooms(graph, zoom: dict[str, int], items_of: dict[str, frozenset[str]],
               poly_z: dict[str, int] | None = None) -> dict[str, int]:
    """(T493 reopened, 2026-11-08) {stream-layer section: its first zoom, holes closed}. Inside one water every piece
    gets its own first zoom (a line by magnitude, a stream's own polygon's route by its neighbours, the polygon by its
    area), so a water could break: Nicomen Slough's pieces 59594 / 1454487 / 59593 hidden z7-z10 between two z7 pieces.

    A piece of water I hidden at zoom z, with drawn water OF I upstream (`up_adj`) AND downstream (`down_adj`) of it
    — looking through hidden pieces of I, never through anything else — is drawn from z WHEN THE WATER VISIBLY BREAKS
    there: the drawn water either side is two SEPARATE drawn runs of I, or on one side a drawn piece DANGLES into the
    hidden stretch (no drawn piece of I next to it on that side) and the other side is another drawn piece (`holed`).
    A braid or side channel leaving and rejoining one drawn run at junctions, or one drawn piece, is no hole. (Measured on the T497 atlas:
    "drawn water of I on both sides" alone filled 16,754 pieces, and "only where I visibly ends" still 5,382 — braids
    and side channels in and out of a river's own polygon, Stikine, Liard, Swanson...) THE SAME WATER ONLY: across
    waters this would draw every lake-outlet creek at its river's zoom. A stream's own polygon counts as drawn from
    its polygon's zoom (`poly_z`; its route need not be).

    Per zoom: the drawn runs of each water (union-find over the edges between drawn pieces of it), then two linear
    floods through hidden pieces of it, one downstream and one upstream, each carrying the run and piece it started
    from and whether that piece dangles; a (piece, water) keeps at most two of each per flood (exactly enough for
    `holed`), so each is pushed a bounded number of times and a braid's loop ends. Repeated only when a piece of two waters filled for one is new drawn water
    of the other, so the result is a fixed point. Zooms are only ever lowered; a section in no water (`items_of`) is
    never filled and never walked through. Deterministic: a function of the sets, not of any order."""
    poly_z = poly_z or {}
    out = dict(zoom)
    if not out:
        return out
    for z in range(min([*out.values(), *poly_z.values()]), max(out.values())):
        def drawn(s: str) -> bool:
            return out[s] <= z or poly_z.get(s, z + 1) <= z

        while True:
            on = {s for s in out if drawn(s) and items_of.get(s)}
            parent: dict[tuple[str, str], tuple[str, str]] = {(s, i): (s, i) for s in on for i in items_of[s]}

            def root(k):
                while parent[k] != k:
                    parent[k] = parent[parent[k]]
                    k = parent[k]
                return k

            for s in on:
                for ei in graph.down_adj.get(s, ()):
                    n = graph.edges[ei].to_node
                    if n in on:
                        for i in items_of[s] & items_of[n]:
                            a, b = root((s, i)), root((n, i))
                            if a != b:
                                parent[max(a, b)] = min(a, b)

            def flood(adj, step) -> dict[tuple[str, str], tuple[set, set, set]]:
                """(hidden piece, water) -> what it is reached from, walking `adj` through hidden pieces of the water:
                (the drawn runs, the drawn pieces, the drawn pieces that DANGLE into it — no drawn piece of the water
                next on that side, so the line visibly stops there). At most two of each are kept: all `holed` asks."""
                got: dict[tuple[str, str], tuple[set, set, set]] = {}
                stack = []
                for s in sorted(on):                                     # DETERMINISM of the walk (not of the answer)
                    nbrs = [step(graph.edges[ei]) for ei in adj.get(s, ())]
                    for i in sorted(items_of[s]):
                        dangles = not any(n in on and i in items_of[n] for n in nbrs)
                        stack.append((s, i, (root((s, i)), s, dangles)))
                while stack:
                    s, i, lab = stack.pop()
                    r, piece, dangles = lab
                    for ei in adj.get(s, ()):
                        n = step(graph.edges[ei])
                        if n in out and n not in on and i in items_of.get(n, ()):
                            runs, pieces, dang = got.setdefault((n, i), (set(), set(), set()))
                            grew = False
                            for bag, v, ok in ((runs, r, True), (pieces, piece, True), (dang, piece, dangles)):
                                if ok and v not in bag and len(bag) < 2:
                                    bag.add(v)
                                    grew = True
                            if grew:
                                stack.append((n, i, lab))
                return got

            def holed(up, down) -> bool:
                """Drawn water of I on both sides that is (a) two separate drawn runs, or (b) a piece dangling into
                the hidden stretch on one side and ANOTHER drawn piece on the other (Nicomen: 1454621 drawn, all its
                water above hidden, the hidden channel back to 1266795 — one run through 59599, still a visible stop).
                A side channel out of and back into the SAME drawn piece is neither (Swanson)."""
                (ur, up_, ud), (dr, dp, dd) = up, down
                if any(a != b for a in ur for b in dr):
                    return True
                return any(a != b for a in ud for b in dp) or any(a != b for a in dd for b in up_)

            from_up = flood(graph.down_adj, lambda e: e.to_node)       # drawn water of I upstream of the piece
            from_down = flood(graph.up_adj, lambda e: e.from_node)     # ...and downstream of it
            new = {s for k, ups in from_up.items() if k in from_down and holed(ups, from_down[k]) for s in (k[0],)}
            if not new:
                break
            # a piece of two waters drawn for one of them is drawn water of the other too: flood again (rare; the
            # rounds are bounded by the pieces filled)
            for s in new:
                out[s] = z
    return out


def lake_link_zooms(graph, lake_z: dict[str, int], drawn_z: dict[str, int]) -> dict[str, int]:
    """(T493, owner 2026-11-07) {lake section: the first zoom its LINK is drawn at} — the lakes a drawn creek runs
    THROUGH at zooms the lake itself is not drawn at (Foley Lake on Foley Creek: the lake from z10, the creek on both
    sides from z8). A link is drawn from the first zoom at which water is drawn on BOTH sides — upstream (`up_adj`)
    and downstream (`down_adj`) — to the zoom before the lake's own (`lake_z`); a lake with drawn water on one side only
    (a headwater lake, a lake whose inflows all come in later) leaves no gap and gets none.

    THE WATER ON EITHER SIDE MAY BE ANOTHER LAKE (a chain of small lakes on one creek): the walk looks THROUGH it —
    a neighbouring lake is drawn from its own zoom, or from the zoom the water beyond it is, whichever is first, since
    below its own zoom it is linked as well, by the same rule. Only lakes are looked through; a section in neither map
    (a wetland, a lake with no polygon) is water not drawn. `drawn_z`: the first zoom of every stream-layer feature
    (lines and a stream's own polygons' routes)."""
    never = 1 << 30

    def side(lake: str, up: bool, path: frozenset) -> int:
        """The first zoom at which water on that side of `lake` is drawn, looking through lakes (never back through
        one already on `path`: a loop of lakes ends). Walked afresh per lake, no memo: a memo filled inside a loop
        would keep the loop's cut-short answer; a chain of lakes is a few long."""
        edges = graph.up_adj.get(lake, ()) if up else graph.down_adj.get(lake, ())
        best = never
        for i in edges:
            e = graph.edges[i]
            n = e.from_node if up else e.to_node
            if n in drawn_z:
                best = min(best, drawn_z[n])
            elif n in lake_z and n not in path:
                best = min(best, lake_z[n], side(n, up, path | {n}))
        return best

    links: dict[str, int] = {}
    for lake in sorted(lake_z):                                          # DETERMINISM
        z = max(side(lake, True, frozenset({lake})), side(lake, False, frozenset({lake})))
        if z < lake_z[lake]:
            links[lake] = z
    return links


#: (T299) The zooms a stream name's lines are merged for: one set of lines per zoom, the last one serving every zoom up
#: to the archive's top. `stream__label` starts at the first.
NAME_BANDS = (8, 9, 10, 11, 12, 13)
#: AND SMOOTHED FOR THAT ZOOM. MapLibre places a line label only where the line turns less than `text-max-angle` within
#: a few characters (`checkMaxAngle` sums every corner under the window), and a river at the label's own scale is all
#: corners: measured on the Lower Mainland at z11, every vertex of the Coquitlam, Alouette, Salmon and Serpentine lines
#: turned more than 20 degrees, 10 px apart, and one stream of ten on screen was named. So each line is simplified by
#: `NAME_SIMPLIFY_PX` at its zoom (Douglas-Peucker) and its corners then rounded (`NAME_CHAIKIN` passes of Chaikin's
#: corner cutting): a name runs along the water's course, within a few pixels of the drawn line, never across it.
NAME_SIMPLIFY_PX = 4.0
NAME_CHAIKIN = 3
_M_PER_PX_Z0 = 102_700            # metres per pixel of a 512 px tile at z0, at 49 N


def _chaikin(pts: list[tuple[float, float]], passes: int) -> list[list[float]]:
    """Chaikin's corner cutting, ends kept: each pass replaces every inner corner with two points a quarter of the way
    along its edges, halving the turn at each vertex."""
    for _ in range(passes):
        if len(pts) < 3:
            break
        out = [pts[0]]
        for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
            out += [(0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1), (0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1)]
        pts = [out[0], *out[2:-1], pts[-1]]
    return [[round(x, _ROUND), round(y, _ROUND)] for x, y in pts]


def export_stream_names(out_dir: Path) -> dict:
    """(T299) The `stream_name` layer, DERIVED FROM THE WRITTEN `stream` LAYER FILE and nothing else: for each zoom band
    (`NAME_BANDS`), the named sections drawn by that zoom, grouped by name and line-merged into the longest lines they
    make, each line carrying the name and the largest `ord` merged into it, and drawn at that band only. Per band, not
    per section's own first zoom: the ladder gives neighbouring sections of one river different first zooms, so merging
    by first zoom left 54,195 lines of 56,443 sections. A pure function of `stream.geojsonl`, so the tiles-only rebuild
    derives it again and it can never disagree with the streams it names (see `layers.stream_name`)."""
    from collections import defaultdict

    from shapely import line_merge
    from shapely.geometry import MultiLineString, shape

    spec = BY_NAME["stream_name"]
    src = out_dir / f"{BY_NAME['stream'].name}.geojsonl"
    pieces: dict[str, list[tuple[int, object]]] = defaultdict(list)
    ords: dict[str, dict[tuple[float, float], int]] = defaultdict(dict)
    with src.open() as fh:
        for line in fh:
            f = json.loads(line)
            name = (f.get("properties") or {}).get("name")
            if not name:
                continue
            z = int(f.get("tippecanoe", {}).get("minzoom", 0))
            g = shape(f["geometry"])
            o = int(f["properties"].get("ord") or 0)
            for p in (g.geoms if g.geom_type == "MultiLineString" else [g]):
                if p.geom_type != "LineString" or len(p.coords) < 2:
                    continue
                pieces[name].append((z, p))
                for c in (p.coords[0], p.coords[-1]):
                    ords[name][c] = max(ords[name].get(c, 0), o)
    write, close = _writer(out_dir, spec)
    for name, parts in pieces.items():
        for i, band in enumerate(NAME_BANDS):
            top = NAME_BANDS[i + 1] - 1 if i + 1 < len(NAME_BANDS) else spec.maxzoom
            drawn = [p for z, p in parts if z <= (band if top < spec.maxzoom else spec.maxzoom)]
            if not drawn:
                continue
            merged = line_merge(MultiLineString([p.coords for p in drawn])) if len(drawn) > 1 else drawn[0]
            tol = NAME_SIMPLIFY_PX * _M_PER_PX_Z0 / 2 ** band / 111_000    # degrees, near enough
            for ln in [x for x in getattr(merged, "geoms", [merged]) if x.geom_type == "LineString"]:
                o = max((ords[name].get(c, 0) for c in ln.coords), default=0)
                ln = ln.simplify(tol, preserve_topology=False)
                if ln.is_empty or len(ln.coords) < 2:
                    continue
                write({"type": "LineString", "coordinates": _chaikin([tuple(c) for c in ln.coords], NAME_CHAIKIN)},
                      {"name": name, "ord": o or None}, band, top)
    return {"stream_name": close()}


#: THE UNDRAWN LIST (T64): the section handles the tile export left out on purpose, by layer, in
#: `<layers>/UNDRAWN_FILE`. The bundle build reads it (`bundle.undrawn.check`) and refuses one a
#: water rule, an item or a gauge would need on the map; `atlas.meta.json` states the counts.
UNDRAWN_FILE = "undrawn.json"


def wetland_is_drawn(nid: str, node, owners: "Owners") -> bool:
    """IS THIS WETLAND POLYGON DRAWN? (T64, owner 2026-10-23: "removing all unnamed wetlands without
    direct regulation ... including dropping ones only regulated by admin area regs like liard").

    KEPT: a polygon in a registry water item — a named wetland, a slough or side channel folded into
    a river (`registry.flowing`), any item a row can name — or a stream-kind polygon, or one with a
    gazetted name of its own. DROPPED: every other wetland — unnamed and in no item, so only zone and
    area-only rules (watershed / admin-area rows like the Liard) can bind it, and its colour was never
    its own. The graph, the atlas and the reach are unchanged: this cuts the DRAWN polygon only. The
    bundle build proves the cut (`bundle.undrawn.check`: no dropped polygon is bound by a water rule,
    owned by an item or gauged)."""
    if owners.name.get(nid) or owners.kind.get(nid) == "stream":
        return True
    return bool((node.display_name or "").strip() or node.name_tuples)


def export_waterbodies(build_dir: Path, gpkg: str, out_dir: Path, *, tidal: frozenset = frozenset(),
                       tidal_geoms: list | None = None) -> dict:
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
    owners = Owners(build_dir)

    writers: dict[str, tuple] = {}
    no_geom = ghosts = 0
    undrawn: list[int] = []
    for nid, node in graph.nodes.items():
        kind = _kind(node)
        if kind not in ("lake", "wetland"):
            continue
        if nid in owners.ghosts:
            ghosts += 1
            continue
        if nid in tidal:                                    # T166: off the map, into `tidal_water`
            g = geoms.get(nid)
            if tidal_geoms is not None and g is not None and not g.is_empty:
                tidal_geoms.append(g)
            continue
        if kind == "wetland" and not wetland_is_drawn(nid, node, owners):
            undrawn.append(sid[nid])
            continue
        g = geoms.get(nid)
        if g is None or g.is_empty:
            no_geom += 1
            continue
        if getattr(node, "not_fishing", ""):
            # NOT FISHING WATER (T152): no water layer, no identity — the decorative layer only,
            # with WHAT it is (T292: sewage | tailings | dump) for the tap sheet.
            spec = BY_NAME["not_fishing_water"]
            if spec.name not in writers:
                writers[spec.name] = _writer(out_dir, spec)
            writers[spec.name][0](_to4326(g, tf), {"kind": not_fishing_kind(node.not_fishing)},
                                  ladder.zoom_for_area(g.area, spec.minzoom))
            continue
        lname = "wetland" if kind == "wetland" else "lake"
        spec = BY_NAME[lname]
        if lname not in writers:
            writers[lname] = _writer(out_dir, spec)
        write, _ = writers[lname]
        write(_to4326(g, tf), {
            "section_id": sid[nid],
            "name": owners.label(nid, node),
            # THE WATER KIND, the registry's: `stream` on a river's wide reach or a slough's
            # polygon, so the style draws it in stream colours (`lake_flowing`); else the
            # layer's own kind. The same `item.kind` the bundle carries, from the same file.
            "water": owners.kind.get(nid) or kind,
            "area_m2": round(g.area),
        }, ladder.zoom_for_area(g.area, spec.minzoom))
    counts = {lname: close() for lname, (_, close) in writers.items()}
    (out_dir / UNDRAWN_FILE).write_text(json.dumps({
        "$comment": "Section handles the tile export did not draw on purpose (T64: unnamed wetlands "
                    "in no item, bound only by zone / area-only rules). Read by the bundle build "
                    "(pipeline.deliver.bundle.undrawn), which refuses one a water rule, item or gauge needs.",
        "wetland": sorted(undrawn),
        # T166: the pieces below the tidal boundaries — every one, stream and polygon (the reach run's set)
        **({"tidal": sorted(sid[s] for s in tidal if s in sid)} if tidal else {})},
        separators=(",", ":")) + "\n", encoding="utf-8")
    for lname, n in counts.items():
        print(f"  {lname:<14} {n:>9,}")
    print(f"  wetland not drawn  {len(undrawn):>9,}  (unnamed, in no item: `{UNDRAWN_FILE}`)")
    if no_geom:
        print(f"  ({no_geom:,} waterbody nodes had no polygon — see build.py minting)")
    if ghosts:
        print(f"  ({ghosts} lake(s) cut into parts not drawn as a whole)")
    return counts


def _kind(node) -> str:
    return node.kind.value if hasattr(node.kind, "value") else str(node.kind)


def is_side_channel_piece(node) -> bool:
    """A stream piece carrying no main-flow line: a braid's side channel or its connectors (FWA 1100,
    1150, 1350, 1450), never a river's own line and never a canal (`fwa_edge_types`)."""
    from pipeline.common import fwa_edge_types as et
    return (_kind(node) == "stream" and bool(node.edge_types) and not et.is_main_flow(node)
            and not et.is_manmade_secondary_only(node))


def side_channel_run_zooms(graph, drawn, zoom_of) -> dict[str, int]:
    """ONE FIRST ZOOM PER SIDE CHANNEL (owner 2026-10-28, FIX "complete"): {section: minzoom} for every
    drawn side-channel section, the LOWEST `zoom_of(section)` over the connected run of side-channel
    sections it belongs to.

    The ladder sets a line's first zoom from its own magnitude, and a braid's channel is cut into sections
    whose FWA magnitudes differ (the Skeena at Salvus: ord-6 pieces from z10, a magnitude-1 piece between
    them from z13). Below z13 that channel left the river and stopped on a bar. Drawn as a run, it is
    there whole or not at all, and the run comes in when its biggest piece does."""
    side = {s for s in drawn if s in graph.nodes and is_side_channel_piece(graph.nodes[s])}
    out: dict[str, int] = {}
    seen: set[str] = set()
    for start in sorted(side):                       # DETERMINISM
        if start in seen:
            continue
        run, stack = [], [start]
        seen.add(start)
        while stack:
            u = stack.pop()
            run.append(u)
            nb = {graph.edges[i].from_node for i in graph.up_adj.get(u, ())}
            nb |= {graph.edges[i].to_node for i in graph.down_adj.get(u, ())}
            for v in nb & side:
                if v not in seen:
                    seen.add(v)
                    stack.append(v)
        z = min(zoom_of(u) for u in run)
        for u in run:
            out[u] = z
    return out


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
        # DRAWN, BUT ONLY THESE (T202, owner 2026-10-27): `tile_names` keeps the named areas in the tile and leaves the
        # rest of the def cut and stamped but undrawn — the WMAs, where only Creston Valley carries a regulation. A
        # listed name the def does not produce fails, so a renamed source area cannot silently empty the layer.
        if ad.get("tile_names"):
            want = set(ad["tile_names"])
            if want - set(polys):
                raise SystemExit(f"areas.json: '{ad['id']}' tile_names {sorted(want - set(polys))} are not areas "
                                 f"it produces")
            polys = {k: v for k, v in polys.items() if k in want}
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
            props = {"area_id": name, "name": area_display(name), "kind": ad.get("tile_kind")}
            if lname == "region":
                # P18: the region layer is keyed by `region_id` (the style's feature id and label). These nine came
                # from areas.json with a name only; give them the id the dissolved units carry ("7A", not "7a").
                props["region_id"] = region_id_of(name)
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
                  {"region_id": region_id_of(key),
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
    from pipeline.common.licence import entries
    for p in entries(json.loads(places_json.read_text()), "places"):   # `_licence` (ODbL) beside the list
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
