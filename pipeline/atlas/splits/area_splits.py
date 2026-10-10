"""Blanket area closures (pipeline/areas.json) — cut ALL streams that cross an admin polygon
at first-enter/last-exit, then flag the inside reaches (in_areas). Unlike splits.json area_boundary
anchors (scoped to ONE system via applies_to), these apply to every stream in the polygon.

Consumed by the graph build; a `within(area)` reg binds to the inside sections at match time.
Selector per area: name_field, plus EITHER a fetched gpkg `layer` (with which:"all" or a SQL
`where`) OR a curated `file` of hand-drawn polygons — see `load_area_polys`.
"""

from __future__ import annotations

import json
from pathlib import Path

from project_config import get_config
from pipeline.common.models import AnchorType, BlkChain, SplitPoint
from pipeline.atlas.splits.anchors import _area_transition_measures
from pipeline.common.curated import CURATED, SOURCE

ROOT = get_config().project_root
_GPKG = str(get_config().fwa_data_gpkg)


#: The cut modes an area may declare (`areas.json` `cut`; see its `_cut_policy`).
CUT_MODES = ("clean", "first_last")


def cut_mode(area_def: dict) -> str | None:
    """`"clean"`, `"first_last"`, or None (membership only). Refuses anything else — including the
    old boolean `true`, which no longer says HOW to cut."""
    v = area_def.get("cut")
    if v is False:
        return None
    if v not in CUT_MODES:
        raise ValueError(f"areas.json {area_def.get('id')!r}: cut must be one of {CUT_MODES} or false, "
                         f"got {v!r}")
    if v == "clean" and not (area_def.get("rejoin_m") or 0) > 0:
        raise ValueError(f"areas.json {area_def.get('id')!r}: a clean cut states rejoin_m")
    if v == "first_last" and (area_def.get("crossing_zone_m") is not None
                              or area_def.get("rejoin_m") is not None):
        raise ValueError(f"areas.json {area_def.get('id')!r}: a first_last area is cut where it "
                         f"crosses, with no zone or rejoin distance")
    if v == "first_last" and area_def.get("membership") != "both_sides":
        raise ValueError(f"areas.json {area_def.get('id')!r}: first_last cuts declare "
                         f"membership: both_sides")
    return v


def load_area_split_defs(path: str | None = None) -> list[dict]:
    p = Path(path) if path else CURATED.waters.areas   # curated: a missing file raises (P2)
    return json.loads(p.read_text()).get("areas", [])


def apply_remap(g, name_field: str, remap: dict | None, layer: str = "") -> None:
    """Reassign ``name_field`` values in place, before a dissolve, because ADMINISTRATION MOVES
    AND GEOGRAPHY DOES NOT.

    Haida Gwaii (MUs 6-12, 6-13) still carries ``REGION_RESPONSIBLE_ID = 6`` in the source layer,
    but the 2025-2027 synopsis administers it from Region 1: *"Freshwater angling regulations and
    fisheries management for Haida Gwaii ... are now within Region 1."* Dissolving on the raw
    field hands Haida Gwaii all 26 of Region 6's zone rules and none of Region 1's.

        {"field": "WILDLIFE_MGMT_UNIT_ID", "values": {"6-12": "1", "6-13": "1"}}

    Keyed on a DIFFERENT column than the one being rewritten, so the rule reads as "this unit
    now belongs to that region" rather than a blanket rename of one region to another.
    """
    if not remap:
        return
    field, values = remap["field"], remap["values"]
    if field not in g.columns:
        raise KeyError(f"remap field {field!r} not in layer {layer!r}")
    g[name_field] = [values.get(str(k), v) for k, v in zip(g[field], g[name_field])]



def mu_region_table(bbox=None) -> dict[str, str]:
    """``{"6-13": "1", "2-18": "2", ...}`` — which region administers each management unit.

    THE SAME TWO INPUTS THE REGION POLYGONS ARE DISSOLVED FROM: the `wmu` layer's
    ``REGION_RESPONSIBLE_ID`` and the curated `remap` beside it in `areas.json`. Derived here
    rather than re-typed, because a second copy of "Haida Gwaii is Region 1" is a second thing
    to forget: the source layer still files MUs 6-12 and 6-13 under Region 6, and reading it
    raw is exactly the mistake this table exists to make impossible.

    NOT A SUBSTITUTE FOR `node.in_areas`, and the difference was measured across all 1,957,890
    sections: the two agree on 1,955,747 of them, the table never loses a region the polygons
    found (0 cases), and it recovers one on 394 the polygons missed. But on 710 sections it is
    WIDER, and wrongly so — `mus` is every unit a piece TOUCHES, so a section cut at the region
    line lies wholly in one region while still touching a unit across it. Those 710 would pick
    up a second region's whole rulebook, which is the defect the cutter exists to prevent.

    So: the polygons decide where a section is; this answers "which region is this unit in",
    which is a question about administration and has one answer.
    """
    import geopandas as gpd

    rd = next((a for a in load_area_split_defs() if a.get("id") == "regions"), None)
    if rd is None:
        raise KeyError("areas.json defines no `regions` area")
    kw: dict = {"engine": "pyogrio", "ignore_geometry": True,
                "columns": ["WILDLIFE_MGMT_UNIT_ID", rd["name_field"]]}
    if bbox is not None:
        kw["bbox"] = tuple(bbox)
    g = gpd.read_file(_GPKG, layer=rd["layer"], **kw)
    apply_remap(g, rd["name_field"], rd.get("remap"), rd["layer"])
    return {str(k): str(v).upper()
            for k, v in zip(g["WILDLIFE_MGMT_UNIT_ID"], g[rd["name_field"]])
            if str(k) and str(k) != "None"}


def _resolve_area_file(name: str) -> Path:
    """A curated area file, by the key it is declared under (`added_areas`) or by a path.

    Going through `CURATED` rather than joining a directory is what keeps the file in the
    manifest: a declared path is checked to exist at load and cannot be deleted by a dead-file
    sweep, which is exactly the protection `ungazetted.json` is documented as needing."""
    got = getattr(CURATED.waters, name, None)
    return Path(got) if got is not None else Path(name)


def load_area_attrs(area_def: dict) -> dict:
    """`{name -> {attr: value}}` for the extra per-feature properties a def asks to `carry`.

    `load_area_polys` returns geometry keyed by name and drops every other column, which is
    right for cutting — the cutter needs a shape and an id and nothing else. A drawn polygon
    needs more: a closure zone that cannot say WHICH WATER it closes is a shape on a map with
    no way back to the regulation. `carry` names the columns that survive to the tile.

    Curated files only; a fetched layer would need the same treatment per source and none of
    them ask for it yet."""
    import geopandas as gpd

    fields = list(area_def.get("carry") or ())
    if not fields or not area_def.get("file"):
        return {}
    g = gpd.read_file(_resolve_area_file(area_def["file"]), engine="pyogrio")
    nf = area_def["name_field"]
    return {str(r[nf]): {f: r[f] for f in fields if f in g.columns}
            for _, r in g.iterrows() if r.get(nf)}


def clips_to_bc(area_def: dict) -> bool:
    """Is this area clipped to the B.C. outline? Every area is, except one drawn FROM the units the
    outline is made of (`layer: wmu` — the regions and the MU groups): those ARE the coverage, and
    clipping them would only re-node edges that already coincide with the outline exactly."""
    return area_def.get("layer") != "wmu"


def _clip_to_bc(g, area_def: dict) -> None:
    """CLIP AN AREA TO THE B.C. OUTLINE, in place (BOUND round, 2026-10-06).

    A B.C. regulation stops at the border, and so does every area it is written for. A park, a
    reserve, a WMA or a watershed polygon that runs past the outline (the Liard watershed into the
    Yukon; a park digitised a few metres across the Alberta line) is cut back to it, so the area's
    border edge IS the outline's edge: a river leaving the province through it is cut ONCE, at the
    border (`sectionizer._coincident`), never again a few metres away at the park's own line.
    Only polygons that actually reach outside are touched — one wholly inside B.C. keeps its
    geometry bit for bit. Every consumer (the cutter, the area catalog, the tiles) reads polygons
    through `load_area_polys`, so all of them see the same clipped shape."""
    if not clips_to_bc(area_def):
        return
    import shapely

    from pipeline.atlas.splits.bc_boundary import load_outline
    outline = load_outline(_GPKG)
    if outline is None:
        raise RuntimeError("no B.C. outline (wmu layer) to clip areas to")
    shapely.prepare(outline)
    geoms = list(g.geometry)
    for i, geom in enumerate(geoms):
        if geom is None or geom.is_empty or outline.covers(geom):
            continue
        cut = geom.intersection(outline)
        if cut.geom_type not in ("Polygon", "MultiPolygon"):   # drop the lines/points of a touch
            polys = [q for q in shapely.get_parts(cut) if q.geom_type in ("Polygon", "MultiPolygon")]
            cut = shapely.union_all(polys) if polys else shapely.Polygon()
        geoms[i] = cut
    g.set_geometry(geoms, inplace=True, crs=g.crs)


def load_area_polys(fwa, area_def: dict, bbox=None) -> dict:
    """{key -> (Multi)Polygon} for one area def. `where` filters the layer (e.g. ecological reserves
    within parks_bc); absent `where` = the whole layer.

    Two modes:
      * Named-area (default): union every row sharing a ``name_field`` value into one polygon, keyed
        by that name — one logical area, possibly multipart (parks, ecological reserves, historic sites).
      * Per-feature (``per_feature: true``): every row is its OWN polygon, keyed by a unique readable
        key ``"{name or label_default} [{id_field}]"``. Used for land_access, where each parcel is an
        independent closure — unioning the (mostly nameless) parcels by name would collapse thousands
        of unrelated province-wide slivers into one polygon and wreck the first-enter/last-exit cutter.
    """
    import geopandas as gpd

    kw: dict = {"engine": "pyogrio"}
    if area_def.get("where"):
        kw["where"] = area_def["where"]
    if area_def.get("file"):
        # A CURATOR'S POLYGON, not a fetched layer.
        #
        # Every other area here selects from a layer somebody downloaded — a park, a WMA, an
        # OSM parcel. Some closures have no such source: "the area bounded by a line from a
        # sign at the eastern end of Landstrom Bar to a sign on the opposite bank, thence …"
        # is a polygon that exists only in the regulation's own words, and until it is drawn
        # the rule gets bound as a reach between two cut-points — which closes the whole
        # width of the river instead of the area the signs enclose.
        #
        # Read straight from the curated file rather than routing it through `fetch_data`,
        # which is a NETWORK pipeline: a curator moving a vertex must reach the next build by
        # editing one file, not by re-running a fetch that needs the internet and rewrites
        # unrelated layers. Everything past this point is identical to a gpkg layer, so the
        # cutter, the catalog and the tile exporter need no idea where the polygon came from.
        #
        # The bbox is applied AFTER reprojection: it arrives in BC Albers, while the file is
        # lon/lat, and handing a 3005 box to a 4326 read silently returns nothing.
        g = gpd.read_file(_resolve_area_file(area_def["file"]), engine="pyogrio")
        if g.crs is not None and g.crs.to_epsg() != 3005:
            g = g.to_crs(3005)
        if bbox is not None:
            g = g.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
    else:
        if bbox is not None:
            kw["bbox"] = tuple(bbox)
        g = gpd.read_file(_GPKG, layer=area_def["layer"], **kw)
    nf = area_def["name_field"]
    out: dict = {}
    if g.empty:
        return out
    _clip_to_bc(g, area_def)

    if area_def.get("combine"):
        # ONE POLYGON FOR A GROUP OF UNITS, because that is the shape the regulation has.
        # The book writes "No Fishing in any stream in Management Units 1-1 to 1-6" — the
        # area is the UNION, and the boundaries between 1-2 and 1-3 are internal to it. Cut
        # per unit and the graph gains five boundaries no regulation asks for, and a rule
        # has to name six areas instead of one.
        geom = g.geometry.union_all()
        return {} if geom is None or geom.is_empty else {str(area_def["combine"]): geom}

    if area_def.get("per_feature"):
        idf = area_def.get("id_field", "")
        default = area_def.get("label_default", "Restricted area")
        for i, row in g.reset_index(drop=True).iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            name = str(row.get(nf) or "").strip() or default
            fid = str(row.get(idf) or i) if idf else str(i)
            out[f"{name} [{fid}]"] = geom     # id suffix keeps nameless parcels distinct
        return out

    # REMAP before the dissolve, because administration moves and geography does not.
    # Haida Gwaii (MUs 6-12, 6-13) still carries REGION_RESPONSIBLE_ID = 6 in the source
    # layer, but the 2025-2027 synopsis administers it from Region 1: "Freshwater angling
    # regulations ... for Haida Gwaii ... are now within Region 1." Dissolving on the raw
    # field hands Haida Gwaii all 26 of Region 6's zone rules and none of Region 1's.
    apply_remap(g, nf, area_def.get("remap"), area_def.get("layer", area_def.get("file", "")))

    for name, sub in g.groupby(nf):
        geom = sub.geometry.union_all()
        if geom is not None and not geom.is_empty:
            out[str(name)] = geom
    return out


def _neighbour_label(polys_by_name: dict, line, m: float, here: str, term: str) -> str:
    """What this cut SEPARATES, in words a person can act on.

    The label used to be the polygon's own name, which for an administrative area whose name
    is its number reads "2" — a boundary labelled with one number and no side. These labels
    surface in the app as the ends of a stretch ("From … / To …"), so a cut has to say what
    is on the other side of it: `Region 2 – Region 3 boundary`.

    The other side is found by stepping 300 m along the chain either way and asking which
    sibling polygon contains each point. A step is needed because the cut sits ON the shared
    edge, where both polygons and neither can claim it; 300 m clears the edge without
    skipping a genuinely narrow neighbour. Where nothing sits on the far side — the coast,
    the provincial border, a gap in the coverage — the cut names the one area it bounds.
    """
    L = line.length
    other = None
    for d in (m - 300.0, m + 300.0):
        if d < 0 or d > L:
            continue
        pt = line.interpolate(d)
        for nm, poly in polys_by_name.items():
            if nm != here and poly.contains(pt):
                other = nm
                break
        if other:
            break
    lead = f"{term} " if term else ""
    if other:
        a, b = sorted((here, other), key=lambda v: (len(v), v))
        return f"{lead}{a} – {lead}{b} boundary"
    return f"{lead}{here} boundary"


def resolve_area_splits(polys_by_name: dict, chains: list[BlkChain],
                        term: str | None = None,
                        scope: dict | None = None,
                        border: tuple | None = None,
                        existing: dict | None = None) -> list[SplitPoint]:
    """Cut every chain crossing each polygon at first-enter/last-exit (transition cutting).

    `term` has three states, because there are three kinds of name:

      * ``None`` — label the cut with the area's own name. What a park wants: "Garibaldi
        Provincial Park" already reads as a place.
      * ``""`` — the name is already a readable phrase but the cut is an EDGE of it:
        "Management Units 1-1 to 1-6 boundary".
      * a word — the name is a bare identifier and needs it: "Region 2 – Region 3 boundary".

    In the last two the cut is labelled by what it SEPARATES rather than by whichever polygon
    it happened to belong to; see `_neighbour_label`.

    `scope` — `{area name: item id}` — NARROWS AN AREA TO THE WATER IT IS ABOUT. Blanket is right
    for a park: everything inside it is closed, whatever stream it is. It is wrong for a sign zone
    drawn across a confluence. The Kispiox ring spans the mouth, so a blanket cut also cuts the
    Kispiox and an unnamed side channel — but the regulation says "MAINSTEM waters within 3 white
    triangular fishing boundary signs", and the other cuts are stubs no rule will ever bind.

    The scope is not a new thing to maintain: it is the `cuts` property the ring already carries to
    say which water it sits on, so the tile link and the cut scope cannot disagree. An area with no
    scope stays blanket, which is every area that existed before this.

    TWO DIFFERENCES FROM BEFORE THE BOUND ROUND (2026-10-06), and only these — the regions keep
    their first-enter/last-exit cuts and their both-sides membership (`border.mark_inside_areas`):

      * A CROSSING ON THE B.C. OUTLINE is the border's (`border` = (outline boundary, {blk: [border
        cut measures]})): the outline is the exact union of the units the regions are dissolved
        from, so a region's edge there IS the border. Where the border pass cut at that place the
        area's cut takes the border cut's measure and becomes its alias (`sectionizer._coincident`:
        one cut, both names, `bc_border`); where it cut nothing (a line's end or a graze inside the
        border's crossing zone) the area does not cut either.
      * A CUT THAT WOULD LEAVE A SLIVER IS NOT MADE (`sliver_gate.SLIVER_M`): one within that
        distance of the line's own mouth or source, a lake edge, a boundary already on the line
        (`existing`), or another cut of the same pass — Contact Creek dipping 2.8 m into Region 6, a
        region line 0.3 m below the West Road's source, the Region 3 line 0.001 m from a lake on
        Slack Creek, the 2/3 line 0.42 mm from the curated Spuzzum Creek cut. A cut within
        `clean_cut.SAME_PLACE_M` of an existing boundary is not a sliver but the same place: it is
        made, and becomes that boundary's alias.
    """
    from shapely.strtree import STRtree

    from pipeline.atlas.splits.clean_cut import SAME_PLACE_M
    from pipeline.atlas.splits.sliver_gate import SLIVER_M

    keep = [c for c in chains if c.geometry is not None and not c.geometry.is_empty]
    if not keep:
        return []
    out_bd, border_cuts = border if border is not None else (None, {})
    if out_bd is not None:
        import shapely
        shapely.prepare(out_bd)
    tree = STRtree([c.geometry for c in keep])
    cands: dict[str, list] = {}                    # blk -> [(M, name, m, chain)]
    import shapely
    for name, poly in polys_by_name.items():
        boundary = poly.boundary
        shapely.prepare(poly)
        want = (scope or {}).get(name)
        # Only a chain meeting the EDGE can be cut: one wholly inside reads inside at every vertex and
        # one wholly outside meets nothing, so both yield no measure. Querying the edge instead of the
        # polygon's bbox gives the same cuts without testing every vertex of every chain in a region
        # (the stage took 2,020 s on the bbox, 88 s on the edge).
        for i in sorted(tree.query(boundary, predicate="intersects")):
            c = keep[i]
            if want and f"gnis:{c.gnis_id}" != want and f"wbk:{getattr(c, 'wbk', '')}" != want:
                continue                           # a water this area is not about — see `scope`
            for m in _area_transition_measures(c.geometry, poly, boundary):
                M = c.mouth_measure + m
                if out_bd is not None and out_bd.distance(c.geometry.interpolate(m)) <= SAME_PLACE_M:
                    same = [b for b in border_cuts.get(c.blk, ()) if abs(b - M) <= SAME_PLACE_M]
                    if not same:
                        continue                   # on the outline, and the border cut nothing here
                    M = same[0]                    # the border's cut: one place, both names
                cands.setdefault(c.blk, []).append((M, name, m, c))
    out: list[SplitPoint] = []
    for blk in sorted(cands):
        got = cands[blk]
        c = got[0][3]
        ends = [c.mouth_measure, c.mouth_measure + c.geometry.length]
        ends += [e for r in (c.waterbody_runs or ()) for e in (r.down_m, r.up_m)]
        ends += list((existing or {}).get(blk, ()))
        places = sorted({M for M, *_ in got})
        for M, name, m, c in sorted(got, key=lambda t: (t[0], t[1])):
            near = [e for e in ends + [P for P in places if abs(P - M) > SAME_PLACE_M]
                    if SAME_PLACE_M < abs(e - M) < SLIVER_M]
            if near:
                continue                           # it would leave a sliver: not cut
            label = (name if term is None
                     else _neighbour_label(polys_by_name, c.geometry, m, name, term))
            out.append(SplitPoint(split_id=f"area:{name}", blk=c.blk,
                                  route_measure=M, fid="",
                                  label=label, anchor_type=AnchorType.area_boundary, source="area"))
    return out
