"""BC border handling (border.py): a cross-border BLK is split at the provincial outline and the
out-of-BC piece is flagged (geometry kept, not a barrier). Synthetic — a box 'BC' outline and a
BLK that loops out and back (the Kootenay shape). No gpkg.

        BC outline = box(0,0,100,100)
        X: (50,50) ─▶ (150,50) ─▶ (150,10) ─▶ (50,10)     leaves at x=100, returns at x=100
"""

from dataclasses import replace

from shapely.geometry import LineString, box

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.splits.border import border_split_points, mark_inside_area, mark_inside_areas, mark_out_of_bc
from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
from pipeline.atlas.splits.sectionizer import split_graph_at


def _fid(fid, blk, wsc, coords, down_m, up_m):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk="", gnis_id="9",
                  gnis_name="Cross Border River", stream_order=1, stream_magnitude=1,
                  down_m=down_m, up_m=up_m, geometry=geom, down_node=dn, up_node=un)


def _setup():
    coords = [(50, 50), (150, 50), (150, 10), (50, 10)]
    ln = LineString(coords)
    fids = [_fid("X1", "X", "100", coords, 0, ln.length)]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})
    outline = box(0, 0, 100, 100)
    return chains, graph, geoms, outline


def test_border_split_points_finds_both_crossings():
    chains, _, _, outline = _setup()
    pts = border_split_points(chains, outline)
    ms = sorted(round(p.route_measure) for p in pts)
    assert ms == [50, 190]                       # exit at m=50, re-entry at m=190
    assert all(p.label == "BC boundary" for p in pts)


def test_border_prefilter_skips_inland_chain():
    # a chain fully inside BC must be pruned by the covered_by prefilter (no crossings, no work)
    coords = [(10, 10), (90, 10), (90, 90)]
    inland = build_blk_chains([_fid("I1", "I", "200", coords, 0, LineString(coords).length)], {})
    outline = box(0, 0, 100, 100)
    assert border_split_points(inland, outline) == []
    # and a cross-border chain in the SAME call still yields its crossings
    chains, _, _, _ = _setup()
    pts = border_split_points(chains + inland, outline)
    assert sorted(round(p.route_measure) for p in pts) == [50, 190]      # inland contributes nothing
    assert {p.blk for p in pts} == {"X"}


def test_mark_inside_areas_matches_per_poly_and_batches():
    # two inland streams: A inside the park box, B outside
    a = _fid("A1", "A", "300", [(10, 10), (20, 10)], 0, 10)
    b = _fid("B1", "B", "400", [(100, 100), (110, 100)], 0, 10)
    fids = [a, b]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})
    park = box(0, 0, 50, 50)

    n = mark_inside_areas(graph, geoms, {"PARK": park})
    inside = [nid for nid, node in graph.nodes.items() if "PARK" in node.in_areas]
    assert n == 1 and len(inside) == 1 and inside[0].startswith("A:")
    # idempotent — re-marking adds nothing
    assert mark_inside_areas(graph, geoms, {"PARK": park}) == 0
    # equivalence with the per-polygon helper on a fresh graph
    graph2 = build_stream_graph(chains, fids, {}, {})
    m = mark_inside_area(graph2, geoms, park, "PARK")
    assert m == 1


def test_out_of_bc_piece_flagged_kept_and_not_barrier():
    chains, graph, geoms, outline = _setup()
    pts = border_split_points(chains, outline)
    split_graph_at(graph, geoms, pts)
    flagged = mark_out_of_bc(graph, geoms, outline)
    assert flagged == 1
    # the MIDDLE piece (the US loop) is out_of_bc; the two BC ends are not.
    assert graph.nodes["X:50"].out_of_bc is True
    assert graph.nodes["X:0"].out_of_bc is False
    assert graph.nodes["X:190"].out_of_bc is False
    # geometry is KEPT for dotted display, and an out-of-BC reach is NOT a flow barrier.
    assert geoms["X:50"] is not None and not geoms["X:50"].is_empty
    assert graph.nodes["X:50"].is_barrier is False


# --- area membership: cut pieces, straddlers, and lakes -------------------------------------------

def _area_setup():
    """Park = box(0,0,100,100).  Y ends exactly ON the boundary x=100 (the shape a cut leaves behind):
    wholly inside, but touching.  Z runs 60->140 and is NOT cut, so it straddles.  O is wholly
    outside.  Lake W sits wholly inside on X."""
    fids = [_fid("Y1", "Y", "200", [(0, 50), (100, 50)], 0, 100),
            _fid("O1", "O", "400", [(120, 50), (180, 50)], 0, 60),
            _fid("Z1", "Z", "300", [(60, 20), (140, 20)], 0, 80),
            _fid("X1", "X", "100", [(10, 80), (40, 80)], 0, 30)]
    fids[3] = replace(fids[3], wbk="W")
    chains = build_blk_chains(fids, {"W": "lake"})
    graph = build_stream_graph(chains, fids, {"W": "lake"}, {})
    geoms = build_section_geometries(chains, fids, {"W": "lake"})
    return graph, geoms, box(0, 0, 100, 100)


def test_area_flags_a_piece_cut_exactly_on_the_boundary():
    """The piece is wholly inside but its endpoint lies ON the polygon edge — `contains` rejects that,
    which is why the pass uses `covers`. 1,453 real pieces fail `contains` for exactly this reason."""
    graph, geoms, park = _area_setup()
    mark_inside_areas(graph, geoms, {"Park": park})
    assert "Park" in graph.nodes["Y:0"].in_areas
    assert "Park" not in graph.nodes["O:0"].in_areas        # a piece outside stays outside


def test_area_flags_an_uncut_straddler():
    """Z is never cut at the boundary (an area_boundary split is scoped to one named water), so half
    of it is in the park. Dropping it would lose real regulated water."""
    graph, geoms, park = _area_setup()
    mark_inside_areas(graph, geoms, {"Park": park})
    assert "Park" in graph.nodes["Z:0"].in_areas


def test_area_flags_a_lake_node():
    """A lake inside a park is regulated by the same closure; the old midpoint pass tested stream
    nodes only, so 1,750 wholly-inside lakes were invisible to every within(area) rule."""
    graph, geoms, park = _area_setup()
    mark_inside_areas(graph, geoms, {"Park": park})
    assert "Park" in graph.nodes["lake:W"].in_areas


def test_area_membership_is_idempotent():
    graph, geoms, park = _area_setup()
    first = mark_inside_areas(graph, geoms, {"Park": park})
    assert first > 0
    assert mark_inside_areas(graph, geoms, {"Park": park}) == 0


def test_area_flags_a_minted_waterbody_from_its_own_polygon():
    """A minted waterbody (isolated lake, marsh) has NO line geometry, so the membership pass has
    nothing to measure — and a pond or marsh inside a park is exactly the water an area closure names.
    `extra` supplies its FWA polygon, keyed by node id."""
    from pipeline.common.models import NameSource, NodeKind
    from pipeline.atlas.graph.names import mint_waterbody_nodes

    graph, geoms, park = _area_setup()
    mint_waterbody_nodes(graph, {"777": (("Hidden Marsh", "1"),)}, NameSource.gazette, NodeKind.wetland)
    assert geoms.get("lake:777") is None                     # no sidecar geometry, by design
    mark_inside_areas(graph, geoms, {"Park": park})
    assert graph.nodes["lake:777"].in_areas == ()            # invisible without its polygon
    mark_inside_areas(graph, geoms, {"Park": park}, extra={"lake:777": box(20, 20, 30, 30)})
    assert "Park" in graph.nodes["lake:777"].in_areas
