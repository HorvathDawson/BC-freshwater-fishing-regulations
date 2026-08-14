"""BC border handling (border.py): a cross-border BLK is split at the provincial outline and the
out-of-BC piece is flagged (geometry kept, not a barrier). Synthetic — a box 'BC' outline and a
BLK that loops out and back (the Kootenay shape). No gpkg.

        BC outline = box(0,0,100,100)
        X: (50,50) ─▶ (150,50) ─▶ (150,10) ─▶ (50,10)     leaves at x=100, returns at x=100
"""

from shapely.geometry import LineString, box

from pipeline.graph import cutting
from pipeline.graph.blk_chains import FidRow, build_blk_chains
from pipeline.splits.border import border_split_points, mark_out_of_bc
from pipeline.graph.graph import build_section_geometries, build_stream_graph
from pipeline.splits.sectionizer import split_graph_at


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
