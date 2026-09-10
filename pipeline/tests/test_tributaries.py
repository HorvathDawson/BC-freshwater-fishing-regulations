"""Tributary reachability tests (03 S6, 08): lake inlets/outlets, lake tributaries (dropping the
through-mainstem), and range selection for 'X from lake A to lake C'. Synthetic, no gpkg.

The fixture is a mainstem X threading three lakes A, B, C:

    mouth 0 ── X ──[A]── X ──[B]── X ──[C]── X ── headwaters 300
                 ▲tSA      ▲tSB      ▲tSC          (side creeks into each lake)

Each lake gets one side creek (tSA/tSB/tSC) plus the through-river inflow/outflow.
"""

from shapely.geometry import LineString

from pipeline.atlas.graph import cutting
from pipeline.atlas.graph.blk_chains import FidRow, build_blk_chains
from pipeline.atlas.graph.graph import build_stream_graph
from pipeline.common.models import AnchorType, SplitPoint
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.atlas.graph.tributaries import (expand, lake_inlets, lake_outlets,
                                         lake_tributaries, piece_above, reach_except,
                                         sections_in_reach, tributary_node_ids,
                                         with_tributaries)


def _fid(fid, blk, wsc, coords, down_m, up_m, wbk="", gnis_name=""):
    geom = LineString(coords)
    dn, un = cutting.blk_endpoints(geom)
    return FidRow(fid=fid, blk=blk, wsc=wsc, edge_type="1000", wbk=wbk,
                  gnis_id="9" if gnis_name else "", gnis_name=gnis_name,
                  stream_order=1, stream_magnitude=1, down_m=down_m, up_m=up_m,
                  geometry=geom, down_node=dn, up_node=un)


def _three_lake_river():
    """X (blk X, mouth 0 -> source 300) threads lakes A[50,100], B[150,200], C[250,300]. Each
    lake is two fids so it has an interior node where a non-lake side creek attaches."""
    fids = [
        _fid("X0", "X", "100", [(0, 0), (50, 0)], 0, 50, gnis_name="X River"),           # below A
        _fid("XA1", "X", "100", [(50, 0), (70, 0)], 50, 70, wbk="A", gnis_name="X River"),
        _fid("XA2", "X", "100", [(70, 0), (100, 0)], 70, 100, wbk="A", gnis_name="X River"),
        _fid("X1", "X", "100", [(100, 0), (150, 0)], 100, 150, gnis_name="X River"),      # A->B
        _fid("XB1", "X", "100", [(150, 0), (170, 0)], 150, 170, wbk="B", gnis_name="X River"),
        _fid("XB2", "X", "100", [(170, 0), (200, 0)], 170, 200, wbk="B", gnis_name="X River"),
        _fid("X2", "X", "100", [(200, 0), (250, 0)], 200, 250, gnis_name="X River"),      # B->C
        _fid("XC1", "X", "100", [(250, 0), (270, 0)], 250, 270, wbk="C", gnis_name="X River"),
        _fid("XC2", "X", "100", [(270, 0), (300, 0)], 270, 300, wbk="C", gnis_name="X River"),
        # a non-lake side creek attached at each lake's interior node (70/170/270)
        _fid("SA", "SA", "100-1", [(70, 0), (70, 40)], 0, 40, gnis_name="Creek A"),
        _fid("SB", "SB", "100-2", [(170, 0), (170, 40)], 0, 40, gnis_name="Creek B"),
        _fid("SC", "SC", "100-3", [(270, 0), (270, 40)], 0, 40, gnis_name="Creek C"),
    ]
    lake_kind = {"A": "lake", "B": "lake", "C": "lake"}
    names = {"A": "Lake A", "B": "Lake B", "C": "Lake C"}
    return build_stream_graph(build_blk_chains(fids, lake_kind), fids, lake_kind=lake_kind,
                              lake_names=names)


def test_lake_inlets_and_outlets():
    g = _three_lake_river()
    # Flow runs source(300) -> mouth(0), so Lake B[150,200] takes inflow from the upstream B->C
    # piece X:200 + Creek B, and drains OUT to the downstream A->B piece X:100.
    assert lake_inlets(g, "lake:B") == {"X:200", "SB:0"}
    assert lake_outlets(g, "lake:B") == {"X:100"}


def test_lake_tributaries_drop_the_mainstem():
    g = _three_lake_river()
    # "tributaries of Lake B" = Creek B only, NOT the through-river (X:200) or anything upstream.
    assert lake_tributaries(g, "lake:B") == {"SB:0"}
    # sanity: the raw inlets include the through-mainstem inflow, which lake_tributaries drops.
    assert "X:200" in lake_inlets(g, "lake:B")


def test_range_reach_from_lake_A_to_lake_C_spans_lake_B():
    g = _three_lake_river()
    # 'X River from Lake A to Lake C': the mainstem pieces between A's upper bound and C's lower
    # bound — spanning Lake B. Bound measures come from the pieces' structured bounds.
    a_upper = g.nodes["X:100"].lower_bound.route_measure     # X:100 starts at Lake A (=100)
    c_lower = g.nodes["X:200"].upper_bound.route_measure     # X:200 ends at Lake C (=250)
    reach = sections_in_reach(g, "X", a_upper, c_lower)
    assert reach == {"X:100", "X:200"}                       # the A->B and B->C mainstem pieces
    # and their location identifiers read naturally:
    assert g.nodes["X:100"].location_identifier == "between Lake A and Lake B"
    assert g.nodes["X:200"].location_identifier == "between Lake B and Lake C"


def test_full_closure_reaches_through_lakes():
    g = _three_lake_river()
    # the lowest mainstem piece's guarded closure includes every lake + side creek + upper piece.
    anc = tributary_node_ids(g, "X:0")
    assert {"lake:A", "lake:B", "lake:C", "SA:0", "SB:0", "SC:0"} <= anc


def _bella_coola_system():
    """The 'ATNARKO/BELLA COOLA [Includes Tributaries] EXCEPT …' shape.

        ocean 0 ── Bella Coola (X) ──────────────────────────── 500 headwaters
                     ▲OK@100  ▲Young@200  ▲BurntBridge@300  ▲Atnarko@400
                                                                 └─ Hunlen@(AT 80)

    Young/Burnt Bridge/Hunlen each get a split so 'upstream of Y' is a real upper piece; the
    Ordinary creek (OK) has no split (must survive the EXCEPT)."""
    fids = [
        _fid("X0", "X", "100", [(0, 0), (100, 0)], 0, 100, gnis_name="Bella Coola River"),
        _fid("Xa", "X", "100", [(100, 0), (200, 0)], 100, 200, gnis_name="Bella Coola River"),
        _fid("Xb", "X", "100", [(200, 0), (300, 0)], 200, 300, gnis_name="Bella Coola River"),
        _fid("Xc", "X", "100", [(300, 0), (400, 0)], 300, 400, gnis_name="Bella Coola River"),
        _fid("Xd", "X", "100", [(400, 0), (500, 0)], 400, 500, gnis_name="Bella Coola River"),
        _fid("OK", "OK", "100-1", [(100, 0), (100, 40)], 0, 40, gnis_name="Ordinary Creek"),
        _fid("YO", "YO", "100-2", [(200, 0), (200, 80)], 0, 80, gnis_name="Young Creek"),
        _fid("BB", "BB", "100-3", [(300, 0), (300, 80)], 0, 80, gnis_name="Burnt Bridge Creek"),
        _fid("AT1", "AT", "100-4", [(400, 0), (400, 80)], 0, 80, gnis_name="Atnarko River"),
        _fid("AT2", "AT", "100-4", [(400, 80), (400, 200)], 80, 200, gnis_name="Atnarko River"),
        _fid("HU", "HU", "100-4-1", [(400, 80), (440, 80)], 0, 40, gnis_name="Hunlen Creek"),
    ]
    g = build_stream_graph(build_blk_chains(fids, {}), fids)
    # curated splits (the three EXCEPT waters); OK gets none.
    split_graph_at(g, {}, [
        SplitPoint("hunlen_falls", "HU", 20.0, "", "Hunlen Falls", AnchorType.point),
        SplitPoint("burnt_bridge", "BB", 40.0, "", "Sitkatapa Creek", AnchorType.confluence),
        SplitPoint("young_hwy20", "YO", 40.0, "", "Hwy 20", AnchorType.point),
    ])
    return g


def test_includes_tributaries_except_upstream_of_splits():
    """ATNARKO/BELLA COOLA [Includes Tributaries] EXCEPT the three upstream-of reaches — pure set
    difference over the pieces the splits already made (docs/04)."""
    g = _bella_coola_system()
    # each 'X upstream of Y' resolves to the upper piece via its split's boundary label:
    hu_up = piece_above(g, "HU", "Hunlen Falls")
    bb_up = piece_above(g, "BB", "Sitkatapa Creek")
    yo_up = piece_above(g, "YO", "Hwy 20")
    assert (hu_up, bb_up, yo_up) == ("HU:20", "BB:40", "YO:40")

    base = with_tributaries(g, ["X:0", "AT:0"])          # Bella Coola ∪ Atnarko + all tributaries
    assert {"X:0", "AT:0"} <= base                        # the base rivers are IN the set
    result = reach_except(g, ["X:0", "AT:0"], [hu_up, bb_up, yo_up])

    # the three excepted upper reaches are gone …
    assert result.isdisjoint({"HU:20", "BB:40", "YO:40"})
    # … but their DOWNSTREAM pieces, the un-split Ordinary creek, and both mainstems remain.
    assert {"X:0", "AT:0", "OK:0", "YO:0", "BB:0", "HU:0"} <= result
    assert result == base - {"HU:20", "BB:40", "YO:40"}


# --------------------------------------------------------------------------------------- #
# `_mouths_at` is exercised DIRECTLY here rather than through a built graph.
#
# Two attempts to reproduce the shape from FidRows both passed with the guard removed, which
# means they never reached this code path at all — a test that cannot fail on the bug is
# worse than none. The seeding turns on three facts about one edge (its blk, its kind, its
# measure) and on the two stream orders, so those are what the stub supplies.
# --------------------------------------------------------------------------------------- #

class _Node:
    def __init__(self, blk, order, wsc="", is_barrier=False):
        self.blk, self.stream_order, self.wsc = blk, order, wsc
        self.is_barrier = is_barrier
        self.kind = None


class _Edge:
    def __init__(self, from_node, to_node, kind, at_measure):
        self.from_node, self.to_node = from_node, to_node
        self.kind, self.at_measure = kind, at_measure


class _Stub:
    """The three lookups `_mouths_at` uses, and nothing else."""
    def __init__(self, nodes, edges, down_adj, up_adj):
        self.nodes, self.edges = nodes, edges
        self.down_adj, self.up_adj = down_adj, up_adj


def _confluence_stub(sibling_order):
    """A reach node whose piece below carries one sibling arriving at the same measure.

    `sibling_order` is the whole experiment: 1 is a creek, 9 is the river the reach drains
    into. Everything else about the two is identical.
    """
    nodes = {"reach": _Node("C", 3, "300"),
             "below": _Node("H", 9, "200"),
             "sibling": _Node("S", sibling_order, "200-1")}
    edges = [_Edge("reach", "below", "confluence", 0.0),
             _Edge("sibling", "below", "confluence", 0.0)]
    return _Stub(nodes, edges, {"reach": [0], "sibling": [1]}, {"below": [0, 1]})


def test_a_side_creek_at_the_mouth_is_seeded():
    from pipeline.atlas.graph.tributaries import _mouths_at
    g = _confluence_stub(sibling_order=1)
    got = _mouths_at(g, "reach", g.nodes["reach"], 0.0, frozenset({"reach"}))
    assert got == {"sibling"}, "a genuine side creek at the mouth was lost"


def test_the_river_the_reach_drains_into_is_not_seeded():
    """The receiving river passes every other test in `_mouths_at`.

    It arrives on its own blue line, by a `confluence` edge, at exactly the reach's lower
    measure — so only the Strahler guard tells it apart from the creek above. The walk has
    always applied that guard; the seeding did not, and so "No Fishing downstream of the main
    logging road bridge, May 1-31" on the CHEHALIS bound two sections of the HARRISON.
    """
    from pipeline.atlas.graph.tributaries import _mouths_at
    g = _confluence_stub(sibling_order=9)
    got = _mouths_at(g, "reach", g.nodes["reach"], 0.0, frozenset({"reach"}))
    assert got == set(), "the river the reach drains into was seeded as its tributary"
