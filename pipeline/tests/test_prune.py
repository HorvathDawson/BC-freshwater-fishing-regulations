"""Braid pruning: drop a river's own braiding, keep anything another water reaches."""

from dataclasses import replace

from pipeline.graph.prune import loop_nodes, prune_mainstem_loops
from pipeline.models import FlowEdge, NameSource, NameTuple, NodeKind, StreamGraph, StreamNode

MAIN = "100-0001"          # the river's watershed code
TRIB = "100-0001-0002"     # a DIFFERENT water draining into it


def _n(nid, blk, *, wsc=MAIN, name="Big River", tuples=(), length=100.0):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc,
                      display_name=name, name_tuples=tuples, length_m=length)


def _graph(edges, nodes):
    g = StreamGraph()
    for n in nodes:
        g.nodes[n.node_id] = n
    for frm, to in edges:
        g.edges.append(FlowEdge(from_node=frm, to_node=to, at_measure=0.0))
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _river(extra_edges=(), extra_nodes=(), braid_name="Big River", braid_tuples=()):
    """Mainstem M (longest, so it is the mainstem) with braid B leaving and rejoining it.

    B leaves the UPPER piece and rejoins the LOWER one, so the mainstem is a way past it. The braid
    used to be wired the other way round, `M:lo -> B -> M:hi`, which is water running uphill and left
    the braid as M:lo's only route downstream — removable only by moving a confluence."""
    nodes = [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
             _n("B:0", "B", name=braid_name, tuples=braid_tuples), *extra_nodes]
    return _graph([("M:hi", "M:lo"), ("M:hi", "B:0"), ("B:0", "M:lo"), *extra_edges], nodes)


def test_a_rivers_own_braid_is_pruned():
    g = _river()
    assert loop_nodes(g) == {"B:0"}
    g, n, _ = prune_mainstem_loops(g)
    assert n == 1 and set(g.nodes) == {"M:hi", "M:lo"}
    assert all(e.from_node in g.nodes and e.to_node in g.nodes for e in g.edges)
    assert g.down_adj.get("M:hi")                     # mainstem still connected


def test_a_nested_braid_goes_with_its_parent():
    """A braid hanging off another braid never touches the mainstem, so a per-piece loop test keeps
    it — and keeping it keeps the parent too. The whole nest must go together."""
    g = _river(extra_edges=[("B:0", "B2:0"), ("B2:0", "M:lo")], extra_nodes=[_n("B2:0", "B2")])
    assert loop_nodes(g) == {"B:0", "B2:0"}


def test_a_braid_a_different_water_flows_into_is_kept():
    """A tributary discharges there, so the channel is reachable and regulable — and pruning it
    would orphan that water."""
    g = _river(extra_edges=[("T:0", "B:0")],
               extra_nodes=[_n("T:0", "T", wsc=TRIB, name="Small Creek")])
    assert loop_nodes(g) == set()


def test_a_nest_is_kept_whole_when_a_tributary_reaches_any_part_of_it():
    g = _river(extra_edges=[("B:0", "B2:0"), ("B2:0", "M:lo"), ("T:0", "B2:0")],
               extra_nodes=[_n("B2:0", "B2"), _n("T:0", "T", wsc=TRIB, name="Small Creek")])
    assert loop_nodes(g) == set()


def test_a_gazetted_name_is_kept():
    assert loop_nodes(_river(braid_name="Herrling Island Side Channel")) == set()


def test_a_curated_name_variant_is_kept_even_when_display_name_is_inherited():
    """McArthur Island Slough displays the mainstem's name; the curated tuple makes it a water."""
    tuples = (NameTuple("Big River", NameSource.side_channel, gnis_id="1"),
              NameTuple("McArthur Island Slough", NameSource.regulation))
    assert loop_nodes(_river(braid_tuples=tuples)) == set()


def test_a_tributary_is_never_pruned():
    """It joins its receiver but takes no water from it, and its watershed code differs."""
    g = _graph([("M:hi", "M:lo"), ("T:0", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                _n("T:0", "T", wsc=TRIB, name="Small Creek")])
    assert loop_nodes(g) == set()


def test_the_mainstem_itself_is_never_a_candidate():
    g = _river()
    assert not ({"M:hi", "M:lo"} & loop_nodes(g))


def test_pruned_fids_are_reported_so_the_integrity_check_can_excuse_them():
    g = _river()
    g.nodes["B:0"] = StreamNode(node_id="B:0", kind=NodeKind.stream, blk="B", wsc=MAIN,
                                display_name="Big River", member_fids=("f1", "f2"))
    _, n, fids = prune_mainstem_loops(g)
    assert n == 1 and fids == {"f1", "f2"}


def test_geometries_are_dropped_alongside():
    from shapely.geometry import Point
    g = _river()
    geoms = {"M:hi": Point(0, 0), "M:lo": Point(0, 10), "B:0": Point(5, 5)}
    prune_mainstem_loops(g, geoms)
    assert set(geoms) == {"M:hi", "M:lo"}


def test_a_name_variants_channel_is_never_pruned():
    """`apply_name_variants` runs LATE in the build (after the splits, so cut pieces get named), long
    after this prune. At prune time a curated channel still carries only its host's inherited name and
    looks anonymous — which silently deleted all four blue lines of "Seabird Island North Side
    Channel" before they could ever be named. The prune is told the targets up front instead."""
    from pipeline.models import NodeKind, StreamGraph, StreamNode
    from pipeline.graph.prune import loop_nodes, nv_blks

    def piece(nid, blk, name="Fraser River"):
        return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc="100",
                          display_name=name, length_m=1000.0)

    host_lo = StreamNode(node_id="1:0", kind=NodeKind.stream, blk="1", wsc="100",
                         display_name="Fraser River", length_m=90000.0)   # mainstem: most length
    host_hi = piece("1:1000", "1")
    braid = piece("9:0", "9")                             # unnamed anabranch, in and back out
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (host_lo, host_hi, braid)}
    g.edges = [FlowEdge(from_node="1:1000", to_node="9:0", at_measure=0.0),
               FlowEdge(from_node="9:0", to_node="1:0", at_measure=0.0),
               FlowEdge(from_node="1:1000", to_node="1:0", at_measure=0.0)]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)

    assert "9:0" in loop_nodes(g), "an unnamed loop is prunable"
    assert "9:0" not in loop_nodes(g, {"9"}), "a curated blk must survive"


def test_nv_blks_reads_both_singular_and_plural_targets():
    from pipeline.graph.prune import nv_blks
    assert nv_blks([{"target": {"blks": ["1", "2"]}}, {"target": {"blk": "3"}}]) == {"1", "2", "3"}
    assert nv_blks([{"target": {"wbks": ["9"]}}]) == set(), "a lake target names no braid piece"


def _braid_with_tributary():
    """Host line 1 (mainstem), braid line 9 leaving it and rejoining, and creek 5 flowing INTO the
    braid. Water runs 1:1000 -> 9:0 -> 1:0, and 5:0 -> 9:0.

    This is the shape that keeps a big river a hairball: the braid is unnamed and pure anabranch, but
    removing it would orphan the creek, so the whole nest survives."""
    main_lo = StreamNode(node_id="1:0", kind=NodeKind.stream, blk="1", wsc="100",
                         display_name="Fraser River", length_m=90000.0)
    main_hi = StreamNode(node_id="1:1000", kind=NodeKind.stream, blk="1", wsc="100",
                         display_name="Fraser River", length_m=1000.0)
    braid = StreamNode(node_id="9:0", kind=NodeKind.stream, blk="9", wsc="100",
                       display_name="Fraser River", length_m=800.0)
    creek = StreamNode(node_id="5:0", kind=NodeKind.stream, blk="5", wsc="200",
                       display_name="Some Creek", length_m=4000.0)
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in (main_lo, main_hi, braid, creek)}
    g.edges = [FlowEdge(from_node="1:1000", to_node="9:0", at_measure=0.0),
               FlowEdge(from_node="9:0", to_node="1:0", at_measure=0.0),
               FlowEdge(from_node="1:1000", to_node="1:0", at_measure=0.0),
               FlowEdge(from_node="5:0", to_node="9:0", at_measure=0.0)]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _braid_with_creek_with_two_mouths():
    """As `_braid_with_tributary`, but the creek also reaches the mainstem directly, so the braid is
    not its only way out and the nest carries nothing that has to be preserved."""
    g = _braid_with_tributary()
    g.edges.append(FlowEdge(from_node="5:0", to_node="1:0", at_measure=0.0))
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def test_a_braid_carrying_a_tributary_is_kept_by_default():
    """Unchanged behaviour: removing it would orphan the creek, so it stays."""
    g = _braid_with_tributary()
    _g, n, _fids = prune_mainstem_loops(g)
    assert n == 0
    assert "9:0" in g.nodes


def test_a_braid_carrying_a_creek_keeps_the_channel_the_creek_needs():
    """The creek's only way out runs through the nest, so one channel of it is load-bearing and stays.

    The old rule deleted the whole nest and moved the creek's mouth onto the exit. Keeping the single
    channel the creek actually uses reaches the same place without relocating a confluence."""
    g = _braid_with_tributary()
    _g, n, _fids = prune_mainstem_loops(g, reconnect_tributaries=True)
    assert n == 0 and "9:0" in g.nodes, "no other route out: that channel carries something"
    assert {g.edges[i].to_node for i in g.down_adj.get("5:0", [])} == {"9:0"}


def test_a_braid_is_removed_once_the_creek_can_get_out_another_way():
    """Give the creek a second mouth onto the mainstem and the nest carries nothing worth keeping."""
    g = _braid_with_tributary()
    g.edges.append(FlowEdge(from_node="5:0", to_node="1:0", at_measure=0.0))
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    _g, n, _fids = prune_mainstem_loops(g, reconnect_tributaries=True)
    assert n == 1 and "9:0" not in g.nodes
    assert g.down_adj.get("5:0"), "the creek keeps a way downstream"
    assert not any(e.from_node == "9:0" or e.to_node == "9:0" for e in g.edges)


def test_reconnect_still_refuses_a_named_channel():
    """The braid is a named slough, so it is a water the synopsis can regulate — never removed, no
    matter how the tributary rule is set."""
    g = _braid_with_tributary()
    g.nodes["9:0"] = replace(g.nodes["9:0"], display_name="Nicomen Slough")
    _g, n, _fids = prune_mainstem_loops(g, reconnect_tributaries=True)
    assert n == 0 and "9:0" in g.nodes


def test_reconnect_still_refuses_a_dead_end_channel():
    """A channel that never rejoins has no downstream exit to re-home anything onto."""
    g = _braid_with_tributary()
    g.edges = [e for e in g.edges if not (e.from_node == "9:0" and e.to_node == "1:0")]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    _g, n, _fids = prune_mainstem_loops(g, reconnect_tributaries=True)
    assert n == 0 and "9:0" in g.nodes


# --- connectivity, and the three things that stopped the Fraser's braids being seen -------------

def test_a_pass_through_that_is_someones_only_route_is_kept():
    """An inflow plus an outflow is a PASS-THROUGH, not a loop.

    Reading the two together as "a loop off the mainstem" and removing the component stranded whatever
    it was the only way downstream for -- 147 nodes in one pass. Asking what each water would LOSE
    settles it directly: the creek has no other route, so the channel stays and nothing is re-homed."""
    g = _graph([("M:hi", "M:lo"), ("S:0", "C:0"), ("C:0", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                _n("S:0", "S", name="Little Creek"), _n("C:0", "C")])
    _g, n, _fids = prune_mainstem_loops(g)
    assert n == 0 and "C:0" in g.nodes
    assert {g.edges[i].to_node for i in g.down_adj.get("S:0", [])} == {"C:0"}


def test_every_surviving_node_keeps_a_way_downstream():
    """The guarantee the module makes, checked as a whole rather than per case."""
    g = _graph([("M:hi", "M:lo"), ("S:0", "C:0"), ("C:0", "M:lo"), ("T:0", "C:0")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                _n("S:0", "S", name="Little Creek"), _n("C:0", "C"),
                _n("T:0", "T", wsc=TRIB, name="Small Creek")])
    had = {e.from_node for e in g.edges}
    prune_mainstem_loops(g, reconnect_tributaries=True)
    for nid in had & set(g.nodes):
        assert g.down_adj.get(nid), f"{nid} had a way downstream and lost it"


def test_the_gazetted_name_in_caps_is_not_a_second_water():
    """FWA carries "FRASER RIVER" alongside "Fraser River". Comparing case-sensitively made every
    piece of a big river look individually named, so none of its braiding was ever a candidate."""
    tuples = (NameTuple("BIG RIVER", NameSource.gazette, gnis_id="1"),)
    assert loop_nodes(_river(braid_tuples=tuples)) == {"B:0"}


def test_a_named_slough_does_not_immunise_the_braids_hanging_off_it():
    """Nicomen Slough on the Fraser. The slough is a water and stays; the anonymous braids looping
    off it are not, and judging one component containing both let the slough's name protect them."""
    g = _graph([("M:hi", "M:lo"), ("M:lo", "N:0"), ("N:0", "M:hi"),
                ("N:0", "X:0"), ("X:0", "N:0")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                _n("N:0", "N", name="Nicomen Slough", length=3000.0),
                _n("X:0", "X")])                            # anonymous braid off the slough
    assert loop_nodes(g) == {"X:0"}


def test_the_river_flowing_through_its_own_lake_is_not_a_foreign_water():
    """Lakes were left out of the watershed-code index, so a lake inflow read as "another water
    discharges here" and the braid below it was kept as if it carried a tributary."""
    from pipeline.models import StreamNode
    lake = StreamNode(node_id="lake:1", kind=NodeKind.lake, blk="", wsc=MAIN, display_name="Big Lake")
    g = _graph([("M:hi", "lake:1"), ("lake:1", "M:lo"), ("lake:1", "C:0"), ("C:0", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0), lake, _n("C:0", "C")])
    assert loop_nodes(g) == {"C:0"}, "no tributary here — just the river past its own lake"


def test_a_channel_is_kept_when_re_homing_would_move_a_mouth_too_far():
    """The exchange -- a channel removed, a confluence relocated -- is only worth making for a small
    anabranch. The distance that matters is from the water being moved to the confluence it is GIVEN;
    comparing the old target with the new one reads ~0 whenever a side channel touches the river it
    feeds, which is true of every braid, so that version never fired where it mattered."""
    from shapely.geometry import LineString
    g = _braid_with_creek_with_two_mouths()
    g.nodes["1:0"] = replace(g.nodes["1:0"], down_m=0.0, up_m=40000.0)
    geoms = {"1:1000": LineString([(0, 40000), (0, 45000)]),
             "1:0": LineString([(0, 0), (0, 40000)]),
             "9:0": LineString([(0, 39000), (0, 40000)]),
             "5:0": LineString([(40000, 0), (41000, 0)])}     # creek sits 40 km off the mainstem
    _g, n, _fids = prune_mainstem_loops(g, geoms, reconnect_tributaries=True, max_shift_m=5000.0)
    assert n == 0 and "9:0" in g.nodes, "40 km is not a braid: keep it rather than guess"
    _g, n, _fids = prune_mainstem_loops(g, geoms, reconnect_tributaries=True, max_shift_m=50_000.0)
    assert n == 1, "raise the cap and the same channel becomes removable"


def test_a_re_homed_confluence_lands_where_the_braid_met_the_river():
    """`at_measure` is a measure on the `to_node`, so moving an edge without recomputing it leaves a
    number belonging to the deleted blue line. Nothing complains at the time -- a later stage files
    each incoming edge into the piece whose measure range contains `at_measure` -- so the confluence
    ends up on the right river in the wrong place. Maria Slough came out attached to the Fraser 43 km
    below where it actually joins.

    The measure to use is where the CHANNEL met the river: the creek's water ran creek -> braid ->
    river and entered the river at a real place, so that place is the confluence it inherits."""
    from shapely.geometry import LineString
    g = _braid_with_creek_with_two_mouths()
    g.nodes["1:0"] = replace(g.nodes["1:0"], down_m=0.0, up_m=5000.0)
    for i, e in enumerate(g.edges):                     # the braid meets the mainstem at 1500
        if e.from_node == "9:0" and e.to_node == "1:0":
            g.edges[i] = replace(e, at_measure=1500.0)
        if e.from_node == "5:0" and e.to_node == "9:0":
            g.edges[i] = replace(e, at_measure=30.0)    # stale: a measure on the BRAID's line
    geoms = {"1:1000": LineString([(5000, 0), (9000, 0)]), "1:0": LineString([(0, 0), (5000, 0)]),
             "9:0": LineString([(1400, 20), (1600, 20)]), "5:0": LineString([(1500, 0), (1500, 900)])}
    _g, n, _fids = prune_mainstem_loops(g, geoms, reconnect_tributaries=True)
    assert n == 1 and "9:0" not in g.nodes
    e = next(e for e in g.edges if e.from_node == "5:0" and e.to_node == "1:0"
             and abs(e.at_measure - 1500.0) < 1.0)
    assert e is not None, "must inherit where the braid met the river"


def test_the_measure_falls_back_to_projection_when_the_braid_left_none():
    """A channel can exit into a lake, which carries no measure to inherit."""
    from shapely.geometry import LineString
    g = _braid_with_creek_with_two_mouths()
    g.nodes["1:0"] = replace(g.nodes["1:0"], down_m=0.0, up_m=5000.0)
    geoms = {"1:1000": LineString([(5000, 0), (9000, 0)]), "1:0": LineString([(0, 0), (5000, 0)]),
             "9:0": LineString([(2400, 20), (2600, 20)]), "5:0": LineString([(2500, 0), (2500, 900)])}
    _g, n, _fids = prune_mainstem_loops(g, geoms, reconnect_tributaries=True)
    assert n == 1
    e = next(e for e in g.edges if e.from_node == "5:0")
    assert e.to_node == "1:0" and 0.0 <= e.at_measure <= 5000.0, "a measure inside the piece it points at"


def test_a_named_water_is_not_left_hanging_short_of_the_river():
    """Maria Slough. The channel that physically carried its mouth to the Fraser was an anonymous
    braid; removing it left the slough's mouth 1 km out in open ground, still attached in the graph
    but visibly detached on the map. An anonymous braid can absorb that gap. A named water — the
    thing a reader looks for and a regulation names — cannot, so the channel stays."""
    from shapely.geometry import LineString
    g = _graph([("M:hi", "M:lo"), ("N:0", "C:0"), ("C:0", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                _n("N:0", "N", name="Maria Slough"), _n("C:0", "C")])
    g.nodes["M:lo"] = replace(g.nodes["M:lo"], down_m=0.0, up_m=5000.0)
    geoms = {"M:hi": LineString([(0, 5000), (0, 9000)]), "M:lo": LineString([(0, 0), (0, 5000)]),
             "N:0": LineString([(1000, 0), (1000, 900)]),   # 1 km out from the river
             "C:0": LineString([(0, 10), (1000, 10)])}      # the channel that bridges the gap
    _g, n, _fids = prune_mainstem_loops(g, geoms)
    assert n == 0 and "C:0" in g.nodes, "the slough's only physical link to the river must stay"

    # the same braid, carrying an anonymous piece instead, is removable
    g2 = _graph([("M:hi", "M:lo"), ("M:hi", "N:0"), ("N:0", "C:0"), ("C:0", "M:lo")],
                [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0),
                 _n("N:0", "N"), _n("C:0", "C")])
    g2.nodes["M:lo"] = replace(g2.nodes["M:lo"], down_m=0.0, up_m=5000.0)
    _g, n, _fids = prune_mainstem_loops(g2, geoms)
    assert n == 2, "an anonymous channel absorbs the gap"


def test_a_component_that_exits_into_a_lake_is_still_capped():
    """A lake has no route measure to interpolate along, so the confluence point is undefined and the
    gap read as zero — which exempted every lake-bound component from the cap. 154 removed components
    exit into a lake, and the promise has to hold for them too."""
    from shapely.geometry import LineString, Polygon
    from pipeline.models import StreamNode
    lake = StreamNode(node_id="lake:1", kind=NodeKind.lake, blk="", wsc=MAIN, display_name="Big Lake")
    g = _graph([("M:hi", "M:lo"), ("N:0", "C:0"), ("C:0", "lake:1"), ("lake:1", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0), lake,
                _n("N:0", "N", name="Maria Slough"), _n("C:0", "C")])
    geoms = {"M:hi": LineString([(0, 5000), (0, 9000)]), "M:lo": LineString([(0, 0), (0, 5000)]),
             "lake:1": Polygon([(0, 0), (100, 0), (100, 100), (0, 100)]),
             "N:0": LineString([(4000, 0), (4000, 900)]),   # 3.9 km from the lake
             "C:0": LineString([(100, 50), (4000, 50)])}
    _g, n, _fids = prune_mainstem_loops(g, geoms)
    assert n == 0 and "C:0" in g.nodes, "the cap must apply when the exit is a lake"




def test_a_braid_that_rejoins_its_own_piece_never_becomes_a_self_loop():
    """A braid can leave a piece and rejoin THAT SAME PIECE, which puts the piece in its own nest's
    exit set. Re-homing the inflow onto the nearest exit then writes `X -> X`: an edge carrying
    nothing, because the water is already where it is being sent, and a trap for any downstream walk.
    The prune produced 54 of these against the source graph's 0."""
    from shapely.geometry import LineString
    g = _graph([("M:hi", "M:lo"), ("M:lo", "B:0"), ("B:0", "M:lo")],
               [_n("M:hi", "M", length=5000.0), _n("M:lo", "M", length=5000.0), _n("B:0", "B")])
    geoms = {"M:hi": LineString([(0, 5000), (0, 9000)]), "M:lo": LineString([(0, 0), (0, 5000)]),
             "B:0": LineString([(10, 1000), (10, 2000)])}
    prune_mainstem_loops(g, geoms, reconnect_tributaries=True)
    assert not [e for e in g.edges if e.from_node == e.to_node], \
        "a piece must never end up flowing into itself"
    assert g.down_adj.get("M:lo"), "and M:lo keeps its way downstream"
