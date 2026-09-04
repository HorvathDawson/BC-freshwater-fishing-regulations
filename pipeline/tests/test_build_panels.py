"""The panel walk: who reaches whom, in which direction, and how it compresses."""

from __future__ import annotations

from dataclasses import dataclass, field

from pipeline.atlas.gauges.build_panels import build, candidates
from pipeline.atlas.graph.drainage import AreaModel

#: area = 1 * mag^1, so a magnitude IS an area here and the arithmetic stays readable.
MODEL = AreaModel(b=1.0, c_default=1.0, c_by_basin={})


@dataclass
class N:
    node_id: str
    stream_magnitude: int = 1
    kind: str = "stream"
    wsc: str = "100-000000"


@dataclass
class E:
    from_node: str
    to_node: str


@dataclass
class G:
    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)


def g(nodes, edges):
    return G(nodes={n.node_id: n for n in nodes}, edges=edges)


AREA = lambda n: MODEL.area_km2(n.stream_magnitude, n.wsc)


# ------------------------------------------------------------------ direction

def test_walking_upstream_from_a_gauge_makes_it_a_DOWNSTREAM_donor():
    """The flip that is invisible when wrong: every weight still computes and the direction
    penalty is simply applied to the wrong half of the province."""
    graph = g([N("upper", 100), N("gauged", 110)], [E("upper", "gauged")])
    got = candidates(graph, AREA, [("S", "gauged", 40, False)])
    assert [c[1] for c in got["upper"]] == ["down"]


def test_walking_downstream_from_a_gauge_makes_it_an_UPSTREAM_donor():
    graph = g([N("gauged", 100), N("lower", 110)], [E("gauged", "lower")])
    got = candidates(graph, AREA, [("S", "gauged", 40, False)])
    assert [c[1] for c in got["lower"]] == ["up"]


# ------------------------------------------------------------------ the bounds

def test_the_walk_stops_where_the_catchments_stop_being_comparable():
    graph = g([N("gauged", 100), N("near", 150), N("far", 100_000)],
              [E("gauged", "near"), E("near", "far")])
    got = candidates(graph, AREA, [("S", "gauged", 40, False)], max_ratio=10.0)
    assert "near" in got and "far" not in got


def test_a_lake_on_the_path_taints_everything_beyond_it():
    """Storage does not un-attenuate: once the signal has been through a lake, every
    section downstream of that lake is equally cut off from the donor."""
    graph = g([N("gauged", 100), N("thelake", 105, kind="lake"), N("below", 110)],
              [E("gauged", "thelake"), E("thelake", "below")])
    got = candidates(graph, AREA, [("S", "gauged", 40, False)])
    assert got["below"][0][5] is True          # crossed_lake


def test_a_lake_is_not_itself_offered_a_panel():
    """A lake's level is set by its outlet and its own storage, so a stream gauge upstream
    has nothing to say about it — `lake_gauge` is that relationship, not this."""
    graph = g([N("gauged", 100), N("thelake", 105, kind="lake")],
              [E("gauged", "thelake")])
    assert "thelake" not in candidates(graph, AREA, [("S", "gauged", 40, False)])


def test_a_gauge_whose_section_is_not_in_the_graph_is_skipped_not_crashed():
    graph = g([N("a", 100)], [])
    assert candidates(graph, AREA, [("S", "missing", 40, False)]) == {}


# ------------------------------------------------------------------ the dictionary

def test_a_stretch_of_river_shares_one_panel():
    """The whole reason this compresses — and the reason the panel stores the donor SET
    rather than the weights.

    Every reach along this chain has the same donor, but each has its OWN catchment, so
    each would compute a slightly different weight. Baking weights in therefore gives five
    panels where there should be one: measured over the province, 16,127 against 1,898.
    """
    chain = [N(f"s{i}", 100 + i) for i in range(6)]
    graph = g(chain, [E(f"s{i}", f"s{i+1}") for i in range(5)])
    got = build(graph, MODEL, [("S", "s0", 40, False)])
    assert len(got.by_section) == 5                             # s1..s5
    assert len({pid for pid, _ in got.by_section.values()}) == 1


def test_each_section_keeps_its_own_area_so_the_weight_stays_exact():
    """The other half: sharing a panel must not mean sharing a weight. The target's own
    catchment rides on the section row, so the client computes the ratio exactly."""
    chain = [N(f"s{i}", 100 + i) for i in range(4)]
    graph = g(chain, [E(f"s{i}", f"s{i+1}") for i in range(3)])
    got = build(graph, MODEL, [("S", "s0", 40, False)])
    areas = {sec: area for sec, (_pid, area) in got.by_section.items()}
    assert areas["s1"] == 101 and areas["s3"] == 103


def test_two_rivers_with_different_donors_do_not_share_a_panel():
    graph = g([N("a0", 100), N("a1", 105), N("b0", 100), N("b1", 105)],
              [E("a0", "a1"), E("b0", "b1")])
    got = build(graph, MODEL, [("A", "a0", 40, False), ("B", "b0", 40, False)])
    assert got.by_section["a1"][0] != got.by_section["b1"][0]


def test_the_rows_carry_facts_rather_than_anything_derived():
    """Not a weight, not a ratio, not a trust band — every one of those is a calibration,
    and freezing a calibration into the bundle means re-measuring needs a rebuild. See
    schema.sql. The client has both areas and can derive all three exactly."""
    graph = g([N("gauged", 100), N("target", 200)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 40, False)])
    (sec, pid, area), = got.section_rows()
    assert (sec, area) == ("target", 200.0)
    (mpid, ordinal, station, role, donor_area, years), = got.member_rows()
    assert (mpid, ordinal, station, role, donor_area, years) == (pid, 0, "S", "up", 100.0, 40)


def test_a_regulated_donor_never_forms_a_panel():
    graph = g([N("gauged", 100), N("target", 110)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 40, True)])
    assert got.by_section == {}


def test_a_section_nothing_qualifies_for_gets_no_panel_at_all():
    """Absent, not empty. The two are different answers and the app's types keep them
    apart — a caller that cannot tell renders silence as a loading state."""
    graph = g([N("gauged", 100), N("target", 110)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 3, False)])     # too short a record
    assert "target" not in got.by_section
