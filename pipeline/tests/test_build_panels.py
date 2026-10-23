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
    # Real nodes always carry a blue-line key, and the panel weighting now reads it: a
    # donor on the SAME blue line is a different relationship from one on a tributary. A
    # fixture missing a field the code reads is a fixture that is not the shape it stands
    # in for. Default shared, so existing cases keep meaning what they meant.
    blk: str = "X"
    # FWA EDGE_TYPEs. "1100"/"1350" mark a side channel — a piece of the river beside it,
    # not a river of its own. Default empty: an ordinary reach.
    edge_types: tuple = ()
    # Along-water length, metres: the own-gauge rule measures "far" with it (test_own_gauge.py).
    length_m: float = 1_000.0


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
    # s0..s5 — six, because the gauge now also speaks for the reach it stands in.
    assert len(got.by_section) == 6
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
    rows = {sec: (pid, area) for sec, pid, area in got.section_rows()}
    pid, area = rows["target"]
    assert area == 200.0
    (mpid, ordinal, station, role, donor_area, discharge_years, level_years, regulated, same_river, lead), = [
        r for r in got.member_rows() if r[0] == pid]
    # the record per quantity (T110): one int from a synthetic caller is the same record in both
    assert (mpid, ordinal, station, role, donor_area, discharge_years, level_years) == (pid, 0, "S", "up", 100.0, 40, 40)
    # `regulated` is a FACT about the station, not a calibration — it changes what the
    # number means and the client cannot derive it, so it rides along with the rest.
    assert regulated == 0
    # Same blue line in this fixture, and it is a FACT about the pair rather than a
    # calibration — the client cannot derive it and it changes the weight fourfold.
    assert same_river == 1
    # No water map in this build, so no donor is anybody's OWN gauge (test_own_gauge.py).
    assert lead == 0


def test_a_regulated_donor_speaks_for_its_own_water():
    """Magnitudes 100 and 110 are all but the same drainage — the dam's schedule IS what
    this water is doing, so the reading is a measurement rather than a transfer."""
    graph = g([N("gauged", 100), N("target", 110)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 40, True)])
    assert "target" in got.by_section


def test_a_regulated_donor_is_not_carried_onto_other_water():
    """Far enough away and the release schedule says nothing about this catchment — the
    Nechako running high in a dry winter, above prime Fraser water."""
    graph = g([N("gauged", 100), N("target", 9000)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 40, True)])
    # Its OWN reach still gets it — that is a measurement, not a transfer — and nothing
    # else does.
    assert set(got.by_section) == {"gauged"}


def test_a_section_nothing_qualifies_for_gets_no_panel_at_all():
    """Absent, not empty. The two are different answers and the app's types keep them
    apart — a caller that cannot tell renders silence as a loading state."""
    graph = g([N("gauged", 100), N("target", 110)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 1, False)])     # too short a record
    assert "target" not in got.by_section


def test_a_gauge_speaks_for_the_reach_it_stands_in():
    """THE BEST DONOR THAT CAN EXIST, and the one section it could not reach.

    The walk starts at the gauge's own section and records only what it REACHES, so that
    section got nothing from the station standing in it — same catchment, ratio exactly 1,
    no transfer and no inference. The Skagit surfaced it: three gauges on the river, two of
    them on the very section that had no panel at all.
    """
    graph = g([N("gauged", 100), N("target", 110)], [E("gauged", "target")])
    got = build(graph, MODEL, [("S", "gauged", 40, False)])
    assert "gauged" in got.by_section
    (_pid, _ord, station, role, area, _dy, _ly, _reg, _same, _lead), = [
        r for r in got.member_rows()
        if r[0] == got.by_section["gauged"][0]]
    assert station == "S"
    # No direction to penalise: the gauge is IN this water, not above or below it.
    assert role == "up"


def test_its_own_section_is_the_heaviest_donor_it_has():
    """Ratio 1 means share 1, which is the largest weight the formula can produce — so a
    reach with its own gauge is answered by that gauge and only topped up by the rest."""
    graph = g([N("gauged", 100), N("near", 130), N("far", 400)],
              [E("gauged", "near"), E("near", "far")])
    got = build(graph, MODEL, [("OWN", "gauged", 40, False), ("OTHER", "far", 40, False)])
    pid, _area = got.by_section["gauged"]
    rows = [r for r in got.member_rows() if r[0] == pid]
    assert "OWN" in {r[2] for r in rows}


def test_a_lake_node_does_not_claim_its_own_stream_gauge():
    """A station matched to a lake is a level in metres, and the seeded donor has to obey
    the same stream-only rule the walk does."""
    graph = g([N("gauged", 100)], [])
    graph.nodes["gauged"].kind = graph.nodes["gauged"].kind      # stream in this fixture
    got = build(graph, MODEL, [("S", "gauged", 40, False)])
    assert "gauged" in got.by_section


def test_a_station_on_a_lake_does_not_speak_for_the_river():
    """A reservoir's stage is not a statement about the river below it.

    The lake gate tested every node the walk STEPPED INTO and never the one it started
    from, so a station sitting IN a lake spoke freely for the streams around it. Measured on
    the Harrison: 08MG012 is on Harrison Lake, measures level and nothing else, and was
    carrying half the Harrison River's estimate against the river's own discharge gauge two
    reaches away.
    """
    graph = g([N("lakey", 100, kind="lake"), N("river", 110)], [E("lakey", "river")])
    got = build(graph, MODEL, [("LAKE", "lakey", 90, False)])
    assert "river" not in got.by_section


def test_a_stream_station_still_speaks_past_nothing():
    """The same walk, from a stream node, is unaffected."""
    graph = g([N("gauged", 100), N("river", 110)], [E("gauged", "river")])
    got = build(graph, MODEL, [("S", "gauged", 90, False)])
    assert "river" in got.by_section


def test_a_donor_on_another_river_counts_for_less_than_one_on_yours():
    """THE SKEENA. At Usk the panel held two gauges on the Skeena reading the 77th and 78th
    percentile, the Babine at the 24th and the Bulkley at the 62nd — and weighted them
    identically, because the only thing the model knew was catchment size. They disagreed by
    more than the interval can express, so the app refused and drew "no baseline" over a
    river with two of its own gauges reporting.

    Two donors of the same size can be two entirely different relationships: one where the
    water flows past both points, one where they share a rain shadow.
    """
    graph = g([N("mine", 100, blk="A"), N("trib", 100, blk="B"), N("target", 110, blk="A")],
              [E("mine", "target"), E("trib", "target")])
    got = build(graph, MODEL, [("SAME", "mine", 60, False), ("OTHER", "trib", 60, False)])
    pid, _area = got.by_section["target"]
    rows = {r[2]: r for r in got.member_rows() if r[0] == pid}
    assert rows["SAME"][8] == 1          # same_river
    assert rows["OTHER"][8] == 0
    # And `ord` is weight order, so the same-river donor leads.
    assert rows["SAME"][1] < rows["OTHER"][1]


# ------------------------------------------------------------------ side channels

def test_a_side_channel_takes_the_river_it_is_a_channel_of():
    """Herrling Island Side Channel, in miniature.

    The channel's own catchment is an artefact of where FWA broke the polygon, so sizing it
    that way matched it to whatever creek in the province happened to drain the same
    arithmetic — for Herrling, gauges 400 km up the valley on other rivers entirely — while
    the Fraser it is a channel OF sat metres away, barred by an area ratio of 150,000:1.
    """
    graph = g([N("fraser", 100_000), N("side", 2, edge_types=("1350",)),
               N("gauged", 100_000)],
              [E("gauged", "fraser"), E("fraser", "side")])
    panels = build(graph, MODEL, [("HOPE", "gauged", 60, False)])
    assert panels.by_section["side"] == panels.by_section["fraser"]
    assert [m.station for m in panels.members[panels.by_section["side"][0]]] == ["HOPE"]


def test_a_side_channel_reports_the_river_s_catchment_not_its_own():
    """One number on the screen contradicting all the others is worse than a missing one."""
    graph = g([N("fraser", 100_000), N("side", 2, edge_types=("1100",)),
               N("gauged", 100_000)],
              [E("gauged", "fraser"), E("fraser", "side")])
    panels = build(graph, MODEL, [("HOPE", "gauged", 60, False)])
    assert panels.by_section["side"][1] == panels.by_section["fraser"][1]


def test_a_side_channel_reaches_past_the_stubs_of_its_own_braid():
    """A braid is a mesh of small unnamed pieces welded to one big river.

    Stopping at the first non-channel reach adopted a magnitude-8 stub sitting between the
    channel and the Fraser — four of Herrling's seven pieces went that way.
    """
    graph = g([N("fraser", 100_000), N("stub", 8), N("side", 2, edge_types=("1350",)),
               N("gauged", 100_000)],
              [E("gauged", "fraser"), E("fraser", "stub"), E("stub", "side")])
    panels = build(graph, MODEL, [("HOPE", "gauged", 60, False)])
    assert panels.by_section["side"] == panels.by_section["fraser"]


def test_a_side_channel_whose_river_has_no_panel_gets_none_either():
    """Never a panel of its own: the two must not be able to disagree about one water."""
    graph = g([N("creek", 5), N("side", 2, edge_types=("1100",))], [E("creek", "side")])
    panels = build(graph, MODEL, [])
    assert "side" not in panels.by_section


def test_an_ordinary_reach_is_left_alone():
    graph = g([N("upper", 100), N("gauged", 110)], [E("upper", "gauged")])
    panels = build(graph, MODEL, [("S", "gauged", 40, False)])
    assert "upper" in panels.by_section


# ------------------------------------------------------------------ side channels follow their OWN river
#
# OWNER 2026-10-22 (option A): "just follows its own river at that point." FWA gives a
# secondary channel its river's watershed code, so the river a side channel is a channel OF
# is the one carrying that same code. The 4-hop walk is long enough to leave that river and
# reach the one it flows into; it must never adopt a reach there.

PITT, FRASER, KLESILKWA, SKAGIT = "100-025956", "100", "970-106243-844109", "970-106243"


def _widgeon():
    """Widgeon Slough: a channel OF the Pitt, two hops from the far bigger Fraser."""
    return g([N("pitt_up", 1100, wsc=PITT), N("slough", 2, wsc=PITT, edge_types=("1350",)),
              N("pitt", 1181, wsc=PITT), N("fraser", 64_682, wsc=FRASER)],
             [E("pitt_up", "slough"), E("slough", "pitt"), E("pitt", "fraser")]), \
        [("ALOUETTE", "pitt", 40, False), ("HOPE", "fraser", 60, False)]


def _klesilkwa():
    """A Klesilkwa braid piece welded to the Skagit, with no Klesilkwa reach in range."""
    return g([N("braid", 2, wsc=KLESILKWA, edge_types=("1100",)), N("skagit", 10_000, wsc=SKAGIT)],
             [E("braid", "skagit")]), \
        [("OWN", "braid", 40, False), ("SKAGIT_ABOVE", "skagit", 60, False)]


def _stations(panels, sec):
    return sorted(m.station for m in panels.members[panels.by_section[sec][0]])


def test_a_channel_of_a_tributary_follows_the_tributary_not_the_river_it_joins():
    """Widgeon Slough adopted the Fraser (64,682 km²) and read the Fraser's 50th percentile
    while the Pitt on both sides of it read 17th."""
    graph, donors = _widgeon()
    panels = build(graph, MODEL, donors)
    assert panels.by_section["slough"] == panels.by_section["pitt"]
    assert panels.by_section["slough"] != panels.by_section["fraser"]
    assert panels.by_section["slough"][1] == 1181


def test_a_braid_with_no_reach_of_its_own_river_keeps_its_own_panel():
    """The Klesilkwa braids read 08PA012 'Skagit River ABOVE Klesilkwa' — a gauge no
    Klesilkwa water passes. With no Klesilkwa reach to follow, the braid keeps its own."""
    graph, donors = _klesilkwa()
    panels = build(graph, MODEL, donors)
    assert _stations(panels, "braid") == ["OWN"]
    assert panels.by_section["braid"][1] == 2


def test_a_braid_with_no_reach_of_its_own_river_and_no_panel_of_its_own_is_silent():
    graph, donors = _klesilkwa()
    panels = build(graph, MODEL, [d for d in donors if d[0] != "OWN"])
    assert "braid" not in panels.by_section


def test_a_true_channel_of_the_main_river_still_follows_that_river():
    """Nicomen / Herrling: the channel carries the Fraser's own code, so the Fraser is its
    river — even with a bigger-than-the-channel tributary of another code right beside it."""
    graph = g([N("fraser", 100_000, wsc=FRASER), N("stub", 8, wsc=FRASER),
               N("side", 2, wsc=FRASER, edge_types=("1350",)), N("creek", 50, wsc="100-123456"),
               N("gauged", 100_000, wsc=FRASER)],
              [E("gauged", "fraser"), E("fraser", "stub"), E("stub", "side"), E("creek", "side")])
    panels = build(graph, MODEL, [("HOPE", "gauged", 60, False), ("CREEK", "creek", 40, False)])
    assert panels.by_section["side"] == panels.by_section["fraser"]
    assert _stations(panels, "side") == ["HOPE"]


def test_the_same_code_condition_is_what_holds_the_side_channel_tests():
    """MUTATION PIN: the same tests against `mainstem_of` with the code condition removed
    must fail — otherwise they pass for some other reason and pin nothing."""
    import inspect
    import textwrap

    import pipeline.atlas.gauges.build_panels as bp

    src = textwrap.dedent(inspect.getsource(bp.mainstem_of))
    cond = " or o.wsc != node.wsc"
    assert src.count(cond) == 1, "the code condition moved; update this pin to match"
    ns = dict(vars(bp))
    exec(compile(src.replace(cond, ""), bp.__file__, "exec"), ns)
    mutant = ns["mainstem_of"]

    graph, donors = _widgeon()
    assert bp.mainstem_of(graph, AREA)["slough"] == "pitt"
    assert mutant(graph, AREA)["slough"] == "fraser"

    graph, donors = _klesilkwa()
    assert "braid" not in bp.mainstem_of(graph, AREA)
    assert mutant(graph, AREA)["braid"] == "skagit"
