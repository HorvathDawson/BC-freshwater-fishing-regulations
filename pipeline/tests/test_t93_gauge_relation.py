"""T93: every (section, gauge) candidate carries its RELATIONSHIP, and which may speak is a POLICY.

The candidates and their facts (where the gauge stands, how far along the channel, the ratio) come from the shed walk
once; a policy only admits relationships. `drainage` is today's gate and the default — the owner's: "keep the current
gate as the default". The two cases the ticket names are built here as small graphs:

    Slesse Creek (gauge on a TRIBUTARY of the Chilliwack): refused for the Chilliwack under `drainage`, admitted under
    `ungated` — the pre-gate behaviour, which is what told 13 Chilliwack reaches a creek's flow.
    The Chilliwack/Vedder (one watercourse, the gauge at Vedder Crossing): the Vedder below hears it as "upstream", a
    tributary above it ("Tamihi") hears it as "downstream on the river it flows into" — and loses it under
    `same_watercourse`.
"""

from __future__ import annotations

import itertools

import pytest

from pipeline.common.models.enums import NodeKind
from pipeline.common.models.graph import FlowEdge, StreamGraph, StreamNode
from pipeline.gauges.consume.shed import (
    DRAINS_THROUGH, GAUGE_POLICY, POLICIES, REL_DOWN, REL_DOWN_TRUNK, REL_ON, REL_UP, REL_UP_BRANCH,
    build_gauge_sheds, drains_through, relate,
)

CHILLIWACK = "100-001000"          # the trunk (the Vedder is the same code below Vedder Crossing)
SLESSE = CHILLIWACK + "-002000"    # a tributary of it
TAMIHI = CHILLIWACK + "-003000"    # another


def _graph(nodes: dict[str, tuple[int, str, float]], edges: list[tuple[str, str]]) -> StreamGraph:
    """`nodes`: id -> (magnitude, code, length_m); `edges`: (from, to) = "from flows into to"."""
    g = StreamGraph()
    for nid, (mag, wsc, length) in nodes.items():
        g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk="1", wsc=wsc, stream_magnitude=mag,
                                  length_m=length)
    for ix, (a, b) in enumerate(edges):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


def _chilliwack() -> StreamGraph:
    """Chilliwack Lake outlet -> above Slesse -> (Slesse joins) -> below Slesse -> (Tamihi joins) -> Vedder Crossing
    -> the Vedder (two reaches). Slesse Creek is a seventh of the river."""
    return _graph({
        "chw_upper": (60, CHILLIWACK, 5_000.0),
        "slesse_up": (8, SLESSE, 3_000.0),
        "slesse": (10, SLESSE, 2_000.0),            # the Slesse gauge's section (gauge at its downstream end)
        "chw_below_slesse": (70, CHILLIWACK, 4_000.0),
        "tamihi": (9, TAMIHI, 2_500.0),
        "chw_vedder_xing": (80, CHILLIWACK, 3_000.0),  # 08MH001's section
        "vedder_1": (82, CHILLIWACK, 6_000.0),
        "vedder_2": (85, CHILLIWACK, 7_000.0),
    }, [("chw_upper", "chw_below_slesse"), ("slesse_up", "slesse"), ("slesse", "chw_below_slesse"),
        ("chw_below_slesse", "chw_vedder_xing"), ("tamihi", "chw_vedder_xing"),
        ("chw_vedder_xing", "vedder_1"), ("vedder_1", "vedder_2")])


STATIONS = [{"station": "08MH001"}, {"station": "08MH056"}]
AT = {"08MH001": "chw_vedder_xing", "08MH056": "slesse"}


def _by_section(links):
    return {l.section_id: l for l in links}


class TestRelate:
    @pytest.mark.parametrize("sec,gauge", list(itertools.product(
        ["", CHILLIWACK, SLESSE, TAMIHI, "100", "100-0010001", "200-000001"], repeat=2)))
    def test_the_gate_reads_the_same_through_the_relationship(self, sec, gauge):
        # the default policy is byte-identical only if `relate(...) in DRAINS_THROUGH` IS `drains_through`, for any
        # walk direction and hop count
        for up, hops in itertools.product((True, False), (0, 3)):
            assert (relate(sec, gauge, hops=hops, upstream_walk=up) in DRAINS_THROUGH) == drains_through(sec, gauge)

    def test_the_default_is_the_gate_plus_continuations(self):
        # owner 2026-10-28 (Q31): the drainage gate, plus `up_branch` across a `water_continue` link — nothing else
        pol = POLICIES[GAUGE_POLICY]
        assert GAUGE_POLICY == "drainage_continued"
        assert pol.admits == DRAINS_THROUGH == POLICIES["drainage"].admits
        assert pol.admits_joined == frozenset({REL_UP_BRANCH})


class TestChilliwack:
    def test_relationships_and_channel_distances(self):
        cands: list = []
        build_gauge_sheds(_chilliwack(), STATIONS, AT, candidates=cands)
        at = {(c.section_id, c.station): c for c in cands}
        # 08MH001 stands at the downstream end of its own section
        assert at[("chw_vedder_xing", "08MH001")].rel == REL_ON and at[("chw_vedder_xing", "08MH001")].dist_m == 0
        # the Vedder below: the gauge is UPSTREAM; the first reach starts at the gauge, the next 6 km further on
        assert (at[("vedder_1", "08MH001")].rel, at[("vedder_1", "08MH001")].dist_m) == (REL_UP, 0)
        assert (at[("vedder_2", "08MH001")].rel, at[("vedder_2", "08MH001")].dist_m) == (REL_UP, 6_000)
        # the Chilliwack above: the gauge is DOWNSTREAM, past the gauge's own 3 km section
        assert (at[("chw_below_slesse", "08MH001")].rel, at[("chw_below_slesse", "08MH001")].dist_m) == (REL_DOWN, 3_000)
        assert at[("chw_upper", "08MH001")].dist_m == 7_000
        # Tamihi Creek: the gauge is downstream on the river it flows into
        assert at[("tamihi", "08MH001")].rel == REL_DOWN_TRUNK
        # Slesse's gauge seen from the Chilliwack below the confluence: on a tributary of it — a candidate, refused
        assert at[("chw_below_slesse", "08MH056")].rel == REL_UP_BRANCH
        assert not at[("chw_below_slesse", "08MH056")].drains

    def test_drainage_keeps_slesse_off_the_chilliwack(self):
        links = _by_section(build_gauge_sheds(_chilliwack(), STATIONS, AT))
        assert links["slesse"].station == "08MH056" and links["slesse_up"].station == "08MH056"
        assert all(l.station == "08MH001" for s, l in links.items() if not s.startswith("slesse"))
        assert links["vedder_2"].rel == REL_UP and links["vedder_2"].dist_m == 6_000

    def test_ungated_lets_slesse_speak_for_the_chilliwack(self):
        # the pre-gate behaviour, as a policy: with no Chilliwack gauge below, Slesse's creek flow is the
        # Chilliwack's answer. (`build_gauge_sheds` with 08MH001 alone in the roster would leave it unanswered.)
        only = {"08MH056": "slesse"}
        drainage = _by_section(build_gauge_sheds(_chilliwack(), STATIONS, only))
        ungated = _by_section(build_gauge_sheds(_chilliwack(), STATIONS, only, policy="ungated"))
        assert "chw_below_slesse" not in drainage
        assert ungated["chw_below_slesse"].station == "08MH056"
        assert ungated["chw_below_slesse"].rel == REL_UP_BRANCH

    def test_same_watercourse_tells_a_tributary_nothing_from_its_trunk(self):
        drainage = _by_section(build_gauge_sheds(_chilliwack(), STATIONS, AT))
        strict = _by_section(build_gauge_sheds(_chilliwack(), STATIONS, AT, policy="same_watercourse"))
        assert drainage["tamihi"].station == "08MH001" and drainage["tamihi"].rel == REL_DOWN_TRUNK
        assert "tamihi" not in strict
        # the Vedder is the Chilliwack's own watercourse: unchanged
        assert strict["vedder_2"].station == "08MH001"

    def test_without_continuations_the_default_is_the_gate(self):
        # no `water_continue` link given: nothing is joined, so the default answers exactly as `drainage`, and every
        # admitted link drains through its gauge (the old filter)
        g = _chilliwack()
        a = build_gauge_sheds(g, STATIONS, AT)
        b = build_gauge_sheds(g, STATIONS, AT, policy="drainage")
        assert a == b and all(drains_through(g.nodes[l.section_id].wsc, g.nodes[AT[l.station]].wsc) for l in a)

    def test_candidates_are_one_per_pair_and_sorted(self):
        cands: list = []
        build_gauge_sheds(_chilliwack(), STATIONS, AT, candidates=cands)
        keys = [(c.section_id, c.station) for c in cands]
        assert keys == sorted(set(keys))

    def test_an_unknown_policy_is_refused(self):
        with pytest.raises(KeyError):
            build_gauge_sheds(_chilliwack(), STATIONS, AT, policy="nearest")


class TestShippedEncoding:
    """The release carries `rel` as an index and the distance as a bucket (conditions' size gate); the app decodes
    both with the emitted GAUGE_RELS / DIST_STEP (`relOfCode` / `metresOfBucket` in core trace.ts)."""

    def test_rel_codes_are_indexes_into_the_vocabulary(self):
        from pipeline.gauges.consume.shed import GAUGE_RELS, rel_code
        assert [rel_code(r) for r in GAUGE_RELS] == list(range(len(GAUGE_RELS)))
        assert rel_code(REL_ON) == 0 and rel_code(REL_DOWN_TRUNK) == 3

    @pytest.mark.parametrize("metres,bucket", [(0, 0), (499, 0), (500, 1), (999, 1), (1000, 2), (1500, 3),
                                               (11_000, 8), (11_391, 8)])
    def test_distance_buckets(self, metres, bucket):
        from pipeline.gauges.consume.shed import dist_bucket
        assert dist_bucket(metres) == bucket


# ---- Q31: the Vedder and the Chilliwack as FWA codes them — the Chilliwack a BRANCH of the Vedder ---------------------
VEDDER = "100-001000"
CHW = VEDDER + "-005000"
SLS = CHW + "-002000"
WATER = {"chw_upper": "chilliwack", "chw_below_slesse": "chilliwack", "chw_vedder_xing": "chilliwack",
         "slesse": "slesse", "vedder_1": "vedder", "vedder_2": "vedder", "fraser": "fraser"}
CONTINUES = [("chilliwack", "vedder")]      # the Chilliwack goes on as the Vedder (a settled `water_continue`)


def _fwa_vedder() -> StreamGraph:
    return _graph({
        "chw_upper": (60, CHW, 5_000.0),
        "slesse": (10, SLS, 2_000.0),
        "chw_below_slesse": (70, CHW, 4_000.0),
        "chw_vedder_xing": (80, CHW, 3_000.0),       # 08MH001
        "vedder_1": (82, VEDDER, 6_000.0),
        "vedder_2": (85, VEDDER, 7_000.0),
        "fraser": (500, "100", 10_000.0),            # 08MH024, the Fraser below
    }, [("chw_upper", "chw_below_slesse"), ("slesse", "chw_below_slesse"), ("chw_below_slesse", "chw_vedder_xing"),
        ("chw_vedder_xing", "vedder_1"), ("vedder_1", "vedder_2"), ("vedder_2", "fraser")])


class TestContinuation:
    ST = [{"station": "08MH001"}, {"station": "08MH024"}, {"station": "08MH056"}]

    def test_the_vedder_takes_the_chilliwack_gauge_across_the_link(self):
        at = {"08MH001": "chw_vedder_xing", "08MH024": "fraser"}
        g = _fwa_vedder()
        gate = _by_section(build_gauge_sheds(g, self.ST, at, policy="drainage", water_of=WATER, continues=CONTINUES))
        dflt = _by_section(build_gauge_sheds(g, self.ST, at, water_of=WATER, continues=CONTINUES))
        # the gate: 08MH001 is up a branch of the Vedder, so the Fraser speaks for it
        assert gate["vedder_2"].station == "08MH024" and gate["vedder_2"].rel == REL_DOWN_TRUNK
        # the default: one river under two names — the Chilliwack's own gauge
        assert dflt["vedder_2"].station == "08MH001"
        assert (dflt["vedder_2"].rel, dflt["vedder_2"].joined) == (REL_UP_BRANCH, True)
        # nothing else moves
        assert {s: l.station for s, l in dflt.items() if not s.startswith("vedder")} == \
               {s: l.station for s, l in gate.items() if not s.startswith("vedder")}

    def test_slesse_still_stays_off_the_chilliwack(self):
        # Slesse Creek is a tributary, not a continuation: its `up_branch` candidates are not joined, so refused
        at = {"08MH056": "slesse"}
        cands: list = []
        dflt = _by_section(build_gauge_sheds(_fwa_vedder(), self.ST, at, water_of=WATER, continues=CONTINUES,
                                             candidates=cands))
        assert "chw_below_slesse" not in dflt and "chw_vedder_xing" not in dflt
        c = {(x.section_id, x.station): x for x in cands}[("chw_below_slesse", "08MH056")]
        assert (c.rel, c.joined) == (REL_UP_BRANCH, False)

    def test_a_water_and_itself_is_not_joined(self):
        cands: list = []
        build_gauge_sheds(_fwa_vedder(), self.ST, {"08MH001": "chw_vedder_xing"}, water_of=WATER,
                          continues=CONTINUES, candidates=cands)
        assert not any(c.joined for c in cands if WATER[c.section_id] == "chilliwack")



def test_a_continuation_never_outranks_the_waters_own_gauge():
    # the Bella Coola case, measured: a side reach of the Bella Coola with its own river's gauge (`up`, ratio 0.018)
    # keeps it rather than taking a continuation's gauge, even one with a large share (`up_branch` across the link)
    from pipeline.gauges.consume.shed import GaugeLink, REL_UP, choose
    own = GaugeLink("s", "08FB011", "fair", 0.018, 9, REL_UP, 37_100)
    cont = GaugeLink("s", "08FB006", "good", 0.6, 20, REL_UP_BRANCH, 72_200, joined=True)
    assert choose({"s": [own, cont]}, {}, "drainage_continued")[0].station == "08FB011"
    assert choose({"s": [cont]}, {}, "drainage_continued")[0].station == "08FB006"      # a gap is filled
    assert choose({"s": [cont]}, {}, "drainage") == []


def test_the_app_hears_a_continued_rivers_gauge_as_upstream():
    from pipeline.gauges.consume.shed import GaugeLink, REL_UP, shown_rel
    assert shown_rel(GaugeLink("v", "08MH001", "good", 0.99, 2, REL_UP_BRANCH, 200, joined=True)) == REL_UP
    assert shown_rel(GaugeLink("c", "08MH056", "fair", 0.14, 2, REL_UP_BRANCH, 200)) == REL_UP_BRANCH
    assert shown_rel(GaugeLink("t", "08MH001", "good", 0.5, 2, REL_DOWN_TRUNK, 200)) == REL_DOWN_TRUNK


def test_a_continuation_speaks_only_with_a_large_share():
    # owner 2026-10-28 (Q31 refinement): the Q44 floor, `panels.LARGE_SHARE`, on the symmetric magnitude ratio
    from pipeline.gauges.consume.panels import LARGE_SHARE
    from pipeline.gauges.consume.shed import GaugeLink, choose
    assert POLICIES["drainage_continued"].joined_min_ratio == LARGE_SHARE == 0.5
    vedder = GaugeLink("v", "08MH001", "good", 0.985, 2, REL_UP_BRANCH, 8_200, joined=True)
    side = GaugeLink("s", "08FB006", "weak", 0.001, 20, REL_UP_BRANCH, 72_200, joined=True)
    edge = GaugeLink("e", "X", "good", LARGE_SHARE, 2, REL_UP_BRANCH, 0, joined=True)
    got = {l.section_id: l.station for l in choose({"v": [vedder], "s": [side], "e": [edge]}, {}, "drainage_continued")}
    assert got == {"v": "08MH001", "e": "X"}
