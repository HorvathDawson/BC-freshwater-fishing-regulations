"""The gauge-shed pass: who a gauge is allowed to speak for.

Every test here is really the same question — can the app end up reporting a number about
water the gauge never measured? That is the failure that matters. A missing gauge is a
disappointment; a confident wrong discharge is how someone wades into a river.
"""

from __future__ import annotations

import pytest

from pipeline.hydro.match import waterbody_name
from pipeline.hydro.shed import (
    TRUST_BANDS,
    build_gauge_sheds,
    downstream_map,
    lake_gauge_links,
    trust_for,
)
from pipeline.models.enums import NodeKind
from pipeline.models.graph import FlowEdge, StreamGraph, StreamNode


def _node(node_id: str, mag: int | None, name: str = "") -> StreamNode:
    return StreamNode(node_id=node_id, kind=NodeKind.stream, blk="1",
                      display_name=name, stream_magnitude=mag)


def _graph(nodes: dict[str, int | None], edges: list[tuple[str, str]]) -> StreamGraph:
    """A tiny flow graph. `edges` are (from, to) = "from flows into to"."""
    g = StreamGraph()
    for nid, mag in nodes.items():
        g.nodes[nid] = _node(nid, mag)
    for ix, (a, b) in enumerate(edges):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


class TestTrust:
    def test_a_gauge_on_the_section_itself_is_the_best_case(self):
        assert trust_for(100, 100) == "good"

    def test_the_ratio_is_symmetric_because_the_question_is(self):
        # Upstream: what fraction of the gauge's reading do I contribute. Downstream: what
        # fraction of my flow has it seen. Same number, and it must not matter which side
        # the user is standing on.
        assert trust_for(5, 100) == trust_for(100, 5)

    @pytest.mark.parametrize("section,gauge,band", [
        (50, 100, "good"),      # half the gauge's water is this
        (10, 100, "good"),      # exactly at the floor
        (5, 100, "fair"),
        (1, 100, "fair"),       # exactly at the floor
        (1, 1000, "weak"),      # exactly at the floor
        (1, 1001, None),        # one part in a thousand and one: say nothing
    ])
    def test_the_bands_are_where_they_are_documented(self, section, gauge, band):
        assert trust_for(section, gauge) == band

    def test_an_unknown_magnitude_is_not_a_small_one(self):
        # A lake node carries no headwater count. Ranking it at the bottom would quietly
        # hand every lake the nearest river's gauge.
        assert trust_for(None, 100) is None
        assert trust_for(100, None) is None
        assert trust_for(0, 100) is None

    def test_the_bands_are_ordered_and_positive(self):
        floors = [f for _, f in TRUST_BANDS]
        assert floors == sorted(floors, reverse=True)
        assert all(f > 0 for f in floors)


class TestShed:
    def test_a_gauge_reaches_its_own_tributaries(self):
        g = _graph({"trib": 40, "main": 100}, [("trib", "main")])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "main"})
        assert {l.section_id for l in links} == {"main", "trib"}

    def test_a_gauge_reaches_what_it_flows_into(self):
        g = _graph({"main": 100, "below": 120}, [("main", "below")])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "main"})
        assert {l.section_id for l in links} == {"main", "below"}

    def test_a_different_drainage_gets_nothing_however_close(self):
        # The whole point. Two rivers a hundred metres apart, never connected.
        g = _graph({"main": 100, "other": 90}, [])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "main"})
        assert {l.section_id for l in links} == {"main"}

    def test_a_trickle_below_the_weak_floor_is_left_unanswered(self):
        g = _graph({"trickle": 1, "main": 5000}, [("trickle", "main")])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "main"})
        assert {l.section_id for l in links} == {"main"}

    def test_the_shed_never_reaches_past_water_the_gauge_lost(self):
        # main(100) -> huge(200000), which fails the floor -> far(100), which on its own
        # numbers would score 1.0. The shed must end at `huge`: whatever is on the far
        # side of a river a thousand times the size is not this gauge's water, however
        # the arithmetic comes out. Magnitude is monotone downstream in a real graph, so
        # this is a guard against braids and bad data rather than the common case.
        g = _graph({"main": 100, "huge": 200_000, "far": 100},
                   [("main", "huge"), ("huge", "far")])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "main"})
        assert {l.section_id for l in links} == {"main"}

    def test_a_reach_keeps_only_the_gauge_that_most_nearly_is_it(self):
        # A headwater gauge (mag 50) and a big mainstem gauge (mag 5000) both reach `here`.
        # The second is not a second opinion — it is the same question answered from
        # further away, so it is dropped rather than stored as an alternative.
        g = _graph({"here": 50, "mid": 400, "big": 5000},
                   [("here", "mid"), ("mid", "big")])
        links = build_gauge_sheds(
            g, [{"station": "08NEAR"}, {"station": "08BIG"}],
            {"08NEAR": "here", "08BIG": "big"})
        assert [l.station for l in links if l.section_id == "here"] == ["08NEAR"]

    def test_different_reaches_may_still_have_different_best_gauges(self):
        # This is where a river's variety actually lives. Capping per reach must not
        # collapse a whole river onto one station.
        g = _graph({"here": 50, "mid": 400, "big": 5000},
                   [("here", "mid"), ("mid", "big")])
        links = build_gauge_sheds(
            g, [{"station": "08NEAR"}, {"station": "08BIG"}],
            {"08NEAR": "here", "08BIG": "big"})
        assert len({l.station for l in links}) == 2

    def test_a_section_appears_exactly_once(self):
        g = _graph({"here": 50, "mid": 400, "big": 5000},
                   [("here", "mid"), ("mid", "big")])
        links = build_gauge_sheds(
            g, [{"station": "08NEAR"}, {"station": "08BIG"}],
            {"08NEAR": "here", "08BIG": "big"})
        ids = [l.section_id for l in links]
        assert len(ids) == len(set(ids))

    def test_a_tie_is_broken_on_the_station_id_so_rebuilds_are_identical(self):
        g = _graph({"a": 100, "here": 100, "b": 100}, [("a", "here"), ("here", "b")])
        first = build_gauge_sheds(g, [{"station": "08B"}, {"station": "08A"}],
                                  {"08A": "a", "08B": "b"})
        again = build_gauge_sheds(g, [{"station": "08A"}, {"station": "08B"}],
                                  {"08B": "b", "08A": "a"})
        assert [(l.section_id, l.station) for l in first] == \
               [(l.section_id, l.station) for l in again]

    def test_a_station_matched_to_a_magnitudeless_node_speaks_for_nothing(self):
        g = _graph({"lake": None, "out": 100}, [("lake", "out")])
        assert build_gauge_sheds(g, [{"station": "08A"}], {"08A": "lake"}) == []

    def test_output_is_sorted_so_the_bundle_bytes_are_stable(self):
        g = _graph({"zzz": 100, "aaa": 90, "mmm": 95},
                   [("aaa", "mmm"), ("mmm", "zzz")])
        links = build_gauge_sheds(g, [{"station": "08A"}], {"08A": "mmm"})
        ids = [l.section_id for l in links]
        assert ids == sorted(ids)


class TestDownstreamMap:
    def test_it_points_each_section_at_what_it_flows_into(self):
        g = _graph({"a": 10, "b": 20}, [("a", "b")])
        assert downstream_map(g, ["a", "b"]) == {"a": "b"}

    def test_the_last_section_before_the_sea_points_nowhere(self):
        g = _graph({"a": 10}, [])
        assert downstream_map(g, ["a"]) == {}


class TestStationNames:
    @pytest.mark.parametrize("raw,want", [
        ("CHILLIWACK RIVER AT VEDDER CROSSING", "chilliwack river"),
        ("ATLIN LAKE NEAR ATLIN", "atlin lake"),
        ("SALMON RIVER ABOVE THE FALLS", "salmon river"),
        ("COLUMBIA RIVER UPSTREAM OF BIRCHBANK", "columbia river"),
        ("POUCE COUPE RIVER NEAR DAWSON CREEK", "pouce coupe river"),
    ])
    def test_the_qualifier_is_stripped_and_the_type_word_is_kept(self, raw, want):
        assert waterbody_name(raw) == want

    def test_the_type_word_is_what_stops_a_lake_matching_its_river(self):
        # 'ATLIN RIVER' is not a substring of 'atlin lake', so the river node cannot match
        # the lake's gauge. Removing the type word from either side breaks this.
        assert "atlin river" not in waterbody_name("ATLIN LAKE NEAR ATLIN")


def _lake_graph():
    """A lake with a gauge on it, an inlet creek and an outlet river."""
    g = StreamGraph()
    g.nodes["inlet"] = _node("inlet", 40)
    g.nodes["lake:99"] = StreamNode(node_id="lake:99", kind=NodeKind.lake, wbk="99",
                                    display_name="Alouette Lake", stream_magnitude=100)
    g.nodes["outlet"] = _node("outlet", 120)
    for ix, (a, b) in enumerate([("inlet", "lake:99"), ("lake:99", "outlet")]):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.up_adj.setdefault(b, []).append(ix)
        g.down_adj.setdefault(a, []).append(ix)
    return g


class TestLakeStations:
    """A lake gauge reports a LEVEL. A river gauge reports a DISCHARGE. Not the same number.

    Before this split, lake nodes carried a stream magnitude, so a lake station walked
    downstream like a river station and 11,049 stream sections were being told a reservoir's
    level — including Mica, Strathcona and Ruskin, where the outflow is whatever an operator
    decided that morning.
    """

    def test_a_lake_station_never_speaks_for_a_stream(self):
        g = _lake_graph()
        links = build_gauge_sheds(g, [{"station": "08MH999"}], {"08MH999": "lake:99"})
        assert links == []

    def test_it_is_linked_to_the_lake_instead_of_being_thrown_away(self):
        g = _lake_graph()
        assert lake_gauge_links(g, [{"station": "08MH999"}], {"08MH999": "lake:99"}) == [
            ("lake:99", "08MH999")]

    def test_a_river_station_does_not_speak_for_a_lake_either(self):
        # The same mistake in the other direction: the outlet river's discharge says
        # nothing about how high the lake is standing.
        g = _lake_graph()
        links = build_gauge_sheds(g, [{"station": "08OUT"}], {"08OUT": "outlet"})
        assert "lake:99" not in {l.section_id for l in links}

    def test_a_river_station_still_reaches_the_streams_around_the_lake(self):
        # The lake is skipped, not treated as a wall — the inlet creek is still this
        # gauge's water. Only the lake node itself is refused.
        g = _lake_graph()
        links = build_gauge_sheds(g, [{"station": "08OUT"}], {"08OUT": "outlet"})
        assert "inlet" in {l.section_id for l in links}

    def test_lake_links_carry_no_trust_band(self):
        # There is no fraction of a level. A band here would be a familiar word attached
        # to a number that does not support it.
        g = _lake_graph()
        for row in lake_gauge_links(g, [{"station": "08MH999"}], {"08MH999": "lake:99"}):
            assert len(row) == 2

    def test_an_unknown_station_is_not_linked_to_a_lake(self):
        g = _lake_graph()
        assert lake_gauge_links(g, [], {"08MH999": "lake:99"}) == []


class TestBandCap:
    """A section keeps exactly one gauge: the one that most nearly is that water."""

    def _many(self, band_mag: int, n: int):
        """`n` stations all reaching one section at the same band."""
        nodes = {"here": 100}
        edges = []
        for i in range(n):
            nodes[f"g{i}"] = band_mag
            edges.append(("here", f"g{i}"))
        g = _graph(nodes, edges)
        return build_gauge_sheds(
            g, [{"station": f"08S{i}"} for i in range(n)],
            {f"08S{i}": f"g{i}" for i in range(n)})

    def test_four_good_stations_still_yield_one_row(self):
        links = [l for l in self._many(200, 4) if l.section_id == "here"]
        assert len(links) == 1
        assert links[0].trust == "good"

    def test_only_one_weak_station_is_kept_since_they_all_say_the_same_thing(self):
        links = [l for l in self._many(60_000, 4) if l.section_id == "here"]
        assert [l.trust for l in links] == ["weak"]

    def test_capping_weak_never_costs_a_section_its_gauge(self):
        # The point of the cap: redundancy goes, coverage does not.
        links = [l for l in self._many(60_000, 4) if l.section_id == "here"]
        assert len(links) == 1

    def test_the_weak_one_that_survives_is_the_most_representative(self):
        g = _graph({"here": 100, "near": 20_000, "far": 90_000},
                   [("here", "near"), ("near", "far")])
        links = [l for l in build_gauge_sheds(
            g, [{"station": "08NEAR"}, {"station": "08FAR"}],
            {"08NEAR": "near", "08FAR": "far"}) if l.section_id == "here"]
        assert [l.station for l in links] == ["08NEAR"]

    def test_the_survivor_is_the_best_band_not_merely_the_first_seen(self):
        g = _graph({"here": 100, "close": 400, "huge": 60_000},
                   [("here", "close"), ("close", "huge")])
        links = [l for l in build_gauge_sheds(
            g, [{"station": "08GOOD"}, {"station": "08WEAK"}],
            {"08GOOD": "close", "08WEAK": "huge"}) if l.section_id == "here"]
        assert [l.station for l in links] == ["08GOOD"]


class TestMatchProvenance:
    """How a station found its node is recorded, and "nothing there" is not "not tried"."""

    def test_a_permanent_no_match_is_not_an_unresolved_gap(self):
        # Comox Harbour is tidal salt water, deliberately outside the atlas. Filing it as
        # unresolved means someone re-investigates it every year and reaches the same
        # conclusion; filing it as no_match records that the conclusion was already reached.
        from pipeline.hydro.match import NO_MATCH
        assert "08HB087" in NO_MATCH
        assert NO_MATCH["08HB087"]

    def test_the_override_file_is_empty_and_should_stay_that_way(self):
        # It once held seven entries. Every one was a name the atlas already carried in
        # `name_tuples`; teaching the matcher to read them removed the need for all seven.
        # A line appearing here again is a signal to look for that fix first.
        from pipeline.hydro.match import load_aliases
        assert load_aliases() == {}

    def test_the_matcher_reads_every_name_a_node_answers_to(self):
        # The bug that produced the alias file: a node displayed as "Lower Arrow Lake"
        # carries "Arrow Reservoir", which is the name ECCC uses. Comparing only against
        # display_name threw that away.
        from pipeline.models.enums import NameSource
        from pipeline.models.names import NameTuple
        from pipeline.hydro.match import _names

        node = StreamNode(node_id="lake:1", kind=NodeKind.lake,
                          display_name="Lower Arrow Lake",
                          name_tuples=(NameTuple("Arrow Reservoir", NameSource.gazette),
                                       NameTuple("LOWER ARROW L.", NameSource.gazette)))
        got = _names(node)
        assert "lower arrow lake" in got and "arrow reservoir" in got
        assert got[0] == "lower arrow lake"          # display name leads
        assert len(got) == len(set(got))             # deduped

    def test_an_override_binds_to_a_node_never_to_a_name(self):
        # A name-keyed override is a second guess at the same ambiguous question: it breaks
        # when two waters share the string and silently follows the wrong one on a rename.
        from pathlib import Path
        import pipeline.hydro.match as m
        src = Path(m.__file__).read_text(encoding="utf-8")
        assert "override in graph.nodes" in src

    def test_no_station_is_both_aliased_and_declared_hopeless(self):
        # The two mechanisms say opposite things. A station in both would silently take
        # whichever the code checked first.
        from pipeline.hydro.match import NO_MATCH, load_aliases
        assert not (set(load_aliases()) & set(NO_MATCH))

    def test_the_alias_file_is_never_the_shared_name_variants_file(self):
        # v1 corrupted display names by letting a matcher write into the variants file that
        # decides what waters are CALLED. This file is read here and nowhere else.
        from pathlib import Path
        import pipeline.hydro.match as m
        src = Path(m.__file__).read_text(encoding="utf-8")
        assert "name_variants" not in src.replace("name_variants.json`", "")

    def test_summarise_counts_by_outcome(self):
        from pipeline.hydro.match import StationMatch, summarise
        got = summarise([
            StationMatch("a", "n1", "matched", "name+radius", 12.0),
            StationMatch("b", "n2", "matched", "alias", 40.0),
            StationMatch("c", None, "no_match", None, None, "tidal"),
            StationMatch("d", None, "unresolved", None, None, "nothing named"),
        ])
        assert "2/4 matched" in got and "1 by alias" in got
        assert "1 no_match" in got and "1 unresolved" in got

    def test_summarise_can_report_only_the_stations_that_matter(self):
        # The roster-wide rate is 89%; among TRANSMITTING stations it is 99%. Reporting the
        # first as the headline hides the only number a user can be affected by.
        from pipeline.hydro.match import StationMatch, summarise
        rows = [StationMatch("live", "n", "matched", "name+radius", 1.0),
                StationMatch("dead", None, "unresolved", None, None, "x")]
        assert "1/1 matched (100.0%)" in summarise(rows, live={"live"})


class TestPreferTransmitting:
    """A reach takes a gauge that can actually report, when one reaches it.

    Measured on the real province: without this, 90% of gauged reaches were assigned a
    station that had closed — BC's roster is 1,885 discontinued against 439 transmitting.
    The map coloured 9% of the province and the sheet 404'd on everything else.
    """

    def _two(self, prefer):
        # `08DEAD` is nearer in magnitude, so it wins on representativeness alone.
        g = _graph({"here": 100, "dead": 110, "live": 400},
                   [("here", "dead"), ("dead", "live")])
        return build_gauge_sheds(
            g, [{"station": "08DEAD"}, {"station": "08LIVE"}],
            {"08DEAD": "dead", "08LIVE": "live"}, prefer=prefer)

    def test_only_stations_eccc_still_lists_as_active_get_a_shed(self):
        links = [l for l in self._two({"08LIVE"}) if l.section_id == "here"]
        assert [l.station for l in links] == ["08LIVE"]

    def test_representativeness_still_decides_among_transmitting_stations(self):
        links = [l for l in self._two({"08LIVE", "08DEAD"}) if l.section_id == "here"]
        assert [l.station for l in links] == ["08DEAD"]      # the nearer magnitude

    def test_a_reach_only_retired_stations_reach_gets_nothing(self):
        # A shed exists so the app can put a NUMBER on a reach. A retired station has no
        # number to give, so its shed is rows nobody can ever read. It keeps its `gauge`
        # row and its climatology; it loses the claim to speak for this reach.
        #
        # NOTE this is RETIRED, not "quiet today". A seasonal gauge shut for the winter is
        # still active and keeps its shed — see the bundler, which passes ECCC's `active`.
        assert [l for l in self._two(set()) if l.section_id == "here"] == []

    def test_passing_no_preference_at_all_keeps_every_station(self):
        # `None` is not an empty set: a caller that has no roster must not silently get an
        # empty result.
        links = [l for l in self._two(None) if l.section_id == "here"]
        assert [l.station for l in links] == ["08DEAD"]

    def test_the_preference_is_not_written_into_the_link(self):
        # It is a ranking input, not a fact about the gauge. Liveness belongs to the feed.
        for l in self._two({"08LIVE"}):
            assert not hasattr(l, "live") and not hasattr(l, "realtime")
