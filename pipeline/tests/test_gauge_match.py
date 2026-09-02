"""The frozen match — one record of where BC's gauges are, addressed in FWA's own terms."""

from __future__ import annotations

import json
from types import SimpleNamespace

from pipeline.hydro import match as M


def _graph(nodes):
    return SimpleNamespace(nodes=nodes)


def _stream(nid, blk, down, up, wsc="100-1"):
    return SimpleNamespace(node_id=nid, blk=blk, down_m=down, up_m=up, wsc=wsc, wbk="")


def _lake(nid, wbk):
    return SimpleNamespace(node_id=nid, blk="", down_m=0.0, up_m=0.0, wsc="", wbk=wbk)


class TestStableAddressing:
    """Node ids move whenever a river is re-sectioned. Blue lines and measures do not."""

    def test_places_a_station_in_whatever_graph_it_is_handed(self):
        # THE WHOLE POINT. The match was frozen against a build where this blue line was one
        # node; the next build cut it at the gauges into three. `(blk, measure)` still names
        # the middle one, where a stored node id would name something that no longer exists.
        before = _graph({"B1:0": _stream("B1:0", "B1", 0, 30_000)})
        after = _graph({"B1:0": _stream("B1:0", "B1", 0, 10_000),
                        "B1:10000": _stream("B1:10000", "B1", 10_000, 20_000),
                        "B1:20000": _stream("B1:20000", "B1", 20_000, 30_000)})
        m = M.StationMatch("08AA001", "matched", "name+radius", 12.0,
                           blk="B1", measure=15_000.0, node_id="B1:0")
        assert M.nodes_for([m], before) == {"08AA001": "B1:0"}
        assert M.nodes_for([m], after) == {"08AA001": "B1:10000"}

    def test_a_lake_station_is_placed_by_waterbody_key(self):
        # A lake station has no measure and that is not a gap: it sits on a body of water,
        # not along a channel. Forgetting this emptied `lake_gauge` entirely — 220 stations
        # fell through a branch that only understood blue lines.
        g = _graph({"lake:99": _lake("lake:99", "99")})
        m = M.StationMatch("08MH999", "matched", "name+radius", 5.0, wbk="99")
        assert M.nodes_for([m], g) == {"08MH999": "lake:99"}

    def test_a_measure_in_a_gap_resolves_to_nothing_rather_than_a_neighbour(self):
        # Under-lake runs and pruned pieces leave holes. A silently-adjacent node is how a
        # reading ends up on the wrong side of a confluence.
        g = _graph({"B1:0": _stream("B1:0", "B1", 0, 1_000),
                    "B1:9000": _stream("B1:9000", "B1", 9_000, 10_000)})
        m = M.StationMatch("08AA001", "matched", "name+radius", 1.0,
                           blk="B1", measure=5_000.0)
        assert M.nodes_for([m], g) == {}

    def test_an_unmatched_station_is_never_placed(self):
        g = _graph({"B1:0": _stream("B1:0", "B1", 0, 10_000)})
        m = M.StationMatch("08AA001", "unresolved", None, None, blk="B1", measure=5_000.0)
        assert M.nodes_for([m], g) == {}

    def test_without_a_graph_it_returns_what_was_recorded(self):
        # The diagnostic path: the node ids as they were when the match was made, correct
        # only for that same build — which is exactly why the argument exists.
        m = M.StationMatch("08AA001", "matched", "name+radius", 1.0, node_id="B1:0")
        assert M.nodes_for([m]) == {"08AA001": "B1:0"}


class TestTheArtifact:
    def test_round_trips(self, tmp_path):
        rows = [M.StationMatch("08AA001", "matched", "name+radius", 12.0,
                               blk="B1", measure=15_000.0, wsc="100-1",
                               name="Test River", node_id="B1:0"),
                M.StationMatch("08ZZ999", "unresolved", None, None, "nothing nearby")]
        p = tmp_path / "gauge_match.json"
        M.write_match(rows, p)
        assert M.read_match(p) == sorted(rows, key=lambda m: m.station)

    def test_sorted_by_station_so_a_diff_reads_as_a_list_of_gauges(self, tmp_path):
        p = tmp_path / "gauge_match.json"
        M.write_match([M.StationMatch("08ZZ999", "unresolved", None, None),
                       M.StationMatch("08AA001", "unresolved", None, None)], p)
        got = json.loads(p.read_text())["stations"]
        assert [r["station"] for r in got] == ["08AA001", "08ZZ999"]

    def test_missing_is_empty_rather_than_an_error(self, tmp_path):
        # A checkout that has never run the matcher builds without gauge tables and says so.
        assert M.read_match(tmp_path / "nope.json") == []

    def test_tolerates_fields_it_does_not_know(self, tmp_path):
        # The artifact is committed and read by two consumers; a new review field must not
        # break a checkout that has not been updated yet.
        p = tmp_path / "gauge_match.json"
        p.write_text(json.dumps({"stations": [
            {"station": "08AA001", "status": "matched", "resolved_by": "name+radius",
             "distance_m": 1.0, "blk": "B1", "measure": 5.0, "_future": "whatever"}]}))
        assert M.read_match(p)[0].station == "08AA001"
