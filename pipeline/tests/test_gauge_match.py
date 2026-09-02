"""The frozen match — one record of where BC's gauges are, addressed in FWA's own terms."""

from __future__ import annotations

import json
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import LineString, Point

from pipeline.hydro import match as M


def _graph(nodes):
    return SimpleNamespace(nodes=nodes)


def _stream(nid, wsc="100-1"):
    return SimpleNamespace(node_id=nid, blk="B1", wsc=wsc, wbk="")


def _lake(nid, wbk):
    return SimpleNamespace(node_id=nid, blk="", wsc="", wbk=wbk)


def _at(x, y, wsc="100-1", station="08AA001"):
    """A match at a BC Albers position, given as the lon/lat the file would hold."""
    p = gpd.GeoSeries([Point(x, y)], crs=3005).to_crs(4326)[0]
    return M.StationMatch(station, "matched", "name+radius", 1.0,
                          lon=p.x, lat=p.y, wsc=wsc)


class TestPlacing:
    """Where a station lands in a graph, projected from its own published coordinate."""

    def test_takes_the_section_that_begins_at_the_gauge_and_runs_upstream(self):
        # AFTER THE GAUGE CUTS, a station sits exactly on a join and both neighbours contain
        # its coordinate at their shared end. The one it belongs to is the upstream one:
        # that is the water that has just flowed past it and been measured. The section
        # below has already taken on whatever joins in between.
        #
        # FWA lines run mouth to source, so `project` == 0 means "this section starts here".
        below = LineString([(0, 0), (100, 0)])       # ends at the gauge
        above = LineString([(100, 0), (200, 0)])     # begins at the gauge
        g = _graph({"below": _stream("below"), "above": _stream("above")})
        geoms = {"below": below, "above": above}
        m = _at(100, 0)
        assert M.nodes_for([m], g, geoms) == {"08AA001": "above"}

    def test_a_station_mid_section_lands_on_that_section(self):
        g = _graph({"only": _stream("only")})
        geoms = {"only": LineString([(0, 0), (200, 0)])}
        assert M.nodes_for([_at(100, 0)], g, geoms) == {"08AA001": "only"}

    def test_never_leaves_its_own_watershed(self):
        # A creek gauge twenty metres from a mainstem must not be placed on the mainstem.
        # This is the Slesse case at placement time rather than at shed time.
        g = _graph({"creek": _stream("creek", wsc="100-1-2"),
                    "main": _stream("main", wsc="100-1")})
        geoms = {"creek": LineString([(0, 20), (200, 20)]),
                 "main": LineString([(0, 0), (200, 0)])}
        assert M.nodes_for([_at(100, 0, wsc="100-1-2")], g, geoms) == {"08AA001": "creek"}

    def test_a_station_too_far_from_its_water_is_not_placed(self):
        g = _graph({"only": _stream("only")})
        geoms = {"only": LineString([(0, 0), (200, 0)])}
        assert M.nodes_for([_at(100, 5_000)], g, geoms) == {}

    def test_a_lake_station_is_named_not_projected(self):
        # It sits on a body of water, not along a channel. Forgetting this emptied
        # `lake_gauge` from 220 rows to 0.
        g = _graph({"lake:99": _lake("lake:99", "99")})
        m = M.StationMatch("08MH999", "matched", "name+radius", 5.0, wbk="99")
        assert M.nodes_for([m], g, {}) == {"08MH999": "lake:99"}

    def test_an_unmatched_station_is_never_placed(self):
        g = _graph({"only": _stream("only")})
        geoms = {"only": LineString([(0, 0), (200, 0)])}
        m = M.StationMatch("08AA001", "unresolved", None, None)
        assert M.nodes_for([m], g, geoms) == {}

    def test_without_a_graph_it_returns_what_was_recorded(self):
        # The diagnostic path: node ids as they were when the match was made, correct only
        # for that build — which is exactly why the argument exists.
        m = M.StationMatch("08AA001", "matched", "name+radius", 1.0, node_id="B1:0")
        assert M.nodes_for([m]) == {"08AA001": "B1:0"}


class TestTheArtifact:
    def test_round_trips(self, tmp_path):
        rows = [M.StationMatch("08AA001", "matched", "name+radius", 12.0,
                               lon=-123.0, lat=49.0, wsc="100-1",
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
             "distance_m": 1.0, "lon": -123.0, "lat": 49.0, "_future": "whatever"}]}))
        assert M.read_match(p)[0].station == "08AA001"
