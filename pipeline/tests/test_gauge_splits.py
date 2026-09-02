"""Gauge positions as cuts — the rules that decide where a river is sectioned."""

from __future__ import annotations

from types import SimpleNamespace

from shapely.geometry import LineString

from pipeline.hydro import splits as GS


def _graph(nodes: dict):
    return SimpleNamespace(nodes=nodes)


def _node(blk: str, down_m: float = 0.0, kind: str = "NodeKind.stream"):
    return SimpleNamespace(blk=blk, down_m=down_m, kind=kind)


# A 10 km straight line in BC Albers, and the lon/lat of points along it.
def _line(length_m: float = 10_000.0) -> LineString:
    return LineString([(1_200_000, 500_000), (1_200_000 + length_m, 500_000)])


def _station(sid: str, x_offset_m: float, name: str = "TEST RIVER") -> dict:
    import geopandas as gpd
    from shapely.geometry import Point
    p = gpd.GeoSeries([Point(1_200_000 + x_offset_m, 500_000)], crs=3005).to_crs(4326)[0]
    return {"station": sid, "name": name, "lon": p.x, "lat": p.y}


class TestWhereTheCutLands:
    def test_measures_from_the_blue_line_not_from_the_node(self):
        # The sectionizer subdivides a BLUE LINE, so a cut on a node that starts 4 km along
        # one has to be expressed at 4 km + its own offset. Measuring from the node would
        # put every cut on a mid-river node hundreds of metres too far downstream.
        geoms = {"n1": _line()}
        graph = _graph({"n1": _node("BLK1", down_m=4_000.0)})
        rows = GS.gauge_splits(graph, geoms, [_station("08AA001", 6_000)],
                               {"08AA001": "n1"})
        assert len(rows) == 1
        assert rows[0]["route_measure"] == 10_000.0
        assert rows[0]["blk"] == "BLK1"

    def test_only_the_node_the_matcher_resolved(self):
        # A creek gauge 40 m from the mainstem must not cut the mainstem. Nothing here
        # re-decides which water a station is on — it cuts what it was handed and no more.
        geoms = {"creek": _line(1_000), "main": _line()}
        graph = _graph({"creek": _node("CREEK"), "main": _node("MAIN")})
        rows = GS.gauge_splits(graph, geoms, [_station("08AA001", 500)],
                               {"08AA001": "creek"})
        assert [r["blk"] for r in rows] == ["CREEK"]


class TestWhatIsRefused:
    def test_a_lake_station_is_not_a_cut(self):
        # A lake reports a level for a body of water. There is no "above it" and "below it"
        # along a channel to separate, so there is nothing to cut.
        geoms = {"lk": _line()}
        graph = _graph({"lk": _node("BLK1", kind="NodeKind.lake")})
        assert GS.gauge_splits(graph, geoms, [_station("08AA001", 5_000)],
                               {"08AA001": "lk"}) == []

    def test_a_cut_at_the_very_end_is_dropped(self):
        # It would leave a stub too short to see, and the station already sits on that
        # boundary — so the cut buys nothing and costs a section.
        geoms = {"n1": _line()}
        graph = _graph({"n1": _node("BLK1")})
        assert GS.gauge_splits(graph, geoms, [_station("08AA001", 20)],
                               {"08AA001": "n1"}) == []

    def test_two_stations_within_the_gap_collapse_to_one(self):
        # Two gauges 100 m apart describe the same water; a 100 m section is a rendering
        # artifact, not a reach anybody fishes. The downstream one is kept.
        geoms = {"n1": _line()}
        graph = _graph({"n1": _node("BLK1")})
        rows = GS.gauge_splits(graph, geoms,
                               [_station("08AA001", 5_000), _station("08AA002", 5_100)],
                               {"08AA001": "n1", "08AA002": "n1"})
        assert len(rows) == 1
        assert rows[0]["station"] == "08AA001"

    def test_two_stations_beyond_the_gap_both_cut(self):
        geoms = {"n1": _line()}
        graph = _graph({"n1": _node("BLK1")})
        rows = GS.gauge_splits(graph, geoms,
                               [_station("08AA001", 3_000), _station("08AA002", 7_000)],
                               {"08AA001": "n1", "08AA002": "n1"})
        assert [r["station"] for r in rows] == ["08AA001", "08AA002"]


class TestDeterminism:
    def test_the_same_input_gives_byte_identical_output(self):
        # AGENTS rule 19. This artifact is committed, so a reordering would show up as a
        # diff of 1,600 lines that means nothing.
        geoms = {"a": _line(), "b": _line()}
        graph = _graph({"a": _node("B1"), "b": _node("B2")})
        st = [_station("08ZZ009", 5_000), _station("08AA001", 5_000)]
        first = GS.gauge_splits(graph, geoms, st, {"08ZZ009": "b", "08AA001": "a"})
        second = GS.gauge_splits(graph, geoms, list(reversed(st)),
                                 {"08AA001": "a", "08ZZ009": "b"})
        assert first == second
