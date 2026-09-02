"""Gauge positions as cuts — the rules that decide where a river gets sectioned."""

from __future__ import annotations

from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import LineString, Point

from pipeline.hydro import splits as GS


def _chain(blk: str, name: str, length_m: float = 10_000.0, mouth: float = 0.0,
           y: float = 500_000.0):
    return SimpleNamespace(
        blk=blk, gnis_name=name, name_tuples=(),
        geometry=LineString([(1_200_000, y), (1_200_000 + length_m, y)]),
        mouth_measure=mouth,
    )


def _station(sid: str, x_offset_m: float, name: str, y: float = 500_000.0) -> dict:
    p = gpd.GeoSeries([Point(1_200_000 + x_offset_m, y)], crs=3005).to_crs(4326)[0]
    return {"station": sid, "name": name, "lon": p.x, "lat": p.y, "realtime": True}


class TestWaterbodyName:
    def test_stops_at_the_landmark(self):
        # "AT VEDDER CROSSING" is a landmark, not a water. Matching against it is how a
        # Chilliwack River station ends up cutting the Vedder.
        assert GS.waterbody_name("CHILLIWACK RIVER AT VEDDER CROSSING") == "chilliwack river"
        assert GS.waterbody_name("FRASER RIVER NEAR AGASSIZ") == "fraser river"
        assert GS.waterbody_name("MCKINLEY CREEK BELOW OUTLET OF MCKINLEY LAKE") \
            == "mckinley creek"

    def test_a_bare_name_survives_intact(self):
        assert GS.waterbody_name("STELLAKO RIVER") == "stellako river"


class TestWhereTheCutLands:
    def test_measures_along_the_blue_line_from_its_mouth(self):
        # The sectionizer subdivides a BLUE LINE, so a cut on a chain whose mouth measure is
        # 4 km has to be expressed at 4 km plus its own offset along the geometry.
        pts = GS.gauge_split_points([_chain("BLK1", "Test River", mouth=4_000.0)],
                                    [_station("08AA001", 6_000, "TEST RIVER AT NOWHERE")])
        assert len(pts) == 1
        assert pts[0].route_measure == 10_000.0
        assert pts[0].blk == "BLK1"

    def test_the_name_decides_which_line_is_cut_not_the_distance(self):
        # THE FAILURE THIS PREVENTS. The creek station is 20 m from the mainstem and 400 m
        # from its own creek. Nearest-line matching cuts the wrong river and then reports
        # the creek's flow for it.
        creek = _chain("CREEK", "Slesse Creek", y=500_400)
        main = _chain("MAIN", "Chilliwack River", y=500_000)
        pts = GS.gauge_split_points(
            [creek, main], [_station("08MH056", 5_000, "SLESSE CREEK NEAR VEDDER CROSSING",
                                     y=500_020)])
        assert [p.blk for p in pts] == ["CREEK"]

    def test_a_station_naming_nothing_nearby_cuts_nothing(self):
        # It does NOT fall back to the closest line. That fallback is the whole failure the
        # representativeness rule exists to refuse.
        pts = GS.gauge_split_points([_chain("BLK1", "Test River")],
                                    [_station("08AA001", 5_000, "OTHER RIVER AT NOWHERE")])
        assert pts == []


class TestWhatIsRefused:
    def test_a_cut_at_the_very_end_is_dropped(self):
        # It would leave a stub too short to see, and the station already sits on that
        # boundary — so the cut buys nothing and costs a section.
        pts = GS.gauge_split_points([_chain("BLK1", "Test River")],
                                    [_station("08AA001", 20, "TEST RIVER AT NOWHERE")])
        assert pts == []

    def test_two_stations_within_the_gap_collapse_to_one(self):
        # Two gauges 100 m apart describe the same water; a 100 m section is a rendering
        # artifact, not a reach anybody fishes. The downstream one is kept.
        pts = GS.gauge_split_points(
            [_chain("BLK1", "Test River")],
            [_station("08AA001", 5_000, "TEST RIVER AT A"),
             _station("08AA002", 5_100, "TEST RIVER AT B")])
        assert len(pts) == 1
        assert pts[0].split_id == "gauge__08AA001"

    def test_two_stations_beyond_the_gap_both_cut(self):
        pts = GS.gauge_split_points(
            [_chain("BLK1", "Test River")],
            [_station("08AA001", 3_000, "TEST RIVER AT A"),
             _station("08AA002", 7_000, "TEST RIVER AT B")])
        assert [p.split_id for p in pts] == ["gauge__08AA001", "gauge__08AA002"]


class TestDeterminism:
    def test_the_same_input_gives_the_same_output_whatever_the_order(self):
        # AGENTS rule 19. Section ids are part of the ABI; a reordering here would move
        # boundaries between builds for no reason at all.
        chains = [_chain("B1", "Alpha River"), _chain("B2", "Beta River", y=600_000)]
        st = [_station("08ZZ009", 5_000, "BETA RIVER AT X", y=600_000),
              _station("08AA001", 5_000, "ALPHA RIVER AT Y")]
        assert GS.gauge_split_points(chains, st) == \
            GS.gauge_split_points(list(reversed(chains)), list(reversed(st)))


class TestRoster:
    def test_only_transmitting_stations_are_cut_at(self, tmp_path):
        # A station discontinued in 1974 still sits somewhere, but cutting the river at it
        # buys a boundary no reading will ever be shown at.
        import json
        p = tmp_path / "roster.json"
        p.write_text(json.dumps([
            {"station": "08AA001", "name": "A RIVER", "lon": -123.0, "lat": 49.0,
             "realtime": True},
            {"station": "08AA002", "name": "B RIVER", "lon": -123.0, "lat": 49.0,
             "realtime": False},
            {"station": "08AA003", "name": "C RIVER", "lon": None, "lat": None,
             "realtime": True},
        ]))
        assert [r["station"] for r in GS.load_roster(p)] == ["08AA001"]
