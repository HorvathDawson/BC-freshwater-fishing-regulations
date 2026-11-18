"""T825 (owner 2026-11-17 15:04, "Yes, use the rule (Recommended)"): a water reads a gauge about its own size, on its
own main river. A section whose walk-chosen gauge drains under a tenth of its water reads the nearest flow gauge on its
main river that drains a tenth to ten times as much, or none (`pipeline.gauges.consume.main_river`)."""

from types import SimpleNamespace

import pytest
from shapely.geometry import LineString, Point, Polygon

from pipeline.gauges.consume import main_river as M
from pipeline.gauges.consume.shed import GaugeLink

FRASER = "356364114"
# a lower-Fraser slough (water ~60,000 km2) led by Red Pass (1,122 km2), 1,200 km up the river
SLOUGH = GaugeLink("355992187:0", "08KA007", "weak", 0.002, 84, "up", 1_187_000)
MAIN = GaugeLink(f"{FRASER}:111136", "08MF035", "good", 0.9, 0, "up", 11_000)        # the river itself: untouched
AREA = {SLOUGH.section_id: 63_129.0, MAIN.section_id: 63_129.0}
DONOR = {"08KA007": 1_122.0, "08MF035": 60_000.0, "08MF038": 63_000.0, "08MH024": 63_600.0, "08MF005": 60_356.0,
         "08XX001": 5_000.0}
FLOWS = {"08KA007", "08MF035", "08MH024", "08MF005", "08XX001"}                        # 08MF038 Cannor: level only
ON = {"08KA007": M.Place(FRASER, 1_314_619), "08MF035": M.Place(FRASER, 122_095), "08MF038": M.Place(FRASER, 128_100),
      "08MH024": M.Place(FRASER, 76_485), "08MF005": M.Place(FRASER, 158_685), "08XX001": M.Place(FRASER, 128_000)}
HERE = {SLOUGH.section_id: M.Place(FRASER, 128_026)}


def _run(links=(SLOUGH, MAIN), **kw):
    args = dict(target_area=AREA, donor_area=DONOR, flows=FLOWS, section_place=HERE, gauge_place=ON)
    args.update(kw)
    return M.rechoose(list(links), **args)


def test_a_slough_reads_the_nearest_flow_gauge_of_its_size_on_its_main_river():
    led, why = _run()
    (s,) = [l for l in led if l.section_id == SLOUGH.section_id]
    # Agassiz, 5.9 km down — not Cannor (nearer, level only), not 08XX001 (nearer, a twelfth of the water's size)
    assert (s.station, s.rel, s.dist_m, s.trust) == ("08MF035", "down", 5931, "good")
    assert why == {"rechosen": [(SLOUGH.section_id, "08KA007", "08MF035")], "none": []}
    assert MAIN in led                                                    # a gauge of its size: left as the walk chose


def test_no_gauge_of_its_size_on_its_river_leaves_it_with_none():
    led, why = _run(flows={"08KA007", "08XX001"})
    assert [l.section_id for l in led] == [MAIN.section_id]
    assert why["none"] == [(SLOUGH.section_id, "08KA007", None)]


def test_only_its_own_main_river_counts():
    led, _ = _run(gauge_place={**ON, "08MF035": M.Place("999", 122_095)})
    assert next(l for l in led if l.section_id == SLOUGH.section_id).station == "08MF005"   # Hope, 31 km up the Fraser (Mission is 52 km down)


def test_the_trigger_is_a_gauge_under_a_tenth_of_the_water():
    near = GaugeLink(SLOUGH.section_id, "08XX001", "fair", 0.08, 3, "up", 30_000)
    assert M.too_small(near, {near.section_id: 49_999.0}, DONOR) is False   # 5,000 km2 is just over a tenth: kept
    assert M.too_small(near, {near.section_id: 50_000.0}, DONOR) is True    # exactly a tenth: re-chosen
    assert M.fits(1000.0, 100.0) and M.fits(1000.0, 10_000.0) and not M.fits(1000.0, 99.9)


def test_mutation_pin_switching_the_rule_off_restores_the_walks_choice(monkeypatch):
    monkeypatch.setattr(M, "MAIN_RIVER_GAUGE", False)
    led, why = _run()
    assert led == [SLOUGH, MAIN] and why == {"rechosen": [], "none": []}


def test_places_project_onto_the_main_river_mouth_first():
    river = {f"{FRASER}:0": LineString([(0, 0), (0, 1000)]), f"{FRASER}:1000": LineString([(0, 1000), (0, 3000)]),
             "777:0": LineString([(50, 0), (50, 5000)]), f"lake:{FRASER}": Polygon([(0, 0), (1, 0), (1, 1)])}
    got = M.place({"a": FRASER, "b": FRASER}, river, {"a": Point(30, 1500), "b": Point(-10, 200)})
    assert got == {"a": M.Place(FRASER, 1500.0), "b": M.Place(FRASER, 200.0)}
    assert M.midpoint(LineString([(0, 0), (0, 10)])).equals(Point(0, 5))


def test_main_river_of_a_side_channel_a_line_and_a_polygon():
    def node(kind, mag):
        return SimpleNamespace(kind=kind, stream_magnitude=mag)
    graph = SimpleNamespace(nodes={"11:0": node("stream", 3), "22:500": node("stream", 900), "33:0": node("stream", 5),
                                   "lake:9": node("lake", 0), "44:0": node("stream", 40), "55:0": node("stream", 7)})
    water = {"lake:9": "w", "44:0": "w", "55:0": "w"}
    got = M.main_blks(["11:0", "33:0", "lake:9"], graph, {"11:0": ["22:500", "11:9"]}, water)
    assert got == {"11:0": "22", "33:0": "33", "lake:9": "44"}


@pytest.mark.parametrize("ratio,band", [(0.5, "good"), (0.1, "good"), (0.05, "fair"), (0.001, "weak")])
def test_band_by_size(ratio, band):
    assert M.band(ratio) == band
