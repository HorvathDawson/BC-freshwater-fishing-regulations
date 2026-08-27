"""underlake — split a line at FWA lake polygons; tag the inside segment with the lake's wbk."""

from shapely.geometry import LineString, Polygon

from pipeline.added_streams.underlake import LakeIndex, assign_under_lake


def test_line_crossing_a_lake_gets_wbk_on_inside_segment():
    # a lake square spanning x in [10,20], y in [-5,5]
    lake = Polygon([(10, -5), (20, -5), (20, 5), (10, 5)])
    idx = LakeIndex([(lake, "WBK99")])
    line = LineString([(0, 0), (30, 0)])                 # crosses the lake left->right
    segs = assign_under_lake(line, idx)
    assert len(segs) == 3
    wbks = [w for _, w in segs]
    assert wbks == ["", "WBK99", ""]                     # outside, under-lake, outside — in order
    inside = [g for g, w in segs if w == "WBK99"][0]
    assert round(inside.length) == 10                    # the 10..20 span


def test_no_lake_crossing_returns_whole_line_untagged():
    idx = LakeIndex([(Polygon([(100, 100), (110, 100), (110, 110), (100, 110)]), "WBKX")])
    line = LineString([(0, 0), (30, 0)])
    assert assign_under_lake(line, idx) == [(line, "")]


def test_empty_index():
    line = LineString([(0, 0), (10, 0)])
    assert assign_under_lake(line, LakeIndex([])) == [(line, "")]
