"""Geometry-cut + section_id tests (03 S5, 02). Synthetic — no gpkg needed."""

from shapely.geometry import LineString

from pipeline.graph import cutting


def test_endpoint_id_rounds_to_3dp():
    assert cutting.endpoint_id(1.23456, 7.6543) == "1.235_7.654"


def test_merge_ordered_stitches_and_dedups_shared_vertex():
    a = LineString([(0, 0), (10, 0)])
    b = LineString([(10, 0), (20, 0)])
    merged = cutting.merge_ordered([a, b])
    assert list(merged.coords) == [(0, 0), (10, 0), (20, 0)]
    assert merged.length == 20


def test_substring_accuracy_cm():
    line = LineString([(0, 0), (100, 0)])   # mouth at x=0
    piece = cutting.substring_cut(line, mouth_measure=0.0, start_m=25.0, end_m=75.0)
    assert abs(piece.length - 50.0) < 0.01
    xs = [c[0] for c in piece.coords]
    assert abs(xs[0] - 25.0) < 0.01 and abs(xs[-1] - 75.0) < 0.01


def test_substring_respects_mouth_measure_offset():
    line = LineString([(0, 0), (100, 0)])   # geometry spans absolute measures 500..600
    piece = cutting.substring_cut(line, mouth_measure=500.0, start_m=520.0, end_m=560.0)
    assert abs(piece.length - 40.0) < 0.01


def test_two_point_linestring_interpolates():
    line = LineString([(0, 0), (100, 0)])   # 2-point straight line: linear interpolation
    piece = cutting.substring_cut(line, 0.0, 10.0, 20.0)
    assert abs(piece.length - 10.0) < 0.01


def test_section_id_readable_and_stable():
    a = cutting.section_id("999", "outlet", "split:x", lower_route_measure=0.0)
    assert a == "999:0"
    # readable id depends only on blk + start measure -> stable regardless of upper bound
    b = cutting.section_id("999", "outlet", "split:y", lower_route_measure=0.0)
    assert a == b


def test_section_id_hash_changes_with_bounds():
    a = cutting.section_id("999", "outlet", "split:x")
    b = cutting.section_id("999", "split:x", "headwaters")
    assert a != b
