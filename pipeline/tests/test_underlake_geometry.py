"""A lake's geometry must never invent a line that is not in the data.

THE BUG. `build_section_geometries` stitched every under-lake fid of a waterbody into ONE
LineString with `merge_ordered`, which extends its coordinate list whether or not the next
piece begins where the last one ended. A lake fed by several tributaries has several
separate through-lines, so the result was a single self-crossing polyline with straight
segments drawn between them — measured at up to 293 times the length of the lake it
supposedly crossed, and drawn on screen as long diagonal streaks running off the water.

The rule is simple and worth pinning: join what touches, keep the rest apart.
"""

from __future__ import annotations

from shapely.geometry import LineString, MultiLineString

from pipeline.graph import cutting


def test_contiguous_pieces_still_become_one_line():
    # A chain of lakes should still read as one river where the data says it is one.
    a = LineString([(0, 0), (10, 0)])
    b = LineString([(10, 0), (20, 0)])
    out = cutting.merge_runs([a, b])
    assert out.geom_type == "LineString"
    assert list(out.coords) == [(0, 0), (10, 0), (20, 0)]


def test_disjoint_pieces_stay_apart():
    a = LineString([(0, 0), (10, 0)])
    b = LineString([(500, 500), (510, 500)])       # nowhere near a
    out = cutting.merge_runs([a, b])
    assert out.geom_type == "MultiLineString", (
        "disjoint under-lake lines were merged into one — that draws a straight segment "
        "between them that exists in no dataset")
    assert len(out.geoms) == 2


def test_the_invented_segment_is_what_made_the_line_too_long():
    """The regression, stated as a measurement rather than a shape."""
    a = LineString([(0, 0), (10, 0)])
    b = LineString([(5000, 0), (5010, 0)])
    old = cutting.merge_ordered([a, b])
    new = cutting.merge_runs([a, b])
    assert old.length > 4000, "merge_ordered should span the gap (this is the old behaviour)"
    assert new.length == 20, "merge_runs must measure only the real linework"


def test_a_single_piece_is_a_plain_linestring():
    out = cutting.merge_runs([LineString([(0, 0), (1, 1)])])
    assert out.geom_type == "LineString"


def test_empty_input_is_an_empty_line_not_a_crash():
    assert cutting.merge_runs([]).is_empty


def test_lake_nodes_use_merge_runs():
    """Pin the call site: the fix is worthless if the builder goes back to merge_ordered."""
    import inspect
    from pipeline.graph import graph as G

    src = inspect.getsource(G.build_section_geometries)
    lake_half = src.split("lake_fids.items()")[1]
    # comments explain the bug and name the old function; the CALL is what is asserted
    calls = [ln for ln in lake_half.splitlines() if "cutting.merge" in ln]
    assert calls, "lake geometry no longer calls a merge at all"
    assert all("merge_runs" in ln for ln in calls), (
        f"lake geometry is stitching disjoint lines again: {calls}")
