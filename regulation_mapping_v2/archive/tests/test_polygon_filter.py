"""
Tests for stream_polygon_mask (regulation_mapping_v2.polygon_filter).

All tests use the same 10×10 square polygon and buffer=2 so the geometry
is easy to reason about:

    y
    10 ┌──────────┐ · · · · ·
       │          │         · ← buffer zone (x=10→12)
     5 │ POLYGON  │         ·
       │  (0,0)   │         ·
     0 └──────────┘ · · · · ·
       0          10        12
                  x
       ←  exact  →← buffer →

A segment is True when:
  • It (or a fellow segment sharing the same call) touches the exact polygon,
    AND it touches the buffered polygon.

A segment is False when:
  • No segment in the call touches the exact polygon (stream never enters), OR
  • The segment itself is beyond the buffered polygon.
"""

import pytest
from shapely.geometry import LineString, box

from regulation_mapping_v2.polygon_filter import stream_polygon_mask

# Shared fixtures
POLYGON = box(0, 0, 10, 10)  # 10×10 square
BUFFER = 2.0  # 2-unit leniency ring outside the polygon


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_stream_returns_empty_list():
    """No segments → empty list (no crash)."""
    assert stream_polygon_mask(POLYGON, [], BUFFER) == []


def test_buffer_param_default_exists():
    """Function can be called without supplying buffer_m."""
    seg = LineString([(2, 5), (8, 5)])
    result = stream_polygon_mask(POLYGON, [seg])
    assert isinstance(result, list)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Single-segment cases
# ---------------------------------------------------------------------------


def test_segment_fully_inside_polygon():
    """
    Segment lies entirely inside → True.

       0        10
     5 ·--seg--·
    """
    seg = LineString([(2, 5), (8, 5)])
    assert stream_polygon_mask(POLYGON, [seg], BUFFER) == [True]


def test_segment_far_outside_buffer():
    """
    Segment is far away — not even close to the buffer → False.

       0   10  12         20    25
             ·            ·--seg--·
    """
    seg = LineString([(20, 5), (25, 5)])
    assert stream_polygon_mask(POLYGON, [seg], BUFFER) == [False]


def test_segment_on_polygon_boundary():
    """
    Segment lies along the polygon's right edge (x=10).
    Touching the boundary counts as intersecting → True.

       0        10
     0 ·        seg-start
               |
    10 ·        seg-end
    """
    seg = LineString([(10, 0), (10, 10)])
    assert stream_polygon_mask(POLYGON, [seg], BUFFER) == [True]


def test_segment_straddles_border():
    """
    Segment crosses the polygon boundary — part inside, part outside → True.

       0        10   12
     5 ·---seg inside---·--seg outside--·
                   ↑ border
    """
    seg = LineString([(8, 5), (12, 5)])
    assert stream_polygon_mask(POLYGON, [seg], BUFFER) == [True]


def test_single_segment_in_buffer_zone_only():
    """
    Only one segment; it only touches the buffer zone, never the exact
    polygon → False.  The stream never enters, so the buffer leniency
    does not apply.

       0        10  10.5  11  12
                     ·--seg--·
                    (buffer zone only)
    """
    seg = LineString([(10.5, 5), (11.0, 5)])
    assert stream_polygon_mask(POLYGON, [seg], BUFFER) == [False]


# ---------------------------------------------------------------------------
# Multi-segment stream cases  (the interesting ones)
# ---------------------------------------------------------------------------


def test_stream_entering_polygon_pulls_in_buffer_segment():
    """
    Two segments share a watershed code (passed together).

    seg_a crosses the polygon border        → enters polygon (triggers leniency)
    seg_b is in the buffer zone only        → included because stream enters

       0        10  11  12  13
     5 ·--seg_a--·-----·
                    ·--seg_b--·
                 (buffer zone)

    Expected mask: [True, True]
    """
    seg_a = LineString([(8, 5), (12, 5)])  # straddles x=10
    seg_b = LineString([(11, 5), (13, 5)])  # buffer zone (x=11→13; buffer ends at 12)

    result = stream_polygon_mask(POLYGON, [seg_a, seg_b], BUFFER)
    assert result == [True, True]


def test_stream_never_entering_polygon_rejects_buffer_segments():
    """
    Neither segment touches the exact polygon.

    seg_a is in the buffer zone   → buffer-only, stream doesn't enter → False
    seg_b is beyond the buffer    → obviously False

       0        10  10.5 11  12         20    25
                     ·seg_a·                ·--seg_b--·
                  (buffer only)

    Expected mask: [False, False]
    """
    seg_a = LineString([(10.5, 5), (11.0, 5)])  # buffer zone, never enters polygon
    seg_b = LineString([(20, 5), (25, 5)])  # far outside

    result = stream_polygon_mask(POLYGON, [seg_a, seg_b], BUFFER)
    assert result == [False, False]


def test_three_segment_stream_mixed_mask():
    """
    Three segments: inside / straddling / buffer-tail.
    All are True because the stream enters via seg_a and seg_b.

       0     8  10  11  13
     5 ·-a-··--b--·
                ·--c--·
                (buffer)

    Expected mask: [True, True, True]
    """
    seg_a = LineString([(2, 5), (8, 5)])  # fully inside
    seg_b = LineString([(8, 5), (12, 5)])  # straddles border
    seg_c = LineString([(11, 5), (13, 5)])  # buffer zone only

    result = stream_polygon_mask(POLYGON, [seg_a, seg_b, seg_c], BUFFER)
    assert result == [True, True, True]


def test_segment_beyond_buffer_excluded_even_when_stream_enters():
    """
    The stream enters the polygon (seg_a), but seg_b is so far out that
    it misses even the buffer zone → seg_b is False.

       0        10  12         20    25
     5 ·--seg_a--···             ·--seg_b--·
            enters  ↑ buffer ends

    Expected mask: [True, False]
    """
    seg_a = LineString([(5, 5), (12, 5)])  # enters polygon, tail in buffer
    seg_b = LineString([(20, 5), (25, 5)])  # beyond buffer

    result = stream_polygon_mask(POLYGON, [seg_a, seg_b], BUFFER)
    assert result == [True, False]


# ---------------------------------------------------------------------------
# Buffer-size sensitivity
# ---------------------------------------------------------------------------


def test_buffer_zero_no_leniency():
    """
    With buffer_m=0 the function degrades to strict exact-polygon matching.
    A segment outside the polygon is False even if the stream enters.

       0        10  10.5  11
     5 ·--seg_a--·
                  ·-seg_b-·   ← just outside polygon, no buffer → False

    Expected mask: [True, False]
    """
    seg_a = LineString([(2, 5), (8, 5)])  # inside
    seg_b = LineString([(10.5, 5), (11, 5)])  # just outside

    result = stream_polygon_mask(POLYGON, [seg_a, seg_b], buffer_m=0)
    assert result == [True, False]


def test_larger_buffer_includes_farther_segment():
    """
    Increasing the buffer includes segments that a smaller buffer misses.

    seg_a enters the polygon (triggers leniency).
    seg_b sits at x=13→14 — beyond the default 2-unit buffer (ends at 12),
    but inside a 5-unit buffer (ends at 15).

    With buffer=2: seg_b → False
    With buffer=5: seg_b → True
    """
    seg_a = LineString([(5, 5), (11, 5)])  # enters polygon
    seg_b = LineString([(13, 5), (14, 5)])  # x=13→14

    assert stream_polygon_mask(POLYGON, [seg_a, seg_b], buffer_m=2) == [True, False]
    assert stream_polygon_mask(POLYGON, [seg_a, seg_b], buffer_m=5) == [True, True]
