"""Which station speaks for each watershed group.

A watershed group is a region — 3,600 km2 on average — and the Province's 246 of them tile
British Columbia exactly once with no overlap. That flatness is what makes this simple: a
group either has a gauge in it or it does not, and the one next door is a different river
system rather than a bigger version of this one, so there is nothing to inherit.
"""

from pipeline.atlas.gauges.basin_station import resolve


def test_a_group_with_one_gauge_takes_it():
    assert resolve({"LFRA": [("S", 500.0)]}) == {"LFRA": ("S", 0)}


def test_the_largest_catchment_speaks_for_the_group():
    """The group is a region and the question is what the region is doing, so the gauge
    draining the most of it is the closest thing to an answer for the whole."""
    got = resolve({"LFRA": [("small", 12.0), ("big", 4000.0), ("mid", 300.0)]})
    assert got["LFRA"] == ("big", 0)


def test_a_group_with_no_gauge_gets_no_row():
    """Absent, not a neighbour's number. Groups do not nest, so there is nothing upstream
    to borrow from — the map draws it as unmeasured, which is the honest answer."""
    assert resolve({"LFRA": []}) == {}
    assert resolve({}) == {}


def test_ties_break_the_same_way_every_build():
    """A colour that changes when the query planner changes its mind is worse than either
    choice."""
    a = resolve({"LFRA": [("bbb", 100.0), ("aaa", 100.0)]})
    b = resolve({"LFRA": [("aaa", 100.0), ("bbb", 100.0)]})
    assert a == b == {"LFRA": ("aaa", 0)}


def test_a_station_with_no_area_does_not_win_over_one_with():
    """An unknown catchment sorts last rather than first — `None` is not "very large"."""
    got = resolve({"LFRA": [("unknown", None), ("known", 50.0)]})
    assert got["LFRA"] == ("known", 0)


def test_levels_up_is_always_zero_and_still_reported():
    """The client reads it, and the shape should not change under it if the groups are ever
    swapped for something that nests."""
    for basin, (_st, up) in resolve({"A": [("S", 1.0)], "B": [("T", 2.0)]}).items():
        assert up == 0, basin
