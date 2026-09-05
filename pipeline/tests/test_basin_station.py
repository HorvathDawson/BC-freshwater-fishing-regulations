"""Which gauges speak for each watershed group.

A watershed group is a region — 3,600 km2 on average — and the Province's 246 of them tile
British Columbia exactly once with no overlap. That flatness is what makes this simple: a
group either has gauges in it or it does not, and the one next door is a different river
system rather than a bigger version of this one, so there is nothing to inherit.

THIS USED TO ELECT ONE. `resolve` returned the largest-catchment station per group, and the
tests below its old name asserted, carefully, that the biggest one won. The election was the
bug: it made a group's colour hostage to a single station, so a group went blank whenever
that one station had no climatology (13 groups, KISP among them, choosing SKEENA RIVER AT
HAZELTON) or simply was not transmitting (Chilliwack, six gauges, grey because the one named
was quiet). The whole roster ships now and the client combines it.
"""

from pipeline.atlas.gauges.basin_station import roster

YEARS = {"S": 40, "small": 40, "big": 40, "mid": 40, "aaa": 40, "bbb": 40,
         "known": 40, "unknown": 40}


def test_a_group_with_one_gauge_takes_it():
    assert roster({"LFRA": [("S", 500.0)]}, YEARS) == {"LFRA": [("S", 500.0, 40)]}


def test_every_gauge_in_the_group_ships_not_just_the_largest():
    """The failure this replaced: one station named, and the group blank whenever that one
    station had nothing to say — however many others were reporting."""
    got = roster({"LFRA": [("small", 12.0), ("big", 4000.0), ("mid", 300.0)]}, YEARS)
    assert [r[0] for r in got["LFRA"]] == ["big", "mid", "small"]


def test_a_group_with_no_gauge_gets_no_rows():
    """Absent, not a neighbour's number. Groups do not nest, so there is nothing upstream
    to borrow from — the map draws it as unmeasured, which is the honest answer."""
    assert roster({"LFRA": []}, YEARS) == {}
    assert roster({}, YEARS) == {}


def test_the_order_is_the_same_every_build():
    """A file that reorders when the query planner changes its mind is a diff nobody can
    read, and the client's tie-breaking would move with it."""
    a = roster({"LFRA": [("bbb", 100.0), ("aaa", 100.0)]}, YEARS)
    b = roster({"LFRA": [("aaa", 100.0), ("bbb", 100.0)]}, YEARS)
    assert a == b == {"LFRA": [("aaa", 100.0, 40), ("bbb", 100.0, 40)]}


def test_a_station_with_no_area_sorts_last_but_still_ships():
    """`None` is not "very large" — but it is not disqualifying either, because a gauge with
    an unrecorded catchment still measures water. It weighs least, and it is still there when
    it is the only one reporting."""
    got = roster({"LFRA": [("unknown", None), ("known", 50.0)]}, YEARS)
    assert [r[0] for r in got["LFRA"]] == ["known", "unknown"]
    assert got["LFRA"][1][1] == 0.0


def test_the_record_length_travels_with_the_station():
    """The client weights by it, so a station whose record is unknown must arrive as 0 years
    rather than as a missing field the client has to guess about."""
    got = roster({"A": [("S", 1.0)], "B": [("T", 2.0)]}, {"S": 12})
    assert got["A"] == [("S", 1.0, 12)]
    assert got["B"] == [("T", 2.0, 0)]
