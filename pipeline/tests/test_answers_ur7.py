"""ROUND UR7 (owner, the final prototype round before the app port; rows 29).

  A  the stream share says nothing the head and the own tier don't: "Only 2 of the 4 may be from streams" — the fish a
     water's own rule lifts out of it (", not counting hatchery rainbow trout") are said ONCE, by that own tier's `relation`
     ("Not part of the 2 from streams; part of the 4 a day across Region 2"); with no relation the share still says them
  1  the worked keep-the-share line reads with the own tier's "Not part of the 2 from streams" (the lift holds only on its own
     waters; on other streams the region's 2 counts them): "If you keep 2 hatchery rainbow trout here: they count as your 2
     from streams anywhere else today — up to 2 more of any kind, from here or from lakes."
  2  a kind row directly under the stream share, with no own rule, says it is part of the share: "Up to 2 hatchery rainbow
     trout (part of the 2 from streams)" (the Chilliwack part 2's rainbow row, after the size section closes); "Only 1 of the
     2 from streams may be …"
"""
from __future__ import annotations

from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _g(water, md=OCT10):
    out, _P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g


def _walk(nodes, parent=None):
    for n in nodes:
        yield n, parent
        yield from _walk(n.get("children") or [], n)


def _tier(g, kind):
    (t,) = [n for n, _p in _walk(g["tree"]) if n.get("tier") == kind]
    return t


# A ---------------------------------------------------------------------------------------------
def test_A_the_share_says_no_lifted_fish_where_the_own_tier_says_its_relation():
    for water in (F.VEDDER, F.VEDDER_TRIB):
        g = _g(water)
        assert _tier(g, "share")["says"] == "Only 2 of the 4 may be from streams"
        assert _tier(g, "own")["relation"] == "Not part of the 2 from streams; part of the 4 a day across Region 2"


def test_A_two_lifts_said_once_by_the_own_tier():
    g = _g(F.TWO)
    assert _tier(g, "share")["says"] == "Only 2 of the 4 may be from streams"
    assert _tier(g, "own")["relation"] == "Not part of the 2 from streams; part of the 4 a day across Region 2"


# 1 ---------------------------------------------------------------------------------------------
def test_1_the_worked_share_line_says_the_fish_count_as_the_2_elsewhere():
    w = {x["kept"]: x for x in _g(F.VEDDER)["worked"] if x["at"] == "here" and x["share"]}
    assert w[2]["q"] == "If you keep 2 hatchery rainbow trout here"
    assert w[2]["a"] == ("they count as your 2 from streams anywhere else today — up to 2 more of any kind, "
                         "from here or from lakes.")
    assert w[4]["q"] == "If you keep 4 hatchery rainbow trout here"
    assert w[4]["a"] == "that’s your trout and char for the day, everywhere."


def test_1_no_worked_line_says_none_from_other_streams():
    for water in (F.VEDDER, F.TWO, F.VEDDER_TRIB):
        assert not [x for x in _g(water)["worked"] if "none from other" in x["a"]]


# 2 ---------------------------------------------------------------------------------------------
def test_2_a_kind_directly_under_the_share_says_it_is_part_of_it():
    g = _g(F.STREAM)
    share = _tier(g, "share")
    direct = [n for n in share["children"] if n["node"] == "kind"]
    assert [k["says"] for k in direct] == ["Up to 2 hatchery rainbow trout (part of the 2 from streams)"]
    # the kinds inside the size section keep their words (the section's head says "Of these")
    size = _tier(g, "size")
    assert [k["says"] for k in size["children"]] == [
        "Up to 2 may be hatchery brook, brown or cutthroat trout",
        "Only 1 of the 2 may be hatchery Dolly Varden/bull trout or lake trout",
        "Up to 2 may be hatchery steelhead only — wild steelhead go back"]


def test_2_every_shape_of_kind_under_the_share():
    g = _g(F.TWO)
    share = _tier(g, "share")
    assert [k["says"] for k in share["children"] if k["node"] == "kind"] == [
        "Up to 2 hatchery brook or brown trout (part of the 2 from streams)",
        "Only 1 of the 2 from streams may be hatchery Dolly Varden/bull trout or lake trout",
        "Up to 2 hatchery steelhead only (part of the 2 from streams) — wild steelhead go back"]


def test_2_kinds_under_an_own_tier_or_a_water_total_are_unchanged():
    g = _g(F.TWO)
    own = _tier(g, "own")
    assert [k["says"] for k in own["children"]] == ["Only 3 of the 4 may be hatchery cutthroat trout",
                                                     "Up to 4 may be hatchery rainbow trout"]
    g = _g(F.COUNT)
    assert all("part of" not in n["says"] for n, _p in _walk(g["tree"]) if n["node"] == "kind")
