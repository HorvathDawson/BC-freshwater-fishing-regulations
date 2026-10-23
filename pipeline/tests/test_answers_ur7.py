"""ROUND UR7 (owner, the final prototype round before the app port; rows 29).

  A  the stream share says nothing the head and the own tier don't: "Only 2 of the 4 may be from streams" — the fish a
     water's own rule lifts out of it (", not counting hatchery rainbow trout") are said ONCE, by that own tier's `relation`
     ("Not part of the 2 from streams; part of the 4 a day across Region 2"); with no relation the share still says them
  1  the worked keep-the-share line reads with the own tier's "Not part of the 2 from streams" (the lift holds only on its own
     waters; on other streams the region's 2 counts them): "If you keep 2 hatchery rainbow trout here: they don’t use up your 2 from streams here, but on other streams they count as them: none from other streams today — up to 2 more of any kind, from here or from lakes."
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
    assert w[2]["a"] == ("they don’t use up your 2 from streams here, but on other streams they count as them: none from other streams today — up to 2 more of any kind, "
                         "from here or from lakes.")
    assert w[4]["q"] == "If you keep 4 hatchery rainbow trout here"
    assert w[4]["a"] == "that’s your trout and char for the day, everywhere."


def test_1_every_keep_the_share_line_says_both_halves():
    for water in (F.VEDDER, F.TWO, F.VEDDER_TRIB):
        lines = [x["a"] for x in _g(water)["worked"] if x["at"] == "here" and x["share"] and "none from other" in x["a"]]
        assert lines and all(a.startswith("they don’t use up your 2 from streams here, but on other streams they count as them: ")
                             for a in lines)


# 2 ---------------------------------------------------------------------------------------------
def test_2_a_kind_directly_under_the_share_says_it_is_part_of_it():
    """T128 (owner 2026-10-22) supersedes "(part of the 2 from streams)": every kind under the share names it first, the
    ones inside the size section too."""
    g = _g(F.STREAM)
    share = _tier(g, "share")
    direct = [n for n in share["children"] if n["node"] == "kind"]
    assert [k["says"] for k in direct] == ["Of your 2 trout and char a day in streams, up to 2 may be hatchery rainbow trout"]
    size = _tier(g, "size")
    assert [k["says"] for k in size["children"]] == [
        "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout",
        "Of your 2 trout and char a day in streams, only 1 may be hatchery Dolly Varden/bull trout or lake trout",
        "Of your 2 trout and char a day in streams, up to 2 may be hatchery steelhead only — wild steelhead go back"]


def test_2_every_shape_of_kind_under_the_share():
    g = _g(F.TWO)
    share = _tier(g, "share")
    assert [k["says"] for k in share["children"] if k["node"] == "kind"] == [
        "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook or brown trout",
        "Of your 2 trout and char a day in streams, only 1 may be hatchery Dolly Varden/bull trout or lake trout",
        "Of your 2 trout and char a day in streams, up to 2 may be hatchery steelhead only — wild steelhead go back"]


def test_2_kinds_under_an_own_tier_or_a_water_total_are_unchanged():
    """No "part of" on them; T128: they name the own rule's total, by whose it is (its fish are only some of the group's,
    so no "your 4 trout and char": T128 review, `tree.total_fish`)."""
    g = _g(F.TWO)
    own = _tier(g, "own")
    assert [k["says"] for k in own["children"]] == [
        "Of the 4 a day under this river’s rule, only 3 may be hatchery cutthroat trout",
        "Of the 4 a day under this river’s rule, up to 4 may be hatchery rainbow trout"]
    g = _g(F.COUNT)
    assert all("part of" not in n["says"] for n, _p in _walk(g["tree"]) if n["node"] == "kind")
