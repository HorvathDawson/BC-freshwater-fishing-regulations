"""ROUND UR7 (owner, the final prototype round before the app port; rows 29).

  A  the stream share says nothing the head and the own tier don't: "Up to 2 of the 4 from streams" — the fish a
     water's own rule lifts out of it (", not counting hatchery rainbow trout") are said ONCE, by that own tier's `relation`
     ("Not part of the 2 from streams; part of the 4 a day across Region 2"); with no relation the share still says them
  1  the worked keep-the-share line reads with the own tier's "Not part of the 2 from streams" (the lift holds only on its own
     waters; on other streams the region's 2 counts them): "If you keep 2 hatchery rainbow trout here: they don’t use up your 2 from streams here, but on other streams they count as them: none from other streams today — up to 2 more of any kind, from here or from lakes."
  2  a kind row directly under the stream share, with no own rule, says it is part of the share: "Up to 2 hatchery rainbow
     trout (part of the 2 from streams)" (the Chilliwack part 2's rainbow row, after the size section closes); "Only 1 of the
     2 from streams may be …"

(T866) A, 1 and 2's words on the Vedder, its tributary, TWO and the plain stream (the share and own tiers, the worked
keep-the-share lines, the kinds under the share, the size tier and the own tier) are pinned once in
test_answers_card_words.py CARDS. What stays here holds every water at once.
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


# 1 ---------------------------------------------------------------------------------------------
def test_1_every_keep_the_share_line_says_both_halves():
    for water in (F.VEDDER, F.TWO, F.VEDDER_TRIB):
        lines = [x["a"] for x in _g(water)["worked"] if x["at"] == "here" and x["share"] and "none from other" in x["a"]]
        assert lines and all(a.startswith("they don’t use up your 2 from streams here, but on other streams they count as them: ")
                             for a in lines)


# 2 ---------------------------------------------------------------------------------------------
def test_2_kinds_under_a_water_total_say_no_part_of():
    """No "part of" on them; T475: one pattern, the fish with their origin (no headline says it for an own rule)."""
    g = _g(F.COUNT)
    assert all("part of" not in n["says"] for n, _p in _walk(g["tree"]) if n["node"] == "kind")
