"""ROUND20b (owner, 2026-10-18: "fewer words without missing anything") — rows 22, the card's word cuts, in the data.

  C1  an own tier holding ONE kind row at the tier's own number says the fish itself and the kind row is FOLDED into it:
      "This river’s rule: up to 4 hatchery rainbow trout a day" (the kind carries `folded: true`; its `says` / `sentence`
      stay for the gate; the page draws no sentence under a folded tier line — the number appears once)
  C2  an own tier's `line` no longer carries "Replaces …" (the share already says ", not counting hatchery rainbow trout";
      the replaced rule is history, not a rule in force); `replaces` stays a field
  C3  under the total, the SHARE tier comes before a water's own tiers (owner: after the header's 4, the 2 is the next line)
  C4  the worked keep-here lines say the two ends of the range only ("If you keep 4 …", "If you keep 2 …"); the middle
      lines are arithmetic the reader does not need
"""
from __future__ import annotations

from pipeline.deliver.answers import card_check as CC
from pipeline.tests import answers28_fixture as F

OCT10, MAY1 = (10, 10), (5, 1)


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


def _tiers(g):
    def walk(ns):
        for n in ns:
            if n["node"] == "tier":
                yield n
                yield from walk(n.get("children") or [])
    return list(walk(g["tree"]))


# C1 ---------------------------------------------------------------------------------------------
def test_c1_an_own_tier_with_one_kind_folds_the_kind_into_its_line():
    g, _o, _P = _g(F.VEDDER)
    (own,) = [t for t in _tiers(g) if t["tier"] == "own"]
    assert own["says"] == "This river’s rule: up to 4 hatchery rainbow trout a day"
    (rb,) = own["children"]
    assert rb["node"] == "kind" and rb["folded"] is True
    assert rb["says"] == "Up to 4 may be hatchery rainbow trout"       # the gate's words stay
    assert rb["sentence"].startswith("Of the 4 a day: up to 4 may be hatchery rainbow trout")


def test_c1_the_fold_words_for_an_own_and_a_share_tier():
    """The share too (Glenn Creek: "Only 2 of the 5 may be from this stream, not counting rainbow trout" over "Up to 2 may
    be brook, brown, cutthroat or lake trout"); no fixture water has a single-kind share, so the words are pinned here."""
    from pipeline.deliver.answers.tree import fold_words
    assert fold_words("This river’s rule: up to 4 a day", "own", 4, "hatchery rainbow trout") == \
        "This river’s rule: up to 4 hatchery rainbow trout a day"
    assert fold_words("This lake’s rule: up to 2 a day (part of the 5)", "own", 2, "lake trout") == \
        "This lake’s rule: up to 2 lake trout a day (part of the 5)"
    assert fold_words("Only 2 of the 5 may be from this stream, not counting rainbow trout", "share", 2,
                      "brook, brown, cutthroat or lake trout") == \
        "Only 2 of the 5 may be brook, brown, cutthroat or lake trout from this stream"
    assert fold_words("2 a day per angler from this stream, not counting rainbow trout", "share", 2,
                      "brook, brown, cutthroat or lake trout") == \
        "2 brook, brown, cutthroat or lake trout a day per angler from this stream"
    # the fixture's share (the Vedder) holds a size tier, not one kind: nothing folds
    g, _o, _P = _g(F.VEDDER)
    (share,) = [t for t in _tiers(g) if t["tier"] == "share"]
    assert share["says"] == "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout"
    assert all(not k.get("folded") for k in share["children"] if k["node"] == "kind")


def test_c1_an_own_tier_with_more_kinds_does_not_fold():
    g, _o, _P = _g(F.BELLA_MAY_ST, MAY1)
    (own,) = [t for t in _tiers(g) if t["tier"] == "own"]
    assert own["says"] == "This river’s rule: up to 2 a day (part of the 4)"
    assert all(not k.get("folded") for k in own["children"] if k["node"] == "kind")
    # a kind row anywhere else is never folded
    assert all(not k.get("folded") for t in _tiers(g) if t["tier"] != "own" for k in t["children"] if k["node"] == "kind")


# C2 ---------------------------------------------------------------------------------------------
def test_c2_the_own_tier_line_drops_replaces():
    g, _o, _P = _g(F.VEDDER)
    (own,) = [t for t in _tiers(g) if t["tier"] == "own"]
    assert own["replaces"].startswith("Replaces Region 2’s 2-from-streams limit")
    assert own["line"] == "This river’s rule"
    g, _o, _P = _g(F.VEDDER_TRIB)               # by the tributary walk: the why stays on the line
    (own,) = [t for t in _tiers(g) if t["tier"] == "own"]
    assert own["line"].endswith("(its row covers its tributaries, so it applies here)")
    assert "Replaces" not in own["line"]


# C3 ---------------------------------------------------------------------------------------------
def test_c3_the_share_comes_before_the_waters_own_tier():
    g, _o, _P = _g(F.VEDDER)
    (root,) = g["tree"]
    assert [t["tier"] for t in root["children"]] == ["share", "own"]


# C4 ---------------------------------------------------------------------------------------------
def test_c4_the_worked_lines_say_the_ends_of_the_range_only():
    g, _o, _P = _g(F.VEDDER)
    here = [w for w in g["worked"] if w["at"] == "here"]
    assert [w["kept"] for w in here] == [4, 2]
    assert here[0]["a"] == "that’s your trout and char for the day, everywhere."
    assert here[1]["a"] == "none from other streams today — up to 2 more from here or from lakes."


# the gate ----------------------------------------------------------------------------------------
def test_the_gate_still_proves_every_card():
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.LAKE, OCT10), (F.BELLA_MAY_ST, MAY1), (F.BELLA, OCT10),
                  (F.VEDDER_TRIB, OCT10), (F.LWA, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w
