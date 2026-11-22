"""ROUND20 (owner review of v24, 2026-10-18) — rows 21: the header IS the total tier, the share inside the total.

  1  the group's header and the tree's first tier are ONE block: `group.header` carries `tier` (the tree root's id) and
     its `says` is the root's line ("4 a day per angler across Region 2, lakes included"); a water's own total says
     "per angler" like a zone's ("This river’s rule: up to 1 a day per angler"), since it is now the card's big line.
  2  the stream share says it sits INSIDE the one total, never beside it: "Up to 2 of the 4 from streams";
     where a water's own rule lifts fish out of it, ", not counting hatchery rainbow trout" (the UR3 "— your hatchery
     rainbow trout count toward the 4, not these 2" read as 4 + 2).
  The gate: the header names the root tier and says its words; a header saying anything else is refused.
(3, the size box's number, is the page's: check.mjs r20-big-numbers.)
"""
from __future__ import annotations

import copy

from pipeline.deliver.answers import card_check as CC
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _trout(out):
    """The trout and char group (a zone total's pool, or a water's own total: by its kinds)."""
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    return _trout(out), out, P


def _tiers(g):
    def walk(ns):
        for n in ns:
            if n["node"] == "tier":
                yield n
                yield from walk(n.get("children") or [])
    return list(walk(g["tree"]))


# 1 ---------------------------------------------------------------------------------------------
def test_1_the_header_is_the_tree_root():
    # (T866) the Vedder root's words, and the lake's header (a zone total with no share: the root's words, no "lakes
    # included"), word for word: card_words CARDS
    g, _o, _P = _g(F.VEDDER)
    (root,) = g["tree"]
    assert g["header"] == {"says": root["says"], "tier": root["id"], "tag": None}
    g, _o, _P = _g(F.LAKE)
    (_root,) = g["tree"]


def test_1_a_waters_own_total_says_per_angler():
    # (T866) the creek's own total, the Bella Coola's total and own tier, and its row by the tributary walk (Ape Creek's
    # shape), word for word: card_words CARDS (COUNT, BELLA, BELLA_TRIB)
    g, _o, _P = _g(F.COUNT)
    (root,) = g["tree"]
    assert g["header"]["says"] == root["says"]
    # T846: the Bella Coola's cutthroat is released at the source, so every fish its 1 holds also counts toward Region
    # 2's 4: the river's rule is the own tier under that total (the shape of its Apr 1-May 31 row), by the walk too
    for w in (F.BELLA, F.BELLA_TRIB):
        g, _o, _P = _g(w)
        (_own,) = [c for c in g["tree"][0]["children"] if c.get("tier") == "own"]


# 2 ---------------------------------------------------------------------------------------------
def test_2_the_lifted_share_sits_inside_the_total():
    # (T866) its share's "of the 4" (ROUND UR7 A: the lift said once, by the own tier's relation), like a plain stream's,
    # and the Bella Coola May own tier's "(part of the 4)", word for word: card_words CARDS
    g, _o, _P = _g(F.VEDDER)
    assert not any("count toward" in t["says"] or "don’t use" in t["says"] for t in _tiers(g))


# caps: one full stop ---------------------------------------------------------------------------
def test_a_cap_sentence_ending_in_bc_has_one_full_stop():
    """`caps.py` appended "." to a sentence that may already end in "B.C." — "No wild steelhead may be kept in B.C.."
    (48 live occurrences; the fixture's wild-steelhead cap is Region 2's, so the words are pinned on the helper and the
    fixture's caps on the rule). A sentence ends in exactly one full stop."""
    from pipeline.deliver.answers import common
    assert common.full_stop("No wild steelhead may be kept in B.C.") == "No wild steelhead may be kept in B.C."
    assert common.full_stop("No wild steelhead may be kept in Region 2") == "No wild steelhead may be kept in Region 2."
    assert common.full_stop("Up to 4 trout and char a day across Region 2") == "Up to 4 trout and char a day across Region 2."
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.BELLA, OCT10), (F.LWA, OCT10)):
        out, _P = F.card(w, md)
        for r in out["rows"]:
            for c in (r.get("group") or {}).get("caps") or []:
                assert c["says"].endswith(".") and not c["says"].endswith(".."), c["says"]


# the gate ----------------------------------------------------------------------------------------
def test_the_gate_refuses_a_header_that_is_not_the_root():
    g, out, P = _g(F.VEDDER)
    assert CC.check_card(P, out) == []

    def mutated(fn):
        o2 = copy.deepcopy(out)
        fn(_trout(o2))
        return CC.check_card(P, o2)
    assert any("header" in p for p in mutated(lambda g: g["header"].__setitem__("says", "2 a day per angler from this stream")))
    assert any("header" in p for p in mutated(lambda g: g["header"].__setitem__("tier", "t2")))

