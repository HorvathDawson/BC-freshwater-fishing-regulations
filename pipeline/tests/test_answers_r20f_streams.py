"""ROUND20f 3 (owner; the 20e reader read "ALL REGION 2 STREAMS" + "from this stream" both ways) — rows 23: a share that is
the REGION'S (a zone / province rule: the Region 2 streams' combined 2) says it plainly — "Only 2 of the 4 may be from streams"
(the headline "2 a day per angler from streams"; the page's pinned line "↳ 2 of them from streams"); a share a WATER'S own row
sets keeps "from this stream". `tree.share_place(water, region_wide)` is the one place the phrase comes from.
"""
from __future__ import annotations

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers.tree import fold_words, share_place
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


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


def test_the_place_phrase():
    assert share_place("stream", True) == "streams"
    assert share_place("lake", True) == "lakes"
    assert share_place("stream", False) == "this stream"
    assert share_place("lake", False) == "this lake"


def test_a_regions_share_says_from_streams():
    g, _o, _P = _g(F.VEDDER)                   # Region 2's "2 from streams" (z2), lifted by the river's row
    (share,) = [t for t in _tiers(g) if t["tier"] == "share"]
    assert share["says"] == "Only 2 of the 4 may be from streams"   # ROUND UR7 A: the lift said once, by the own tier's relation
    assert g["headline"]["says"] == "2 a day per angler from streams"
    assert g["headline"]["unit"] == "a day per angler from streams"
    g, _o, _P = _g(F.STREAM)                   # a plain Region 2 stream
    (share,) = [t for t in _tiers(g) if t["tier"] == "share"]
    assert share["says"] == "Only 2 of the 4 may be from streams"
    assert all("this stream" not in t["says"] for t in _tiers(g))


def test_the_fold_keeps_the_place():
    assert fold_words("Only 2 of the 5 may be from streams, not counting rainbow trout", "share", 2,
                      "brook, brown, cutthroat or lake trout") == \
        "Only 2 of the 5 may be brook, brown, cutthroat or lake trout from streams"
    assert fold_words("2 a day per angler from this stream", "share", 2, "brook or brown trout") == \
        "2 brook or brown trout a day per angler from this stream"


def test_the_gate_still_proves_every_card():
    for w in (F.VEDDER, F.STREAM, F.LAKE, F.TWO, F.COUNT):
        out, P = F.card(w, OCT10)
        assert CC.check_card(P, out) == [], w
