"""ROUND20g (owner, rows 24 / glossary 3):

1. THE ORIGIN RULE SITS WHERE ITS SCOPE IS. Region 2's "And you must release: Wild trout/char from streams" (printed p.21) holds on
   STREAMS only — the Region 2 total (4 a day) counts lakes too, where wild trout may be kept. So "Keep hatchery fish only — wild
   trout and char go back" belongs to the stream share's tier ("Only 2 of the 4 may be from streams…"), not the group head: the
   share tier carries `origin` ("From streams, keep hatchery fish only — wild trout and char go back."), the group's `origin` is
   None. The group keeps it only where the rules sending the other origin back hold wherever the total does.
2. THE STEELHEAD ROW SAYS LESS. The size band's head already says "— 2 if one is a hatchery steelhead", so the kind's
   `size_note` is gone; duties "10 a year · stop fishing here after 2 a day · record each on your licence right away"; the presence
   line "A rainbow trout over 50 cm (20 in) here counts as a steelhead."
3. THE POSSESSION LINE IS ONE PLAIN SENTENCE PAIR. "Possession: 2 days’ quota. Fish at home don’t count."; the glossary term
   says one short line and one short example; the book's definition stays VERBATIM in its own field (`quote`), for a fold.
"""
from __future__ import annotations

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as X
from pipeline.deliver.answers import glossary as G
from pipeline.deliver.answers.tree import walk
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    return F.trout_group(out), out, P


def _tiers(g):
    return [n for n, _p in walk(g["tree"]) if n["node"] == "tier"]


def _st(g):
    return next(n for n, _p in walk(g["tree"]) if n["node"] == "kind" and n["species"] == ["ST"])


# 1 -----------------------------------------------------------------------------------------------------------------
def test_the_streams_origin_rule_sits_on_the_stream_share():
    for w in (F.VEDDER, F.STREAM):
        g, _o, _P = _g(w)
        assert g["origin"] is None, w                         # the total counts lakes too
        (share,) = [t for t in _tiers(g) if t["tier"] == "share"]
        assert share["origin"] == "From streams, keep hatchery fish only — wild trout and char go back.", w
        assert [t["id"] for t in _tiers(g) if t.get("origin")] == [share["id"]], w


def test_a_waters_own_total_keeps_the_origin_on_the_group():
    out, _P = F.card(F.BELLA, OCT10)                          # the Bella Coola's own total is the river's: same scope
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "RB" in r["group"]["kinds"]]
    assert g["origin"] == "Keep hatchery fish only — wild trout and char go back."
    assert not any(t.get("origin") for t in _tiers(g))


def test_a_lake_says_no_origin():
    g, _o, _P = _g(F.LAKE)                                    # a Region 2 lake keeps wild trout
    assert g["origin"] is None
    assert not any(t.get("origin") for t in _tiers(g))


# 2 -----------------------------------------------------------------------------------------------------------------
def test_the_steelhead_row_says_it_once():
    for w in (F.VEDDER, F.STREAM, F.TWO):
        g, _o, _P = _g(w)
        st = _st(g)
        assert "size_note" not in st, w
        assert not any("once you keep one" in s for s in [st["sentence"]] + [a["says"] for a in st["also_counts"]])
    g, _o, _P = _g(F.VEDDER)
    (size,) = [t for t in _tiers(g) if t["tier"] == "size"]
    assert size["says"].endswith("— 2 if one is a hatchery steelhead")      # the band head says it
    st = _st(g)
    assert st["duties"] == ["10 a year", "stop fishing here after 2 a day", "record each on your licence right away"]
    assert st["presence"]["says"] == "A rainbow trout over 50 cm (20 in) here counts as a steelhead."


# 3 -----------------------------------------------------------------------------------------------------------------
def test_the_possession_line():
    x = {"type": "retention_limit", "species": ["ALL_GAME_FISH"], "period": "possession", "per_daily": 2}
    assert X.plain(x) == "Possession: 2 days’ quota. Fish at home don’t count."
    y = {"type": "retention_limit", "species": ["KO"], "period": "possession", "per_daily": 1}
    assert X.plain(y) == "Possession: the daily quota for kokanee. Fish at home don’t count."


def _doc(per_daily=2, total=5):
    return {"rules": {"z1:possession_quota::possession_quota.r1": {
                "entry_id": "z1:possession_quota", "rule_id": "possession_quota.r1",
                "fields": {"per_daily": per_daily}},
            "z1:trout_char_quota::trout_char_quota.r1": {
                "entry_id": "z1:trout_char_quota", "rule_id": "trout_char_quota.r1",
                "fields": {"species": ["TROUT_CHAR"], "take": total}}},
            "entries": {"z1:possession_quota": {"pages": [13]}}, "licensing": {}, "licences": {}}


def test_the_possession_term_is_short_and_keeps_the_book_verbatim():
    B = G.Book()
    t = G.t_possession(B, G.Data(_doc()))
    # ROUND20h 2: the term says only where possession holds (the plain line says the number and the exception)
    assert t["says"] == "The fish you have with you at any time — in camp, a cooler or a vehicle."
    assert t["example"] == "With a daily quota of 5 trout you may have 10: 2 days' fish."
    assert t["quote"] == f"possession quota: {B.define('possession quota')}"          # verbatim, its own field
    t3 = G.t_possession(B, G.Data(_doc(per_daily=3, total=4)))
    assert t3["example"] == \
        "With a daily quota of 4 trout you may have 12: 3 days' fish."
    assert G.VERSION == 4                                                            # ROUND20h


def test_the_gate_still_proves_every_card():
    for w in (F.VEDDER, F.STREAM, F.LAKE, F.BELLA, F.COUNT):
        out, P = F.card(w, OCT10)
        assert CC.check_card(P, out) == [], w
