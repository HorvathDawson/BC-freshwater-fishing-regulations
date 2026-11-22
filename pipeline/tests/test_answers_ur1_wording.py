"""USER ROUND 1 (six-angler panel on v21, 2026-10-17) — small wording fixes, no data, CI's no-data tier.

  A  totals say "per angler" ("4 a day per angler across Region 2, lakes included", the header too); on a water whose
     own rule lifts kinds out of the stream share, the share tier says the lifted fish don't use it ("2 a day from this
     stream (part of the 4) — your hatchery rainbow trout don't use these 2"), from the lift data
  B  a size tier says which fish: "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery
     steelhead" (the page draws a size tier as a box)
  D  a count tier: "1 cutthroat or rainbow trout in all"
  F  a species closure says what "No fishing for" means in the book (p.4 legend): not even to release; release any
     caught by accident
  G  "Write each one on your licence right away"; weight "1 kg (2.2 lb)"; a boat that lifts the line count reads "1 line
     from shore; any number from a boat" (Kootenay Lake's main body)

(T866) Every sentence above, read off the fixture cards (the Vedder's total, header, share, size tier; the Bella Coola's
May tiers and its steelhead closure) and off the gear answers (the Vedder's, Kootenay Lake's, Okanagan Lake's), is pinned
once in test_answers_card_words.py CARDS / GEAR. What stays here is not a card's words.
"""
from __future__ import annotations

from pipeline.deliver.answers import display as D
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


def _walk(nodes):
    for n in nodes:
        yield n
        yield from _walk(n.get("children") or [])


# A ---------------------------------------------------------------------------------------------
def test_a_a_plain_share_says_nothing_lifted():
    g, _o, _P = _g(F.STREAM)                    # a plain Region 2 stream: nothing lifted, nothing said
    share = next(t for t in _walk(g["tree"]) if t.get("tier") == "share")
    assert "don’t use" not in share["says"]


# G ---------------------------------------------------------------------------------------------
def test_g_write_each_one_on_your_licence_right_away():
    # (T866) the Vedder steelhead row's duties, word for word (ROUND20g 2): card_words CARDS "the tree kind ST: duties"
    x = {"type": "retention_limit", "species": ["ST"], "record_retention": True, "origin": "hatchery"}
    assert D.plain(x) == "Write each hatchery steelhead on your licence right away."
    assert D.title(x) == "Write each hatchery steelhead on your licence right away"
