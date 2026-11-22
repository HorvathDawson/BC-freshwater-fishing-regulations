"""USER ROUND 2 (six-angler panel on v22, 2026-10-17) — small wording fixes, no data, CI's no-data tier.

  1  the headline's unit says whose number it is: "2 a day per angler from streams" (a lake "4 a day per angler")
  2  the worked lines in plain words: "If you keep 3 hatchery rainbow trout: 1 more fish of any kind, from here or from a
     lake." / "If you keep 2 hatchery rainbow trout: none from other streams today — up to 2 more of any kind, from here or from
     lakes."; the gate proves every number they state
  3  a kind held to fewer than its count tier says the count: "Only 1 of the 2 may be … Dolly Varden/bull trout …"
  4  a steelhead kind keeping hatchery fish only says "hatchery steelhead only — wild steelhead go back"; the presence
     line says what it means where a big rainbow is a steelhead (p.80: "a rainbow trout longer than 50 cm in waters
     where anadromous rainbow trout are found")
  5  the stop duty: "after you keep 2, stop fishing for the day"
(6 and 7 are the page's: check.mjs ur2-closed-until, ur2-bait-allowed-note.)

(T866) The cards' words for 1-4 (the Vedder's and the Lower West Arm's headline and header, the Vedder's worked lines, its
Dolly Varden and steelhead kinds and their presence line) are pinned once in test_answers_card_words.py CARDS. What stays
here is the gate, the presence words of the composer and the stop duty.
"""
from __future__ import annotations

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
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


def _kind(g, sp):
    return next(k for k in _walk(g["tree"]) if k["node"] == "kind" and sp in k["species"])


# 2 ---------------------------------------------------------------------------------------------
def test_2_the_gate_proves_the_numbers_a_worked_line_states():
    g, _o, P = _g(F.VEDDER)
    assert CC.check_group(P, g) == []
    w = next(w for w in g["worked"] if w["at"] == "here" and w["kept"] == 2)       # ROUND20b C4: the ends only
    w["a"] = "they don’t use up your 2 from streams here, but on other streams they count as them: none from other streams today — up to 3 more of any kind, from here or from lakes."
    assert any("worked line" in p and "[3.0]" in p for p in CC.check_group(P, g))


# 4 ---------------------------------------------------------------------------------------------
def test_4_the_presence_line_where_a_big_rainbow_is_not_a_steelhead():
    # where a big rainbow is not a steelhead (a lake whose own row prints steelhead), the line stays as it was
    assert RW.presence_words("known_with_rules", False) == "Steelhead are known to be in this water."
    assert RW.presence_words("possible_with_rules", True) == ("Steelhead rules apply here; steelhead may not be "
                                                             "present in this water.")


# 5 ---------------------------------------------------------------------------------------------
def test_5_after_you_keep_2_stop_fishing_for_the_day():
    g, _o, _P = _g(F.VEDDER)
    duties = _kind(g, "ST")["duties"]           # (T866) its words (ROUND20g 2): card_words CARDS "the tree kind ST: duties"
    assert not any("stop fishing for the day after" in d for d in duties)
