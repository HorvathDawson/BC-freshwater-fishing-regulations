"""USER ROUND 2 (six-angler panel on v22, 2026-10-17) — small wording fixes, no data, CI's no-data tier.

  1  the headline's unit says whose number it is: "2 a day per angler from this stream" (a lake "4 a day per angler")
  2  the worked lines in plain words: "If you keep 3 hatchery rainbow trout: 1 more fish of any kind, from here or from a
     lake." / "If you keep 2 hatchery rainbow trout: none from other streams today — up to 2 more from here or from
     lakes."; the gate proves every number they state
  3  a kind held to fewer than its count tier says the count: "Only 1 of the 2 may be … Dolly Varden/bull trout …"
  4  a steelhead kind keeping hatchery fish only says "hatchery steelhead only — wild steelhead go back"; the presence
     line says what it means where a big rainbow is a steelhead (p.80: "a rainbow trout longer than 50 cm in waters
     where anadromous rainbow trout are found")
  5  the stop duty: "after you keep 2, stop fishing for the day"
(6 and 7 are the page's: check.mjs ur2-closed-until, ur2-bait-allowed-note.)
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


# 1 ---------------------------------------------------------------------------------------------
def test_1_the_headline_says_per_angler():
    g, _o, _P = _g(F.VEDDER)
    assert g["headline"]["says"] == "2 a day per angler from this stream"
    assert g["header"]["says"] == "4 a day per angler across Region 2, lakes included"        # ROUND20 1: the root
    g, _o, _P = _g(F.LWA)
    assert g["headline"]["says"] == "4 a day per angler"


# 2 ---------------------------------------------------------------------------------------------
def test_2_the_worked_lines_are_plain():
    g, _o, _P = _g(F.VEDDER)
    here = [(w["q"], w["a"]) for w in g["worked"] if w["at"] == "here"]
    assert here == [
        ("If you keep 4 hatchery rainbow trout", "that’s your trout and char for the day, everywhere."),
        ("If you keep 2 hatchery rainbow trout", "none from other streams today — up to 2 more from here or from lakes."),
    ]


def test_2_the_gate_proves_the_numbers_a_worked_line_states():
    g, _o, P = _g(F.VEDDER)
    assert CC.check_group(P, g) == []
    w = next(w for w in g["worked"] if w["at"] == "here" and w["kept"] == 2)       # ROUND20b C4: the ends only
    w["a"] = "none from other streams today — up to 3 more from here or from lakes."
    assert any("worked line" in p and "[3.0]" in p for p in CC.check_group(P, g))


# 3 ---------------------------------------------------------------------------------------------
def test_3_a_kind_held_to_fewer_says_of_how_many():
    g, _o, _P = _g(F.VEDDER)
    dv = _kind(g, "DV")
    assert dv["says"] == "Only 1 of the 2 may be hatchery Dolly Varden/bull trout or lake trout"
    assert dv["sentence"].startswith("Of the 2 a day: only 1 may be hatchery Dolly Varden/bull trout or lake trout")
    g, _o, _P = _g(F.LWA)
    assert _kind(g, "DV")["says"] == "Only 1 of the 4 may be Dolly Varden/bull trout or lake trout"


# 4 ---------------------------------------------------------------------------------------------
def test_4_hatchery_steelhead_only_and_wild_go_back():
    g, _o, _P = _g(F.VEDDER)
    assert _kind(g, "ST")["says"] == "Up to 2 may be hatchery steelhead only — wild steelhead go back"


def test_4_the_presence_line_says_a_big_rainbow_is_a_steelhead_where_it_is():
    g, _o, _P = _g(F.VEDDER)
    assert _kind(g, "ST")["presence"]["says"] == ("Steelhead are known to be in this water, so a rainbow trout over "
                                                  "50 cm (20 in) counts as a steelhead.")
    # where a big rainbow is not a steelhead (a lake whose own row prints steelhead), the line stays as it was
    assert RW.presence_words("known_with_rules", False) == "Steelhead are known to be in this water."
    assert RW.presence_words("possible_with_rules", True) == ("Steelhead rules apply here; steelhead may not be "
                                                             "present in this water.")


# 5 ---------------------------------------------------------------------------------------------
def test_5_after_you_keep_2_stop_fishing_for_the_day():
    g, _o, _P = _g(F.VEDDER)
    duties = _kind(g, "ST")["duties"]
    assert "after you keep 2, stop fishing for the day" in duties
    assert not any("stop fishing for the day after" in d for d in duties)


# the gate ----------------------------------------------------------------------------------------
def test_the_gate_still_proves_every_card():
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.BELLA_MAY_ST, (5, 1)), (F.BELLA, OCT10), (F.LWA, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w
