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
"""
from __future__ import annotations

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as D
from pipeline.tests import answers28_fixture as F
from pipeline.tests import test_answers_r11_wording as R11

OCT10, MAY1 = (10, 10), (5, 1)


def _g(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


def _walk(nodes):
    for n in nodes:
        yield n
        yield from _walk(n.get("children") or [])


# A ---------------------------------------------------------------------------------------------
def test_a_the_total_and_header_say_per_angler():
    g, _o, _P = _g(F.VEDDER)
    (total,) = g["tree"]
    assert total["says"] == "4 a day per angler across Region 2, lakes included"
    assert g["header"]["says"] == "4 a day per angler across Region 2, lakes included"        # ROUND20 1: the root
    assert g["total_line"] == "No more than 4 trout and char a day per angler across Region 2, lakes included."


def test_a_the_share_says_the_lifted_fish_do_not_use_it():
    g, _o, _P = _g(F.VEDDER)
    share, _own = g["tree"][0]["children"]                       # ROUND20b C3: the share first
    assert share["says"] == "Only 2 of the 4 may be from streams, not counting hatchery rainbow trout"   # ROUND20 2
    g, _o, _P = _g(F.STREAM)                    # a plain Region 2 stream: nothing lifted, nothing said
    share = next(t for t in _walk(g["tree"]) if t.get("tier") == "share")
    assert "don’t use" not in share["says"]


# B ---------------------------------------------------------------------------------------------
def test_b_a_size_tier_says_the_fish_in_its_box():
    g, _o, _P = _g(F.VEDDER)
    (size,) = g["tree"][0]["children"][0]["children"]
    assert size["says"] == "Only 1 of the fish in this box may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"
    g, _o, _P = _g(F.BELLA_MAY_ST, MAY1)
    (own,) = g["tree"][0]["children"]
    assert own["children"][0]["says"] == "Only 1 of the fish in this box may be over 50 cm (20 in)"


# D ---------------------------------------------------------------------------------------------
def test_d_a_count_tier_says_how_many_in_all():
    g, _o, _P = _g(F.BELLA_MAY_ST, MAY1)
    (own,) = g["tree"][0]["children"]
    count = own["children"][1]
    assert count["says"] == "1 cutthroat or rainbow trout in all (part of the 2)"           # UR3 3
    assert count["children"][0]["sentence"] == ("Of the 1 cutthroat or rainbow trout in all: up to 1 may be hatchery "
                                                "cutthroat trout, 30–33 cm (12–13 in)")


# F ---------------------------------------------------------------------------------------------
def test_f_a_species_closure_says_not_even_to_release():
    g, _o, _P = _g(F.BELLA)
    (nk,) = g["not_kept"]
    assert nk["says"] == "Steelhead: no fishing for them, not even catch and release — release any you hook by accident"


# G ---------------------------------------------------------------------------------------------
def test_g_write_each_one_on_your_licence_right_away():
    g, _o, _P = _g(F.VEDDER)
    st = next(k for k in _walk(g["tree"]) if k["node"] == "kind" and k["species"] == ["ST"])
    assert "write each one on your licence right away" in st["duties"]
    x = {"type": "retention_limit", "species": ["ST"], "record_retention": True, "origin": "hatchery"}
    assert D.plain(x) == "Write each hatchery steelhead on your licence right away."
    assert D.title(x) == "Write each hatchery steelhead on your licence right away"


def test_g_weight_in_kg_and_lb_and_boat_lines_from_shore():
    vedder = R11._rules("stream", hook=R11.Z2_HOOK)["summary"]["says"]
    assert vedder == ("1 line with a single barbless hook (one hook, lure or fly). Weight on the line up to 1 kg "
                      "(2.2 lb).")
    kootenay = R11._rules("lake", kootenay=True)
    assert kootenay["summary"]["says"] == ("1 line from shore; any number from a boat, each with one hook, lure or "
                                           "fly. Weight on the line up to 1 kg (2.2 lb).")
    assert kootenay["counts"]["lines_per_angler"]["says"] == "1 line (rod) from shore; no limit from a boat"
    okanagan = R11._rules("lake")                # alone in a boat is not "from a boat": unchanged
    assert okanagan["counts"]["lines_per_angler"]["says"] == "1 line (rod); 2 while alone in a boat"


# the gate ----------------------------------------------------------------------------------------
def test_the_gate_still_proves_every_card():
    for w, md in ((F.VEDDER, OCT10), (F.STREAM, OCT10), (F.BELLA_MAY_ST, MAY1), (F.BELLA, OCT10), (F.LWA, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w
