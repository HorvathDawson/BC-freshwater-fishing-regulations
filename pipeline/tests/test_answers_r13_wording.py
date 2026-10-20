"""ROUND 13 WORDING (owner review of v14, 2026-10-16) — no data, CI's no-data tier.

  Z  a kind keeping NONE (every band of the species 0) says no size: the Bella Coola's cutthroat on Oct 10
     ("none under 25 cm and all cutthroat trout catch and release") is "catch and release", never
     "25 cm or longer"; a kind keeping some keeps its size words.
  S  a species the card's row holds but nobody may keep is SAID on the card (`group.not_kept`), whose rule
     with it (the page cites its Book page): "Steelhead: no fishing for them, not even catch and release — release any you hook by accident" (Bella Coola r13, undated;
     the Fraser above Mission's r5, Aug 15-Dec 31 — not on Jul 15). The gate proves none may be kept.
  G  `gear.summary.says` names no hook when there is no hook restriction ("any hook" is gone); "a single
     barbless hook" stays. P: the weight is "weight on the line".
  D  a water's own-rule bucket says what it replaces (`buckets[].replaces`), generated from the lifts: each
     lifted cap of the region named — the Vedder's "Replaces Region 2’s 2-from-streams limit and 30 cm
     minimum size for these fish".
  P  "Kept 1 at a lake?" says how many are left here (the gate proves `here`); the book is named by its
     title where a line cites it.
  A  a water row printing "(any size)" lifts EVERY size clause of its region for its fish (derived in the
     bundle, `basis: any_size`): Robb Lake's "Bull trout daily quota = 1 (any size)" lifts Zone 7A's "no
     more than 1 over 50 cm" for bull trout.

Z/S/D/P run on `answers28_fixture` (the real reader and rows); G on `gear.resolve`; A on `rules._exempts`.
"""
from __future__ import annotations

import copy
import json

from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import display as D
from pipeline.deliver.bundle import rules as RM
from pipeline.regs.parsing import catalogue as C
from pipeline.tests import answers28_fixture as F
from pipeline.tests import test_answers_r11_wording as R11

OCT10, JUL15 = (10, 10), (7, 15)
KNOWN = {"line": "known_with_rules", "says": "A rainbow trout over 50 cm (20 in) here counts as a steelhead."}


def _group(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "RB" in r["group"]["kinds"]]
    return g, out, P


def _kinds(g):
    return {k["name"]: k for b in g["buckets"] for k in b["kinds"]}


# Z ---------------------------------------------------------------------------------------------
def test_z_a_kind_keeping_none_says_no_size():
    g, _o, _P = _group(F.BELLA)
    ct = _kinds(g)["Cutthroat trout"]
    assert (ct["n"], ct["detail"]) == (0, "catch and release")


def test_z_a_kind_keeping_some_keeps_its_size_words():
    g, _o, _P = _group(F.BELLA)
    assert _kinds(g)["Rainbow trout"]["detail"] == "hatchery, 30–50 cm"      # ROUND15 8: its origin too
    g, _o, _P = _group(F.SLOT)
    assert _kinds(g)["Rainbow trout"]["detail"] == "60 cm or less, or 90 cm or longer"


# S ---------------------------------------------------------------------------------------------
def test_s_an_undated_species_closure_is_said_on_the_card():
    g, _o, _P = _group(F.BELLA)
    assert g["not_kept"] == [{"species": ["ST"], "status": "closed", "says": "Steelhead: no fishing for them, not even catch and release — release any you hook by accident",
                              "by": F.BELLA_ST, "rules": [F.BELLA_ST], "whose": "this river’s rule",
                              "presence": KNOWN}]                                   # ROUND14 4/6


def test_s_a_dated_species_closure_is_said_on_its_dates_only():
    g, _o, _P = _group(F.FRASER)
    assert g["not_kept"] == [{"species": ["ST"], "status": "closed", "says": "Steelhead: no fishing for them, not even catch and release — release any you hook by accident",
                              "by": F.FRASER_ST, "rules": [F.FRASER_ST], "whose": "this river’s rule",
                              "presence": KNOWN}]                                   # ROUND14 4/6
    g, _o, _P = _group(F.FRASER, JUL15)
    assert g["not_kept"] == [] and "ST" in g["kinds"]


def test_s_the_gate_proves_none_may_be_kept():
    """MUTATION: rainbow trout added to the steelhead closure line."""
    for w in (F.BELLA, F.FRASER):
        out, P = F.card(w, OCT10)
        assert CC.check_card(P, out) == [], w
    out, P = F.card(F.BELLA, OCT10)
    bad = copy.deepcopy(out)
    g = next(r["group"] for r in bad["rows"] if r.get("group") and "RB" in r["group"]["kinds"])
    g["not_kept"][0]["species"].append("RB")
    assert any("not_kept" in p for p in CC.check_card(P, bad))


# G / P weight ----------------------------------------------------------------------------------
def test_g_no_hook_clause_without_a_hook_restriction():
    vedder = R11._rules("stream", hook=R11.Z2_HOOK)["summary"]["says"]
    okanagan = R11._rules("lake")["summary"]["says"]
    kootenay = R11._rules("lake", kootenay=True)["summary"]["says"]
    cowichan = R11._rules("stream", hook=R11.Z1_HOOK, fly=R11.COWICHAN_FLY)["summary"]["says"]
    assert vedder == "1 line with a single barbless hook, no trebles (one hook, lure or fly). Weight on the line up to 1 kg (2.2 lb)."
    assert okanagan == "1 line (2 while alone in a boat) with one hook, lure or fly. Weight on the line up to 1 kg (2.2 lb)."
    assert kootenay == ("1 line from shore; any number from a boat, each with one hook, lure or fly. "
                        "Weight on the line up to 1 kg (2.2 lb).")              # UR1 G
    assert cowichan == ("Fly fishing only: 1 line with a single barbless hook, no trebles (one fly). No float or strike indicator, no sinker or "
                        "split shot, no lure, spinner or other attracting device.")
    assert not any("any hook" in s for s in (vedder, okanagan, kootenay, cowichan))


# D ---------------------------------------------------------------------------------------------
def test_d_the_own_rule_bucket_says_what_it_replaces():
    g, _o, _P = _group(F.VEDDER)
    own, share = g["buckets"]
    assert own["title"] == "This river’s rule"
    assert own["replaces"] == {
        "says": "Replaces Region 2’s 2-from-streams limit and 30 cm minimum size for hatchery rainbow trout",
        "rules": [F.SHARE, "z2:trout_char_quota::trout_char_quota.r8"]}
    assert share["replaces"] is None


def test_d_each_lifted_cap_for_the_fish_it_is_lifted_for():
    """The Vedder with a second lifting rule (hatchery cutthroat 3, lifting the share only)."""
    g, _o, _P = _group(F.TWO)
    own = g["buckets"][0]
    # ROUND19 6: the fish by name, with their origin, never "these fish"
    assert own["replaces"]["says"] == ("Replaces Region 2’s 2-from-streams limit for hatchery cutthroat trout and "
                                       "rainbow trout, and its 30 cm minimum size for hatchery rainbow trout")


# P ---------------------------------------------------------------------------------------------
def test_p_the_lake_line_says_how_many_are_left():
    g, _o, _P = _group(F.STREAM)
    lake = next(w for w in g["worked"] if w["at"] == "lake")
    assert (lake["q"], lake["a"], lake["kept"], lake["here"]) == (
        "Kept 1 at a lake?",
        "Keep 2 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams.", 1, 2)
    g, _o, _P = _group(F.VEDDER)
    lake = next(w for w in g["worked"] if w["at"] == "lake")
    assert lake["a"].startswith("Keep 3 more here. ") and lake["here"] == 3


def test_p_the_gate_fails_a_wrong_lake_count():
    out, P = F.card(F.STREAM, OCT10)
    bad = copy.deepcopy(out)
    g = next(r["group"] for r in bad["rows"] if r.get("group") and "RB" in r["group"]["kinds"])
    next(w for w in g["worked"] if w["at"] == "lake")["here"] = 3
    assert any("worked" in p for p in CC.check_card(P, bad))


def test_p_the_book_is_named_by_its_title():
    x = {"id": "r1:cowichan_river@1-4::cowichan_river.r5", "entry_id": "r1:cowichan_river@1-4",
         "provenance": {"rank": 0, "entry_name": "Cowichan River"}}
    doc = {"entries": {"r1:cowichan_river@1-4": {"name": "Cowichan River", "kind": "water"}}, "waters": {}}
    assert D.applies_here(x, doc)[0] == ("This water’s own row in the 2025–2027 BC Freshwater Fishing "
                                         "Regulations Synopsis: Cowichan River.")


# A ---------------------------------------------------------------------------------------------
def _zone_7a():
    V = C.CatalogueRule.model_validate
    region = [{"op": "within", "area_kind": "region"}]
    return {"z7a:q": {
        "q.r1": V({"rule_id": "q.r1", "type": "retention_limit", "verbatim": "Trout/char: 5",
                   "species": ["TROUT_CHAR"], "take": 5, "extents": region}),
        "q.r2": V({"rule_id": "q.r2", "type": "retention_limit", "verbatim": "Trout/char: only 1 over 50 cm",
                   "species": ["TROUT_CHAR"], "take": 1, "within": "q.r1", "lengths": [{"min_cm": 50}],
                   "extents": region}),
        "q.r9": V({"rule_id": "q.r9", "type": "retention_limit", "verbatim": "Lake trout: none under 30 cm",
                   "species": ["LT"], "lengths": [{"max_cm": 30, "take": 0}], "extents": region}),
        "q.r10": V({"rule_id": "q.r10", "type": "retention_limit", "verbatim": "Bull trout: none under 40 cm",
                    "species": ["DV"], "lengths": [{"max_cm": 40, "take": 0}], "extents": region}),
        "q.r5": V({"rule_id": "q.r5", "type": "retention_limit", "verbatim": "Bull trout: 1, 30-50 cm only",
                   "species": ["DV"], "take": 1, "lengths": [{"min_cm": 30, "max_cm": 50}],
                   "extents": region})}}


def _robb(verbatim):
    r = C.CatalogueRule.model_validate({"rule_id": "robb_lake.r1", "type": "retention_limit",
                                        "verbatim": verbatim, "species": ["DV"], "take": 1,
                                        "extents": [{"op": "whole"}]})
    got = RM._exempts("r7:robb_lake", r, {}, _zone_7a())
    return [] if got is None else json.loads(got)


def test_a_any_size_lifts_every_size_clause_of_its_region_for_its_fish():
    got = _robb("Bull trout daily quota = 1 (any size)")
    assert [(x["entry_id"], x["rule_id"], x.get("species"), x["basis"]) for x in got] == [
        ("z7a:q", "q.r10", None, RM.ANY_SIZE_BASIS), ("z7a:q", "q.r2", ["DV"], RM.ANY_SIZE_BASIS)]


def test_a_without_any_size_nothing_is_derived():
    assert _robb("Bull trout daily quota = 1") == []
