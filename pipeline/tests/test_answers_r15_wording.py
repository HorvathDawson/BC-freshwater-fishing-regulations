"""ROUND 15 WORDING (owner review of v16, 2026-10-16) — no data, CI's no-data tier.

  1  A size cap is a BOXED GROUP like the count groups (`buckets[].sizes`): it names exactly the kinds it can
     bind that day — kept AND able to exceed the size (Bella Coola May 1: brook and brown trout, Dolly Varden
     and lake trout; never cutthroat ≤33, rainbow ≤50 or the closed steelhead). The gate proves it.
  2  The licence frame carries the under-16 summary per residency (`under_16`) and the statics label every
     residency and profile.
  3  A weekday window says its status and both day-parts (`week.status`, `week.parts` with `today`); a rule
     restating another of its row on the same days is one line, its words a note (`rows[].restates`): the
     Lower West Arm's r5 (the combined quota with the Upper West Arm).
  5  An own-rule bucket says its shared cap ("Up to 2 a day between them").
  6  A tributaries-only rule reached by the walk is "the Bella Coola River’s rule for its tributaries".
  7  No steelhead presence line where the whole water is closed.
  8  No "the" before a lake's name; each kept kind says "hatchery" when the card is hatchery-only; no worked
     answer is a bare 0.
"""
from __future__ import annotations

import re

from pipeline.deliver import calendar as K
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common
from pipeline.deliver.answers import licence as LI
from pipeline.deliver.answers import rows as RW
from pipeline.tests import answers28_fixture as F

OCT10, MAY1, MAY15, JUN15 = (10, 10), (5, 1), (5, 15), (6, 15)
SAT = K.Moment(K.weekday_mask(["Saturday", "Sunday"]))
WEEK = K.Moment(K.weekday_mask(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]))


def _group(water, md=OCT10):
    out, P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and "pool" in r["group"]
            and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, out, P


# 1 ---------------------------------------------------------------------------------------------
def test_1_a_size_cap_is_a_group_naming_the_kinds_it_can_bind():
    g, _o, _P = _group(F.BELLA_MAY_ST, MAY1)
    (b,) = [b for b in g["buckets"] if b["by"] == F.BELLA_R5]
    assert b["notes"] == []
    (sz,) = b["sizes"]
    assert (sz["n"], sz["over_cm"]) == (1, 50)
    assert sz["species"] == ["GB", "DV", "LT", "EB"]                       # book order, no CT/RB/ST
    assert sz["says"] == ("Brown trout, Dolly Varden/bull trout, lake trout or brook trout: "
                          "only 1 over 50 cm between them")
    assert "ST" not in {S for x in g["not_kept"] for S in x["species"]} or "ST" not in sz["species"]


def test_1_the_gate_proves_the_size_group_and_its_kinds():
    for w, md in ((F.BELLA_MAY_ST, MAY1), (F.STREAM, OCT10), (F.CLAKE, OCT10), (F.NESTED, OCT10),
                  (F.SLOT, OCT10), (F.WAHLEACH, OCT10), (F.VEDDER, OCT10)):
        out, P = F.card(w, md)
        assert CC.check_card(P, out) == [], w


def test_1_the_gate_refuses_a_kind_the_cap_cannot_bind():
    out, P = F.card(F.BELLA_MAY_ST, MAY1)
    g = [r["group"] for r in out["rows"] if r.get("group")][0]
    sz = [b for b in g["buckets"] if b["sizes"]][0]["sizes"][0]
    sz["species"] = sz["species"] + ["CT"]                                 # cutthroat never pass 33 cm
    assert any("cannot be kept over" in p for p in CC.check_card(P, out))


def test_1_nested_sizes_are_one_group():
    g, _o, _P = _group(F.NESTED)
    (sz,) = [s for b in g["buckets"] for s in b["sizes"]]
    assert sz["says"].endswith("only 2 over 30 cm between them, and only 1 over 50 cm")
    assert [(t["n"], t["over_cm"]) for t in sz["then"]] == [(1, 50)]


# 2 ---------------------------------------------------------------------------------------------
def _ans(none, docs=(), paths=None):
    reqs = [{"req": 98, "when": {"act": "fishing"}, "paths": paths}] if paths else []
    return {"documents": [{"doc": d, "when": {"act": "fishing"}, "base": True, "prices": p} for d, p in docs],
            "none_needed": none, "requirements": reqs}


def test_2_the_under_16_summary_per_residency():
    P = LI.profiles()
    ix = {(p["residency"], p["age"]): i for i, p in enumerate(P)
          if p["guidance"] == "non_guided" and p["status"] == "none"}
    basic = {"year": 62.87, "day": [22.86], "eight_days": [41.15]}
    youth = [{"accompanied_by": {"holding": "what_this_fishing_requires", "who": {"age": ["16_plus"]}},
              "quota": "counts_to_companion"},
             {"as": {"age": ["16_plus"], "residency": ["non_resident", "non_resident_alien"]}, "quota": "own"}]
    answers = [_ans(True)] * len(P)
    answers = list(answers)
    for res in ("non_resident", "non_resident_alien"):
        answers[ix[(res, "16_plus")]] = _ans(False, [("basic_licence", basic)])
        answers[ix[(res, "under_16")]] = _ans(False, paths=youth)
    got = LI.under_16_summary(answers, {"basic_licence": "basic angling licence"})
    assert [x["residency"] for x in got] == ["resident", "non_resident", "non_resident_alien"]
    assert got[0] == {"residency": "resident", "label": "B.C. resident", "none_needed": True,
                      "with_adult": None, "buy": [], "own_limit": False, "says": "No licence needed."}
    nr = got[1]
    assert nr["label"] == "Canadian, other province" and not nr["none_needed"]
    assert nr["with_adult"] == {"who": "16 or older", "counts": True}
    assert nr["buy"] == [{"doc": "basic_licence", "name": "Basic angling licence", "prices": basic}]
    assert nr["says"] == ("Fish with a licensed adult (16 or older): what you keep counts toward their limit. "
                          "Or buy your own basic angling licence ($62.87 a year · $22.86 a day · $41.15 for "
                          "8 days) and keep your own limit.")


def test_2_every_profile_and_residency_is_labelled():
    labels = LI.profile_labels()
    assert len(labels) == len(LI.profiles())
    assert labels[0] == "B.C. resident, 16 or older"
    assert LI.RESIDENCY_LABELS == {"resident": "B.C. resident", "non_resident": "Canadian, other province",
                                   "non_resident_alien": "From outside Canada"}


# 3 ---------------------------------------------------------------------------------------------
def _ko(frame):
    (ko,) = [r for r in frame["rows"] if r["all_members"] == ["KO"]]
    return ko


def test_3_a_restating_rule_is_one_line_and_a_note():
    sat, _P = F.card(F.LWA, (10, 17), at=SAT)
    ko = _ko(sat)
    assert not [x for x in ko["everyone"] if x["r"] == F.LWA_R5]
    assert ko["restates"] == [{"r": F.LWA_R5, "of": F.LWA_R3,
                               "says": "The combined daily quota for kokanee from the Upper West Arm (when open "
                                       "to kokanee harvest) and the Lower West Arm (when open to kokanee harvest) "
                                       "cannot exceed 5."}]
    roles = {k: (role, by) for k, role, by in sat["fish"]["KO"]["hatchery"]["roles"]}
    assert roles[F.LWA_R5] == ("agrees", F.LWA_R3)


def test_3_the_weekday_split_says_its_status_and_both_parts():
    sat, _P = F.card(F.LWA, (10, 17), at=SAT)
    mon, _P = F.card(F.LWA, (10, 19), at=WEEK)
    got = RW.say_weekdays([sat, mon], [SAT.as_json(), WEEK.as_json()])
    for frame, today in zip(got, ("Sat–Sun", "Mon–Fri")):
        w = _ko(frame)["week"]
        assert w["says"] == "Keep up to 5 on Saturdays and Sundays; release Monday to Friday"
        assert w["status"] == "Weekends: keep 5 · Weekdays: release"
        assert [(p["days"], p["when"], p["status"], p["n"], p["says"], p["rules"]) for p in w["parts"]] == [
            ("Sat–Sun", "Saturday and Sunday", "keep", 5, "keep up to 5", [F.LWA_R3]),
            ("Mon–Fri", "Monday to Friday", "release", None, "release every one", [F.LWA_R4])]
        assert [p["days"] for p in w["parts"] if p["today"]] == [today]
        # the combined-quota note on every moment's frame, the weekday one included
        assert [x["r"] for x in _ko(frame)["restates"]] == [F.LWA_R5]


# 5 ---------------------------------------------------------------------------------------------
def test_5_an_own_rule_bucket_says_its_shared_cap():
    g, _o, _P = _group(F.BELLA_MAY_ST, MAY1)
    (b,) = [b for b in g["buckets"] if b["by"] == F.BELLA_R5]
    assert b["between"] == "Up to 2 a day between them"
    g2, _o, _P = _group(F.TWO)
    (b2,) = [b for b in g2["buckets"] if b["why"] == "rule"]
    assert b2["between"] == "Up to 4 a day between them"


def test_5_one_kind_needs_no_shared_cap():
    g, _o, _P = _group(F.VEDDER)
    (b,) = [b for b in g["buckets"] if b["why"] == "rule"]
    assert b["between"] is None


# 6 ---------------------------------------------------------------------------------------------
def test_6_a_tributaries_only_rule_is_the_tributaries_rule():
    g, _o, _P = _group(F.BELLA_TRIB, OCT10)
    whose = {tuple(x["species"]): x["whose"] for x in g["not_kept"]}
    assert whose[("DV", "LT", "EB")] == "the Bella Coola River’s rule for its tributaries"
    assert whose[("ST",)] == "the Bella Coola River’s rule"


def test_6_the_tributaries_rule_holds_only_on_its_dates():
    g, _o, _P = _group(F.BELLA_TRIB, (7, 15))
    assert not [x for x in g["not_kept"] if "DV" in x["species"]]


# 7 ---------------------------------------------------------------------------------------------
def test_7_no_presence_line_where_the_whole_water_is_closed():
    out, _P = F.card(F.SHUT, JUN15)
    assert {r["kind"] for r in out["rows"]} == {"closed"}
    assert not [r for r in out["rows"] if r.get("presence")]
    out, _P = F.card(F.SHUT, OCT10)
    assert [r for r in out["rows"] if r.get("presence")] or any(
        k.get("presence") for r in out["rows"] for b in (r.get("group") or {}).get("buckets") or []
        for k in b["kinds"])


# 8 ---------------------------------------------------------------------------------------------
def test_8_no_the_before_a_lake():
    T = common.the_name
    assert T("Trout Lake") == "Trout Lake"
    assert T("Kootenay Lake — Main Body") == "Kootenay Lake — Main Body"
    assert T("Bella Coola River") == "the Bella Coola River"
    assert T("Kootenay Lake — Lower West Arm") == "Kootenay Lake — Lower West Arm"
    assert T("Ape Creek") == "Ape Creek"
    assert T("Powerhouse Channel") == "the Powerhouse Channel"


def test_8_each_kept_kind_says_hatchery_on_a_hatchery_only_card():
    g, _o, _P = _group(F.VEDDER_MAY, MAY15)
    assert g["origin"] == "Hatchery only — release wild trout and char."
    details = [k["detail"] for b in g["buckets"] for k in b["kinds"]]
    assert details and all(d.startswith("hatchery") for d in details), details


def test_8_no_worked_answer_is_a_bare_zero():
    for w, md in ((F.VEDDER_MAY, MAY15), (F.STREAM, OCT10), (F.VEDDER, OCT10)):
        g, _o, _P = _group(w, md)
        assert not [x["a"] for x in g["worked"] if re.search(r"\b0\b", x["a"])], w
