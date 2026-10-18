"""ROUND 16 (owner review of v17, 2026-10-16) items 2, 3, 4 and the owner's answers to r15_questions Q1/Q2 —
no data, CI's no-data tier.

  2  A water's own limit says WHOSE it is, its number and which fish kept elsewhere count toward it (Ape Creek:
     "This river’s own limit. Fish you kept elsewhere today count toward it too." said nothing).
  3  Every rule has a `title` stating its effect from its fields ("ALL STEELHEAD" -> "Release all steelhead").
  4  The under-16 answers say what the book says (p.6): a B.C. resident needs no licence or stamp, need not be
     accompanied and keeps their own quota.
  Q1 A farther ✱ row's exemption is applied where a nearer row is silent — and said (`caution`), naming both.
  Q2 Two rows printing the identical statement show it once, credited to the nearer row (`restates`).
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import licence as LI
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import nearer as NR
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)


# 2 ---------------------------------------------------------------------------------------------
def test_2_a_waters_own_limit_says_whose_its_number_and_what_counts():
    out, _P = F.card(F.BELLA_TRIB, OCT10)
    (row,) = [r for r in out["rows"] if r.get("elsewhere") and r["scope"]["of"] == "water"]
    assert row["elsewhere"]["says"] == ("The Bella Coola River’s rule: 1 a day here. Brown trout, cutthroat trout "
                                        "or rainbow trout you kept anywhere else today count toward that 1.")
    out, _P = F.card(F.WAHLEACH, OCT10)
    (row,) = [r for r in out["rows"] if r.get("elsewhere") and r["scope"]["of"] == "water"]
    assert row["elsewhere"]["says"] == ("This lake’s rule: 4 a day here. Trout and char other than steelhead you "
                                        "kept anywhere else today count toward those 4.")


# 3 ---------------------------------------------------------------------------------------------
def _rule(**f):
    return {"entry": "z5:trout_char_quota", "rule": "x.r1", "type": "retention_limit", "family": "retention",
            "dimension": "daily", "species_except": [], **f}


def test_3_every_rule_title_states_its_effect():
    st = _rule(species=["ST"], take=0, may_target=1, verbatim="ALL STEELHEAD", label="Steelhead — release all")
    lt = _rule(species=["LT"], take=0, may_target=1, verbatim="Lake trout, Oct 1-Nov 30",
               when={"dates": [{"from_month": 10, "from_day": 1, "to_month": 11, "to_day": 30}]})
    dv = _rule(species=["DV"], take=0, may_target=1, water="stream",
               verbatim="Bull trout (Dolly Varden) from streams, Aug 1-Oct 31",
               when={"dates": [{"from_month": 8, "from_day": 1, "to_month": 10, "to_day": 31}]})
    floor = _rule(species=["DV", "LT"], take=None, dimension="daily/size", lengths=[{"max_cm": 60, "take": 0}],
                  within="x.r0", verbatim="none under 60 cm")
    pool = _rule(species=["TROUT_CHAR"], take=2, water="stream", within="x.r0", verbatim="2 from streams")
    over = _rule(species=["TROUT_CHAR"], take=1, lengths=[{"min_cm": 50}], within="x.r0", verbatim="1 over 50 cm")
    assert [D.title(x) for x in (st, lt, dv, floor, pool, over)] == [
        "Release all steelhead", "Release lake trout, Oct 1–Nov 30",
        "Release Dolly Varden/bull trout from streams, Aug 1–Oct 31",
        "No Dolly Varden/bull trout or lake trout under 60 cm (24 in)", "At most 2 trout and char a day from streams",
        "At most 1 trout or char over 50 cm (20 in) a day"]     # ROUND19 2: sizes in cm (inches)
    # a rule with no generated form takes its label, dates with an en dash; the facts always carry a title
    gear = {"entry": "z1:x", "rule": "x.r2", "type": "bait_restriction", "family": "gear_and_method",
            "label": "Bait ban, Apr 1-Oct 31", "verbatim": "bait ban"}
    assert D.title(gear) == "Bait ban, Apr 1–Oct 31"
    assert D.rule_facts(st)["title"] == "Release all steelhead"


# 4 ---------------------------------------------------------------------------------------------
def _ans(none, docs=(), paths=None):
    reqs = [{"req": 98, "when": {"act": "fishing"}, "paths": paths}] if paths else []
    return {"documents": [{"doc": d, "when": {"act": "fishing"}, "base": True, "prices": p} for d, p in docs],
            "none_needed": none, "requirements": reqs}


def test_4_the_under_16_answers_say_what_the_book_says():
    P = LI.profiles()
    ix = {(p["residency"], p["age"]): i for i, p in enumerate(P)
          if p["guidance"] == "non_guided" and p["status"] == "none"}
    basic = {"year": 62.87, "day": [22.86], "eight_days": [41.15]}
    youth = [{"accompanied_by": {"holding": "what_this_fishing_requires", "who": {"age": ["16_plus"]}},
              "quota": "counts_to_companion"},
             {"as": {"age": ["16_plus"], "residency": ["non_resident", "non_resident_alien"]}, "quota": "own"}]
    answers = list([_ans(True)] * len(P))
    for res in ("non_resident", "non_resident_alien"):
        answers[ix[(res, "16_plus")]] = _ans(False, [("basic_licence", basic)])
        answers[ix[(res, "under_16")]] = _ans(False, paths=youth)
    got = LI.under_16_summary(answers, {"basic_licence": "basic angling licence"})
    # p.6: "You may sport fish without any licence or stamp … You do not need to be accompanied by a licence
    # holder. You are entitled to your own quota of fish."
    assert got[0]["own_limit"] is True and got[0]["with_adult"] is None
    assert got[0]["says"] == ("No licence or stamp needed, and you don’t have to fish with a licence holder. "
                              "You keep your own limit.")
    # p.6: "… accompanied by a person 16 years or older who holds the appropriate licences and stamps … counted
    # as part of the catch … of your accompanying licence holder … buy a licence and any necessary
    # supplementary licences and stamps."
    assert got[1]["says"] == ("Fish with someone 16 or older who holds the licences and stamps this fishing needs: "
                              "what you keep counts toward their limit. Or buy your own basic angling licence "
                              "($62.87 a year · $22.86 a day · $41.15 for 8 days) and any supplementary licences "
                              "and stamps you need, and keep your own limit.")


# Q1 --------------------------------------------------------------------------------------------
_DUNCAN, _TROUT, _WILKIE = "r4:duncan_river@4-19", "r4:trout_lake_s_tributaries@4-30", "r4:wilkie_creek@4-30"
_ZONE = ("z4:spring_stream_closure", "spring_stream_closure.r1")


def _rules():
    def w(e, r, name, **f):
        return (e, r), {"entry": e, "rule": r, "entry_name": name, "extents": [{"op": "whole"}],
                        "type": "retention_limit", **f}
    return dict([
        w(_DUNCAN, "duncan_river.r1", "Duncan River", exempts=[{"entry_id": _ZONE[0], "rule_id": _ZONE[1]}]),
        w(_TROUT, "trout_lake.r1", "Trout Lake’s tributaries", species=["DV"], take=0),
        w(_WILKIE, "wilkie_creek.r1", "Wilkie Creek", species=["RB"], take=1),
        w("r4:lardeau_river@4-30", "lardeau_river.r1", "Lardeau River",
          exempts=[{"entry_id": _ZONE[0], "rule_id": _ZONE[1]}])])


def test_q1_a_farther_exemption_under_a_silent_nearer_row_is_said():
    rules = _rules()
    lifter = f"{_DUNCAN}::duncan_river.r1"
    pairs = {(_TROUT, _DUNCAN), ("r4:lardeau_river@4-30", _DUNCAN)}
    glenn = [(_DUNCAN, "duncan_river.r1", "trib"), (_TROUT, "trout_lake.r1", "trib")]
    assert NR.silent_nearer(glenn, rules, pairs, lifter) == [(_TROUT, "nearer_row")]
    wilkie = glenn + [(_WILKIE, "wilkie_creek.r1", "reach")]
    assert NR.silent_nearer(wilkie, rules, pairs, lifter) == [(_TROUT, "nearer_row"), (_WILKIE, "own_row")]
    # a nearer row printing the exemption itself (the Lardeau's tributaries): nothing to say
    lardeau = [(_DUNCAN, "duncan_river.r1", "trib"), ("r4:lardeau_river@4-30", "lardeau_river.r1", "trib")]
    assert NR.silent_nearer(lardeau, rules, pairs, lifter) == []
    # in words, naming both rows
    path = "fixture:r16_q1"
    read._NEARER_BY_PATH[path] = frozenset(pairs)
    B = SimpleNamespace(path=path, rules=rules)
    assert D.inherited_caution(B, glenn, lifter) == (
        "Trout Lake’s tributaries’ row, nearer this stream, prints no such exemption; the Duncan River’s is "
        "applied here because its row covers all its tributaries.")
    assert D.inherited_caution(B, lardeau, lifter) is None


# Q2 --------------------------------------------------------------------------------------------
def test_q2_an_identical_statement_of_a_farther_row_is_shown_once():
    row = {"entry": "r7:williston_lake_in_zone_b@7-31+7-36", "rule": "williston_lake_zone_b.r3",
           "entry_name": "Williston Lake — Zone B", "type": "retention_limit", "family": "retention",
           "dimension": "daily", "species": ["GR"], "species_except": [], "take": 0, "may_target": 1,
           "extents": [{"op": "whole"}], "verbatim": "Arctic grayling catch and release"}
    zone = {**row, "entry": "z7b:arctic_grayling", "rule": "arctic_grayling.r5", "entry_name": "Zone B",
            "verbatim": "all from Williston Lake and its tributaries"}
    for x in (row, zone):
        x["_rank"] = read.source_of(x).rank
    W, Z = RW.R(f"{row['entry']}::{row['rule']}", row, "trib"), RW.R(f"{zone['entry']}::{zone['rule']}", zone, "trib")
    P = SimpleNamespace(by_key={W.key: W, Z.key: Z}, B=SimpleNamespace())
    v = {"win": W, "roles": {W.key: {"role": "governs"}, Z.key: {"role": "agrees"}}}
    model = {"R": {"GR": {"H": v, "W": v}}}
    got = RW.row_restates(P, model, {"allMembers": ["GR"]})
    assert got == [{"r": Z.key, "of": W.key,
                    "says": "Zone B also prints this: “All from Williston Lake and its tributaries”."}]
    # not identical (another number): no note
    Z.f = {**zone, "take": 1}
    assert RW.row_restates(P, model, {"allMembers": ["GR"]}) == []
