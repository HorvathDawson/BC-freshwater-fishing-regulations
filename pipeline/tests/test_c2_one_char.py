"""RULINGS C2 AS AMENDED BY THE OWNER, 2026-11-18: ONE CHAR'S OWN LINE TAKES ONLY THAT CHAR OUT OF "TROUT".

THE TEXT (synopsis 2025-2027, printed p.22): CHILLIWACK LAKE "No wild trout over 50 cm, 1 bull trout over 60 cm".
p.80: "all regulations that apply to trout (as a group) also apply to char unless char are specifically excluded".
The owner, on that line: "only bull trout is out it needs to change the char group as a whole to get the char to change
compeltely out of the group". So the size release is `TROUT_CHAR` less `DV` (it was less `CHAR`): a wild brook trout or
lake trout over 50 cm goes back; bull trout follow their own "1 over 60 cm". All char leave a trout line only when the
char GROUP has the related line ("char", or every char named) — Region 1's "All char" release, Alta Lake's "1 char".

The whole answer stands on the answers 2.8 fixture's plain Region 2 lake (p.21: 4 a day, 1 over 50 cm, 1 Dolly Varden/
lake trout, none of those under 60 cm) plus Chilliwack Lake's two rules as the bundle stores them — r1's `species_except`
read from the CURATED row, so the card follows the corpus — through the REAL reader and the REAL rows (no data).
"""
from __future__ import annotations

import json

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.tree import walk
from pipeline.deliver.bundle import read
from pipeline.regs.parsing import catalogue as C
from pipeline.tests import answers28_fixture as F
from pipeline.tests import under_test

E = "r2:chilliwack_lake@2-4"
R1, R2 = f"{E}::chilliwack_lake.r1", F.CLAKE_DV
Z2 = "z2:daily_quotas::trout_char_quota"
JUL15 = (7, 15)


def _corpus(region: str) -> dict:
    p = under_test.curated().regulations.synopsis / f"region-{region}.json"
    return {e.entry_id: e for e in C.CatalogueFile.model_validate(json.loads(p.read_text())).entries}


def _chilliwack():
    return _corpus("2")[E]


def _r1(species_except) -> dict:
    """chilliwack_lake.r1 as the bundle stores it (bundle of 2026-11-16), with the exclusion given."""
    return {"entry_id": E, "rule_id": "chilliwack_lake.r1", "type": "retention_limit", "family": "retention",
            "dimension": "daily/size@origin=wild", "label": "Trout (none over 50 cm), wild only",
            "parts": json.dumps({"what": "Trout", "size": "none over 50 cm", "conditions": "wild only"}),
            "scope": "section", "species": ["TROUT_CHAR"], "species_except": list(species_except), "take": None,
            "may_target": 1, "uncertain": 0, "unresolved": None, "verbatim": "No wild trout over 50 cm",
            "extent_text": None, "undrawn_part": None, "extents": [{"op": "whole"}],
            "lengths": [{"min_cm": 50, "take": 0}], "origin": "wild", "period": "daily",
            "rule": "chilliwack_lake.r1", "entry": E, "entry_name": "Chilliwack Lake"}


def _card(species_except):
    path = "fixture:c2_one_char:" + ",".join(species_except)
    got = read._RULES_BY_PATH.get(path)
    if got is None:
        got = {(x["entry"], x["rule"]): x for x in json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"]}
        got[tuple(R2.split("::"))] = dict(F._CHILLIWACK_LAKE_DV)
        got[tuple(R1.split("::"))] = _r1(species_except)
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[path] = got
    lift = tuple(F.LIFT.split("::"))
    bound = sorted((e, r, "reach") for (e, r), x in got.items() if (e, r) != lift and x.get("water") != "stream")
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, JUL15, f, path, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    P = RW.Part(F._B(rules=got, sets={5: bound}), 5, False, True, "lake", None, lad, JUL15, {})
    return RW.produce(P), P


def _lines(v: dict) -> list:
    return [{k: l.get(k) for k in ("t", "r", "a", "b", "take")} for l in v["lines"]]


def _kinds(out: dict) -> dict:
    return {tuple(n["species"]): n["sentence"] for r in out["rows"] if r.get("group")
            for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind"}


@pytest.fixture(scope="module")
def card():
    r1 = next(r for r in _chilliwack().rules if r.rule_id == "chilliwack_lake.r1")
    return _card(r1.species_except)


def test_the_curated_chilliwack_line_leaves_out_bull_trout_alone():
    e = _chilliwack()
    r1 = next(r for r in e.rules if r.rule_id == "chilliwack_lake.r1")
    assert r1.verbatim == "No wild trout over 50 cm" and r1.species_except == ["DV"]
    assert C.trout_char_excluded(r1, e.rules) == ["DV"]


def test_a_wild_brook_trout_over_50_goes_back_a_hatchery_one_is_the_1_over_50(card):
    out, _P = card
    wild, hatch = out["fish"]["EB"]["wild"], out["fish"]["EB"]["hatchery"]
    assert wild["status"] == hatch["status"] == "keep"                       # under 50 cm both are kept
    assert {"t": "rel", "r": R1, "a": 50, "b": None, "take": None} in _lines(wild)
    assert {"t": "cap", "r": f"{Z2}.r2", "a": 50, "b": None, "take": 1} in _lines(hatch)
    assert R1 not in [r for r, *_ in hatch["roles"]]
    assert _kinds(out)[("EB", "GB", "CT", "RB")] == (
        "Up to 4 brook, brown, cutthroat or rainbow trout combined, any size — no wild brook, brown, cutthroat or "
        "rainbow trout over 50 cm (20 in)")


def test_a_wild_lake_trout_goes_back_a_hatchery_one_60_or_over_is_the_1(card):
    """Region 2: none under 60 cm; the lake: no wild one over 50 cm — no wild lake trout may be kept."""
    out, _P = card
    wild, hatch = out["fish"]["LT"]["wild"], out["fish"]["LT"]["hatchery"]
    assert wild["status"] == "release" and wild["win"] == R1
    assert hatch["status"] == "keep"
    got = _lines(hatch)
    assert {"t": "subcap", "r": f"{Z2}.r5", "a": None, "b": None, "take": None} in got
    assert {"t": "rel", "r": f"{Z2}.r5b", "a": 0, "b": 60, "take": None} in got
    assert _kinds(out)[("LT",)] == ("Up to 1 hatchery lake trout, 60 cm (24 in) or longer — no more than 1 counting "
                                   "any Dolly Varden/bull trout you keep")


def test_bull_trout_follow_their_own_line_as_before(card):
    out, _P = card
    for o in ("wild", "hatchery"):
        v = out["fish"]["DV"][o]
        assert (v["status"], v["win"], v["daily"]) == ("keep", R2, 1), o
        assert {"t": "rel", "r": R2, "a": 0, "b": 60, "take": None} in _lines(v)
        assert R1 not in [r for r, *_ in v["roles"]]
    assert _kinds(out)[("DV",)] == ("Up to 1 Dolly Varden/bull trout, 60 cm (24 in) or longer — no more than 1 "
                                   "counting any lake trout you keep")


def test_the_card_gate_passes(card):
    out, P = card
    groups = [r["group"] for r in out["rows"] if r.get("group")]
    assert groups and all(CC.check_tree(P, g) == [] for g in groups)


def test_mutation_the_old_reading_keeps_a_wild_brook_trout_and_lake_trout_over_50():
    """`species_except: [CHAR]` (C2 before the amendment): the lake's line held no char at all."""
    out, _P = _card(["CHAR"])
    assert R1 not in [l["r"] for l in out["fish"]["EB"]["wild"]["lines"]]
    assert out["fish"]["LT"]["wild"]["status"] == "keep"
    assert _kinds(out)[("EB", "GB", "CT", "RB")].endswith("no wild brown, cutthroat or rainbow trout over 50 cm (20 in)")


# --------------------------------------------------------------------------------------------- the validator
def _with(entry, rule_id: str, species_except) -> list:
    return [C.CatalogueRule.model_validate({**json.loads(r.model_dump_json(by_alias=True, exclude_none=True)),
                                           "species_except": list(species_except)})
            if r.rule_id == rule_id else r for r in entry.rules]


@pytest.mark.parametrize("bad", [["CHAR"], [], ["DV", "LT"], ["LT"]])
def test_the_validator_refuses_chilliwack_excluding_any_other_char(bad):
    """MUTATION both ways: too wide (all char, two char, the wrong char) and too narrow (none)."""
    e = _chilliwack()
    assert C.trout_scope_problems(e.entry_id, e.regs_verbatim, e.rules) == []
    got = C.trout_scope_problems(e.entry_id, e.regs_verbatim, _with(e, "chilliwack_lake.r1", bad))
    assert len(got) == 1 and "species_except ['DV']" in got[0] and got[0].startswith("chilliwack_lake.r1:")


@pytest.mark.parametrize("region, entry_id, rule_id", [
    ("1", "z1:daily_quotas", "trout_quota.r4"),        # "All char" (includes Dolly Varden)
    ("2", "r2:alta_lake@2-9", "alta_lake.r3"),         # "no more than 1 char (none under 60 cm)"
])
def test_the_char_group_still_takes_all_char_out(region, entry_id, rule_id):
    e = _corpus(region)[entry_id]
    r = next(x for x in e.rules if x.rule_id == rule_id)
    assert r.species_except[-1:] == ["CHAR"] and C.trout_char_excluded(r, e.rules) == ["CHAR"]
    mutated = _with(e, rule_id, [c for c in r.species_except if c != "CHAR"] + ["DV"])
    assert [p for p in C.trout_scope_problems(e.entry_id, "", mutated) if p.startswith(rule_id + ":")]


def _cr(**kw):
    return C.CatalogueRule.model_validate({"type": "retention_limit", "extents": [{"op": "whole"}], **kw})


TROUT_RELEASE = dict(rule_id="x.r1", verbatim="Trout catch and release", species=["TROUT_CHAR"], take=0,
                     may_target=True)


def _char_release(n: int, code: str):
    return _cr(rule_id=f"x.r{n}", verbatim=f"{code} catch and release", species=[code], take=0, may_target=True)


def test_every_char_named_one_by_one_is_the_group():
    rules = [_cr(**TROUT_RELEASE)] + [_char_release(i + 2, c) for i, c in enumerate(("DV", "LT", "EB"))]
    assert C.trout_char_excluded(rules[0], rules) == ["CHAR"]
    two = rules[:3]
    assert C.trout_char_excluded(two[0], two) == ["DV", "LT"]
    assert C.trout_scope_problems("r9:x", "", [_cr(**TROUT_RELEASE, species_except=["DV", "LT"])] + two[1:]) == []
    assert C.trout_scope_problems("r9:x", "", [_cr(**TROUT_RELEASE, species_except=["CHAR"])] + two[1:])


def test_an_unrelated_char_line_still_takes_no_char_out():
    t = _cr(rule_id="x.r1", verbatim="no trout under 30 cm", species=["TROUT_CHAR"], lengths=[{"max_cm": 30, "take": 0}])
    dv = _cr(rule_id="x.r2", verbatim="bull trout release Aug 1-Oct 31", species=["DV"], take=0, may_target=True,
             when={"dates": [{"from_month": 8, "from_day": 1, "to_month": 10, "to_day": 31}]})
    assert C.trout_char_excluded(t, [t, dv]) == []
    assert C.trout_scope_problems("r9:x", "", [_cr(rule_id="x.r1", verbatim="no trout under 30 cm",
                                                   species=["TROUT_CHAR"], species_except=["DV"],
                                                   lengths=[{"max_cm": 30, "take": 0}]), dv])


#: THE 2026-11-18 SWEEP: every bare-"trout" line of the corpus that leaves char out, and what it leaves out.
SWEEP = {
    ("z1:daily_quotas", "trout_quota.r1"): ["CHAR"],
    ("z1:daily_quotas", "trout_quota.r2"): ["CHAR", "ST"],
    ("z1:daily_quotas", "trout_quota.r2b"): ["CHAR"],
    ("z1:daily_quotas", "trout_quota.r4"): ["CHAR"],
    ("z1:daily_quotas", "trout_quota.r6"): ["CHAR"],
    ("r2:alta_lake@2-9", "alta_lake.r3"): ["CHAR"],
    ("r2:chehalis_lake@2-19", "chehalis_lake.r1"): ["DV"],
    ("r2:chilliwack_lake@2-4", "chilliwack_lake.r1"): ["DV"],
    ("r2:cultus_lake@2-3", "cultus_lake.r1"): ["DV"],
    ("r2:harrison_lake@2-18", "harrison_lake.r1"): ["DV"],
    ("r2:lillooet_lake@2-10", "lillooet_lake.r1"): ["DV"],
    ("r2:pitt_lake@2-8", "pitt_lake.r1"): ["DV"],
    ("r2:ross_lake_boundary_between_ross_lake_and_skagit_river_is_mar@2-2", "ross_lake.r3"): ["DV", "EB"],
    ("r3:nicola_river@3-13", "nicola_river.r3"): ["DV"],
    ("r3:nicola_river@3-13", "nicola_river.r3x"): ["DV"],
    ("r4:flathead_river@4-1", "flathead_river.r2"): ["DV"],
    ("r4:flathead_river_s_tributaries@4-1", "flathead_river_s_tributaries.r3"): ["DV"],
    ("r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35",
     "kootenay_river_upper.r6"): ["DV"],
    ("r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35",
     "kootenay_river_upper.r9"): ["DV"],
    ("z6:daily_quotas", "trout_char_quota.r4"): ["DV", "LT"],
    ("z6:daily_quotas", "trout_char_quota.r7"): ["DV", "LT"],
}


def test_the_corpus_lines_that_leave_char_out_are_the_swept_ones():
    char = {"CHAR", *C.BOOK_FAMILIES["CHAR"]}
    got = {}
    for region in ("1", "2", "3", "4", "5", "6", "7", "7a", "7b", "8", "provincial"):
        for e in _corpus(region).values():
            for r in e.rules:
                if "TROUT_CHAR" in r.species and set(r.species_except) & char:
                    got[(e.entry_id, r.rule_id)] = list(r.species_except)
    assert got == SWEEP
