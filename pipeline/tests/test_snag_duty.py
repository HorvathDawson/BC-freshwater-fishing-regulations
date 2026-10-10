"""A SNAGGED FISH MUST BE RELEASED, HOWEVER IT WAS HOOKED (user ruling Q38 / RULINGS G6, 2026-10-07).

p.8 "Snag (foul hook) fish (see definition, page 80). Any fish willfully or accidentally snagged must
be released immediately."; p.80 "snagging (foul hooking): hooking a fish in any other part of its body
other than the mouth."

`zp:prohibited_methods.r3` was `while: [snagging]` (interim RR22): a MEANS, which an accidental snag
while angling never meets. It is now a condition on the FISH, `caught: [foul_hooked]` — and a
condition, so it never becomes an outright release of every fin fish in B.C. (it never decides a
quota for a fish hooked in the mouth). Pinned here: the rule's shape, the model's refusals, every
reader that must see it as a condition (mutation-guarded), the plain words, the gear answer, and on a
built answers file that it decides no quota anywhere and is in force on every non-tidal water.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

import pytest

from pipeline.deliver.answers import display as X
from pipeline.deliver.answers import gear as GR
from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle import rules as RU
from pipeline.regs.parsing import catalogue as C
from pipeline.regs.parsing import io
from pipeline.tests.conftest import EXPORT_HINT, need
from pipeline.tests.test_competition import _rules_of, _tiny

CAT = Path(__file__).resolve().parents[2] / "data/curated/regulations/entries/catalogue"
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
SNAG = "zp:prohibited_methods::prohibited_methods.r3"
WORDS = "Any fish snagged — even by accident — must be released."

#: the rule as a bundle reader holds it (`read.rules`): conditions merged into the dict
R3 = {"entry": "zp:prohibited_methods", "rule": "prohibited_methods.r3", "type": "retention_limit",
      "species": ["ALL_FIN_FISH"], "caught": ["foul_hooked"], "take": 0, "may_target": 1,
      "dimension": "daily@caught=foul_hooked", "_rank": 4}


def _r3_curated() -> dict:
    ents = io.read_entryfile(CAT / "region-provincial.json")
    return next(r for r in ents["zp:prohibited_methods"]["rules"]
                if r["rule_id"] == "prohibited_methods.r3")


# --------------------------------------------------------------------------------------------
# The rule's shape, and the model
# --------------------------------------------------------------------------------------------

def test_the_snag_release_binds_the_fish_not_the_means():
    r = _r3_curated()
    assert r["caught"] == ["foul_hooked"] and "while" not in r
    assert r["take"] == 0 and r["may_target"] is True and r["species"] == ["ALL_FIN_FISH"]
    rule = C.CatalogueRule.model_validate(r)
    assert rule.dimension == "daily@caught=foul_hooked"
    assert "snagged" in C.label(rule) and "even by accident" in C.label(rule)


@pytest.mark.parametrize("bad, says", [
    ({"while": ["snagging"]}, "binds the FISH"),            # the interim shape is refused now
    ({"caught": ["gill_netted"]}, "not a way a fish is caught"),
])
def test_the_model_refuses_a_snag_release_on_the_means_or_an_unknown_way(bad, says):
    r = {k: v for k, v in _r3_curated().items() if k != "caught"}
    with pytest.raises(ValueError, match=says):
        C.CatalogueRule.model_validate({**r, **bad})


def test_caught_belongs_to_a_retention_rule():
    with pytest.raises(ValueError, match="caught belongs to retention_limit"):
        C.CatalogueRule.model_validate({
            "rule_id": "x.r1", "type": "handling_rule", "verbatim": "release it",
            "conduct": ["release_immediately"], "caught": ["foul_hooked"],
            "extents": [{"op": "within", "area_kind": "region"}]})


# --------------------------------------------------------------------------------------------
# The readers: a condition, never an outright release (MUTATION: drop `caught` from any of them)
# --------------------------------------------------------------------------------------------

def test_every_reader_reads_caught_as_a_condition():
    assert RU.release_origins(R3) is None
    assert RU.closure_grade(R3) is None
    assert "caught" in RU.CLOSURE_CONDITIONS
    plain_release = {k: v for k, v in R3.items() if k != "caught"}
    assert RU.release_origins(plain_release) == RU.ORIGINS            # what it would have been
    assert RU.statement(R3) != RU.statement(plain_release)
    assert not RU.same_statement(R3, plain_release)


def test_the_snag_duty_never_displaces_a_quota_for_a_fish_hooked_in_the_mouth(tmp_path):
    """The region's 'Trout/char: 5' and a lake's 'kokanee 10' keep speaking beside the snag duty;
    the duty speaks too (its own key: `daily@caught=foul_hooked`, a statement of its own).
    MUTATION: the same rule without `caught` is a plain release of every fin fish on the quotas'
    own key — it competes with them, and the region's 5 displaces it: the duty vanishes wherever
    a quota stands (and where none does, every fish would read "release")."""
    quotas = [{"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
              {"entry": "r9:lake", "rule": "lake.r1", "species": ["KO"], "take": 10, "_rank": 0}]
    path = _tiny(tmp_path, quotas + [R3])
    assert _rules_of(path, (7, 1), "RB") == {"q.r1", "prohibited_methods.r3"}
    assert _rules_of(path, (7, 1), "KO") == {"lake.r1", "prohibited_methods.r3"}
    outright = {**{k: v for k, v in R3.items() if k != "caught"}, "dimension": "daily"}
    d = tmp_path / "mut"
    d.mkdir()
    path = _tiny(d, quotas + [outright])
    assert _rules_of(path, (7, 1), "RB") == {"q.r1"}
    assert RU.release_origins(outright) == RU.ORIGINS


@pytest.mark.parametrize("cond", [{"caught": ["foul_hooked"]}])
def test_a_zone_release_conditioned_on_how_the_fish_was_caught_leaves_its_tables_quotas(tmp_path,
                                                                                       cond):
    """RU-4's generalised 4b reaches only an UNCONDITIONED zone release (`release_origins`)."""
    path = _tiny(tmp_path, [
        {"entry": "z9:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 5, "_rank": 3},
        {"entry": "z9:q", "rule": "q.r7", "species": ["TROUT_CHAR"], "take": 0, "may_target": 1,
         "dimension": "daily@caught=foul_hooked", **cond, "_rank": 3}])
    assert "q.r1" in _rules_of(path, (7, 1), "RB")


# --------------------------------------------------------------------------------------------
# The answers: plain words, a kind of its own, the gear answer
# --------------------------------------------------------------------------------------------

def test_the_answers_say_it_in_plain_words():
    assert X.plain(R3) == WORDS
    assert X.kind_of(R3) == "caught"
    assert X.rule_facts(R3) == {"kind": "caught", "plain": WORDS}
    with pytest.raises(Exception, match="not a release"):
        X.caught_sentence({**R3, "take": 2})


def test_the_gear_answer_lists_the_duty_and_never_as_a_way_to_fish():
    r = GR.Rule(7, 4, R3)
    ans = GR.resolve([], "stream", ["angling"], caught=[r])
    assert ans["caught"] == [7] and 7 not in ans["while_rules"] and 7 not in ans["decides"]
    assert GR.resolve([], "stream", ["angling"])["caught"] == []


# --------------------------------------------------------------------------------------------
# On a built answers file (the live set, or `ANSWERS_EXPORT_DIR`)
# --------------------------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _wire():
    need(None, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    need(None, "bundle", EXPORT_DIR / "ui-rules-answers.json", EXPORT_HINT)
    data = json.loads((EXPORT_DIR / "ui-rules-export.json").read_text())
    wire = json.loads((EXPORT_DIR / "ui-rules-answers.json").read_text())
    return data, wire


@pytest.mark.needs_bundle
def test_on_the_built_file_the_snag_duty_decides_no_fish_and_holds_on_every_water():
    data, wire = _wire()
    ix = data["rule_ids"].index(SNAG)
    S = wire["sections"]
    assert S["display"]["rules"][ix]["plain"] == WORDS
    assert S["display"]["rules"][ix]["kind"] == "caught"
    # it wins no fish's answer and is no role of any decided answer
    for d in S["rows"]["decided"]:
        assert d["win"] != ix and all(r != ix for r, _, _ in d["roles"])
    # every freshwater gear answer holds it, as a duty for the snagged fish, never a `while` rule
    gear = [g for g in S["gear"]["frames"] if not g.get("tidal")]
    assert gear and all(g["caught"] == [ix] and ix not in g["while_rules"] for g in gear)
