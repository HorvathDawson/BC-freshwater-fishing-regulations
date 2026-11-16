"""T763 (trout display review 2026-11-15, data note 1): a yearly limit or record duty the rule limits to a length band
says the band. Kootenay Lake's main body (p.37): rainbow 10 a day any size, "annual quota = 20" and the record duty
for rainbow OVER 50 cm only — the card read "20 a year · record each on your licence right away" under "any size".

The cards stand on data built here: the answers 2.8 fixture's plain Region 2 lake plus the Kootenay rows copied from
the bundle (`kootenay_lake_main_body.r4/r6`, `zp:kootenay_rainbow_stamp.r1`) onto a Region 2 entry, run through the
REAL reader and the REAL rows.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card as C
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.tree import walk
from pipeline.deliver.bundle import read
from pipeline.tests import answers28_fixture as F

PATH = "fixture:t763_kootenay_main_body"
E = "r2:kootenay_lake_main_body@2-3"
OCT10 = (10, 10)

_ROWS = [
    ("kootenay_lake_main_body.r4", {"label": "Rainbow trout — 10 per day", "species": ["RB"], "take": 10,
                                    "verbatim": "rainbow trout daily quota = 10 (any size)"}),
    ("kootenay_lake_main_body.r6", {"label": "Rainbow trout — 20 per licence year over 50 cm", "species": ["RB"],
                                    "take": 20, "dimension": "annual", "period": "annual",
                                    "lengths": [{"min_cm": 50}],
                                    "verbatim": "keep rainbow trout over 50 cm, annual quota = 20"}),
    ("kootenay_rainbow_stamp.r1", {"label": "Rainbow trout over 50 cm — record your retention on your licence "
                                            "immediately", "species": ["RB"], "take": None,
                                   "dimension": "daily/size@record", "lengths": [{"min_cm": 50}],
                                   "record_retention": True,
                                   "verbatim": "You must immediately record your retention on your basic angling "
                                               "licence."}),
]


def _bundle():
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(F.FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        for r, f in _ROWS:
            x = {"entry_id": E, "rule_id": r, "type": "retention_limit", "family": "retention",
                 "dimension": "daily", "parts": json.dumps({"what": f["label"]}), "scope": "section",
                 "species_except": [], "may_target": None, "uncertain": 0, "unresolved": None,
                 "extent_text": None, "undrawn_part": None, "extents": [{"op": "whole"}], "period": "daily",
                 "rule": r, "entry": E, "entry_name": "Kootenay Lake, Main Body", **f}
            got[(E, r)] = x
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift = tuple(F.LIFT.split("::"))
    bound = sorted((e, r, "reach") for (e, r), x in got.items()
                   if (e, r) != lift and x.get("water") != "stream")
    return F._B(rules=got, sets={1: bound}), bound


def _kinds():
    B, bound = _bundle()
    lad = {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, False, OCT10, f, PATH, steelhead_rules_here=True, origin=None if o == "none" else o, trace=True))
        for o in F.ORIGINS} for f in F.FISH}
    out = RW.produce(RW.Part(B, 1, False, True, "lake", None, lad, OCT10, {}))
    return {tuple(n["species"]): n for r in out["rows"] if r.get("group")
            for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind"}


def test_a_banded_yearly_limit_and_record_duty_say_the_band():
    rb = _kinds()[("RB",)]
    assert rb["detail"] == "any size"
    assert " · ".join(rb["duties"]) == ("20 over 50 cm (20 in) a year · "
                                        "record each one over 50 cm (20 in) on your licence right away")


def test_an_unbanded_steelhead_yearly_limit_reads_as_before():
    k = {tuple(n["species"]): n for r in F.card(F.VEDDER, OCT10)[0]["rows"] if r.get("group")
         for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind"}
    assert k[("ST",)]["duties"] == ["10 a year", "stop fishing this river after 2 a day",
                                    "record each on your licence right away"]


def _rule(lengths, k="annual"):
    return SimpleNamespace(x={"lengths": lengths}, k=k, key="t763.r1")


def test_a_band_holding_the_whole_kind_keeps_the_short_form():
    """Shuswap's shape: "5 a year" over 50 cm on a kind kept at 50 cm (20 in) or longer — the kind's detail says it."""
    assert C._duty_band(_rule([{"min_cm": 50}]), 50, C.INF) == ""
    assert C._duty_band(_rule([{"min_cm": 60}]), 60, C.INF) == ""
    assert C._duty_band(_rule(None), 0, C.INF) == ""
    assert C._duty_band(_rule([{"min_cm": 50}]), 0, C.INF) == "over 50 cm (20 in)"
    assert C._duty_band(_rule([{"min_cm": 50}], k="size"), 30, C.INF) == "over 50 cm (20 in)"


def test_two_bands_on_a_duty_are_refused():
    with pytest.raises(AnswersError, match="2 bands"):
        C._duty_band(_rule([{"max_cm": 30}, {"min_cm": 50}]), 0, C.INF)
