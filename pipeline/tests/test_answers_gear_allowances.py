"""T797 item 2 (owner 2026-11-17 "Show it"): A BAIT ALLOWANCE IS SAID ONCE PER PART, citing every rule that allows it.

A Fraser part holds the Fraser row's own "Dead fin fish may be used for bait only when fishing for sturgeon" AND the
province's bait.r3 (zp:bait), the same allowance: `also_allowed` keeps one circumstance per clause, so a reader drawing
it drew two near-identical lines. `BaitPick.allowances` [{says, by}] says each distinct allowance once, `by` every
clause allowing it (merged, in order). The set-lining allowance (Region 6 and Zone 7A lakes, bait.r2) is kept. No data.
"""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from pipeline.deliver.answers import gear as G
from pipeline.deliver.answers.model import Allowance, BaitPick

BAN = G.Rule("zp:bait::bait.r1", 4, {"gear": [{"ban": ["fin_fish"], "except": ["roe"], "slot": "bait"}]})
DEAD = [{"allow": ["dead_fin_fish"], "slot": "bait"}]
FRASER = G.Rule("r2:fraser::fraser.r1", 0, {"gear": DEAD, "when_targeting": ["WSG"]})
R3 = G.Rule("zp:bait::bait.r3", 2, {"gear": DEAD, "when_targeting": ["WSG"]})
R2 = G.Rule("zp:bait::bait.r2", 4, {"gear": DEAD, "while": ["set_lining"], "water": "lake"})


def fin(active, kind):
    return next(b for b in G.resolve(active, kind, list(G.METHODS))["bait"] if b["element"] == "fin_fish")


def test_a_two_rule_part_says_the_sturgeon_allowance_once_citing_both():
    b = fin([BAN, FRASER, R3], "stream")
    assert b["allowances"] == [{"says": "Dead fin fish may be used when fishing for white sturgeon.",
                                "by": [["r2:fraser::fraser.r1", 0], ["zp:bait::bait.r3", 0]]}]
    # `also_allowed` is unchanged: one circumstance per clause
    assert [c["clause"] for c in b["also_allowed"]] == [["r2:fraser::fraser.r1", 0], ["zp:bait::bait.r3", 0]]
    # on the wire a rule is its export index
    wire = json.dumps(b)
    for n, k in enumerate(("zp:bait::bait.r1", "r2:fraser::fraser.r1", "zp:bait::bait.r3")):
        wire = wire.replace(json.dumps(k), str(n))
    BaitPick.model_validate_json(wire)


def test_one_rule_says_it_once_and_the_set_lining_allowance_is_kept():
    assert fin([BAN, R3], "stream")["allowances"] == [
        {"says": "Dead fin fish may be used when fishing for white sturgeon.", "by": [["zp:bait::bait.r3", 0]]}]
    assert fin([BAN, R2], "lake")["allowances"] == [
        {"says": "Dead fin fish may be used while set lining.", "by": [["zp:bait::bait.r2", 0]]}]


def test_different_allowances_stay_apart():
    """Mutation: an allowance for another fish is not the sturgeon's (dedup is by the words, not the element)."""
    other = G.Rule("r9:x::x.r1", 0, {"gear": DEAD, "when_targeting": ["BB"]})
    got = fin([BAN, R3, other], "stream")["allowances"]
    assert [a["says"] for a in got] == ["Dead fin fish may be used when fishing for white sturgeon.",
                                        "Dead fin fish may be used when fishing for burbot."]


def test_no_allowance_no_field():
    assert "allowances" not in fin([BAN], "stream")


def test_the_model_refuses_an_allowance_with_no_rule():
    with pytest.raises(ValidationError):
        Allowance.model_validate_json(json.dumps({"says": "Dead fin fish may be used.", "by": []}))
