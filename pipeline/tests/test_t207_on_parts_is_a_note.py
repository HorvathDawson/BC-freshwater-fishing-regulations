"""T207 item 1 (owner 2026-10-26): "on parts" is not an error.

A rule held as an undrawn part because the book says "on parts" and never which (Hatzic's speed
limits) is an honest note. It is no severity-A pre-check, no risk factor, no flag, and the review
screen shows it without a warning. A part that DOES name a place stays a severity-A finding; the
pair below is the mutation that proves the predicate decides it.
"""
from __future__ import annotations

import json

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.review import precheck as P
from pipeline.regs.review import tiers as T
from pipeline.regs.review.scope_words import Names, rule_scope

SPEED = {"rule_id": "x.r2", "type": "vessel_rule", "verbatim": "speed restriction on parts (8 and 50 km/h)",
         "aspect": "speed", "max_kmh": 8.0, "includes_tributaries": False, "extents": [{"op": "whole"}],
         "review_reason": P.UNDRAWN_REASON + ": 'on parts' implies specific reaches", "undrawn_part": "on parts"}


def _entry(rule: dict) -> dict:
    return {"entry_id": "r2:x@2-8", "matched": ["gnis:1"], "rules": [rule],
            "regs_verbatim": rule["verbatim"]}


@pytest.mark.parametrize("part", ["on parts", "on part", "on a part", "various locations (as buoyed and signed)",
                                  "parts of the lake (not specified in the book)"])
def test_an_unidentified_part_is_a_note_never_a_finding(part):
    e = _entry(dict(SPEED, undrawn_part=part))
    found = P.prechecks(e, binds=True)
    assert [(f["code"], f["severity"]) for f in found] == [("unidentified_part", "I")]
    assert P.severe(found) == 0
    assert T.risks(e, binds=True) == [] and not T.flagged(e)
    assert T.entry_tier(e, binds=True) == "P4"


def test_a_part_that_names_a_place_is_still_a_fact():
    """The mutation: the same rule, its part naming a place (Pitt River's "at Grant Narrows")."""
    e = _entry(dict(SPEED, undrawn_part="at Grant Narrows"))
    found = P.prechecks(e, binds=True)
    assert [(f["code"], f["severity"]) for f in found] == [("undrawn_part", "A")]
    assert T.risks(e, binds=True) == ["undrawn part", "flag"]
    assert T.entry_tier(e, binds=True) == "P3"


def test_a_parser_doubt_beside_on_parts_is_still_a_flag():
    """Only the normaliser's own note on the part is quiet; another review_reason still flags."""
    e = _entry(dict(SPEED, review_reason="the km/h is unclear"))
    codes = {f["code"] for f in P.prechecks(e, binds=True)}
    assert codes == {"unidentified_part", "parser_uncertain"} and T.flagged(e)


def test_the_screen_words_it_as_a_note():
    entry = {"matched": ["gnis:1"], "rules": [SPEED]}
    sw = rule_scope(dict(SPEED), entry, Names())
    assert sw["in_part"] == "on parts" and sw["in_part_identified"] is False
    named = dict(SPEED, undrawn_part="at Grant Narrows")
    assert rule_scope(named, entry, Names())["in_part_identified"] is True


def test_hatzic_has_no_severity_a_finding_from_its_speed_limits():
    entries = json.loads((CURATED.regulations.entries.catalogue / "region-2.json").read_text())["entries"]
    e = next(x for x in entries if x["entry_id"] == "r2:hatzic_lake_and_slough@2-8")
    found = P.prechecks(e, binds=True)
    on = [f for f in found if f["rule_id"] in ("hatzic_lake_and_slough.r2", "hatzic_lake_and_slough.r3")]
    assert on and all(f["code"] == "unidentified_part" and f["severity"] == "I" for f in on)
    assert "undrawn part" not in T.risks(e, binds=True) and "flag" not in T.risks(e, binds=True)


def test_cranberry_canyon_says_the_owner_must_locate_its_cut():
    """Never guessed (owner 2026-10-26): the canyon's signs are held undrawn, and the finding the
    review screen badges says the cut is the owner's to locate."""
    entries = json.loads((CURATED.regulations.entries.catalogue / "region-6.json").read_text())["entries"]
    e = next(x for x in entries if x["entry_id"] == "r6:cranberry_river@6-15")
    r1 = next(r for r in e["rules"] if r["rule_id"] == "cranberry_river.r1")
    assert r1["extents"] == [{"op": "whole"}] and r1["undrawn_part"]
    f = next(f for f in P.prechecks(e, binds=True) if f["rule_id"] == "cranberry_river.r1")
    assert f["code"] == "undrawn_part" and f"the parser: {P.NEEDS_SPLIT}" in f["what"]
