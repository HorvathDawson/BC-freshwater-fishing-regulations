"""WHAT A ZONE RULE BINDS TO when it carries no extents of its own.

The bundle ships a rule's OWN extents and nothing else: it used to copy the entry's onto a rule
with none, and a rule the reach builder could not place then claimed its entry's whole water.
That fix was right, and it moved 14 region-wide zone rules ("Bass: illegal to possess in the
Cariboo Region", the ice-hut removal date) out of their region's standing table — `source_of`
read "no extents" as "names a water". The model says a rule with `extents: None` inherits its
entry's, so the scope is the ENTRY's; whether the rule was placed is `uncertain`, a different
question. Nothing is guessed: no entry extents passed is refused, and so is a rule with no
extents by either route that names no place.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.regs.table import corpus
from pipeline.regs.table.authority import Authority, Scope, source_of

REGION_5 = [{"op": "within", "area_id": "area:region:5"}]


def _rule(**kw):
    return {"entry": "z5:bass_illegal", "rule": "bass_illegal.r1", "entry_name": "Bass",
            "verbatim": "Bass are illegal to possess in the Cariboo Region", **kw}


def test_no_own_extents_takes_the_entrys_region_scope():
    s = source_of(_rule(extents=[], entry_extents=REGION_5,
                        extent_text="the Cariboo Region"))
    assert (s.authority, s.scope, s.region) == (Authority.region, Scope.region, "5")
    assert s.is_base


def test_no_own_extents_takes_the_entrys_area_scope():
    s = source_of(_rule(extents=[], entry_extents=[
        {"op": "within", "area_id": "area:mu_group:management_units_1_1_to_1_6"}]))
    assert s.scope is Scope.area and s.place == "MUs 1-1 to 1-6"


def test_own_extents_decide_over_the_entrys():
    """A region's table writing about one lake is an override on that lake."""
    s = source_of(_rule(extents=[{"op": "whole", "item_id": "wbk:329518145"}],
                        entry_extents=REGION_5, extent_text="Shuswap Lake"))
    assert s.scope is Scope.water and s.place == "Shuswap Lake"


def test_a_zone_rule_without_its_entrys_extents_is_refused():
    with pytest.raises(ValueError, match="entry_extents"):
        source_of(_rule(extents=[]))


def test_no_extents_either_way_and_no_named_place_is_refused():
    with pytest.raises(ValueError, match="unknown"):
        source_of(_rule(extents=[], entry_extents=[]))


def test_no_extents_either_way_but_a_named_place_is_that_place():
    s = source_of(_rule(extents=[], entry_extents=[],
                        extent_text="Rubble Creek Landslide Hazard Area"))
    assert s.scope is Scope.water and s.place == "Rubble Creek Landslide Hazard Area"


def test_a_water_entry_is_water_whatever_it_carries():
    s = source_of({"entry": "r5:fraser_river@5-2", "rule": "fraser_river.r2",
                   "entry_name": "Fraser River", "extents": []})
    assert s.scope is Scope.water


# ---------------------------------------------------------------------------------------
# Against a bundle: corpus.rules() hands source_of what it needs, and the unplaced zone rules
# land in their region's table.
# ---------------------------------------------------------------------------------------
BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or corpus.BUNDLE)


@pytest.fixture(scope="module")
def rules():
    return corpus.rules(str(BUNDLE))


def test_every_rule_in_the_corpus_has_a_source(rules):
    for x in rules:
        source_of(x)


def test_unplaced_zone_rules_on_a_region_wide_entry_are_region_scoped(rules):
    got = [x for x in rules
           if x["entry"].startswith("z") and not x["extents"] and x["entry_extents"]
           and all(e.get("op") == "within" and str(e.get("area_id") or "").startswith(
               "area:region:") for e in x["entry_extents"])]
    assert got, "no zone rule without extents of its own — nothing here was tested"
    wrong = [corpus.rid(x) for x in got if source_of(x).scope is not Scope.region]
    assert wrong == []


def test_corpus_carries_the_lifts(rules):
    """`exempts` is a column now; the corpus decodes it, never leaves it as text."""
    lifts = [x for x in rules if x.get("exempts")]
    assert lifts
    assert all(isinstance(x["exempts"], list) and x["exempts"][0].get("entry_id")
               for x in lifts)
