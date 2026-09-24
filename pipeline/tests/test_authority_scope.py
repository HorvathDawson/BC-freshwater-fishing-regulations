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

from pipeline.deliver.bundle import read as corpus
from pipeline.deliver.bundle.read import Authority, Scope, source_of

REGION_5 = [{"op": "within", "area_id": "area:region:5"}]


def _rule(**kw):
    return {"entry": "z5:bass_illegal", "rule": "bass_illegal.r1", "entry_name": "Bass",
            "verbatim": "Bass are illegal to possess in the Cariboo Region", **kw}


def test_own_region_extents_are_region_scope():
    s = source_of(_rule(extents=REGION_5))
    assert (s.authority, s.scope, s.region) == (Authority.region, Scope.region, "5")
    assert s.rank == 3


def test_own_area_extents_are_area_scope():
    s = source_of(_rule(extents=[
        {"op": "within", "area_id": "area:mu_group:management_units_1_1_to_1_6"}]))
    assert s.scope is Scope.area and s.place == "MUs 1-1 to 1-6"


def test_a_zone_rule_naming_a_water_is_that_water():
    """A region's table writing about one lake is an override on that lake."""
    s = source_of(_rule(extents=[{"op": "whole", "item_id": "wbk:329518145"}],
                        extent_text="Shuswap Lake"))
    assert s.scope is Scope.water and s.place == "Shuswap Lake"


def test_nothing_inherits_the_entry():
    """`None inherits the entry` is gone: a caller that offers the entry's extents is refused,
    so no reader can quietly fill a rule's scope from its row."""
    with pytest.raises(ValueError, match="nothing inherits"):
        source_of(_rule(extents=[], entry_extents=REGION_5, extent_text="the Cariboo Region"))


def test_a_region_table_with_a_carve_out_it_cannot_draw_is_still_the_region():
    """"Bass: 20, excluding Mill Lake": the rule states Region 2 and the carve-out as a locator
    (which keeps it unbound); its scope is its own `within area:region:2`."""
    s = source_of(_rule(entry="z2:species_quotas", extents=[
        {"op": "within", "area_id": "area:region:2"}], unresolved_locators=["Mill Lake"]))
    assert s.scope is Scope.region


def test_no_extents_and_no_named_place_is_refused():
    with pytest.raises(ValueError, match="unknown"):
        source_of(_rule(extents=[]))


def test_no_extents_but_a_named_place_is_that_place():
    s = source_of(_rule(extents=[], extent_text="Rubble Creek Landslide Hazard Area"))
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


def test_the_corpus_carries_no_entry_extents(rules):
    """The corpus hands `source_of` each rule's own extents and nothing of its entry's."""
    assert rules and not any("entry_extents" in x for x in rules)


def test_zone_rules_that_state_their_region_are_region_scoped_placed_or_not(rules):
    """A region table's rule states its region itself, so it stays in that region's table
    without inheriting it — whether or not the reach builder placed it. (The carve-out rules
    that used to be the unplaced examples, "Bass: 20 (excluding Mill Lake)", are placed since
    the ruling that the excepted water's own row overrides; `uncertain` is set here to keep the
    unplaced case tested — `source_of` must not read placement.)"""
    got = [x for x in rules if x["entry"].startswith("z")
           and x["extents"] and all(e.get("op") == "within" and str(e.get("area_id") or "")
                                    .startswith("area:region:") for e in x["extents"])]
    assert got, "no zone rule states a region — nothing here was tested"
    for x in got:
        for y in (x, {**x, "uncertain": True}):
            assert source_of(y).scope is Scope.region, corpus.rid(y)


def test_corpus_carries_the_lifts(rules):
    """`exempts` is a column now; the corpus decodes it, never leaves it as text."""
    lifts = [x for x in rules if x.get("exempts")]
    assert lifts
    assert all(isinstance(x["exempts"], list) and x["exempts"][0].get("entry_id")
               for x in lifts)
