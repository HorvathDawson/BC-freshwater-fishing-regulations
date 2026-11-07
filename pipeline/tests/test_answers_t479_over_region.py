"""T479 (owner 2026-11-07): "if a quota is bigger then the regional quota it should say exception to regional quota".

A water's own tier (or a water's own total) carries `over_region: true` exactly where one of its fish may be kept above
the take of a regional or B.C.-wide quota (`tree.QUOTA_KINDS`) that a rule of its own entry lifts or displaces here —
read from the resolved ladder, never from the words. Over the real water shapes T475 names:

  flagged    the Vedder's 4 hatchery rainbow trout (Region 2's 2 from streams lifted), two lifts from one row (3 cutthroat,
             4 rainbow over the 2), Polley Lake's own 8 (Region 5's 5 displaced);
  not        a lake's 2 rainbow trout "part of the 4" (only the size cap is lifted), Chilliwack Lake's own 1 Dolly Varden,
             a lake's own 4 at Region 2's 4 (equal is no exception), the Bella Coola's 1 under Region 5's 5, the Dean's
             1, the Coquitlam's 2 at the share's 2, every region total and share.

No data: Region 2's real table (`answers28_fixture`) and the gate-class waters, through the real reader and rows.
"""
from __future__ import annotations

import copy

import pytest

from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.model.rows import TreeTier
from pipeline.deliver.answers.tree import walk
from pipeline.tests import test_answers_t475_quota_wording as T5

OCT10, JUL15 = (10, 10), (7, 15)

FLAGGED = {("vedder", OCT10): ["This river’s rule: up to 4 hatchery rainbow trout a day"],
           ("two_lifts", OCT10): ["This river’s rule: up to 4 a day"],
           ("polley", JUL15): ["This lake’s rule: up to 8 a day per angler"]}


def _tiers(water, md):
    out, P = T5._card(water, md)
    return [t for row in out["rows"] if row.get("group") for t, _p in walk(row["group"]["tree"]) if t["node"] == "tier"], P


@pytest.mark.parametrize("water,md,_what", T5.FIXTURE_WATERS + T5.GATE_WATERS)
def test_exception_exactly_where_a_regional_quota_is_exceeded(water, md, _what):
    ts, _P = _tiers(water, md)
    got = [t["says"] for t in ts if t.get("over_region")]
    assert got == FLAGGED.get((water, md), [])
    for t in ts:
        if t.get("over_region"):
            assert t["tier"] in ("own", "total") and t["whose"]


def test_at_the_regions_number_is_no_exception():
    """Mutation: the Vedder's own 4 held to the 2 it lifts — no longer above the region's quota, so no flag."""
    ts, P = _tiers("vedder", OCT10)
    own = next(t for t in ts if t.get("over_region"))
    assert TR.over_region(P, own["by"], own)
    held = copy.deepcopy(own)
    for k, _p in walk(held["children"]):
        if k["node"] == "kind":
            k["n"] = 2
    assert not TR.over_region(P, held["by"], held)


def test_a_tier_of_another_entry_is_not_the_exception():
    """The lift belongs to the Vedder's entry; the region's own total (another entry) never claims it."""
    ts, P = _tiers("vedder", OCT10)
    own = next(t for t in ts if t.get("over_region"))
    root = next(t for t in ts if t["tier"] == "total")
    assert not TR.over_region(P, root["by"], own)


def test_only_a_waters_own_tier_may_carry_it():
    base = {"node": "tier", "id": "t1", "n": 8, "by": 0, "children": ()}
    TreeTier.model_validate({**base, "tier": "total", "says": "This lake’s rule: up to 8 a day per angler",
                             "whose": "This lake’s rule", "over_region": True})
    with pytest.raises(ValueError, match="exception to a regional quota"):
        TreeTier.model_validate({**base, "tier": "share", "says": "Up to 2 of the 4 from streams", "over_region": True})
