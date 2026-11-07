"""T481 (owner 2026-11-07, approved): the pin bar "here-max + one breadcrumb" — the store's half.

  water_max   the most one angler may keep of a group here today (`tree.water_max`): the Vedder's 4 (2 from streams + 4
              hatchery rainbow, held to Region 2's 4), a share stream's 2, Polley Lake's own 8
  short       every tier but the total, as a clause with no origin clause ("under this river’s rule") and a lower-case
              start (the "of those, Up to" capital, ~1,878 waters, is gone); a size tier's without its number
  within/fish a water's own tier inside the share or beside it ("total"), its fish in short ("hatchery rainbow")
  size words  ONE fish across the group, not one per species: "Only 1 of all your trout and char may be over 50 cm (20 in)
              — not 1 per species. 2 if one of them is a hatchery steelhead."

Pinned: nothing the bar can say sums above water_max; mutations move water_max with the tree.
"""
from __future__ import annotations

import copy
import re

import pytest

from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.tree import walk
from pipeline.tests import test_answers_t475_quota_wording as T5

OCT10, JUL15 = (10, 10), (7, 15)
WATERS = [(w, md) for w, md, _ in T5.FIXTURE_WATERS + T5.GATE_WATERS]


def _groups(water, md):
    out, _P = T5._card(water, md)
    return [row["group"] for row in out["rows"] if row.get("group")]


def _g(water, md=OCT10):
    return _groups(water, md)[0]


def test_water_max_on_the_real_shapes():
    assert _g("vedder")["water_max"] == 4          # min(4, 2 from streams + 4 hatchery rainbow)
    assert _g("two_lifts")["water_max"] == 4
    assert _g("stream")["water_max"] == 2          # the share binds every kind
    assert _g("polley", JUL15)["water_max"] == 8   # a water's own total
    assert _g("dean", JUL15)["water_max"] == 1     # the river's own 1 under Region 5's 5


def _ints(s: str):
    # the numbers a pin line counts fish by (not a size's cm / in)
    return [int(x) for x in re.findall(r"\b(\d+)\b(?! cm| in\))", s)]


@pytest.mark.parametrize("water,md", WATERS)
def test_nothing_the_bar_says_sums_above_water_max(water, md):
    for g in _groups(water, md):
        wm = g["water_max"]
        root = g["tree"][0]
        if root["n"] is None:
            assert wm is None
            continue
        assert wm is not None and wm <= root["n"]
        share = next((t for t, _p in walk(g["tree"]) if t["node"] == "tier" and t["tier"] == "share"), None)
        if share is not None:
            assert share["n"] <= wm
            # the exception line: "{wm} a day — up to {share} of any kind, the rest {fish}"
            outside = [t for t in root["children"] if t["node"] == "tier" and t["tier"] == "own" and t["within"] == "total"]
            if outside:
                assert wm <= share["n"] + sum(t["n"] for t in outside if t["n"] is not None) or \
                    any(t["n"] is None for t in outside)
        for t, _p in walk(g["tree"]):
            if t["node"] == "tier" and t["tier"] != "total":
                assert all(x <= max(wm, (t.get("extra") or {}).get("n", 0) + (t["n"] or 0)) for x in _ints(t["short"])), t


@pytest.mark.parametrize("water,md", WATERS)
def test_every_tier_but_the_total_has_a_short_clause(water, md):
    for g in _groups(water, md):
        for t, _p in walk(g["tree"]):
            if t["node"] != "tier":
                continue
            if t["tier"] == "total":
                assert t.get("short") is None
                continue
            s = t["short"]
            assert s and not s[0].isupper(), s                  # no "of those, Up to"
            assert "rule" not in s and "under " not in s, s      # no origin clause
            if t["tier"] == "own":
                assert t["within"] in ("share", "total")
            else:
                assert t.get("within") is None and t.get("fish") is None


def test_the_vedder_own_tier_is_beside_the_share():
    own = next(t for t, _p in walk(_g("vedder")["tree"]) if t["node"] == "tier" and t["tier"] == "own")
    assert (own["within"], own["fish"], own["short"]) == ("total", "hatchery rainbow", "up to 4 hatchery rainbow trout")
    own2 = next(t for t, _p in walk(_g("two_lifts")["tree"]) if t["node"] == "tier" and t["tier"] == "own")
    assert own2["fish"] == "hatchery rainbow or cutthroat"


def test_an_own_tier_holding_every_fish_says_of_them():
    own = next(t for t, _p in walk(_g("dean", JUL15)["tree"]) if t["node"] == "tier" and t["tier"] == "own")
    assert own["short"] == "up to 1 of them"


def test_the_size_words_are_one_fish_across_the_group():
    size = next(t for t, _p in walk(_g("vedder")["tree"]) if t["node"] == "tier" and t["tier"] == "size")
    assert size["says"] == ("Only 1 of all your trout and char may be over 50 cm (20 in) — not 1 per species. "
                            "2 if one of them is a hatchery steelhead.")
    assert size["short"] == "may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"
    lake = next(t for t, _p in walk(_g("lake")["tree"]) if t["node"] == "tier" and t["tier"] == "size")
    assert lake["says"] == "Only 1 of all your trout and char may be over 50 cm (20 in) — not 1 per species."
    assert lake["short"] == "may be over 50 cm (20 in)"        # no steelhead clause where none applies


@pytest.mark.parametrize("water,md", WATERS)
def test_every_size_tier_says_its_number_once_for_the_fish_together(water, md):
    for g in _groups(water, md):
        for t, _p in walk(g["tree"]):
            if t["node"] == "tier" and t["tier"] == "size":
                n = t["n"]
                assert t["says"].startswith(f"Only {n} of " if n else "None of "), t["says"]
                assert t["says"].endswith("."), t["says"]
                assert not re.match(r"^\d", t["short"]) and t["short"].startswith("may be over "), t["short"]


def test_mutation_water_max_follows_the_tree():
    g = _g("vedder")
    tree = copy.deepcopy(g["tree"])
    own = next(t for t, _p in walk(tree) if t["node"] == "tier" and t["tier"] == "own")
    own["n"] = 1
    for k, _p in walk(own["children"]):
        if k["node"] == "kind":
            k["n"] = 1
    assert TR.water_max(tree) == 3                 # 2 from streams + 1
    tree[0]["n"] = None
    assert TR.water_max(tree) is None


def test_short_fish():
    assert TR.short_fish("hatchery rainbow trout") == "hatchery rainbow"
    assert TR.short_fish("cutthroat or rainbow trout") == "cutthroat or rainbow"
    assert TR.short_fish("lake trout") == "lake trout"
    assert TR.short_fish("Dolly Varden/bull trout") == "Dolly Varden/bull trout"
