"""USER ROUND 3 (six-angler panel on v23, 2026-10-18) — small wording fixes, no data, CI's no-data tier.

  2  the no-fishing buffers say feet too: "23 m (75 ft)", "100 m (330 ft)" (`common.m_ft`, the nearest 5 ft)
  3  a count tier inside a water's own tier or the stream share says it is part of it: "1 cutthroat or rainbow trout
     in all (part of the 2)"
  4  the lifted share says where the lifted fish go: "2 a day per angler from this stream — your hatchery rainbow
     trout count toward the 4, not these 2"
  6  a combined quota across two waters in plain words: "Kokanee from the Lower and Upper West Arms together: no more
     than 5 a day."
(1 and 5 are the page's: check.mjs ur3-howtell-steelhead, ur3-licence-not-resident.)
"""
from __future__ import annotations

from types import SimpleNamespace

from pipeline.deliver.answers import common
from pipeline.deliver.answers import display as D
from pipeline.deliver.answers import rows as RW
from pipeline.tests import answers28_fixture as F

OCT10, MAY1 = (10, 10), (5, 1)


def _tiers(water, md):
    out, _P = F.card(water, md)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]

    def walk(ns):
        for n in ns:
            if n["node"] == "tier":
                yield n
                yield from walk(n.get("children") or [])
    return list(walk(g["tree"]))


# 2 ---------------------------------------------------------------------------------------------
def test_2_metres_say_feet_to_the_nearest_5():
    assert common.m_ft(23) == "23 m (75 ft)"
    assert common.m_ft(100) == "100 m (330 ft)"
    assert common.m_ft(500) == "500 m (1,640 ft)"


def test_2_the_buffers_say_feet():
    x = {"type": "retention_limit", "family": "retention", "standing": True, "take": 0, "may_target": False,
         "species": ["ALL_GAME_FISH"],
         "verbatim": "Within 23 m downstream of the lower entrance to any fishway, canal, obstacle or leap."}
    assert D.rule_facts(x)["plain"] == ("No fishing within 23 m (75 ft) downstream of the lower entrance to any "
                                        "fishway, canal, obstacle or leap.")
    y = {**x, "verbatim": "Within a 100 m radius of any government facility operated for counting, passing or "
                          "rearing of fish (e.g., fishway, fish hatchery) unless otherwise designated."}
    assert D.rule_facts(y)["plain"].startswith("No fishing within a 100 m (330 ft) radius of any government")


# 3 ---------------------------------------------------------------------------------------------
def test_3_a_count_tier_in_an_own_tier_says_part_of_it():
    (ct,) = [t for t in _tiers(F.BELLA_MAY_ST, MAY1) if t["tier"] == "count"]
    assert ct["says"] == "1 cutthroat or rainbow trout in all (part of the 2)"


# 4 ---------------------------------------------------------------------------------------------
def test_4_the_lifted_share_says_the_fish_count_toward_the_total():
    # ROUND20 2 (owner: the UR3 words read as 4 + 2): the share sits inside the total, the lifted fish not counted in it
    (sh,) = [t for t in _tiers(F.VEDDER, OCT10) if t["tier"] == "share"]
    assert sh["says"] == "Only 2 of the 4 may be from streams"   # ROUND UR7 A: the lift said once, by the own tier's relation


# 6 ---------------------------------------------------------------------------------------------
def _restating(verbatim, entry_name, take=5, species=("KO",)):
    x = {"verbatim": verbatim, "entry_name": entry_name, "take": take, "species": list(species)}
    return SimpleNamespace(f=x, x=x)


LWA_NOTE = ("NOTE: the combined daily quota for kokanee from the Upper West Arm (when open to\nkokanee harvest) and "
            "the Lower West Arm (when open to kokanee harvest) cannot exceed 5")


def test_6_a_combined_quota_in_plain_words_this_water_first():
    assert RW.restated_words(_restating(LWA_NOTE, "Kootenay Lake — Lower West Arm")) == (
        "Kokanee from the Lower and Upper West Arms together: no more than 5 a day.")
    assert RW.restated_words(_restating("when daily quota = 5; " + LWA_NOTE, "Kootenay Lake — Upper West Arm")) == (
        "Kokanee from the Upper and Lower West Arms together: no more than 5 a day.")


def test_6_any_other_restatement_keeps_the_books_words():
    # the number disagrees with the rule's: the book's words stand
    assert RW.restated_words(_restating(LWA_NOTE, "Kootenay Lake — Lower West Arm", take=4)).startswith(
        "The combined daily quota for kokanee from the Upper West Arm")
    assert RW.restated_words(_restating("NOTE: Arctic grayling catch and release", "Williston Lake")) == (
        "Arctic grayling catch and release.")
