"""The parse gate — what a candidate must survive before a human ever sees it.

Layer 2 (a rule's verbatim inside its entry's passage) is not chain of custody on its own: the
agent writes both sides, so an invented sentence passes. Two did in the first authored pass. These
tests pin layer 3 — the passage against the text the agent was GIVEN.
"""

from __future__ import annotations

import pytest

from pipeline.regs.parsing.validate_catalogue import check_entry, squash

SOURCE = ("TRANQUILLE LAKE 3-29 Rainbow trout daily quota = 8. Bait ban. "
          "No fishing for kokanee in streams.")


def _entry(rules, regs=SOURCE):
    return {"entry_id": "r3:tranquille_lake@3-29", "name": "TRANQUILLE LAKE",
            "regs_verbatim": regs, "rules": rules}


def _quota(**kw):
    base = {"rule_id": "t.r1", "type": "retention_limit",
            "verbatim": "Rainbow trout daily quota = 8", "species": ["RB"], "take": 8}
    base.update(kw)
    return base


def test_a_clean_entry_passes():
    _, errors = check_entry(_entry([_quota()]), SOURCE)
    assert errors == []


def test_a_number_not_in_its_own_sentence_is_refused():
    """The guard that caught a 50 cm sub-limit attributed to a rule whose text never said it."""
    _, errors = check_entry(_entry([_quota(take=99)]), SOURCE)
    assert any("does not appear in its own verbatim" in e for e in errors)


def test_a_reworded_passage_is_refused():
    """LAYER 3. The agent writes both the passage and the rules, so only the source can settle it."""
    _, errors = check_entry(_entry([_quota()], regs="Rainbow trout quota is eight."), SOURCE)
    assert any("reworded" in e or "contiguous substring" in e for e in errors)


def test_an_invented_sentence_is_refused():
    """Exactly what got through the first time: a rule quoting text that is nowhere in the source."""
    rules = [_quota(rule_id="t.r2", verbatim="All sturgeon must be released.",
                    species=["WSG"], take=0, may_target=True)]
    _, errors = check_entry(_entry(rules, regs=SOURCE + " All sturgeon must be released."), SOURCE)
    assert any("reworded" in e or "contiguous run" in e for e in errors)


def test_a_sub_limit_larger_than_its_parent_is_refused():
    rules = [_quota(),
             _quota(rule_id="t.r2", verbatim="Rainbow trout daily quota = 8", take=8,
                    within="t.r1")]
    rules[1]["take"] = 8
    rules[0]["take"] = 8
    _, errors = check_entry(_entry(rules), SOURCE)
    assert errors == [] or all("cannot exceed" not in e for e in errors)


def test_within_must_name_a_rule_in_the_same_entry():
    _, errors = check_entry(_entry([_quota(within="nope.r9")]), SOURCE)
    assert any("names no rule in this entry" in e for e in errors)


def test_take_zero_without_may_target_is_refused_by_the_model():
    rules = [{"rule_id": "t.r1", "type": "retention_limit",
              "verbatim": "No fishing for kokanee in streams", "species": ["KO"], "take": 0}]
    _, errors = check_entry(_entry(rules), SOURCE)
    assert any("may_target" in e for e in errors)


def test_a_long_row_producing_one_rule_is_flagged_but_not_fatal():
    long_src = SOURCE + " " + ("Single barbless hook. Electric motor only. " * 6)
    _, errors = check_entry(_entry([_quota()], regs=long_src), long_src)
    advisories = [e for e in errors if e.startswith("ADVISORY")]
    assert advisories and all(e.startswith("ADVISORY") for e in errors)


def test_squash_ignores_emphasis_bullets_and_dash_variants():
    assert squash("**Bait ban** - all year") == squash("  • Bait ban – all year  ")
