"""Tests for the per-rule verbatim `exception` field on Rule.

The `exception` field makes a rule self-contained: it carries the verbatim
"except ..." carve-out that qualifies the rule, so the UI can render all of a
rule's context on one card.  Like `location_text`, it is anti-hallucination
enforced — it must be an exact substring of `rule_text`.
"""

from __future__ import annotations

import pytest

from pipeline.parsing.models import Rule


def test_exception_defaults_to_empty_string():
    rule = Rule(
        rule_text="No powered boats",
        restriction_type="vessel_restriction",
        details="No powered boats",
    )
    assert rule.exception == ""
    assert rule.model_dump()["exception"] == ""


def test_exception_accepts_verbatim_substring():
    rule = Rule(
        rule_text="No powered boats, except electric motors.",
        restriction_type="vessel_restriction",
        details="No powered boats",
        exception="except electric motors",
    )
    assert rule.exception == "except electric motors"


def test_exception_not_in_rule_text_rejected():
    with pytest.raises(Exception, match="exception not found in rule_text"):
        Rule(
            rule_text="No powered boats.",
            restriction_type="vessel_restriction",
            details="No powered boats",
            exception="except electric motors",
        )


def test_exception_with_ellipsis_rejected():
    with pytest.raises(Exception, match="contains '...'"):
        Rule(
            rule_text="No powered boats, except electric motors.",
            restriction_type="vessel_restriction",
            details="No powered boats",
            exception="except ... motors",
        )


def test_exception_normalizes_bold_and_whitespace():
    # rule_text carries bold markup; exception should still chain after
    # normalization (same rule as location_text).
    rule = Rule(
        rule_text="**No Fishing**, except as noted below.",
        restriction_type="closure",
        details="No fishing",
        exception="except as noted below",
    )
    assert rule.exception == "except as noted below"
