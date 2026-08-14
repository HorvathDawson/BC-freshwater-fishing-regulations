"""Tests for the per-rule includes_tributaries override.

Covers the two layers that resolve tributary scope in Python:
  - Rule.includes_tributaries — the 3-valued per-rule field (None/True/False)
  - effective_includes_tributaries() — whether a parsed regulation BFS-expands
    to tributaries in Phase 3, honouring per-rule overrides.

The webapp mirror of this logic (rule filtering on tributary reaches) is
covered by the TypeScript type-check and behaves per the same rule:
    effective(rule) = rule.includes_tributaries if set else entry flag.
"""

from __future__ import annotations

import pytest

from pipeline.parsing.models import ParsedEntry, Rule
from pipeline.enrichment.feature_resolver import effective_includes_tributaries

# ---------------------------------------------------------------------------
# Rule model: 3-valued per-rule field
# ---------------------------------------------------------------------------


def test_rule_includes_tributaries_defaults_to_none():
    rule = Rule(rule_text="No fishing", restriction_type="closure", details="No fishing")
    assert rule.includes_tributaries is None


def test_rule_includes_tributaries_accepts_true_and_false():
    r_true = Rule(
        rule_text="No fishing including tributaries",
        restriction_type="closure",
        details="No fishing",
        includes_tributaries=True,
    )
    r_false = Rule(
        rule_text="No fishing (tributaries not included)",
        restriction_type="closure",
        details="No fishing",
        includes_tributaries=False,
    )
    assert r_true.includes_tributaries is True
    assert r_false.includes_tributaries is False


def test_none_serializes_as_inherit():
    rule = Rule(rule_text="Bait ban", restriction_type="gear_restriction", details="Bait ban")
    assert rule.model_dump()["includes_tributaries"] is None


def test_bracket_marker_in_rule_text_requires_true():
    """A '[Includes Tributaries]' marker inside rule_text forces the flag True."""
    rule = Rule(
        rule_text="No Fishing downstream of Bannon Creek[Includes Tributaries] July 1-Sept 30",
        restriction_type="closure",
        details="No fishing",
        includes_tributaries=True,
    )
    assert rule.includes_tributaries is True


def test_bracket_marker_with_none_flag_raises():
    with pytest.raises(ValueError, match=r"\[Includes Tributaries\]"):
        Rule(
            rule_text="No Fishing[Includes Tributaries] July 1-Sept 30",
            restriction_type="closure",
            details="No fishing",
        )


def test_bracket_marker_with_false_flag_raises():
    with pytest.raises(ValueError, match=r"\[Includes Tributaries\]"):
        Rule(
            rule_text="No Fishing[Includes Tributaries] July 1-Sept 30",
            restriction_type="closure",
            details="No fishing",
            includes_tributaries=False,
        )


# ---------------------------------------------------------------------------
# effective_includes_tributaries(): Phase 3 expansion decision
# ---------------------------------------------------------------------------


def _parsed(entry_incl: bool, rule_flags, tributary_only: bool = False):
    """Build a minimal parsed dict with the given per-rule override flags."""
    return {
        "includes_tributaries": entry_incl,
        "tributary_only": tributary_only,
        "rules": [{"includes_tributaries": f} for f in rule_flags],
    }


def test_no_parsed_data_is_false():
    assert effective_includes_tributaries(None) is False
    assert effective_includes_tributaries({}) is False


def test_entry_false_all_inherit_is_false():
    assert effective_includes_tributaries(_parsed(False, [None, None])) is False


def test_entry_true_all_inherit_is_true():
    assert effective_includes_tributaries(_parsed(True, [None, None])) is True


def test_entry_false_one_rule_true_is_true():
    # Somass / Pitt-inverse: a single rule opts into tributaries.
    assert effective_includes_tributaries(_parsed(False, [None, True])) is True


def test_entry_true_all_rules_false_is_false():
    # Empty-card guard: every rule opts out -> no expansion at all.
    assert effective_includes_tributaries(_parsed(True, [False, False])) is False


def test_entry_true_one_rule_false_still_true():
    # Pitt River: one rule excludes tributaries but others still apply.
    assert effective_includes_tributaries(_parsed(True, [False, None])) is True


def test_tributary_only_forces_true_even_if_rules_opt_out():
    assert effective_includes_tributaries(_parsed(True, [False], tributary_only=True)) is True


def test_no_rules_falls_back_to_entry_flag():
    assert effective_includes_tributaries({"includes_tributaries": True, "rules": []}) is True
    assert effective_includes_tributaries({"includes_tributaries": False, "rules": []}) is False


# ---------------------------------------------------------------------------
# End-to-end: ParsedEntry round-trips through the resolver helper
# ---------------------------------------------------------------------------


def test_parsed_entry_with_override_drives_expansion():
    entry = ParsedEntry(
        regs_verbatim="[Includes Tributaries] Bait ban. No fishing (tributaries not included).",
        includes_tributaries=True,
        rules=[
            Rule(rule_text="Bait ban.", restriction_type="gear_restriction", details="Bait ban"),
            Rule(
                rule_text="No fishing (tributaries not included).",
                restriction_type="closure",
                details="No fishing",
                includes_tributaries=False,
            ),
        ],
    )
    # Entry includes tributaries and the bait-ban rule inherits True, so the
    # regulation still expands even though one rule opts out.
    assert effective_includes_tributaries(entry.model_dump()) is True
