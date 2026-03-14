"""Tests for v2 parsing models — Rule, ParsedEntry, ParsedBatch, validate_batch."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from regulation_mapping_v2.parsing.models import (
    ParsedBatch,
    ParsedEntry,
    RestrictionType,
    Rule,
    validate_batch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_rule(**overrides: Any) -> Dict[str, Any]:
    """Build a minimal valid Rule dict with optional overrides."""
    base = {
        "rule_text": "No powered boats",
        "restriction_type": "vessel_restriction",
        "details": "No powered boats",
        "location_text": "",
        "dates": [],
    }
    base.update(overrides)
    return base


def _simple_entry(**overrides: Any) -> Dict[str, Any]:
    """Build a minimal valid ParsedEntry dict with optional overrides."""
    base = {
        "regs_verbatim": "No powered boats",
        "includes_tributaries": False,
        "tributary_only": False,
        "entry_location_text": "",
        "rules": [_simple_rule()],
        "audit_log": [],
    }
    base.update(overrides)
    return base


# ===========================================================================
# RestrictionType
# ===========================================================================


class TestRestrictionType:
    def test_all_values(self) -> None:
        expected = {
            "closure",
            "harvest",
            "gear_restriction",
            "vessel_restriction",
            "licensing",
            "note",
        }
        assert {t.value for t in RestrictionType} == expected

    def test_from_string(self) -> None:
        assert RestrictionType("closure") == RestrictionType.CLOSURE


# ===========================================================================
# Rule
# ===========================================================================


class TestRule:
    def test_valid_rule(self) -> None:
        rule = Rule.model_validate(_simple_rule())
        assert rule.rule_text == "No powered boats"
        assert rule.restriction_type == RestrictionType.VESSEL_RESTRICTION
        assert rule.details == "No powered boats"
        assert rule.location_text == ""
        assert rule.dates == []

    def test_frozen(self) -> None:
        rule = Rule.model_validate(_simple_rule())
        with pytest.raises(Exception):
            rule.details = "changed"

    def test_empty_rule_text_fails(self) -> None:
        with pytest.raises(Exception, match="rule_text is empty"):
            Rule.model_validate(_simple_rule(rule_text=""))

    def test_empty_details_fails(self) -> None:
        with pytest.raises(Exception, match="details is empty"):
            Rule.model_validate(_simple_rule(details=""))

    def test_ellipsis_in_rule_text_fails(self) -> None:
        with pytest.raises(Exception, match="contains '...'"):
            Rule.model_validate(_simple_rule(rule_text="No powered boats..."))

    def test_ellipsis_in_location_text_fails(self) -> None:
        with pytest.raises(Exception, match="contains '...'"):
            Rule.model_validate(
                _simple_rule(
                    rule_text="No fishing upstream of the bridge",
                    location_text="upstream of...",
                )
            )

    def test_location_not_in_rule_text_fails(self) -> None:
        with pytest.raises(Exception, match="location_text not found"):
            Rule.model_validate(
                _simple_rule(
                    rule_text="No fishing here",
                    location_text="upstream of McKinley Lake",
                )
            )

    def test_location_in_rule_text_passes(self) -> None:
        rule = Rule.model_validate(
            _simple_rule(
                rule_text="No fishing upstream of McKinley Lake",
                location_text="upstream of McKinley Lake",
            )
        )
        assert rule.location_text == "upstream of McKinley Lake"

    def test_empty_location_always_passes(self) -> None:
        rule = Rule.model_validate(_simple_rule(location_text=""))
        assert rule.location_text == ""

    def test_date_in_rule_text_passes(self) -> None:
        rule = Rule.model_validate(
            _simple_rule(
                rule_text="Jan 1 - Apr 30: No fishing",
                restriction_type="closure",
                details="No fishing Jan 1 - Apr 30",
                dates=["Jan 1 - Apr 30"],
            )
        )
        assert rule.dates == ["Jan 1 - Apr 30"]

    def test_date_not_in_rule_text_fails(self) -> None:
        with pytest.raises(Exception, match="not found in rule_text"):
            Rule.model_validate(
                _simple_rule(
                    rule_text="No fishing in summer",
                    dates=["Jan 1 - Apr 30"],
                )
            )

    def test_date_with_newline_fails(self) -> None:
        with pytest.raises(Exception, match="contains newlines"):
            Rule.model_validate(_simple_rule(dates=["Jan 1\n- Apr 30"]))

    def test_date_with_asterisk_fails(self) -> None:
        with pytest.raises(Exception, match="contains asterisks"):
            Rule.model_validate(_simple_rule(dates=["Jan 1* - Apr 30"]))

    def test_bold_markers_normalized_for_chain(self) -> None:
        """Bold markers (**) should be stripped during validation comparison."""
        rule = Rule.model_validate(
            _simple_rule(
                rule_text="**Class II water.**",
                restriction_type="licensing",
                details="Class II water",
            )
        )
        assert rule.rule_text == "**Class II water.**"


# ===========================================================================
# ParsedEntry
# ===========================================================================


class TestParsedEntry:
    def test_valid_entry(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry())
        assert entry.regs_verbatim == "No powered boats"
        assert entry.includes_tributaries is False
        assert entry.tributary_only is False
        assert len(entry.rules) == 1

    def test_frozen(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry())
        with pytest.raises(Exception):
            entry.tributary_only = True

    def test_empty_regs_verbatim_fails(self) -> None:
        with pytest.raises(Exception, match="regs_verbatim is empty"):
            ParsedEntry.model_validate(_simple_entry(regs_verbatim=""))

    def test_no_rules_fails(self) -> None:
        with pytest.raises(Exception):
            ParsedEntry.model_validate(_simple_entry(rules=[]))

    def test_rule_text_not_in_regs_fails(self) -> None:
        with pytest.raises(Exception, match="rule_text not found in regs_verbatim"):
            ParsedEntry.model_validate(
                _simple_entry(
                    regs_verbatim="Bait ban",
                    rules=[_simple_rule(rule_text="No powered boats")],
                )
            )

    def test_rule_text_in_regs_passes(self) -> None:
        entry = ParsedEntry.model_validate(
            _simple_entry(
                regs_verbatim="No powered boats. Bait ban.",
                rules=[
                    _simple_rule(rule_text="No powered boats."),
                    _simple_rule(
                        rule_text="Bait ban.",
                        restriction_type="gear_restriction",
                        details="Bait ban",
                    ),
                ],
            )
        )
        assert len(entry.rules) == 2

    def test_keyword_coverage_catches_missed_restriction(self) -> None:
        with pytest.raises(Exception, match="Keyword 'bait ban'.*missing"):
            ParsedEntry.model_validate(
                _simple_entry(
                    regs_verbatim="Bait ban. No powered boats.",
                    rules=[_simple_rule(rule_text="No powered boats.")],
                )
            )

    def test_includes_tributaries_bool(self) -> None:
        for val in (True, False):
            entry = ParsedEntry.model_validate(_simple_entry(includes_tributaries=val))
            assert entry.includes_tributaries is val

    def test_tributary_only(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry(tributary_only=True))
        assert entry.tributary_only is True

    def test_entry_location_text(self) -> None:
        entry = ParsedEntry.model_validate(
            _simple_entry(
                entry_location_text="downstream of Adams Lake",
            )
        )
        assert entry.entry_location_text == "downstream of Adams Lake"

    def test_multi_rule_entry(self) -> None:
        regs = "Bait ban.\nDaily quota of 2 trout."
        entry = ParsedEntry.model_validate(
            _simple_entry(
                regs_verbatim=regs,
                rules=[
                    _simple_rule(
                        rule_text="Bait ban.",
                        restriction_type="gear_restriction",
                        details="Bait ban",
                    ),
                    _simple_rule(
                        rule_text="Daily quota of 2 trout.",
                        restriction_type="harvest",
                        details="Daily quota of 2 trout",
                    ),
                ],
            )
        )
        assert len(entry.rules) == 2


# ===========================================================================
# ParsedBatch
# ===========================================================================


class TestParsedBatch:
    def test_valid_batch(self) -> None:
        batch = ParsedBatch.model_validate(
            {
                "entries": [_simple_entry(), _simple_entry()],
            }
        )
        assert len(batch.entries) == 2

    def test_empty_batch(self) -> None:
        batch = ParsedBatch.model_validate({"entries": []})
        assert len(batch.entries) == 0


# ===========================================================================
# validate_batch
# ===========================================================================


class TestValidateBatch:
    def test_matching_batch(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry())
        row = {"water": "ALICE LAKE", "raw_regs": "No powered boats", "symbols": []}
        errors = validate_batch([entry], [row])
        assert errors == []

    def test_regs_mismatch(self) -> None:
        entry = ParsedEntry.model_validate(
            _simple_entry(regs_verbatim="No powered boats")
        )
        row = {"water": "ALICE LAKE", "raw_regs": "Different text", "symbols": []}
        errors = validate_batch([entry], [row])
        assert len(errors) == 1
        assert "regs_verbatim mismatch" in errors[0]["errors"][0]

    def test_count_mismatch(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry())
        errors = validate_batch([entry], [])
        assert len(errors) == 1
        assert "Count mismatch" in errors[0]["errors"][0]

    def test_includes_tribs_symbol_check(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry(includes_tributaries=False))
        row = {
            "water": "TEST LAKE",
            "raw_regs": "No powered boats",
            "symbols": ["Incl. Tribs"],
        }
        errors = validate_batch([entry], [row])
        assert len(errors) == 1
        assert "Incl. Tribs" in errors[0]["errors"][0]

    def test_includes_tribs_symbol_passes_when_true(self) -> None:
        entry = ParsedEntry.model_validate(_simple_entry(includes_tributaries=True))
        row = {
            "water": "TEST LAKE",
            "raw_regs": "No powered boats",
            "symbols": ["Incl. Tribs"],
        }
        errors = validate_batch([entry], [row])
        assert errors == []

    def test_includes_tribs_true_without_evidence_fails(self) -> None:
        """includes_tributaries=True with no symbol or text marker should fail."""
        entry = ParsedEntry.model_validate(_simple_entry(includes_tributaries=True))
        row = {
            "water": "TEST LAKE",
            "raw_regs": "No powered boats",
            "symbols": [],
        }
        errors = validate_batch([entry], [row])
        assert len(errors) == 1
        assert "no tributary marker" in errors[0]["errors"][0].lower()

    def test_includes_tribs_true_with_text_marker_passes(self) -> None:
        """includes_tributaries=True justified by [Includes Tributaries] in raw_regs."""
        entry = ParsedEntry.model_validate(
            _simple_entry(
                regs_verbatim="[Includes Tributaries] No powered boats",
                includes_tributaries=True,
            )
        )
        row = {
            "water": "TEST LAKE",
            "raw_regs": "[Includes Tributaries] No powered boats",
            "symbols": [],
        }
        errors = validate_batch([entry], [row])
        assert errors == []


# ===========================================================================
# Examples validation
# ===========================================================================


class TestExamplesValidation:
    """Run every example from examples.json through model + batch validation."""

    def test_all_examples_pass_model_validation(self) -> None:
        examples_path = (
            Path(__file__).resolve().parent.parent / "parsing" / "examples.json"
        )
        examples = json.loads(examples_path.read_text(encoding="utf-8"))
        assert len(examples) > 0, "No examples found"

        for i, ex in enumerate(examples):
            water = ex["input"]["water"]
            # Model validation
            entry = ParsedEntry.model_validate(ex["output"])
            assert (
                entry is not None
            ), f"Example {i} ({water}): model_validate returned None"
            # Batch validation (regs_verbatim echo + includes_tributaries checks)
            errors = validate_batch([entry], [ex["input"]])
            assert errors == [], f"Example {i} ({water}): {errors}"


# ===========================================================================
# Round-trip serialization
# ===========================================================================


class TestSerialization:
    def test_entry_round_trip(self) -> None:
        data = _simple_entry()
        entry = ParsedEntry.model_validate(data)
        dumped = entry.model_dump(mode="json")
        rebuilt = ParsedEntry.model_validate(dumped)
        assert rebuilt.regs_verbatim == entry.regs_verbatim
        assert rebuilt.rules[0].rule_text == entry.rules[0].rule_text

    def test_batch_round_trip(self) -> None:
        data = {"entries": [_simple_entry(), _simple_entry()]}
        batch = ParsedBatch.model_validate(data)
        dumped = batch.model_dump(mode="json")
        rebuilt = ParsedBatch.model_validate(dumped)
        assert len(rebuilt.entries) == 2

    def test_json_schema_generation(self) -> None:
        """ParsedBatch must produce a valid JSON schema for structured output."""
        schema = ParsedBatch.model_json_schema()
        assert "properties" in schema
        assert "entries" in schema["properties"]
