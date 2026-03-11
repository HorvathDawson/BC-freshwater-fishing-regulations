"""
Tests for the post-parsing reviewer module.

Tests cover:
- Prompt building (structure, content)
- ReviewSession (save/load/resume)
- Report generation (pass/fail/warning counting)
"""

import json
import os
import pytest

from synopsis_pipeline.review_parsed import (
    build_review_prompt,
    ReviewSession,
    generate_report,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_entry(name: str, regs: str, rules: list) -> dict:
    """Build a minimal parsed entry dict for testing."""
    return {
        "identity": {"name_verbatim": name, "lookup_name": name.upper()},
        "regs_verbatim": regs,
        "rules": rules,
    }


SAMPLE_ENTRY = _make_entry(
    "TEST CREEK",
    "No Fishing Jan 1-Mar 31. Bait ban Apr 1-Jun 30.",
    [
        {
            "rule_text_verbatim": "No Fishing Jan 1-Mar 31.",
            "scope": {"type": "WHOLE_SYSTEM"},
            "restriction": {"type": "closure", "details": "No Fishing"},
        },
        {
            "rule_text_verbatim": "Bait ban Apr 1-Jun 30.",
            "scope": {"type": "WHOLE_SYSTEM"},
            "restriction": {"type": "gear", "details": "Bait ban"},
        },
    ],
)


# ── Prompt Building ──────────────────────────────────────────────────────────


class TestBuildReviewPrompt:
    def test_prompt_contains_waterbody_name(self):
        prompt = build_review_prompt([SAMPLE_ENTRY])
        assert "TEST CREEK" in prompt

    def test_prompt_contains_regs_verbatim(self):
        prompt = build_review_prompt([SAMPLE_ENTRY])
        assert "No Fishing Jan 1-Mar 31" in prompt

    def test_prompt_contains_parsed_rules(self):
        prompt = build_review_prompt([SAMPLE_ENTRY])
        assert "Bait ban" in prompt

    def test_prompt_num_items(self):
        prompt = build_review_prompt([SAMPLE_ENTRY, SAMPLE_ENTRY])
        # The prompt template says "You will receive {num_items} parsed regulation entries"
        assert "2 parsed regulation entries" in prompt

    def test_prompt_loads_template(self):
        prompt = build_review_prompt([SAMPLE_ENTRY])
        # Should contain key phrases from the template
        assert "SENIOR DATA QUALITY REVIEWER" in prompt
        assert "COMPLETENESS" in prompt


# ── ReviewSession Persistence ────────────────────────────────────────────────


class TestReviewSession:
    def test_create_session(self):
        session = ReviewSession(total_items=5)
        assert session.total_items == 5
        assert len(session.verdicts) == 5
        assert all(v is None for v in session.verdicts)
        assert session.processed == []

    def test_save_and_load(self, tmp_path):
        filepath = str(tmp_path / "review_session.json")
        session = ReviewSession(total_items=3)
        session.verdicts[0] = {"water": "TEST", "verdict": "PASS", "issues": []}
        session.processed = [0]

        session.save(filepath)
        loaded = ReviewSession.load(filepath)

        assert loaded is not None
        assert loaded.total_items == 3
        assert loaded.processed == [0]
        assert loaded.verdicts[0]["verdict"] == "PASS"
        assert loaded.verdicts[1] is None

    def test_load_nonexistent_returns_none(self, tmp_path):
        result = ReviewSession.load(str(tmp_path / "nope.json"))
        assert result is None

    def test_resume_preserves_progress(self, tmp_path):
        filepath = str(tmp_path / "review_session.json")
        session = ReviewSession(total_items=10)
        session.verdicts[0] = {"water": "A", "verdict": "PASS", "issues": []}
        session.verdicts[1] = {
            "water": "B",
            "verdict": "FAIL",
            "issues": [
                {
                    "severity": "CRITICAL",
                    "category": "missing_rule",
                    "description": "missing",
                }
            ],
        }
        session.processed = [0, 1]
        session.save(filepath)

        # Simulate resume
        resumed = ReviewSession.load(filepath)
        pending = [i for i in range(10) if i not in resumed.processed]
        assert len(pending) == 8
        assert 0 not in pending
        assert 1 not in pending


# ── Report Generation ────────────────────────────────────────────────────────


class TestGenerateReport:
    def test_all_pass(self):
        session = ReviewSession(total_items=3)
        for i in range(3):
            session.verdicts[i] = {"water": f"W{i}", "verdict": "PASS", "issues": []}
            session.processed.append(i)

        report = generate_report(session, [{} for _ in range(3)])
        assert report["pass"] == 3
        assert report["fail"] == 0
        assert report["pass_rate"] == "100.0%"
        assert report["failures"] == []

    def test_mixed_results(self):
        session = ReviewSession(total_items=4)
        session.verdicts[0] = {"water": "A", "verdict": "PASS", "issues": []}
        session.verdicts[1] = {
            "water": "B",
            "verdict": "FAIL",
            "issues": [
                {
                    "severity": "CRITICAL",
                    "category": "missing_rule",
                    "description": "Class I water missing",
                }
            ],
        }
        session.verdicts[2] = {
            "water": "C",
            "verdict": "PASS",
            "issues": [
                {
                    "severity": "WARNING",
                    "category": "scope_error",
                    "description": "minor scope concern",
                }
            ],
        }
        # verdicts[3] is None (not reviewed)
        session.processed = [0, 1, 2]

        report = generate_report(session, [{} for _ in range(4)])
        assert report["pass"] == 2
        assert report["fail"] == 1
        assert report["error"] == 1  # the None verdict
        assert report["warnings"] == 1
        assert len(report["failures"]) == 1
        assert report["failures"][0]["water"] == "B"

    def test_failure_details_populated(self):
        session = ReviewSession(total_items=1)
        session.verdicts[0] = {
            "water": "DEAN RIVER",
            "verdict": "FAIL",
            "issues": [
                {
                    "severity": "CRITICAL",
                    "category": "dropped_restriction",
                    "description": "Class I water dropped",
                },
                {
                    "severity": "CRITICAL",
                    "category": "compound_split",
                    "description": "Fly fishing only fragment",
                },
            ],
            "rule_count_expected": 14,
            "rule_count_actual": 10,
            "summary": "Missing 4 rules including classified water designations",
        }
        session.processed = [0]

        report = generate_report(session, [{}])
        fail = report["failures"][0]
        assert fail["rule_count_expected"] == 14
        assert fail["rule_count_actual"] == 10
        assert len(fail["issues"]) == 2
