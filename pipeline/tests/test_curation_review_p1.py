"""P1 quick confirm (owner 2026-10-28, curation-review backend `p1`): a row is quick only when every
check agrees; each disagreement is named."""
from __future__ import annotations

import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
sys.path.insert(0, str(BACKEND))

import p1  # noqa: E402

ROW = {"verify": "unmarked", "vintage": "current", "pv": {"status": "pre-verified"},
       "unused_curated_splits": 0, "low_cuts": None, "t89": None}
CUT = {"code": "ambiguous_cut", "severity": "A"}
PROP = {"kind": "ambiguous_cut", "pick": {"agrees": True}}


def test_quick_when_every_check_agrees():
    assert p1.why_not_quick(ROW, [CUT, {"code": "cut_picked", "severity": "B"}], [PROP]) == []


def test_each_disagreement_is_named():
    assert p1.why_not_quick({**ROW, "verify": "stale"}, [CUT], [PROP]) == ["mark stale"]
    assert p1.why_not_quick({**ROW, "vintage": "awaiting build"}, [], [PROP]) == ["awaiting build"]
    assert p1.why_not_quick({**ROW, "pv": None}, [], []) == ["second check: no proposal"]
    assert p1.why_not_quick(ROW, [], [{**PROP, "pick": {"agrees": False}}]) == [
        "a proposal's pick is not the builder's"]
    assert p1.why_not_quick(ROW, [{"code": "split_to_verify", "severity": "A"}], [PROP]) == [
        "pre-check split_to_verify"]
    assert p1.why_not_quick(ROW, [{"code": "unquoted_text", "severity": "B"}], [PROP]) == [
        "pre-check unquoted_text"]
    assert p1.why_not_quick(ROW, [], [PROP, {"kind": "unplaced", "pick": None}]) == [
        "an open proposal other than a cut pick"]
