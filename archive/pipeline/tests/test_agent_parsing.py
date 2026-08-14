"""Focused unit tests for the agent-parsing workflow.

All tests use synthetic, injected rows — they do not depend on the real
synopsis data or a live session — so they exercise the pure logic (index
parity, prompt rendering, response parsing, validation, digest drift) in
isolation.
"""

import json
from pathlib import Path

import pytest

from pipeline.agent_parsing.batch_exporter import build_batches
from pipeline.agent_parsing.common import compute_rows_digest
from pipeline.agent_parsing.compare import compare_entry, compare_sessions
from pipeline.agent_parsing.ingest import (
    ingest_responses,
    parse_response_text,
    validate_entry,
)
from pipeline.agent_parsing.prompt_render import render_batch_prompt, render_review_prompt
from pipeline.agent_parsing.review_exporter import render_review_for_response
from pipeline.parsing.rows import load_synopsis_rows

# A known-good ParsedEntry (mirrors the ALICE LAKE canonical example).
ALICE_RAW = "[Includes Tributaries] No powered boats"
ALICE_ROW = {"water": "ALICE LAKE", "raw_regs": ALICE_RAW, "symbols": ["Stocked", "Incl. Tribs"]}
ALICE_ENTRY = {
    "regs_verbatim": ALICE_RAW,
    "includes_tributaries": True,
    "tributary_only": False,
    "entry_location_text": "",
    "rules": [
        {
            "rule_text": "No powered boats",
            "restriction_type": "vessel_restriction",
            "details": "No powered boats",
            "location_text": "",
            "dates": [],
        }
    ],
    "audit_log": [],
}


def _response(objs):
    return json.dumps(objs)


def test_load_synopsis_rows_index_parity(tmp_path: Path):
    pages = [
        {"context": {"region": "Region 1"}, "rows": [
            {"water": "A", "raw_regs": "x"},
            {"water": "B", "raw_regs": "y", "region": "Override"},
        ]},
        {"context": {}, "rows": [{"water": "C", "raw_regs": "z"}]},
    ]
    p = tmp_path / "raw.json"
    p.write_text(json.dumps(pages), encoding="utf-8")

    rows = load_synopsis_rows(p)
    assert [r["water"] for r in rows] == ["A", "B", "C"]
    assert rows[0]["region"] == "Region 1"  # backfilled
    assert rows[1]["region"] == "Override"  # not overwritten
    assert "region" not in rows[2]  # no page region, none on row


def test_build_batches_chunks():
    assert build_batches([0, 1, 2, 3, 4], 2) == [[0, 1], [2, 3], [4]]
    with pytest.raises(ValueError):
        build_batches([0], 0)


def test_render_includes_index_and_envelope():
    prompt = render_batch_prompt([{"index": 7, "water": "ALICE LAKE", "raw_regs": ALICE_RAW}])
    assert '"index": 7' in prompt
    assert ALICE_RAW in prompt
    assert "OUTPUT FORMAT" in prompt


def test_render_review_prompt_includes_spec_candidate_and_rubric():
    items = [{"index": 7, "water": "ALICE LAKE", "raw_regs": ALICE_RAW}]
    results = [{"index": 7, "entry": ALICE_ENTRY}]
    prompt = render_review_prompt(items, results)
    # Reviewer must see the same canonical spec + the batch row...
    assert ALICE_RAW in prompt
    assert '"index": 7' in prompt
    # ...the candidate output it is auditing...
    assert "No powered boats" in prompt
    # ...and the review framing (not the parse output envelope).
    assert "REVIEW AGENT" in prompt
    assert "CROSS-ROW CONSISTENCY" in prompt
    assert '"verdict"' in prompt
    assert ".validate()" in prompt
    # It must NOT carry the parse-mode output envelope (that would tell the
    # reviewer to emit ParsedEntry objects instead of a review report).
    assert "Copy each input item's" not in prompt


def test_render_review_for_response_writes_prompt(tmp_path: Path):
    out_dir = tmp_path / "agent"
    (out_dir / "batches").mkdir(parents=True)
    (out_dir / "responses").mkdir(parents=True)
    items = [{"index": 7, "water": "ALICE LAKE", "raw_regs": ALICE_RAW, "symbols": []}]
    (out_dir / "batches" / "batch_000.json").write_text(
        json.dumps({"batch": 0, "indices": [7], "items": items}), encoding="utf-8"
    )
    resp = out_dir / "responses" / "batch_000.json"
    resp.write_text(_response([{"index": 7, "entry": ALICE_ENTRY}]), encoding="utf-8")

    review_file = render_review_for_response(resp, out_dir)
    assert review_file == out_dir / "batches" / "batch_000.review.prompt.txt"
    text = review_file.read_text(encoding="utf-8")
    assert "REVIEW AGENT" in text
    assert ALICE_RAW in text
    assert '"verdict"' in text


def test_render_review_for_response_missing_batch(tmp_path: Path):
    out_dir = tmp_path / "agent"
    (out_dir / "responses").mkdir(parents=True)
    resp = out_dir / "responses" / "batch_099.json"
    resp.write_text(_response([{"index": 0, "entry": ALICE_ENTRY}]), encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        render_review_for_response(resp, out_dir)


def test_parse_response_strips_fences():
    text = "```json\n[{\"index\": 0, \"entry\": {}}]\n```"
    data = parse_response_text(text)
    assert data == [{"index": 0, "entry": {}}]


def test_parse_response_rejects_non_array():
    with pytest.raises(ValueError):
        parse_response_text('{"index": 0}')


def test_validate_entry_accepts_valid():
    parsed, errors = validate_entry(0, ALICE_ENTRY, ALICE_ROW)
    assert parsed is not None
    assert errors == []


def test_validate_entry_echo_mismatch():
    bad = dict(ALICE_ENTRY, regs_verbatim="not the same")
    parsed, errors = validate_entry(0, bad, ALICE_ROW)
    assert parsed is None
    assert errors


def test_ingest_accepts_and_reports():
    text = _response([{"index": 0, "entry": ALICE_ENTRY}])
    results, report = ingest_responses([text], [ALICE_ROW], ["pending"], {0}, force=False)
    assert set(results) == {0}
    assert report["accepted"] == [0]


def test_ingest_dupe_rejection():
    text = _response([
        {"index": 0, "entry": ALICE_ENTRY},
        {"index": 0, "entry": ALICE_ENTRY},
    ])
    results, report = ingest_responses([text], [ALICE_ROW], ["pending"], {0}, force=False)
    assert report["duplicates"] == [0]
    assert set(results) == {0}


def test_ingest_echo_mismatch_not_applied():
    bad = dict(ALICE_ENTRY, regs_verbatim="wrong")
    text = _response([{"index": 0, "entry": bad}])
    results, report = ingest_responses([text], [ALICE_ROW], ["pending"], {0}, force=False)
    assert results == {}
    assert report["failed"]


def test_ingest_skips_success_without_force():
    text = _response([{"index": 0, "entry": ALICE_ENTRY}])
    results, report = ingest_responses([text], [ALICE_ROW], ["success"], {0}, force=False)
    assert results == {}
    assert report["skipped_success"] == [0]

    results2, _ = ingest_responses([text], [ALICE_ROW], ["success"], {0}, force=True)
    assert set(results2) == {0}


def test_ingest_out_of_range():
    text = _response([{"index": 5, "entry": ALICE_ENTRY}])
    results, report = ingest_responses([text], [ALICE_ROW], ["pending"], {0}, force=False)
    assert report["out_of_range"] == [5]
    assert results == {}


def test_compute_rows_digest_changes_on_data_change():
    d1 = compute_rows_digest([ALICE_ROW])
    d2 = compute_rows_digest([dict(ALICE_ROW, raw_regs="different")])
    assert d1 != d2


# --- comparison -----------------------------------------------------------

def _entry(regs=ALICE_RAW, incl=True, trib_only=False, rules=None):
    return {
        "regs_verbatim": regs,
        "includes_tributaries": incl,
        "tributary_only": trib_only,
        "entry_location_text": "",
        "rules": rules if rules is not None else [
            {"rule_text": "No powered boats", "restriction_type": "vessel_restriction",
             "details": "x", "location_text": "", "dates": []}
        ],
        "audit_log": [],
    }


def test_compare_entry_identical():
    assert compare_entry(_entry(), _entry()) == {}


def test_compare_entry_rule_count():
    a = _entry(rules=[
        {"rule_text": "a", "restriction_type": "note", "details": "a", "location_text": "", "dates": []},
        {"rule_text": "b", "restriction_type": "note", "details": "b", "location_text": "", "dates": []},
    ])
    diffs = compare_entry(_entry(rules=a["rules"][:1]), a)
    assert diffs["rule_count"] == (1, 2)


def test_compare_entry_tributary_and_types_and_dates():
    g = _entry(incl=True, rules=[
        {"rule_text": "r", "restriction_type": "closure", "details": "d",
         "location_text": "", "dates": ["Jan 1"]},
    ])
    a = _entry(incl=False, rules=[
        {"rule_text": "r", "restriction_type": "harvest", "details": "d",
         "location_text": "", "dates": ["Feb 2"]},
    ])
    diffs = compare_entry(g, a)
    assert diffs["includes_tributaries"] == (True, False)
    assert diffs["restriction_types"] == ({"closure": 1}, {"harvest": 1})
    assert diffs["dates"] == (["Jan 1"], ["Feb 2"])


def test_compare_sessions_coverage_and_counts():
    rows = [{"water": f"W{i}"} for i in range(4)]
    g = [_entry(), _entry(incl=True), None, _entry()]
    a = [_entry(), _entry(incl=False), _entry(), None]
    report = compare_sessions(g, a, rows)
    assert report["both_parsed"] == 2
    assert report["only_gemini"] == 1
    assert report["only_agent"] == 1
    assert report["identical"] == 1
    assert report["category_counts"]["includes_tributaries"] == 1
    assert [r["index"] for r in report["rows"]] == [1]
