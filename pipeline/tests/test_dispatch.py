"""Smoke tests for the dispatch fan-out loop — run with the real claude CLI stubbed out.

These exist because the loop shells out to `claude`, so it isn't otherwise unit-tested — and a missing
`as_completed` import once crashed the whole run AFTER the pool had queued every batch (the pool's
shutdown then drained them, burning credits). These assert the loop's control flow: happy path writes
run_state with all batches done; a CreditExhausted stops cleanly and cancels the rest.
"""

import json
from pathlib import Path

import pipeline.regs.parsing.dispatch as d


def _work(tmp_path: Path, n_batches: int) -> Path:
    work = tmp_path / "parse"
    (work / "batches").mkdir(parents=True)
    for b in range(n_batches):
        (work / "batches" / f"batch_{b:03d}.prompt.txt").write_text("prompt", encoding="utf-8")
    (work / "manifest.json").write_text(json.dumps(
        {"batches": [{"id": b, "count": 1, "indices": [b]} for b in range(n_batches)]}), encoding="utf-8")
    return work


def test_dispatch_happy_path_writes_run_state(tmp_path, monkeypatch):
    work = _work(tmp_path, 3)

    def fake(prompt_path, response_path, **kw):            # stub the CLI: write a response, return rows
        response_path.parent.mkdir(parents=True, exist_ok=True)
        response_path.write_text("[]", encoding="utf-8")
        return []

    monkeypatch.setattr(d, "dispatch_prompt", fake)
    monkeypatch.setattr("sys.argv", ["dispatch", "--work-dir", str(work), "--concurrency", "2"])
    d.main()

    state = json.loads((work / "run_state.json").read_text())
    assert state["done"] == 3 and state["remaining"] == 0 and not state["credit_stop"]
    assert all(v["status"] == "done" for v in state["batches"].values())


def test_extract_json_obj():
    assert d._extract_json_obj(json.dumps({"result": '{"verdict":"pass","issues":[]}'})) == \
        {"verdict": "pass", "issues": []}
    assert d._extract_json_obj('```json\n{"a": 1}\n```') == {"a": 1}
    assert d._extract_json_obj("not json") == {}


def test_flagged_batch_ids_picks_high_medium(tmp_path):
    manifest = {"batches": [{"id": 0, "indices": [0, 1]}, {"id": 1, "indices": [2, 3]}]}
    reviews = tmp_path / "reviews"
    reviews.mkdir()
    # batch 0: high finding (envelope-wrapped) -> flagged; batch 1: only low (bare json) -> not flagged
    (reviews / "batch_000.review.json").write_text(json.dumps(
        {"result": json.dumps({"verdict": "changes_requested",
                               "issues": [{"index": 1, "severity": "high", "problem": "wrong reach"}]})}))
    (reviews / "batch_001.review.json").write_text(json.dumps(
        {"verdict": "pass", "issues": [{"index": 2, "severity": "low", "problem": "nit"}]}))
    assert d._flagged_batch_ids(manifest, reviews) == [0]


def test_invalid_batch_ids_flags_bad_response(tmp_path):
    manifest = {"batches": [{"id": 0, "indices": [0]}, {"id": 1, "indices": [1]}]}
    batches = tmp_path / "batches"
    responses = tmp_path / "responses"
    batches.mkdir()
    responses.mkdir()
    (batches / "batch_000.json").write_text(json.dumps({"items": [
        {"index": 0, "entry_id": "e0", "raw_regs": "No fishing.", "bindable_ids": [],
         "no_registry": True, "registry_note": "unmatched: x"}]}))
    (batches / "batch_001.json").write_text(json.dumps({"items": [
        {"index": 1, "entry_id": "e1", "raw_regs": "No fishing.", "bindable_ids": []}]}))
    # batch 0: valid content-only entry
    (responses / "batch_000.json").write_text(json.dumps([{"index": 0, "entry": {
        "identity": {"name": "A"}, "regs_verbatim": "x", "rules": [
            {"rule_id": "r", "restriction_type": "closure", "details": "No fishing",
             "rule_text": "No fishing.", "extents": [], "needs_review": True, "review_reason": "nr"}]}}]))
    # batch 1: binds a split id that isn't in bindable_ids -> invalid
    (responses / "batch_001.json").write_text(json.dumps([{"index": 1, "entry": {
        "identity": {"name": "B"}, "regs_verbatim": "x", "rules": [
            {"rule_id": "r", "restriction_type": "closure", "details": "c", "rule_text": "No fishing.",
             "extents": [{"op": "upstream_of", "splits": ["nope"]}]}]}}]))
    assert d._invalid_batch_ids(manifest, batches, responses) == [1]


def test_covered_batch_ids(tmp_path):
    manifest = {"batches": [{"id": 0, "indices": [0, 1]}, {"id": 1, "indices": [2, 3]}]}
    batches = tmp_path / "batches"
    batches.mkdir()
    (batches / "batch_000.json").write_text(json.dumps({"items": [
        {"index": 0, "entry_id": "a"}, {"index": 1, "entry_id": "b"}]}))
    (batches / "batch_001.json").write_text(json.dumps({"items": [
        {"index": 2, "entry_id": "c"}, {"index": 3, "entry_id": "d"}]}))
    entries = tmp_path / "entries"
    entries.mkdir()
    # entries cover batch 0 fully (a, b); batch 1 only partially (c) -> not covered
    (entries / "region-1.json").write_text(json.dumps(
        {"region": "1", "entries": [{"entry_id": "a"}, {"entry_id": "b"}, {"entry_id": "c"}]}))
    assert d._covered_batch_ids(manifest, batches, entries) == {0}


def test_dispatch_skips_already_ingested_batches(tmp_path, monkeypatch):
    work = _work(tmp_path, 2)
    (work / "batches" / "batch_000.json").write_text(json.dumps({"items": [{"index": 0, "entry_id": "done0"}]}))
    (work / "batches" / "batch_001.json").write_text(json.dumps({"items": [{"index": 1, "entry_id": "todo1"}]}))
    entries = tmp_path / "entries"
    entries.mkdir()
    (entries / "region-1.json").write_text(json.dumps({"region": "1", "entries": [{"entry_id": "done0"}]}))

    dispatched: list[str] = []

    def fake(prompt_path, response_path, **kw):
        dispatched.append(prompt_path.name)
        response_path.parent.mkdir(parents=True, exist_ok=True)
        response_path.write_text("[]", encoding="utf-8")
        return []

    monkeypatch.setattr(d, "dispatch_prompt", fake)
    monkeypatch.setattr("sys.argv", ["dispatch", "--work-dir", str(work),
                                     "--entries-dir", str(entries), "--concurrency", "1"])
    d.main()
    assert dispatched == ["batch_001.prompt.txt"]         # batch 0 skipped (already ingested)
    state = json.loads((work / "run_state.json").read_text())
    assert state["batches"]["0"]["status"] == "ingested" and state["done"] == 2


def test_dispatch_credit_stop_halts_and_records(tmp_path, monkeypatch):
    work = _work(tmp_path, 4)

    def fake(prompt_path, response_path, **kw):
        raise d.CreditExhausted("usage limit")

    monkeypatch.setattr(d, "dispatch_prompt", fake)
    monkeypatch.setattr("sys.argv", ["dispatch", "--work-dir", str(work), "--concurrency", "1"])
    d.main()

    state = json.loads((work / "run_state.json").read_text())
    assert state["credit_stop"] is True
    assert state["done"] == 0                              # no response written on a credit stop
