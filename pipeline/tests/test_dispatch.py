"""Smoke tests for the dispatch fan-out loop — run with the real claude CLI stubbed out.

These exist because the loop shells out to `claude`, so it isn't otherwise unit-tested — and a missing
`as_completed` import once crashed the whole run AFTER the pool had queued every batch (the pool's
shutdown then drained them, burning credits). These assert the loop's control flow: happy path writes
run_state with all batches done; a CreditExhausted stops cleanly and cancels the rest.
"""

import json
from pathlib import Path

import pipeline.parsing.dispatch as d


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
