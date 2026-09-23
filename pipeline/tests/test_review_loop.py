"""The review -> repass loop, end to end on fixture files. NOTHING here dispatches: the `claude`
CLI is stubbed wherever it would run.

The loop was broken in four places at once and no test noticed, because nothing rendered a review
prompt or read a review back: the prompt read a deleted file, `repass` passed a flag that does not
exist, the flagged set was read off a field no catalogue entry can carry, and an unreadable review
was stored as a clean one. Reviews live in the work dir's reviews/ and are never stamped on an
entry; these tests pin each step of reading them back.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from pipeline.regs.parsing import io

SRC = "TRANQUILLE LAKE Rainbow trout daily quota = 8. Bait ban."


def _item(index: int, entry_id: str) -> dict:
    return {"index": index, "entry_id": entry_id, "item_id": "gnis:1", "name": entry_id.upper(),
            "region": "3", "raw_regs": SRC, "bindable_ids": ["dam"],
            "boundaries": [["dam", "Tranquille Dam", "split", []]], "symbols": ["Classified"]}


def _entry(entry_id: str) -> dict:
    return {"entry_id": entry_id, "name": "X", "region": "3", "regs_verbatim": SRC,
            "rules": [{"rule_id": "t.r1", "type": "retention_limit", "species": ["RB"],
                       "take": 8, "verbatim": "Rainbow trout daily quota = 8"}]}


def _work(tmp_path: Path) -> Path:
    """A work dir holding one parsed batch of two rows: indices 5 and 6."""
    work = tmp_path / "parse"
    for sub in ("batches", "responses", "reviews"):
        (work / sub).mkdir(parents=True)
    (work / "batches" / "batch_000.json").write_text(json.dumps(
        {"batch": 0, "rows_digest": "x", "items": [_item(5, "r3:a@3-1"), _item(6, "r3:b@3-2")]}))
    (work / "responses" / "batch_000.json").write_text(json.dumps(
        [{"index": 5, "entry": _entry("r3:a@3-1")}, {"index": 6, "entry": _entry("r3:b@3-2")}]))
    (work / "manifest.json").write_text(json.dumps(
        {"batches": [{"id": 0, "count": 2, "indices": [5, 6]}]}))
    return work


def _review(work: Path, obj: dict, bid: int = 0) -> Path:
    p = work / "reviews" / f"batch_{bid:03d}.review.json"
    p.write_text(json.dumps(obj))
    resp = work / "responses" / f"batch_{bid:03d}.json"
    if resp.exists():                                   # a review is written after its response
        os.utime(p, (resp.stat().st_mtime + 5,) * 2)
    return p


FINDINGS = {"verdict": "changes_requested", "issues": [
    {"index": 6, "severity": "high", "problem": "quota is 8, not 18", "fix": "take: 8"},
    {"index": 5, "severity": "low", "problem": "nit"},
    {"index": 6, "severity": "medium", "problem": "bait ban dropped"}]}


# --- rendering ---------------------------------------------------------------------------------

def test_a_review_prompt_renders_from_a_batch_and_its_response(tmp_path):
    """The smoke test that was missing when the prompt read a deleted RULE_STANDARDS.md."""
    from pipeline.regs.parsing.review_exporter import render_from_files
    work = _work(tmp_path)
    text = render_from_files(work / "batches" / "batch_000.json",
                             work / "responses" / "batch_000.json")
    assert "## ITEM index=5" in text and "## ITEM index=6" in text
    assert "`dam`" in text                                      # the boundary menu
    assert '"entry_id": "r3:b@3-2"' in text                     # the produced entry
    assert text.rstrip().endswith("do not call any tools.")    # one envelope, last


def test_the_review_prompt_has_one_output_contract():
    """The checklist and the envelope used to ask for two different shapes. Only the envelope,
    which `dispatch` and `read_review_findings` read, may describe output."""
    from pipeline.regs.parsing.review_exporter import _ENVELOPE, _REVIEW_PROMPT
    checklist = _REVIEW_PROMPT.read_text(encoding="utf-8")
    assert '"verdict"' not in checklist and "What to output" not in checklist
    for key in ('"index"', '"severity"', '"problem"', '"fix"', "changes_requested"):
        assert key in _ENVELOPE


# --- dispatch records what came back ---------------------------------------------------------

def _run_review(work: Path, monkeypatch, stdout: str) -> dict:
    import pipeline.regs.parsing.dispatch as d
    monkeypatch.setattr(d.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(
        a, 0, stdout=stdout, stderr=""))
    d._dispatch_reviews(work / "batches", work / "responses", work / "reviews", [0],
                        claude_bin="claude-stub", cli_flags=[], cwd=work, timeout=5,
                        concurrency=1, force=True, review_model="stub")
    return json.loads((work / "reviews" / "batch_000.review.json").read_text())


@pytest.mark.parametrize("reply", ["I think these look fine!", '{"issues": []}',
                                   '{"verdict": "", "issues": []}', "[]"])
def test_an_unreadable_review_is_a_FAILED_review_not_a_clean_one(tmp_path, monkeypatch, reply):
    """It was normalised to `{verdict: "", issues: []}` — indistinguishable from a pass."""
    work = _work(tmp_path)
    stored = _run_review(work, monkeypatch, reply)
    assert stored["error"] == io.REVIEW_FAILED and "verdict" not in stored
    assert io.read_reviews(work)[0]["state"] == "failed"
    assert io.read_review_findings(work) == {}


def test_a_readable_review_is_stored_with_its_verdict(tmp_path, monkeypatch):
    work = _work(tmp_path)
    stored = _run_review(work, monkeypatch, json.dumps({"result": json.dumps(FINDINGS)}))
    assert stored["verdict"] == "changes_requested" and len(stored["issues"]) == 3
    assert io.read_reviews(work)[0]["state"] == "changes_requested"


# --- reading findings back --------------------------------------------------------------------

def test_findings_are_joined_to_entry_ids_through_the_batch(tmp_path):
    work = _work(tmp_path)
    _review(work, FINDINGS)
    got = io.read_review_findings(work)
    assert list(got) == ["r3:b@3-2"]                          # index 6; the low nit on 5 is not
    assert got["r3:b@3-2"] == ["[high] quota is 8, not 18 -> fix: take: 8",
                               "[medium] bait ban dropped"]


def test_a_review_of_an_older_parse_is_stale_and_says_nothing(tmp_path):
    work = _work(tmp_path)
    p = _review(work, FINDINGS)
    resp = work / "responses" / "batch_000.json"
    os.utime(resp, (p.stat().st_mtime + 5,) * 2)             # re-parsed after the review
    assert io.read_reviews(work)[0]["state"] == "stale"
    assert io.read_review_findings(work) == {}


def test_an_issue_naming_a_row_not_in_its_batch_is_an_error(tmp_path):
    """The batch was replaced under the review. Acting on it would re-parse the wrong water."""
    work = _work(tmp_path)
    _review(work, {"verdict": "changes_requested",
                   "issues": [{"index": 99, "severity": "high", "problem": "x"}]})
    with pytest.raises(ValueError, match="index 99"):
        io.read_review_findings(work)


# --- batch_exporter --flagged -----------------------------------------------------------------

def _run_exporter(monkeypatch, work: Path) -> dict:
    """`batch_exporter.main(--flagged)` with the corpus, registry and export stubbed. Returns what
    reached `export`, and whether the reviews still existed when it was called."""
    import pipeline.regs.parsing.batch_exporter as be
    seen: dict = {}

    def fake_export(rows, registry, out_dir, *a, only_ids=None, review_hints=None, **k):
        seen.update(only_ids=only_ids, review_hints=review_hints,
                    reviews_present=bool(list((out_dir / "reviews").glob("*.review.json"))))
        for f in (out_dir / "reviews").glob("*"):          # what the real export does to them
            f.unlink()
        return {"pending_count": len(only_ids or ()), "batches": [], "no_registry_count": 0,
                "unmatched": [], "excluded_empty": [], "skipped_existing": []}

    monkeypatch.setattr(be, "load_synopsis_rows", lambda: [])
    monkeypatch.setattr(be, "load_registry", lambda *_: {})
    monkeypatch.setattr(be, "load_overrides", lambda *_: {})
    monkeypatch.setattr(be, "export", fake_export)
    monkeypatch.setattr("sys.argv", ["batch_exporter", "--flagged", "--registry", "x",
                                     "--out-dir", str(work),
                                     "--entries-dir", str(work / "no-entries")])
    be.main()
    return seen


def test_flagged_reads_the_findings_BEFORE_the_export_clears_them(tmp_path, monkeypatch):
    work = _work(tmp_path)
    _review(work, FINDINGS)
    seen = _run_exporter(monkeypatch, work)
    assert seen["reviews_present"]
    assert seen["only_ids"] == {"r3:b@3-2"}
    assert seen["review_hints"]["r3:b@3-2"][0].startswith("[high] quota is 8")
    kept = json.loads((work / "repass.json").read_text())          # survives the wipe
    assert kept["findings"] == seen["review_hints"]


def test_flagged_with_nothing_flagged_exports_nothing(tmp_path, monkeypatch):
    """An empty selection used to export zero batches — which deletes every response and review
    as an orphan — and hand dispatch a manifest to `--force` over."""
    work = _work(tmp_path)
    _review(work, {"verdict": "pass", "issues": []})
    with pytest.raises(SystemExit, match="nothing to re-parse"):
        _run_exporter(monkeypatch, work)
    assert (work / "reviews" / "batch_000.review.json").exists()
    assert not (work / "repass.json").exists()


def test_run_parse_repass_passes_flags_the_scripts_accept():
    """`repass` passed `--only-flagged`, which argparse rejects, AFTER the preflight."""
    script = Path("pipeline/regs/parsing/run_parse.sh").read_text(encoding="utf-8")
    block = script[script.index("  repass)"):script.index("  status)")]
    assert '"${EXPORT[@]}" --flagged' in block
    assert '"${DISPATCH[@]}" --model "$ESCALATE_MODEL" --force' in block
    assert "parse_review" not in script and "prune" not in script
    assert subprocess.run(["bash", "-n", "pipeline/regs/parsing/run_parse.sh"]).returncode == 0
