"""A RE-PARSE NEVER REPLACES AN ENTRY THE OWNER MARKED, AND THE LEDGER HOLDS THE ONE ENTRY DIGEST (T628 items 2-3).

`--replace-entry` / `--replace-edited` put a re-parse over a curator's edited entry (T553 item 4). An entry that carries
an owner mark is the owner's reviewed copy: either flag refuses it, naming the mark, and there is no flag past that.
The ledger that decides "edited since ingest wrote it" now holds `io.entry_digest` and names it; a ledger from before
cannot be compared and says so instead of comparing two different digests.

Every test builds its own tree, marks store and ledger under `tmp_path`: nothing here reads or writes the live corpus.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from pipeline.regs.parsing import ingest_catalogue as IC
from pipeline.regs.parsing import io
from pipeline.regs.review import marks as M
from pipeline.tests.test_ingest_catalogue import SRC, _ingest_once, _on_disk

EID = "r3:tranquille@3-29"
CURATED_EXTENT = [{"op": "whole", "within_area": "area:region:3"}]


def _mark(*, scope="entry", rule_id=None, verdict="verified", target=EID, date="2026-11-01") -> M.Mark:
    answers = {k: {"wrong": "n", "unsure": "?"}.get(verdict, "y") for k in M.CHECKS[scope]}
    return M.Mark(scope=scope, target=target, rule_id=rule_id, verdict=verdict, answers=answers,
                  problem="placement" if verdict == "wrong" else None,
                  note="the stretch is the wrong one" if verdict == "wrong" else "", reviewer="owner", date=date,
                  pdf_sha256="a" * 64, pages=[30],
                  prints=M.Prints(text="t", records=tuple(
                      M.RecordPrint(key=k, meaning="m", binding="b")
                      for k in ["entry"] + ([f"rule:{rule_id}"] if rule_id else []))))


def _store(tmp_path, *marks: M.Mark):
    path = tmp_path / "review" / "marks.jsonl"
    path.parent.mkdir(exist_ok=True)
    path.write_text("".join(M.line(m) for m in marks), encoding="utf-8")
    return path


def _edited_entry(tmp_path):
    """An entry ingest wrote, then a curator edited (placed); and a second parse of the same row, ready to write."""
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    IC.write(first, out, ledger=ledger)
    doc = json.loads((out / "region-3.json").read_text())
    doc["entries"][0]["rules"][0]["extents"] = CURATED_EXTENT
    (out / "region-3.json").write_text(json.dumps(doc))
    _, second = _ingest_once(tmp_path, rules=[
        {"rule_id": "t.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
         "gear": [{"slot": "bait", "ban": ["any_bait"]}]}])
    return out, ledger, second


# ------------------------------------------------------------------------------------------ item 2: a marked entry
@pytest.mark.parametrize("how", [{"replace_ids": frozenset({EID})}, {"replace_edited": True},
                                 {"replace_edited": True, "carry": False}])
def test_a_replace_refuses_an_entry_that_carries_an_owner_mark_and_names_it(tmp_path, how):
    out, ledger, second = _edited_entry(tmp_path)
    before, ledger_before = (out / "region-3.json").read_bytes(), ledger.read_bytes()
    marked = IC.owner_marks(second, _store(tmp_path, _mark()))
    assert marked == {EID: ["verified on the entry, owner 2026-11-01"]}
    refused: list[str] = []
    report: list[str] = []
    written, kept = IC.write(second, out, ledger=ledger, marked=marked, refused=refused, report=report, **how)
    assert refused == [f"{EID}: carries an owner mark (verified on the entry, owner 2026-11-01)"]
    assert kept == [EID] and written == {"region-3.json": 0} and report == []
    assert (out / "region-3.json").read_bytes() == before, "the marked entry is byte for byte as the owner left it"
    assert ledger.read_bytes() == ledger_before


def test_the_same_replace_goes_through_when_the_entry_has_no_mark_in_force(tmp_path):
    """The refusal is the mark's doing, nothing else's: no mark, a mark on another entry, or a mark the owner took
    back (cleared), and the same call replaces the entry."""
    for i, marks in enumerate([(), (_mark(target="r3:somewhere_else@3-1"),),
                               (_mark(), M.cleared_mark(_mark(), reviewer="owner", note="by mistake",
                                                        date="2026-11-02"))]):
        sub = tmp_path / str(i)
        sub.mkdir()
        out, ledger, second = _edited_entry(sub)
        marked = IC.owner_marks(second, _store(sub, *marks))
        assert marked == {}
        refused: list[str] = []
        written, kept = IC.write(second, out, ledger=ledger, replace_ids=frozenset({EID}), marked=marked,
                                 refused=refused)
        assert (written, kept, refused) == ({"region-3.json": 1}, [], [])
        assert _on_disk(out)[0]["rules"][0]["type"] == "bait_restriction"


def test_every_mark_in_force_is_named_whatever_its_verdict(tmp_path):
    """A mark on one of the entry's rules is the entry's; a "wrong" or "unsure" mark is the owner's finding on this
    copy as much as a "verified" one is a sign-off. The latest mark per target is the one in force."""
    store = _store(tmp_path, _mark(verdict="unsure", date="2026-10-30"), _mark(),
                   _mark(scope="rule", rule_id="t.r1", verdict="wrong", date="2026-11-03"),
                   _mark(scope="dfo", target=EID))                 # another domain's target of the same name: not it
    assert IC.owner_marks([EID], store) == {EID: ["verified on the entry, owner 2026-11-01",
                                                  "wrong on rule t.r1, owner 2026-11-03"]}
    assert IC.owner_marks(["r3:other@3-1"], store) == {}


def test_a_missing_marks_store_stops_a_replace_and_is_not_read_as_no_marks(tmp_path):
    with pytest.raises(FileNotFoundError, match="no marks store"):
        IC.owner_marks([EID], tmp_path / "review" / "marks.jsonl")


def _run_files(tmp_path, second):
    """The batch, rows and response files `run` reads for the one row, and `run`'s fixed arguments."""
    from pipeline.regs.parsing.batch_exporter import compute_rows_digest
    from pipeline.regs.parsing.rows import load_synopsis_rows
    rows = tmp_path / "synopsis_raw_data.json"
    rows.write_text(json.dumps([{"context": {"region": "3"}, "rows": [{"water": "TRANQUILLE LAKE", "raw_regs": SRC}]}]),
                    encoding="utf-8")
    batch = tmp_path / "batch_000.json"
    batch.write_text(json.dumps({"batch": 0, "rows_digest": compute_rows_digest(load_synopsis_rows(rows)), "items": [
        {"index": 0, "entry_id": EID, "raw_regs": SRC, "name": "TRANQUILLE LAKE", "region": "3"}]}), encoding="utf-8")
    response = tmp_path / "response_000.json"
    response.write_text(json.dumps([{"index": 0, "entry": io.dump_entry(second[EID])}]), encoding="utf-8")
    return [str(batch)], [str(response)], rows


def test_the_command_refuses_names_the_mark_and_exits_1(tmp_path, monkeypatch, capsys):
    """Through `run`, the function the CLI calls: `--replace-entry` on a marked entry prints REFUSED with the mark,
    writes nothing and exits 1; the marks store is read for a replace and only for a replace."""
    monkeypatch.setattr(IC, "ingest_registry", lambda: None)       # the registry check is not what is under test
    out, ledger, second = _edited_entry(tmp_path)
    batches, responses, rows = _run_files(tmp_path, second)
    before = (out / "region-3.json").read_bytes()
    store = _store(tmp_path, _mark())
    args = dict(ledger=ledger, rows_path=rows, marks_path=store)

    assert IC.run(batches, responses, str(out), replace_ids=frozenset({EID}), **args) == 1
    said = capsys.readouterr().out
    assert (f"REFUSED --replace-entry {EID}: carries an owner mark (verified on the entry, owner 2026-11-01) — "
            "not replaced; a reviewed entry changes by a reviewed edit, never by a re-parse") in said
    assert "REPLACED" not in said and (out / "region-3.json").read_bytes() == before

    assert IC.run(batches, responses, str(out), replace_edited=True, **args) == 1
    assert f"REFUSED --replace-edited {EID}: carries an owner mark" in capsys.readouterr().out
    assert (out / "region-3.json").read_bytes() == before

    gone = dict(args, marks_path=tmp_path / "no-such" / "marks.jsonl")
    assert IC.run(batches, responses, str(out), **gone) == 1       # no replace asked: KEPT, the store never opened
    assert "KEPT 1" in capsys.readouterr().out
    with pytest.raises(FileNotFoundError, match="no marks store"):
        IC.run(batches, responses, str(out), replace_ids=frozenset({EID}), **gone)
    assert (out / "region-3.json").read_bytes() == before

    store.write_text("", encoding="utf-8")                         # the owner's store with no mark on it: replaced
    assert IC.run(batches, responses, str(out), replace_ids=frozenset({EID}), **args) == 0
    assert f"REPLACED {EID} (edited)" in capsys.readouterr().out
    assert _on_disk(out)[0]["rules"][0]["type"] == "bait_restriction"


# ------------------------------------------------------------------------------------------ item 3: the ledger's digest
def test_the_ledger_holds_io_entry_digest_and_says_so(tmp_path):
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    IC.write(first, out, ledger=ledger)
    stored = io.read_entries_dir(out)[EID]
    assert json.loads(ledger.read_text()) == {"digest": "io.entry_digest", "entries": {EID: io.entry_digest(stored)}}
    assert IC.digest(stored) == io.entry_digest(stored)
    assert not IC.ledger_is_legacy(ledger) and not IC.ledger_is_legacy(tmp_path / "none.json")
    assert IC.seed_ledger(out, tmp_path / "seeded.json") == 1
    assert (tmp_path / "seeded.json").read_text() == ledger.read_text(), "a seed records what a write records"


def _legacy_ledger(path, out):
    """The ledger as written before T628: flat, the old full SHA-256 of each entry exactly as it is on disk."""
    old = {eid: hashlib.sha256(json.dumps(e, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
           for eid, e in io.read_entries_dir(out).items()}
    path.write_text(json.dumps(old, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def test_a_ledger_from_before_keeps_every_entry_until_the_reseed_and_the_run_says_why(tmp_path, monkeypatch, capsys):
    """The old ledger's digests are another function's. Its entries are never compared (so never "unchanged" by
    accident, and never replaced): every one is KEPT, the run prints why, and a re-seed is what ends it."""
    monkeypatch.setattr(IC, "ingest_registry", lambda: None)
    out, first = _ingest_once(tmp_path)
    IC.write(first, out, ledger=tmp_path / "elsewhere.json")
    ledger = tmp_path / "ingested.json"
    _legacy_ledger(ledger, out)                                    # untouched since: the old code would replace it
    legacy_bytes = ledger.read_bytes()
    assert IC.ledger_is_legacy(ledger)
    assert IC.write(first, out, ledger=ledger) == ({"region-3.json": 0}, [EID])
    assert ledger.read_bytes() == legacy_bytes, "nothing written, so the old ledger is left as it was"

    batches, responses, rows = _run_files(tmp_path, first)
    assert IC.run(batches, responses, str(out), ledger=ledger, rows_path=rows) == 1
    said = capsys.readouterr().out
    assert "was written before the ledger's digest became io.entry_digest (T628)" in said and "KEPT 1" in said
    assert "annual-synopsis.md step 9" in said

    assert IC.seed_ledger(out, ledger) == 1 and not IC.ledger_is_legacy(ledger)
    assert IC.run(batches, responses, str(out), ledger=ledger, rows_path=rows) == 0
    assert "was written before" not in capsys.readouterr().out


def test_a_ledger_naming_another_digest_is_refused(tmp_path):
    out, first = _ingest_once(tmp_path)
    ledger = tmp_path / "ingested.json"
    ledger.write_text(json.dumps({"digest": "sha1", "entries": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="not an ingest ledger this code reads"):
        IC.write(first, out, ledger=ledger)
    assert not (out / "region-3.json").exists()
