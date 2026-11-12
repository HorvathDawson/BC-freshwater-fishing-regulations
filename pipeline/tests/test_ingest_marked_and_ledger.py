"""A RE-PARSE NEVER REPLACES AN ENTRY THE OWNER MARKED, AND THE LEDGER HOLDS THE ONE ENTRY DIGEST (T628 items 2-3, 5).

`--replace-entry` / `--replace-edited` put a re-parse over a curator's edited entry (T553 item 4). An entry that carries
an owner mark is the owner's reviewed copy: either flag refuses it, naming the mark, and there is no flag past that.
The ledger that decides "edited since ingest wrote it" now holds `io.entry_digest` and names it; a ledger from before
cannot be compared and says so instead of comparing two different digests.

Item 5: the same holds with no flag at all. An entry nobody edited is replaced by a plain re-parse — unless it carries an
owner mark and the re-parse says something else than the reviewed copy; then it is refused the same way. `write` takes
the marks store as a required argument and reads it before it writes anything.

Every test builds its own tree, marks store and ledger under `tmp_path`: nothing here reads or writes the live corpus.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from pipeline.regs.parsing import ingest_catalogue as IC
from pipeline.regs.parsing import io
from pipeline.regs.review import marks as M
from pipeline.regs.review import fingerprint as F
from pipeline.tests.test_ingest_catalogue import SRC, _ingest_once, _on_disk, no_marks, write as write_unmarked

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
    write_unmarked(first, out, ledger=ledger)
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
    store = _store(tmp_path, _mark())
    assert IC.owner_marks(second, store) == {EID: ["verified on the entry, owner 2026-11-01"]}
    refused: list[str] = []
    report: list[str] = []
    unedited: list[str] = []
    written, kept = IC.write(second, out, ledger=ledger, marks_path=store, refused=refused, report=report,
                             unedited=unedited, **how)
    assert refused == [f"{EID}: carries an owner mark (verified on the entry, owner 2026-11-01)"]
    assert unedited == [], "refused under a replace flag, not on the plain path"
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
        store = _store(sub, *marks)
        assert IC.owner_marks(second, store) == {}
        refused: list[str] = []
        written, kept = IC.write(second, out, ledger=ledger, replace_ids=frozenset({EID}), marks_path=store,
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


# ------------------------------------------------------------------------- item 5: a marked entry nobody edited
BAIT_BAN = [{"rule_id": "t.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
             "gear": [{"slot": "bait", "ban": ["any_bait"]}]}]
REFUSAL = f"{EID}: carries an owner mark (verified on the entry, owner 2026-11-01)"


def _unedited_entry(tmp_path):
    """An entry ingest wrote and nobody edited since (the ledger holds its digest), and a second parse of the same
    row that reads it another way."""
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    write_unmarked(first, out, ledger=ledger)
    _, second = _ingest_once(tmp_path, rules=BAIT_BAN)
    return out, ledger, first, second


@pytest.mark.parametrize("mark, words", [
    (dict(), "verified on the entry, owner 2026-11-01"),
    (dict(verdict="wrong"), "wrong on the entry, owner 2026-11-01"),
    (dict(scope="rule", rule_id="t.r1"), "verified on rule t.r1, owner 2026-11-01")])
@pytest.mark.parametrize("dry_run", [False, True])
def test_a_plain_reparse_that_differs_refuses_an_unedited_entry_that_carries_an_owner_mark(tmp_path, mark, words,
                                                                                         dry_run):
    out, ledger, _, second = _unedited_entry(tmp_path)
    before, ledger_before = (out / "region-3.json").read_bytes(), ledger.read_bytes()
    refused: list[str] = []
    unedited: list[str] = []
    written, kept = IC.write(second, out, dry_run=dry_run, ledger=ledger, marks_path=_store(tmp_path, _mark(**mark)),
                             refused=refused, unedited=unedited)
    assert refused == [f"{EID}: carries an owner mark ({words})"] and unedited == [EID]
    assert kept == [EID] and written == {"region-3.json": 0}
    assert (out / "region-3.json").read_bytes() == before, "the marked entry is byte for byte as the owner reviewed it"
    assert ledger.read_bytes() == ledger_before


def test_the_same_plain_reparse_replaces_the_entry_when_no_mark_is_in_force(tmp_path):
    """The refusal is the mark's doing: an empty store, a mark on another entry, or a mark the owner cleared, and the
    plain re-parse replaces the unedited entry as it always did."""
    for i, marks in enumerate([(), (_mark(target="r3:somewhere_else@3-1"),),
                               (_mark(), M.cleared_mark(_mark(), reviewer="owner", note="by mistake",
                                                        date="2026-11-02"))]):
        sub = tmp_path / str(i)
        sub.mkdir()
        out, ledger, _, second = _unedited_entry(sub)
        refused: list[str] = []
        assert IC.write(second, out, ledger=ledger, marks_path=_store(sub, *marks), refused=refused) == (
            {"region-3.json": 1}, [])
        assert refused == [] and _on_disk(out)[0]["rules"][0]["type"] == "bait_restriction"


def test_a_reparse_equal_to_the_marked_copy_is_not_refused_and_opens_no_store(tmp_path):
    """What is refused is OTHER content over a reviewed entry. A re-parse that says what the copy on disk says moves
    nothing the owner reviewed: it is written as before, and the store is not even read (a missing one is no error)."""
    out, ledger, first, _ = _unedited_entry(tmp_path)
    before = (out / "region-3.json").read_bytes()
    refused: list[str] = []
    assert IC.write(first, out, ledger=ledger, marks_path=_store(tmp_path, _mark()), refused=refused) == (
        {"region-3.json": 1}, [])
    assert refused == [] and (out / "region-3.json").read_bytes() == before
    assert IC.write(first, out, ledger=ledger, marks_path=tmp_path / "no-such" / "marks.jsonl") == (
        {"region-3.json": 1}, [])


def test_write_requires_the_marks_store_and_reads_it_before_it_writes_anything(tmp_path):
    """`write` has no default store and no way to be called without one. A store that is missing when it is needed
    stops the run BEFORE the first file is written: the other region's new entry is not on disk either."""
    out, ledger, _, second = _unedited_entry(tmp_path)
    with pytest.raises(TypeError, match="marks_path"):
        IC.write(second, out, ledger=ledger)
    other, problems = IC.ingest(
        [{"entry_id": "r2:alouette@2-8", "name": "ALOUETTE LAKE", "region": "2", "regs_verbatim": "Bait ban.",
          "rules": [dict(BAIT_BAN[0], rule_id="a.r1")]}],
        {"r2:alouette@2-8": {"entry_id": "r2:alouette@2-8", "raw_regs": "Bait ban.", "name": "ALOUETTE LAKE",
                             "region": "2"}}, registry=None)
    assert list(other) == ["r2:alouette@2-8"], problems
    before, ledger_before = (out / "region-3.json").read_bytes(), ledger.read_bytes()
    with pytest.raises(FileNotFoundError, match="no marks store"):
        IC.write({**other, **second}, out, ledger=ledger, marks_path=tmp_path / "no-such" / "marks.jsonl")
    assert not (out / "region-2.json").exists(), "region 2 sorts first and was not written"
    assert (out / "region-3.json").read_bytes() == before and ledger.read_bytes() == ledger_before
    # with the store there, the new entry is written and the marked one is still refused
    refused: list[str] = []
    assert IC.write({**other, **second}, out, ledger=ledger, marks_path=_store(tmp_path, _mark()),
                    refused=refused) == ({"region-2.json": 1, "region-3.json": 0}, [EID])
    assert refused == [REFUSAL] and (out / "region-3.json").read_bytes() == before


def test_the_command_refuses_a_plain_reparse_of_a_marked_entry_names_the_mark_and_exits_1(tmp_path, monkeypatch,
                                                                                         capsys):
    """Through `run`, with no flag: REFUSED naming the mark, the entry listed as not replaced (not as "edited"),
    nothing written, exit 1 — in a dry run too, so the dry run says what the real run will do."""
    monkeypatch.setattr(IC, "ingest_registry", lambda: None)
    out, ledger, _, second = _unedited_entry(tmp_path)
    batches, responses, rows = _run_files(tmp_path, second)
    before = (out / "region-3.json").read_bytes()
    args = dict(ledger=ledger, rows_path=rows, marks_path=_store(tmp_path, _mark()))
    for dry_run in (True, False):
        assert IC.run(batches, responses, str(out), dry_run=dry_run, **args) == 1
        said = capsys.readouterr().out
        assert (f"REFUSED re-parse of {REFUSAL} — not replaced; a reviewed entry changes by a reviewed edit, never by "
                "a re-parse") in said
        assert (f"NOT REPLACED 1 unedited entr(y/ies) that carry an owner mark and whose re-parse differs from the "
                f"reviewed copy (REFUSED above): {EID}") in said
        assert "KEPT" not in said, "it was not edited: the edited-entry advice (--replace-entry) is not printed"
        assert (out / "region-3.json").read_bytes() == before
    assert IC.run(batches, responses, str(out), replace_ids=frozenset({EID}), **args) == 1     # no flag goes past it
    assert f"REFUSED --replace-entry {REFUSAL}" in capsys.readouterr().out
    assert IC.run(batches, responses, str(out), replace_edited=True, **args) == 1
    assert f"REFUSED re-parse of {REFUSAL}" in capsys.readouterr().out
    assert (out / "region-3.json").read_bytes() == before

    _store(tmp_path)                                               # the owner's store with no mark on it: replaced
    assert IC.run(batches, responses, str(out), **args) == 0
    assert _on_disk(out)[0]["rules"][0]["type"] == "bait_restriction"


def test_what_the_refusal_prevents_the_mark_would_have_read_stale_in_the_review_app_only(tmp_path):
    """WHY item 5 exists (measured 2026-11-11). Before it, this re-parse was written with one line printed ("wrote 1
    entries") and no word about the mark. The mark kept its target (the entry id is the batch's), so it was no orphan:
    it read STALE, "rule changed", and only on the review app's banner. Pinned on the two copies: the mark holds on
    the reviewed one and is stale on the re-parse the write refuses."""
    out, ledger, _, second = _unedited_entry(tmp_path)
    ctx = F.Context()
    reviewed = io.read_entries_dir(out)[EID]
    mark = M.Mark(scope="entry", target=EID, verdict="verified", answers={k: "y" for k in M.CHECKS["entry"]},
                  reviewer="owner", date="2026-11-01", pdf_sha256="a" * 64, pages=[30],
                  prints=M.Prints.model_validate(F.entry_fingerprints(reviewed, ctx)))
    store = _store(tmp_path, mark)
    reparse = io.dump_entry(second[EID])
    assert M.status(mark, F.entry_fingerprints(reviewed, ctx)) == {"status": "verified", "verdict": "verified",
                                                                   "reasons": []}
    assert M.status(mark, F.entry_fingerprints(reparse, ctx)) == {"status": "stale", "verdict": "verified",
                                                                  "reasons": ["rule changed"]}
    health = M.health([mark], entries={EID: reparse}, ctx=ctx)
    assert (health["n_orphans"], health["n_stale"]) == (0, 1)
    assert IC.write(second, out, ledger=ledger, marks_path=store) == ({"region-3.json": 0}, [EID])
    health = M.health(M.load(store), entries=io.read_entries_dir(out), ctx=ctx)
    assert (health["n_orphans"], health["n_stale"]) == (0, 0), "refused: the mark still holds on what is on disk"


# ------------------------------------------------------------------------------------------ item 3: the ledger's digest
def test_the_ledger_holds_io_entry_digest_and_says_so(tmp_path):
    ledger = tmp_path / "ingested.json"
    out, first = _ingest_once(tmp_path)
    write_unmarked(first, out, ledger=ledger)
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
    write_unmarked(first, out, ledger=tmp_path / "elsewhere.json")
    ledger = tmp_path / "ingested.json"
    _legacy_ledger(ledger, out)                                    # untouched since: the old code would replace it
    legacy_bytes = ledger.read_bytes()
    assert IC.ledger_is_legacy(ledger)
    assert write_unmarked(first, out, ledger=ledger) == ({"region-3.json": 0}, [EID])
    assert ledger.read_bytes() == legacy_bytes, "nothing written, so the old ledger is left as it was"

    batches, responses, rows = _run_files(tmp_path, first)
    run = lambda: IC.run(batches, responses, str(out), ledger=ledger, rows_path=rows, marks_path=no_marks(out))
    assert run() == 1
    said = capsys.readouterr().out
    assert "was written before the ledger's digest became io.entry_digest (T628)" in said and "KEPT 1" in said
    assert "annual-synopsis.md step 9" in said

    assert IC.seed_ledger(out, ledger) == 1 and not IC.ledger_is_legacy(ledger)
    assert run() == 0
    assert "was written before" not in capsys.readouterr().out


def test_a_ledger_naming_another_digest_is_refused(tmp_path):
    out, first = _ingest_once(tmp_path)
    ledger = tmp_path / "ingested.json"
    ledger.write_text(json.dumps({"digest": "sha1", "entries": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="not an ingest ledger this code reads"):
        write_unmarked(first, out, ledger=ledger)
    assert not (out / "region-3.json").exists()
