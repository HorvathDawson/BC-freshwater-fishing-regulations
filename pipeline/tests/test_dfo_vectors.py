"""T556: THE DFO PARITY VECTORS are what the Python reads today (`pipeline.tools.emit.dfo_vectors`).

The DFO page parser, the location fingerprint, the name key and the row typing exist in Python and in the feeds worker.
The committed vector file is the one pin between them: this test fails when the Python moves without the file (so the
worker's `dfo.vectors.test.ts`, which must reproduce the file, gets its say in the same change), and the mutations below
prove the file would notice. Everything stands on the saved pages and hand rows committed beside the file (frozen
fixtures) — no build, no release.
"""
from __future__ import annotations

import json

from pipeline.regs.dfo_salmon import locations as LOC
from pipeline.tools.emit import dfo_vectors as E


def _committed() -> dict:
    return json.loads(E.OUT.read_text(encoding="utf-8"))


def test_the_committed_vectors_are_what_the_python_reads_today():
    assert E.main(["--check"]) == 0, "run: python -m pipeline.tools.emit.dfo_vectors (then the worker's dfo.vectors.test.ts)"


def test_the_file_holds_every_saved_page_and_every_shared_row():
    doc = _committed()
    assert doc["format"] == E.FORMAT
    assert sorted(doc["pages"]) == sorted(p.name[len("region"):-len("-eng.html")] for p in E.PAGES.glob("region*-eng.html"))
    assert len(doc["pages"]) == 9
    assert sum(len(p["locators"]) for p in doc["pages"].values()) > 200
    assert all(p["locators"] and p["locations"] for p in doc["pages"].values())
    assert sorted(doc["rows"]) == sorted(c["name"] for c in json.loads(E.ROWS.read_text(encoding="utf-8"))["cases"])
    # every table locator is a location of its page, by fingerprint
    for page in doc["pages"].values():
        assert {l["fingerprint"] for l in page["locators"]} <= {l["fingerprint"] for l in page["locations"]}


def test_a_synonym_added_on_the_python_side_only_fails_the_check(monkeypatch, capsys):
    """The ticket's example (a synonym edited on one side): every place printing it is re-keyed here; the worker would
    still mint the old key, and its rows would fall to unplaced."""
    monkeypatch.setattr(LOC, "_SYNONYMS", [(r"\bhwy\.?\b", "road"), *LOC._SYNONYMS])
    assert E.main(["--check"]) == 1
    assert "is stale" in capsys.readouterr().err
    before = {c["fingerprint"] for c in _committed()["text"]}
    assert {c["fingerprint"] for c in E.build()["text"]} != before


def test_a_row_typed_differently_fails_the_check(monkeypatch):
    from pipeline.regs.dfo_salmon import typed as T
    monkeypatch.setattr(T, "TBD", "DFO will say later")
    assert E.main(["--check"]) == 1


def test_a_missing_file_is_stale_never_passed(monkeypatch, tmp_path):
    monkeypatch.setattr(E, "OUT", tmp_path / "absent.json")
    assert E.main(["--check"]) == 1


def test_t676_the_join_header_and_date_rows_are_in_the_file():
    doc = _committed()
    for slug, page in doc["pages"].items():
        assert page["entries"]["by_fingerprint"], slug          # every saved page has its curated fixture bound
        assert {"notes", "digest", "scopes", "unresolved", "answers", "closure"} <= set(page["header"]), slug
    pd = doc["joins"]["page_default"]
    assert not [o for o in pd["reconcile"]["outcomes"] if o["location_id"] == "8:page-default"]
    assert len(doc["dates"]) > 1500
    assert {w["kind"] for _, w in doc["dates"]} == {"ok", "unparsed", "tbd", "empty"}


def test_t676_a_reconcile_change_on_the_python_side_only_fails_the_check(monkeypatch):
    from pipeline.regs.dfo_salmon import entries as ENT
    monkeypatch.setattr(ENT, "PAGE_DEFAULT", "no-such-kind")   # the T349 guard dropped: the page default goes dormant
    assert E.main(["--check"]) == 1


def test_t676_a_header_change_on_the_python_side_only_fails_the_check(monkeypatch):
    from pipeline.regs.dfo_salmon import header as H
    monkeypatch.setattr(H, "digest", lambda notes: "0" * 64)
    assert E.main(["--check"]) == 1


def test_t676_a_date_reader_change_on_the_python_side_only_fails_the_check(monkeypatch):
    from pipeline.regs.dfo_salmon import dates as D
    monkeypatch.setattr(D, "_FULL", [*D._FULL[:8], "sept", *D._FULL[9:]])   # "September" spelled only as "sept"
    assert E.main(["--check"]) == 1
