"""T556: THE DFO PARITY VECTORS are what the Python reads today (`pipeline.tools.emit.dfo_vectors`).

The DFO page parser, the location fingerprint, the name key and the row typing exist in Python and in the feeds worker.
The committed vector file is the one pin between them: this test fails when the Python moves without the file (so the
worker's `dfo.vectors.test.ts`, which must reproduce the file, gets its say in the same change), and the mutations below
prove the file would notice. Everything stands on the saved pages and hand rows committed beside the file (frozen
fixtures) — no build, no release.
"""
from __future__ import annotations

import json

import pytest

from pipeline.regs.dfo_salmon import locations as LOC
from pipeline.tools.emit import dfo_vectors as E


def _committed() -> dict:
    return json.loads(E.OUT.read_text(encoding="utf-8"))


def test_the_committed_vectors_are_what_the_python_reads_today():
    assert E.main(["--check"]) == 0, "run: python -m pipeline.tools.emit.dfo_vectors (then the worker's dfo.vectors.test.ts)"


@pytest.fixture
def live_as_committed(monkeypatch):
    """(T759) The vectors' LIVE FEED part read from the committed file instead of re-run. A mutation test below proves a
    reader the other parts carry (they predate the live part, T676); re-running the live feed (a run per case, each
    twice) in every one of them took the CI tier past its time limit. The live part itself is proven by the test above,
    and `test_the_committed_live_part_stands_in_whole` proves this stand-in alone leaves the check passing — so a
    mutation test still fails only by its own change."""
    live = _committed()["live"]
    base = {k: v for k, v in live.items() if k not in ("cases", "carry")}
    monkeypatch.setattr(E, "_live", lambda pages, case=None: base if case is None else live["cases"][case["name"]])
    monkeypatch.setattr(E, "_carry_case", lambda: live["carry"])


def test_the_committed_live_part_stands_in_whole(live_as_committed):
    assert E.main(["--check"]) == 0


@pytest.mark.smoke  # T667 D6: quicker than the file's first test
def test_the_file_holds_every_saved_page_and_every_shared_row():
    doc = _committed()
    assert doc["format"] == E.FORMAT
    assert sorted(doc["pages"]) == sorted(p.name[len("region"):-len("-eng.html")] for p in E.PAGES.glob("region*-eng.html"))
    assert len(doc["pages"]) == 9
    assert sum(len(p["locators"]) for p in doc["pages"].values()) > 200
    assert all(p["locators"] and p["locations"] for p in doc["pages"].values())
    assert sorted(doc["rows"]) == sorted(c["name"] for c in json.loads(E.ROWS.read_text(encoding="utf-8"))["cases"])
    # every table locator is a location of its page, by fingerprint; (T758) the rest are the page's default sentences
    # (`cascade.statement_locators`, `page:<slug>:<scope>`), one at least on every saved page
    for slug, page in doc["pages"].items():
        fps = {l["fingerprint"] for l in page["locators"]}
        table = {f for f in fps if not f.startswith(f"page:{slug}:")}
        assert table <= {l["fingerprint"] for l in page["locations"]}
        assert fps - table


def test_a_synonym_added_on_the_python_side_only_fails_the_check(monkeypatch, capsys, live_as_committed):
    """The ticket's example (a synonym edited on one side): every place printing it is re-keyed here; the worker would
    still mint the old key, and its rows would fall to unplaced."""
    monkeypatch.setattr(LOC, "_SYNONYMS", [(r"\bhwy\.?\b", "road"), *LOC._SYNONYMS])
    assert E.main(["--check"]) == 1
    assert "is stale" in capsys.readouterr().err
    before = {c["fingerprint"] for c in _committed()["text"]}
    assert {c["fingerprint"] for c in E.build()["text"]} != before


def test_a_row_typed_differently_fails_the_check(monkeypatch, live_as_committed):
    from pipeline.regs.dfo_salmon import typed as T
    monkeypatch.setattr(T, "TBD", "DFO will say later")
    assert E.main(["--check"]) == 1


def test_a_missing_file_is_stale_never_passed(monkeypatch, tmp_path, live_as_committed):
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


def test_t676_a_reconcile_change_on_the_python_side_only_fails_the_check(monkeypatch, live_as_committed):
    from pipeline.regs.dfo_salmon import entries as ENT
    monkeypatch.setattr(ENT, "PAGE_DEFAULT", "no-such-kind")   # the T349 guard dropped: the page default goes dormant
    assert E.main(["--check"]) == 1


def test_t676_a_header_change_on_the_python_side_only_fails_the_check(monkeypatch, live_as_committed):
    from pipeline.regs.dfo_salmon import header as H
    monkeypatch.setattr(H, "digest", lambda notes: "0" * 64)
    assert E.main(["--check"]) == 1


def test_t676_a_date_reader_change_on_the_python_side_only_fails_the_check(monkeypatch, live_as_committed):
    from pipeline.regs.dfo_salmon import dates as D
    monkeypatch.setattr(D, "_FULL", [*D._FULL[:8], "sept", *D._FULL[9:]])   # "September" spelled only as "sept"
    assert E.main(["--check"]) == 1
