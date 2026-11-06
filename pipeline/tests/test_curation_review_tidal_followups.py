"""T359: the review app's Tidal follow-ups — the queue's `followup` filter and each row's `followup_why`, taken from the
tidal writer's own `skipped` list (`tools.tidal_limits.build`), never a second classifier; a "whole water is tidal" mark
leaves the follow-ups. Temp candidates and marks store; the live tidal.json / splits.json are not read."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from pipeline.atlas.splits import tidal as TA
from pipeline.common.curated import CURATED
from pipeline.regs.review import proposals as P
from pipeline.regs.review import tidal as T
from pipeline.tests import test_tidal_limits as TT
from pipeline.tests.conftest import curated_root

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"


@pytest.fixture()
def app(tmp_path, monkeypatch):
    cands = tmp_path / "tidal-candidates.json"
    cands.write_text(json.dumps({"coverage": {}, "candidates": [{"enters": "ocean", "agreement": "-", **c}
                                                                for c in TT.CANDS]}))
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(cands))
    curated_root(monkeypatch, tmp_path / "curated")           # T422: the whole curated tree, its marks store empty
    marks = Path(CURATED.review.marks)
    T._load.cache_clear()
    (tmp_path / "cat").mkdir()
    (tmp_path / "dfo").mkdir()
    sp = tmp_path / "splits.json"
    sp.write_text(json.dumps({"waterbodies": []}))
    src = P.Sources(entries_dir=tmp_path / "cat", dfo_dir=tmp_path / "dfo", splits_path=sp)
    monkeypatch.setattr(P.Sources, "live", classmethod(lambda cls: src))
    monkeypatch.setattr(TA, "load", lambda path=None: [])             # no owner rows: the live file is not read
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in ("tidal", "reuse", "preverify"):
        sys.modules.pop(mod, None)
    import tidal as BT
    monkeypatch.setattr(BT.preverify, "tidal", lambda _id: None)

    def mark(*ms):
        """Append marks (the store is append-only); the writer decides from the store in memory."""
        with marks.open("a") as f:
            for m in ms:
                f.write(m.model_dump_json(exclude_none=False) + "\n")
    mark()
    yield BT, mark
    T._load.cache_clear()
    for mod in ("tidal", "reuse", "preverify"):
        sys.modules.pop(mod, None)


def test_the_follow_ups_are_the_writers_skipped_list_with_the_last_note(app):
    BT, mark = app
    assert BT.queue(followup=True) == [] and BT.progress()["followups"] == 0
    mark(TT._tidal(TT.BEAR, verdict="wrong", note="most likely the split is at the bridge"),
         TT._tidal(TT.COPPER, verdict="unsure", note="this entire channel is tidal"),
         TT._tidal(TT.NANAIMO))                                   # a cut at its words: decided, no follow-up
    got = {r["id"]: (r["followup_why"], r["mark_verdict"], r["mark_note"]) for r in BT.queue(followup=True)}
    assert got == {
        TT.BEAR: ("a follow-up (wrong): no tidal limit until a Place y mark", "wrong",
                  "most likely the split is at the bridge"),
        TT.COPPER: ("a follow-up (unsure): no tidal limit until a Place y mark", "unsure",
                    "this entire channel is tidal")}
    rows = {r["id"]: r for r in BT.queue()}
    assert rows[TT.NANAIMO]["followup_why"] is None and rows[TT.TLELL]["mark_note"] is None
    p = BT.progress()
    assert (p["followups"], p["followups_error"]) == (2, None)
    # "the whole water is tidal" decides Copper: it leaves the follow-ups (the store only grows)
    mark(TT._whole(TT.COPPER))
    assert [r["id"] for r in BT.queue(followup=True)] == [TT.BEAR]
    row = {r["id"]: r for r in BT.queue()}[TT.COPPER]
    assert (row["followup_why"], row["mark_verdict"], row["mark_call"]) == (None, "verified", "whole")


def test_a_writer_that_cannot_run_is_said_not_hidden(app, monkeypatch):
    BT, _ = app
    from pipeline.regs.review import export as E

    def refuse(*_a, **_k):
        raise ValueError("review.export: conflicting decisions")
    monkeypatch.setattr(E, "load", refuse)
    p = BT.progress()
    assert p["followups"] == 0 and p["followups_error"].startswith("the tidal writer could not run: ")
