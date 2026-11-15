"""T742 (owner 2026-11-14: "remove the grouping option in the review app … one at a time is better"): a DFO mark is
saved on ONE place. A group save (several `targets`, or `members`) is refused by the route, by name, and nothing is
written; every wording is its own queue row; a mark a group save wrote earlier stays exactly what it says and is only
flagged "marked in a group". The marks store is a TEMP copy: nothing under data/curated is written."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pipeline.tests import under_test
from pipeline.tests.conftest import curated_root

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
LOC = "2:test-river:from-the-bridge"


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("one_at_a_time")
    mp = pytest.MonkeyPatch()
    curated_root(mp, tmp / "curated")                         # the whole curated tree is a temp copy
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "dfo", "checks", "writes", "review"):
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "app": app_mod, "marks": Path(under_test.curated().review.marks), "tmp": tmp}
    mp.undo()


def _lines(p: Path) -> int:
    return len(p.read_text().splitlines()) if p.exists() else 0


@pytest.mark.parametrize("extra", [{"targets": [LOC, LOC + "~2"]},
                                   {"members": {LOC + "~2": {"extent_before": {"extents": [], "tributaries": None}}}},
                                   {"targets": [LOC, LOC + "~2"], "checklist": {"digest": "0" * 16, "rows": []}}])
def test_a_dfo_save_on_several_places_is_refused_and_writes_nothing(env, extra):
    before = _lines(env["marks"])
    r = env["client"].post("/api/marks", json={"scope": "dfo", "target": LOC, "verdict": "verified",
                                               "answers": {"T": "y", "R": "y", "P": "y"}, "reviewer": "test",
                                               "extent_before": {"extents": [], "tributaries": None}, **extra})
    assert r.status_code == 422, r.text
    assert "one place at a time" in r.text
    assert _lines(env["marks"]) == before                     # nothing written
    assert "one place at a time" in env["app"].refused_log().read_text()   # the refusal is on file, never silent


def test_the_store_refuses_a_group_save_too(env):
    """below the route, the one write path refuses it as well — no second way in"""
    import reuse
    before = _lines(env["marks"])
    with pytest.raises(ValueError, match="one place at a time"):
        reuse.post_mark({"scope": "dfo", "target": LOC, "targets": [LOC, LOC + "~2"], "verdict": "verified",
                         "answers": {"T": "y"}, "reviewer": "test"})
    assert _lines(env["marks"]) == before


def test_every_wording_on_a_live_water_is_its_own_place_and_group_marks_are_flagged(env, monkeypatch):
    import dfo
    from pipeline.regs.review import marks as M
    live = {"location_id": LOC, "status": "active", "water_id": "w1"}
    older = {"location_id": LOC + "~2", "status": "dormant", "water_id": "w1", "duplicate_of": LOC,
             "duplicate_confirmed": True}
    gone = {"location_id": "2:other:x", "status": "dormant", "water_id": "w9"}
    # a dormant wording on a live water is its own place, whatever it rewords; one on a water off the page is not
    assert dfo.historic_only(older, {"w1"}) and not dfo.historic_only(live, {"w1"})
    assert not dfo.historic_only(gone, {"w1"})
    # the water view: one place per location, never folded together
    monkeypatch.setattr(dfo, "_locations", lambda: {LOC: ("2", live, None), LOC + "~2": ("2", older, None)})
    monkeypatch.setattr(dfo, "_rows", lambda: [{"id": LOC, "verify": "unmarked"}, {"id": LOC + "~2", "verify": "verified"}])
    assert [(p["id"], p["members"]) for p in dfo.water_places("w1", LOC)] == [(LOC, [LOC]), (LOC + "~2", [LOC + "~2"])]
    # a mark a group save wrote: flagged with its group and date; any other mark is not
    mk = lambda **kw: M.Mark.model_construct(scope="dfo", target=LOC, date="2026-11-14", **kw)  # noqa: E731
    assert dfo.group_marked(mk(group="grp:" + LOC)) == {"group": "grp:" + LOC, "date": "2026-11-14"}
    assert dfo.group_marked(mk(group=None)) is None and dfo.group_marked(None) is None
