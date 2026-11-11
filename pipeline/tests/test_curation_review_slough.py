"""T55 (owner 2026-10-28): the curation review's Slough mode — pre-marks are PROPOSALS, never marks; the
owner's call is one mark (scope "slough", `call`), and "accept all visible" is one commit of many marks,
in a TEMP marks store. Nothing under data/curated is written."""
from __future__ import annotations

import gzip
import hashlib
import json
import sys
from pathlib import Path

import pytest

from pipeline.tests import under_test
from pipeline.tests.conftest import curated_root

REPO = Path(__file__).resolve().parents[2]
#: the LIVE curated tree, taken at import (before any test points CURATED_ROOT at a sandbox): the tests below prove it is not written
LIVE_CURATED = under_test.curated().base
BACKEND = REPO / "curation-review" / "backend"


def _row(i, decision, width, fwa="Lake-Definite", **kw):
    return {"id": i, "name": kw.pop("name", None), "stream": f"Creek {i}", "region": "3", "rule_class": "LIKELY_STREAM",
            "fwa": fwa, "area_ha": 1.0, "width_m": width, "img_veg": 0.5, "img_water": 0.1, "p_stream": 0.9,
            "impact": None, "complex": None, "lat": 50.0, "lon": -120.0, "decision": decision,
            "verdict": kw.pop("verdict", None), "by": kw.pop("by", None), "why": "w", **kw}


ROWS = [
    _row("1", "auto", 18.0, confidence="very_high"),                       # narrow -> pre-mark
    _row("2", "auto", 30.0, fwa="Lake-Intermittent", confidence="high"),   # intermittent -> owner list
    _row("3", "owner_list", 80.0, fwa="Marsh", wide_wetland=True, owner_rank=2, name="Quarry Slough"),  # named wide marsh -> pre-mark
    _row("4", "owner_list", 90.0, wide_wetland=False, owner_rank=1),      # wide lake -> owner list
    _row("5", "owner", 12.0, verdict="stream", by="owner"),               # the review page's mark
    _row("6", "default", 70.0, fwa="Lake-Definite"),
    # (owner 2026-10-29) a sewage basin / tailing pond / dump never reaches the queue
    _row("8", "default", 60.0, fwa="Settling Basin-Sewage"),
    # (owner 2026-10-29) an UNNAMED wetland-layer piece never reaches the queue: its call changes nothing
    _row("7", "owner_list", 85.0, fwa="Swamp", wide_wetland=True, owner_rank=3),
]


def _digest(d: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(d.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(d)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("slough")
    (tmp / "automark.json").write_text(json.dumps({"rows": ROWS}))
    with gzip.open(tmp / "finder.json.gz", "wt") as f:
        json.dump({"candidates": [{"id": r["id"], "outline": [[-120, 50], [-120.001, 50.001], [-120, 50]]}
                                  for r in ROWS]}, f)
    (tmp / "page.json").write_text(json.dumps({"verdicts": {
        "5": {"verdict": "stream", "note": "part of a slough nearby"},
        "6": {"verdict": None, "note": "similar this can be on a lake"},     # verdict cleared, note kept
        "3": {"verdict": "stream", "note": "complex:cx-3"}}}))                # the page's own tag, not a note
    mp = pytest.MonkeyPatch()
    # T331: a curated path is never env-overridable — the test points the module's own constant at a temp copy
    from pipeline.regs.review import slough as _S
    mp.setattr(_S, "PAGE_MARKS", tmp / "page.json")
    mp.setenv("CURATION_SLOUGH_AUTOMARK", str(tmp / "automark.json"))
    mp.setenv("CURATION_SLOUGH_FINDER", str(tmp / "finder.json.gz"))
    curated_root(mp, tmp / "curated")                         # T422: the whole curated tree is a temp copy
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "sloughs", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import slough as S
    S._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp, "marks": Path(under_test.curated().review.marks)}
    mp.undo()


def test_premarks_are_proposals_in_review_order(env):
    c = env["client"]
    q = c.get("/api/sloughs/queue").json()
    assert [r["id"] for r in q] == ["1", "3", "4", "2", "5", "6"]
    g = {r["id"]: (r["group"], (r["premark"] or {}).get("basis")) for r in q}
    assert g == {"1": ("premark", "narrow"), "3": ("premark", "wide marsh"), "4": ("owner list", None),
                 "2": ("owner list", None), "5": ("owner", None), "6": ("default", None)}
    assert all(r["verify"] == "unmarked" for r in q)          # a pre-mark is never a mark
    assert env["marks"].read_text() == ""
    assert [r["id"] for r in c.get("/api/sloughs/queue", params={"basis": "intermittent"}).json()] == ["2"]
    assert c.get("/api/sloughs/queue", params={"group": "nope"}).status_code == 422
    d = c.get("/api/sloughs/candidates/3").json()
    assert d["row"]["premark"]["basis"] == "wide marsh" and d["outline"]["features"]
    assert c.get("/api/sloughs/candidates/0").status_code == 404
    notes = {r["id"]: r["page_note"] for r in q if r["page_note"]}
    assert notes == {"5": "part of a slough nearby", "6": "similar this can be on a lake"}
    p = c.get("/api/sloughs/progress").json()
    assert p["total"] == 4 and p["by_basis"] == {"narrow": 1, "wide marsh": 1} and p["intermittent"] == 1


def test_one_call_and_accept_all_visible_are_marks(env):
    c = env["client"]
    curated = _digest(LIVE_CURATED)
    base = {"scope": "slough", "answers": {"P": "y"}, "reviewer": "test"}
    # a verified slough mark needs its call; a call on another scope is refused
    assert c.post("/api/marks", json={**base, "target": "4"}).status_code == 422
    assert c.post("/api/marks", json={**base, "scope": "tidal", "target": "x", "call": "stream"}).status_code == 422
    assert c.post("/api/marks", json={**base, "target": "4", "call": "lake"}).status_code == 422
    r = c.post("/api/marks", json={**base, "target": "4", "call": "not stream"})
    assert r.status_code == 200, r.text
    r = c.post("/api/marks", json={**base, "targets": ["1", "3"], "call": "stream"})
    assert r.status_code == 200 and r.json()["n"] == 2
    assert c.post("/api/marks", json={**base, "targets": ["1", "nope"], "call": "stream"}).status_code == 404
    lines = [json.loads(x) for x in env["marks"].read_text().splitlines()]
    assert [(m["target"], m["call"]) for m in lines] == [("4", "not stream"), ("1", "stream"), ("3", "stream")]
    q = {r["id"]: r for r in c.get("/api/sloughs/queue").json()}
    assert q["1"]["verify"] == "verified" and q["1"]["call"] == "stream" and q["4"]["call"] == "not stream"
    assert [r["id"] for r in c.get("/api/sloughs/queue", params={"verify": "todo", "group": "premark"}).json()] == []
    assert _digest(LIVE_CURATED) == curated


def test_clear_and_undo_batch(env):
    """Owner 2026-10-29: a mark is CLEARED through the app (a newer line; the store is never edited) — one
    target, or an Undo of an accept-all batch that clears only targets whose mark in force is that batch's."""
    c = env["client"]
    store = env["marks"]
    base = {"scope": "slough", "answers": {"P": "y"}, "reviewer": "test"}
    clr = {"scope": "slough", "verdict": "cleared", "answers": {}, "reviewer": "test"}
    assert c.post("/api/marks", json={**base, "targets": ["2", "6"], "call": "stream",
                                      "note": "batch"}).status_code == 200
    assert c.post("/api/marks", json={**base, "target": "6", "call": "not stream"}).status_code == 200
    before = store.read_text()
    assert c.post("/api/marks", json={**clr, "target": "2"}).status_code == 422          # no note
    assert c.post("/api/marks", json={**clr, "target": "nope", "note": "x"}).status_code == 422
    assert c.post("/api/marks", json={**base, "target": "2", "call": "stream",
                                      "if_last": {"note": "x"}}).status_code == 422
    # Undo: "6" was marked again since, so only "2" is cleared
    r = c.post("/api/marks", json={**clr, "targets": ["2", "6"], "note": "undo",
                                   "if_last": {"verdict": "verified", "note": "batch", "call": "stream"}})
    assert r.status_code == 200, r.text
    assert r.json()["n"] == 1 and r.json()["targets"] == ["2"] and r.json()["skipped"] == ["6"]
    assert store.read_text().startswith(before)                                          # appended only
    q = {r["id"]: r for r in c.get("/api/sloughs/queue").json()}
    assert q["2"]["verify"] == "unmarked" and q["6"]["call"] == "not stream"
    assert "2" in [r["id"] for r in c.get("/api/sloughs/queue", params={"verify": "todo"}).json()]
    # a second clear of the same target has nothing to clear; a new mark after it stands
    assert c.post("/api/marks", json={**clr, "target": "2", "note": "again"}).status_code == 422
    assert c.post("/api/marks", json={**base, "target": "2", "call": "not stream"}).status_code == 200
    q = {r["id"]: r for r in c.get("/api/sloughs/queue").json()}
    assert q["2"]["verify"] == "verified" and q["2"]["call"] == "not stream"
