"""Q154 (owner 2026-10-28): the Sloughs tab's "Small ponds (Q154)" view — the cutoff's prediction is shown, never
a mark; the owner's pool / real lake / unsure call is one mark (scope "q154"), in a TEMP marks store; the tally
sets the calls against the prediction. Nothing under data/curated is written."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"


def _pond(i, caught, s="example", **kw):
    return {"id": f"lake:{i}", "set": s, "n": i, "stratum": "caught" if caught else "not caught", "why": "w",
            "region": "7", "river": None, "order": 3, "ha": 0.2, "avg_width_m": 10.0, "length_m": 50,
            "elongation": 2.0, "river_width_m": 8.0, "ratio": 1.25, "man_made": False, "caught": caught,
            "md_read": None, "lat": 55.0, "lon": -125.0, "rings": [[[-125, 55], [-125.001, 55.001], [-125, 55]]],
            "lines": [{"coords": [[-125.002, 55], [-125, 55.002]], "through": True, "name": None}], **kw}


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("q154")
    (tmp / "sample.json").write_text(json.dumps({"cutoff": {"max_ha": 1.0}, "rows": [
        _pond(1, True), _pond(2, False), _pond(3, True, "sample")]}))
    (tmp / "marks.jsonl").write_text("")
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_Q154_SAMPLE", str(tmp / "sample.json"))
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "ponds", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import q154 as Q
    Q._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp}
    mp.undo()


def test_prediction_is_not_a_mark_and_calls_are_tallied(env):
    c = env["client"]
    curated = sorted((p, p.stat().st_mtime) for p in (REPO / "data/curated").rglob("*") if p.is_file())
    q = c.get("/api/q154/queue").json()
    assert [r["id"] for r in q] == ["lake:1", "lake:2", "lake:3"] and all(r["verify"] == "unmarked" for r in q)
    assert [r["id"] for r in c.get("/api/q154/queue", params={"set": "sample"}).json()] == ["lake:3"]
    d = c.get("/api/q154/ponds/lake:1").json()
    assert d["map"]["features"] and d["bounds"] and d["mark"] is None
    assert c.get("/api/q154/ponds/lake:9").status_code == 404
    base = {"scope": "q154", "answers": {"P": "y"}, "reviewer": "test"}
    assert c.post("/api/marks", json={**base, "target": "lake:1"}).status_code == 422          # no call
    assert c.post("/api/marks", json={**base, "target": "lake:1", "call": "stream"}).status_code == 422
    assert c.post("/api/marks", json={**base, "target": "lake:9", "call": "pool"}).status_code == 404
    assert c.post("/api/marks", json={**base, "target": "lake:1", "targets": ["lake:2"], "call": "pool"}).status_code == 422
    assert c.post("/api/marks", json={**base, "scope": "slough", "target": "x", "call": "pool"}).status_code in (404, 422)
    for t, call in (("lake:1", "pool"), ("lake:2", "pool")):
        r = c.post("/api/marks", json={**base, "target": t, "call": call})
        assert r.status_code == 200, r.text
    r = c.post("/api/marks", json={"scope": "q154", "target": "lake:3", "answers": {"P": "?"}, "reviewer": "test"})
    assert r.status_code == 200, r.text
    lines = [json.loads(x) for x in (env["dir"] / "marks.jsonl").read_text().splitlines()]
    assert [(m["target"], m.get("call"), m["verdict"]) for m in lines] == [
        ("lake:1", "pool", "verified"), ("lake:2", "pool", "verified"), ("lake:3", None, "unsure")]
    t = c.get("/api/q154/tally").json()
    assert (t["done"], t["called"], t["agree"], t["missed_pool"], t["caught_but_lake"]) == (3, 2, 1, 1, 0)
    assert t["by_prediction"] == {"caught": {"pool": 1, "unsure": 1}, "not caught": {"pool": 1}}
    assert [r["id"] for r in c.get("/api/q154/queue", params={"verify": "todo"}).json()] == []
    assert sorted((p, p.stat().st_mtime) for p in (REPO / "data/curated").rglob("*") if p.is_file()) == curated
