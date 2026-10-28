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

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"


def _row(i, decision, width, fwa="Lake-Definite", **kw):
    return {"id": i, "name": None, "stream": f"Creek {i}", "region": "3", "rule_class": "LIKELY_STREAM",
            "fwa": fwa, "area_ha": 1.0, "width_m": width, "img_veg": 0.5, "img_water": 0.1, "p_stream": 0.9,
            "impact": None, "complex": None, "lat": 50.0, "lon": -120.0, "decision": decision,
            "verdict": kw.pop("verdict", None), "by": kw.pop("by", None), "why": "w", **kw}


ROWS = [
    _row("1", "auto", 18.0, confidence="very_high"),                       # narrow -> pre-mark
    _row("2", "auto", 30.0, fwa="Lake-Intermittent", confidence="high"),   # intermittent -> owner list
    _row("3", "owner_list", 80.0, fwa="Marsh", wide_wetland=True, owner_rank=2),   # wide marsh -> pre-mark
    _row("4", "owner_list", 90.0, wide_wetland=False, owner_rank=1),      # wide lake -> owner list
    _row("5", "owner", 12.0, verdict="stream", by="owner"),               # the review page's mark
    _row("6", "default", 70.0, fwa="Settling Basin-Sewage"),
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
    (tmp / "marks.jsonl").write_text("")
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_SLOUGH_AUTOMARK", str(tmp / "automark.json"))
    mp.setenv("CURATION_SLOUGH_FINDER", str(tmp / "finder.json.gz"))
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "sloughs", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import slough as S
    S._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp}
    mp.undo()


def test_premarks_are_proposals_in_review_order(env):
    c = env["client"]
    q = c.get("/api/sloughs/queue").json()
    assert [r["id"] for r in q] == ["1", "3", "4", "2", "5", "6"]
    g = {r["id"]: (r["group"], (r["premark"] or {}).get("basis")) for r in q}
    assert g == {"1": ("premark", "narrow"), "3": ("premark", "wide marsh"), "4": ("owner list", None),
                 "2": ("owner list", None), "5": ("owner", None), "6": ("default", None)}
    assert all(r["verify"] == "unmarked" for r in q)          # a pre-mark is never a mark
    assert (env["dir"] / "marks.jsonl").read_text() == ""
    assert [r["id"] for r in c.get("/api/sloughs/queue", params={"basis": "intermittent"}).json()] == ["2"]
    assert c.get("/api/sloughs/queue", params={"group": "nope"}).status_code == 422
    d = c.get("/api/sloughs/candidates/3").json()
    assert d["row"]["premark"]["basis"] == "wide marsh" and d["outline"]["features"]
    assert c.get("/api/sloughs/candidates/0").status_code == 404
    p = c.get("/api/sloughs/progress").json()
    assert p["total"] == 4 and p["by_basis"] == {"narrow": 1, "wide marsh": 1} and p["intermittent"] == 1


def test_one_call_and_accept_all_visible_are_marks(env):
    c = env["client"]
    curated = _digest(REPO / "data/curated")
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
    lines = [json.loads(x) for x in (env["dir"] / "marks.jsonl").read_text().splitlines()]
    assert [(m["target"], m["call"]) for m in lines] == [("4", "not stream"), ("1", "stream"), ("3", "stream")]
    q = {r["id"]: r for r in c.get("/api/sloughs/queue").json()}
    assert q["1"]["verify"] == "verified" and q["1"]["call"] == "stream" and q["4"]["call"] == "not stream"
    assert [r["id"] for r in c.get("/api/sloughs/queue", params={"verify": "todo", "group": "premark"}).json()] == []
    assert _digest(REPO / "data/curated") == curated
