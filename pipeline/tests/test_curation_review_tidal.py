"""T212: the curation review's Tidal mode, through its HTTP API — the queue and one candidate are
read only, and ACCEPTING a candidate is one mark (scope "tidal", Place y + the cut as a new split)
in a TEMP marks store; nothing under data/curated is written.

Runs the FastAPI app with `TestClient` against a temp candidates file (one real candidate from
`handoff/PLAN/curation-review/tidal/tidal-candidates.json`) and a temp marks store."""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.tests.conftest import ATLAS_HINT, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
CANDIDATES = REPO / "handoff/PLAN/curation-review/tidal/tidal-candidates.json"
#: Pitt River: the synopsis p.21 box's CPR Bridge, the polygon and a tidal gauge all agree
PITT = "tidal:356360869"

pytestmark = pytest.mark.needs_atlas


def _digest(d: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(d.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(d)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    tmp = tmp_path_factory.mktemp("tidal")
    rows = [r for r in json.loads(CANDIDATES.read_text())["candidates"] if r["id"] == PITT]
    assert rows, f"{PITT} is not in {CANDIDATES}"
    (tmp / "tidal-candidates.json").write_text(json.dumps({"coverage": {"named_ocean_entering_total": 1},
                                                           "candidates": rows}))
    (tmp / "marks.jsonl").write_text("")
    shutil.copy2(CURATED.waters.splits, tmp / "splits.json")
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_TIDAL_CANDIDATES", str(tmp / "tidal-candidates.json"))
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_SPLITS_JSON", str(tmp / "splits.json"))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "dfo", "tidal", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import tidal as T
    T._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp}
    mp.undo()


def test_the_queue_and_a_candidate_are_served_read_only(env):
    c = env["client"]
    curated = _digest(REPO / "data/curated")
    q = c.get("/api/tidal/queue").json()
    assert [r["id"] for r in q] == [PITT] and q[0]["verify"] == "unmarked"
    assert c.get("/api/tidal/queue", params={"confidence": "nope"}).status_code == 422
    d = c.get(f"/api/tidal/candidates/{PITT}").json()
    assert d["candidate"]["water"] == "Pitt River"
    kinds = [f["properties"]["kind"] for f in d["points"]["features"]]
    assert kinds[0] == "proposed"
    assert c.get("/api/tidal/candidates/tidal:0").status_code == 404
    assert c.get("/api/tidal/progress").json()["total"] == 1
    assert _digest(REPO / "data/curated") == curated


def test_accepting_records_one_mark_and_no_curated_write(env):
    c = env["client"]
    curated = _digest(REPO / "data/curated")
    cand = c.get(f"/api/tidal/candidates/{PITT}").json()["candidate"]
    body = {"scope": "tidal", "target": PITT, "answers": {"P": "y"}, "reviewer": "test", "note": "",
            "new_splits": [{"ref": f"tidal-{cand['blk']}", "anchor": "coord", "name": cand["proposed"]["split_name"],
                            "on_item": cand["items"][0], "coord": cand["proposed"]["coord"]}]}
    # Place y without the cut is refused: an accepted boundary carries its one cut
    bad = c.post("/api/marks", json={**body, "new_splits": []})
    assert bad.status_code == 422
    r = c.post("/api/marks", json=body)
    assert r.status_code == 200, r.text
    got = r.json()
    assert got["scope"] == "tidal" and got["verdict"] == "verified"
    ns = got["new_splits"][0]
    assert ns["blk"] == cand["blk"] and abs(ns["measure"] - cand["proposed"]["measure"]) < 25
    lines = (env["dir"] / "marks.jsonl").read_text().splitlines()
    assert len(lines) == 1
    q = c.get("/api/tidal/queue").json()
    assert q[0]["verify"] == "verified"
    assert c.get("/api/tidal/candidates/" + PITT).json()["mark"]["status"] == "verified"
    assert _digest(REPO / "data/curated") == curated
