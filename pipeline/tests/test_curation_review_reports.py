"""T237: the review app's Reports tab backend — READ ONLY. `GET /api/reports` asks the feeds worker (run token) and
says, per report, which water it names and — only when the report's tiles are this build's vintage — which section the
tapped handle is here. A report from another vintage is never resolved (its handle names another piece). Nothing is
written: the safety pins (`test_curation_review_safety`) hold with the module in place, and data/curated is unchanged."""
from __future__ import annotations

import io
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo", "review", "evidence",
         "inseason", "reports")


def _stamps(root: Path) -> dict[str, str]:
    import hashlib
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("reports")
    (tmp / "marks.jsonl").write_text("")
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp / "backups"))
    mp.setenv("CURATION_REPORTS_TOKEN", "tok")
    mp.setenv("CURATION_REPORTS_URL", "https://worker.invalid/reports")
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reports                                            # noqa: E402
    from fastapi.testclient import TestClient
    curated = _stamps(CURATED.base)
    yield {"client": TestClient(app_mod.app), "reports": reports, "mp": mp}
    assert _stamps(CURATED.base) == curated, "a reports route wrote under data/curated"
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _a_section(reports) -> tuple[int, str]:
    """A section handle the served bundle files under a named water, and that water."""
    from pipeline.deliver.bundle import read as R
    if reports._vintage() is None or not Path(R.BUNDLE).exists():
        pytest.skip("no served build / bundle on this machine")
    db = sqlite3.connect(f"file:{R.BUNDLE}?mode=ro", uri=True)
    try:
        if dict(db.execute("SELECT k, v FROM meta WHERE k = 'section_handles'")).get("section_handles") != reports._vintage():
            pytest.skip("the bundle is another vintage than the served build")
        sid, item = db.execute("SELECT x.sid, i.item_id FROM item_section x JOIN item i ON i.ord = x.ord "
                               "WHERE i.item_id LIKE 'gnis:%' ORDER BY x.sid LIMIT 1").fetchone()
    finally:
        db.close()
    return int(sid), item


def _report(**ctx):
    return {"format": "report/1", "id": "0123456789abcdef", "created": "2026-10-28T20:00:00Z",
            "received": "2026-10-28T20:00:05Z", "reason": "place", "text": "x", "email": None, "context": ctx}


class _Answer(io.BytesIO):
    def __enter__(self): return self
    def __exit__(self, *a): self.close()


def test_the_list_asks_the_worker_with_the_token_and_resolves_this_vintage(env):
    reports = env["reports"]
    sid, item = _a_section(reports)
    tiles = f"{reports._vintage()}.abc"
    seen = {}

    def opener(req, timeout):
        seen["url"], seen["auth"] = req.full_url, req.headers.get("Authorization")
        return _Answer(json.dumps({"reports": [_report(section=sid, tiles=tiles),
                                               _report(section=sid, tiles="0000000000000000.x", water=item)]}).encode())
    env["mp"].setattr(reports.urllib.request, "urlopen", opener)
    got = env["client"].get("/api/reports?since=2026-10-01")
    assert got.status_code == 200, got.text
    assert seen == {"url": "https://worker.invalid/reports?since=2026-10-01", "auth": "Bearer tok"}
    same, other = got.json()["reports"]
    assert same["section"]["vintage"] == "same" and same["section"]["node"] and item in same["section"]["items"]
    assert same["item"]["id"] == same["section"]["items"][0]          # no water named: the section's
    # another vintage: the handle is NOT resolved here (it would name a different piece)
    assert other["section"] == {"sid": sid, "node": None, "items": [], "vintage": "other"}
    assert other["item"]["id"] == item and other["item"]["known"] is True
    geo = env["client"].get(f"/api/reports/section?sid={sid}&tiles={tiles}").json()
    assert geo["vintage"] == "same" and geo["node"] == same["section"]["node"] and geo["features"]
    assert env["client"].get(f"/api/reports/section?sid={sid}&tiles=0000000000000000.x").json()["features"] == []


def test_an_unreachable_worker_is_a_503_that_says_why(env):
    reports = env["reports"]

    def down(req, timeout):
        raise reports.urllib.error.URLError("offline")
    env["mp"].setattr(reports.urllib.request, "urlopen", down)
    got = env["client"].get("/api/reports")
    assert got.status_code == 503 and "could not be reached" in got.json()["detail"]
    assert env["client"].get("/api/reports?since=yesterday").status_code == 422


def test_no_token_is_said(env, monkeypatch):
    reports = env["reports"]
    monkeypatch.delenv("CURATION_REPORTS_TOKEN")
    monkeypatch.setattr(reports, "_TOKEN_FILE", Path("/nonexistent/.run-token"))
    with pytest.raises(reports.ReportsUnavailable, match="no run token"):
        reports.fetch()


def test_the_reports_routes_only_read(env):
    import app as app_mod
    routes = {(m, r.path) for r in app_mod.app.routes for m in getattr(r, "methods", None) or ()
              if r.path.startswith("/api/reports")}
    assert routes == {("GET", "/api/reports"), ("GET", "/api/reports/section")}
