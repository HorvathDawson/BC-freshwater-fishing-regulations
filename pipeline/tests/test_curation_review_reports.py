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

from pipeline.common.curated import load_repo
from pipeline.tests import under_test
from pipeline.tests.conftest import ATLAS_HINT, BUNDLE_HINT, curated_root, need

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
    mp = pytest.MonkeyPatch()
    curated_root(mp, tmp / "curated")                         # T422: the whole curated tree is a temp copy
    mp.setenv("CURATION_BACKUP_DIR", str(tmp / "backups"))
    mp.setenv("CURATION_REPORTS_TOKEN", "tok")
    mp.setenv("CURATION_REPORTS_URL", "https://worker.invalid/reports")
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reports                                            # noqa: E402
    from fastapi.testclient import TestClient
    curated = _stamps(load_repo().base)
    yield {"client": TestClient(app_mod.app), "reports": reports, "mp": mp}
    assert _stamps(load_repo().base) == curated, "a reports route wrote under data/curated"
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _a_section(reports) -> tuple[int, str]:
    """A section handle the served bundle files under a named water, and that water."""
    bundle = under_test.bundle()
    assert reports._vintage() is not None, f"the served build {reports.reuse._BUILD} has no section handles — {ATLAS_HINT}"
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    try:
        got = dict(db.execute("SELECT k, v FROM meta WHERE k = 'section_handles'")).get("section_handles")
        assert got == reports._vintage(), f"the bundle ({got}) is another vintage than the served build ({reports._vintage()})"
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


@pytest.mark.needs_bundle
@pytest.mark.needs_atlas
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
    assert seen == {"url": "https://worker.invalid/reports?since=2026-10-01&test=1", "auth": "Bearer tok"}
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


def test_a_reply_draft_only_when_the_reporter_allowed_contact(env):
    """(owner 2026-10-29) "Reply by email": a mailto: draft for the owner's own mail client — to the reporter, subject
    "Your report about <water> — canifishthis", their words quoted, a line for the question. Never without the tick."""
    from urllib.parse import parse_qs, unquote, urlsplit
    reports = env["reports"]
    r = {**_report(place="Vedder River"), "text": "Closure ends at the bridge.\nNot the forks.",
         "email": "angler+x@example.org", "contact": True}
    m = reports.reply_mailto(r, "Vedder River")
    u = urlsplit(m)
    assert u.scheme == "mailto" and unquote(u.path) == "angler+x@example.org"
    q = parse_qs(u.query)
    assert q["subject"] == ["Your report about Vedder River — canifishthis"]
    assert "%0D%0A" in m and "%0A%0A" not in m.replace("%0D%0A", "")
    assert [x.replace("\r\n", "\n") for x in q["body"]] == ["Hello,\n\nThank you for your report about Vedder River, sent 2026-10-28. You wrote:\n\n"
                         "> Closure ends at the bridge.\n> Not the forks.\n\n[your question here]\n\n— canifishthis\n"]
    assert reports.reply_mailto({**r, "contact": False}, "Vedder River") is None
    assert reports.reply_mailto({**r, "contact": None}, "Vedder River") is None
    assert reports.reply_mailto({**r, "email": None}, "Vedder River") is None
    assert "about the app" in parse_qs(urlsplit(reports.reply_mailto(r, None)).query)["subject"][0]
