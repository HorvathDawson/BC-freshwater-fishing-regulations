"""(T994 #2) The review backend refuses a request another web page makes in the owner's browser.

CORS stops a page READING an answer, never SENDING the request: a body-less cross-site POST is a "simple" request with
no preflight, and `POST /api/inseason/resolve` re-resolved every match and started a staging publish. A DNS-rebound
name reaches 127.0.0.1 with the attacker's Host. One middleware (`app.browser_refusal`): a browser's request (it carries
Origin or Sec-Fetch-Site) needs a loopback Host, and a write needs the Vite origin. Mutation: drop the middleware ->
the foreign-Origin and rebound-Host tests fail (the route runs; the GET answers).
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from pipeline.tests.conftest import curated_root

BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
_MODS = ("app", "reuse", "writes", "review", "write_through", "inseason")


@pytest.fixture()
def env(tmp_path, monkeypatch):
    curated_root(monkeypatch, tmp_path / "curated")
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                                  # noqa: E402
    ran = []
    # the route's own work stands in: a resolve that only records it ran, a sync that is off
    monkeypatch.setitem(sys.modules, "inseason", types.SimpleNamespace(resolve_all=lambda: ran.append(1) or {}))
    monkeypatch.setattr(app_mod, "INPUTS_SYNC", types.SimpleNamespace(request=lambda why: {"state": "off"}))
    yield app_mod, ran
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _client(app_mod, host="127.0.0.1:8787"):
    from fastapi.testclient import TestClient
    return TestClient(app_mod.app, base_url=f"http://{host}", raise_server_exceptions=False)


def test_a_post_from_another_page_is_refused_and_runs_nothing(env):
    app_mod, ran = env
    for origin in ("https://evil.example", "null", "http://127.0.0.1:9999", "http://localhost:5173"):
        r = _client(app_mod).post("/api/inseason/resolve", headers={"Origin": origin})
        assert r.status_code == 403, (origin, r.text)
        assert "only the review app" in r.json()["detail"]
    r = _client(app_mod).post("/api/marks", headers={"Origin": "https://evil.example"}, json={})
    assert r.status_code == 403
    assert ran == []


def test_the_review_app_itself_still_writes(env):
    app_mod, ran = env
    for host in ("127.0.0.1:8787", "localhost:8787"):
        r = _client(app_mod, host=host).post("/api/inseason/resolve",
                                             headers={"Origin": app_mod.VITE_ORIGIN, "Sec-Fetch-Site": "same-origin"})
        assert r.status_code == 200, r.text
    assert ran == [1, 1]


def test_a_rebound_host_is_refused_even_for_a_read(env):
    app_mod, _ = env
    r = _client(app_mod, host="evil.example:8787").get("/api/review/closed", headers={"Sec-Fetch-Site": "same-origin"})
    assert r.status_code == 403 and "loopback" in r.json()["detail"]
    r = _client(app_mod, host="evil.example:8787").post("/api/inseason/resolve",
                                                         headers={"Origin": "http://evil.example:8787"})
    assert r.status_code == 403


def test_a_local_process_is_no_browser_and_passes(env):
    app_mod, ran = env
    # no Origin, no Sec-Fetch-*: curl or a script on this machine (the tests' own clients too)
    assert _client(app_mod, host="testserver").post("/api/inseason/resolve").status_code == 200
    assert ran == [1]


def test_the_refusal_rule(env):
    VITE_ORIGIN, browser_refusal = env[0].VITE_ORIGIN, env[0].browser_refusal
    ok = {"host": "127.0.0.1:8787", "origin": VITE_ORIGIN}
    assert browser_refusal("POST", ok) is None
    assert browser_refusal("POST", {**ok, "host": "[::1]:8787"}) is None
    assert browser_refusal("GET", {"host": "localhost:8787", "sec-fetch-site": "none"}) is None
    assert browser_refusal("POST", {**ok, "origin": "https://evil.example"}) is not None
    assert browser_refusal("GET", {"host": "127.0.0.1.evil.example:8787", "sec-fetch-site": "cross-site"}) is not None
    assert browser_refusal("POST", {"host": "evil.example"}) is None          # no browser header: a local process
