"""(T1006) `python -m pipeline reports`: every HTTP call, subprocess and Google session is mocked; a test that reads the
network or a secret would be a defect. Each test can fail: the mutation notes say what breaks it."""
from __future__ import annotations

import io
import json
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline import reports as R

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)


class Resp:
    def __init__(self, body):
        self._b = json.dumps(body).encode()

    def read(self):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def opener_for(routes: dict, seen: list):
    def opener(req, timeout=0):
        seen.append((req.get_method(), req.full_url, req.get_header("Authorization")))
        for key, body in routes.items():
            if key in req.full_url:
                if isinstance(body, Exception):
                    raise body
                return Resp(body)
        raise AssertionError(f"unmocked URL {req.full_url}")
    return opener


REPORTS = {"reports": [
    {"id": "rep-aaaa1111", "received": "2026-11-21T10:00:00Z", "reason": "wrong", "text": "Closed sign\non the Vedder",
     "email": "angler@example.com", "context": {"water": "Vedder River"}},
    {"id": "rep-bbbb2222", "received": "2026-11-20T10:00:00Z", "reason": "bug", "text": "map freezes", "context": {}}]}
CRASHES = {"crashes": [{"hash": "deadbeef0001", "last": "2026-11-21", "count": 4, "versions": {"0.8.4": 4},
                        "sample": {"name": "TypeError", "message": "x is undefined"}}],
           "counts": {"defect": 1, "network": 9}}


@pytest.fixture
def env(tmp_path, monkeypatch):
    tok = tmp_path / "tok"
    tok.write_text("SECRET-TOKEN\n")
    monkeypatch.setattr(R, "WORKERS", {"prod": ("https://prod.example", tok), "staging": ("https://stg.example", tok)})
    monkeypatch.setattr(R, "REPO", tmp_path)
    td = tmp_path / "tickets"
    td.mkdir()
    (td / "T1005-crash.md").write_text("# T1005\nStatus: OPEN.\ncrash deadbeef0001 is ticketed here\n")
    return SimpleNamespace(tmp=tmp_path, tickets=td, state=tmp_path / "cache" / "last.json", tok=tok)


def run(env, argv, routes=None, seen=None, runner=None, session=None):
    out = io.StringIO()
    rc = R.main(argv, now=NOW, opener=opener_for(routes if routes is not None else
                {"/reports": REPORTS, "/crashes": CRASHES}, seen if seen is not None else []),
                runner=runner, session=session, tickets_dir=env.tickets, state_path=env.state, out=out)
    return rc, out.getvalue()


def test_workers_are_read_with_get_only_and_the_bearer_token(env):
    seen: list = []
    rc, text = run(env, ["--only", "worker-prod,worker-staging", "--no-save"], seen=seen)
    assert rc == 0
    assert {m for m, _, _ in seen} == {"GET"}                      # mutation: any POST/PUT makes this fail
    assert {a for _, _, a in seen} == {"Bearer SECRET-TOKEN"}
    assert any(u.startswith("https://prod.example/reports") for _, u, _ in seen)
    assert any(u.startswith("https://stg.example/crashes") for _, u, _ in seen)
    assert "SECRET-TOKEN" not in text and "angler@example.com" not in text   # never printed


def test_an_item_a_ticket_cites_is_marked_with_it_and_the_rest_are_new(env):
    rc, text = run(env, ["--only", "worker-prod", "--no-save"])
    assert "T1005 crash deadbeef0001" in text                      # cited: its ticket, not NEW
    assert "NEW report rep-aaaa1111" in text and "NEW report rep-bbbb2222" in text
    assert "NEW crash deadbeef0001" not in text                    # mutation: no ticket scan -> NEW
    assert "-- 2 NEW item(s)" in text
    assert "network 9" in text                                     # the hidden crash classes are counted, not dropped


def test_a_failed_source_is_said_and_makes_the_exit_nonzero(env):
    boom = urllib.error.HTTPError("u", 500, "x", {}, None)
    rc, text = run(env, ["--only", "worker-prod", "--no-save"], routes={"/reports": REPORTS, "/crashes": boom})
    assert rc == 1
    assert "UNREAD" in text and "HTTP 500" in text
    assert "rep-aaaa1111" in text                                  # the half that read is still shown


def test_a_missing_token_is_reported_not_skipped(env):
    env.tok.unlink()
    rc, text = run(env, ["--only", "worker-prod", "--no-save"])
    assert rc == 1 and "no run token" in text


def test_last_check_moves_only_when_every_source_was_read(env, monkeypatch):
    ok_runner = lambda cmd, **k: SimpleNamespace(returncode=0, stdout=json.dumps({"screenshots": [], "crashes": []})
                                                 if "feedback" in cmd[1] else "external state READY_FOR_BETA_SUBMISSION\n", stderr="")
    session = SimpleNamespace(get=lambda *a, **k: SimpleNamespace(status_code=200, json=lambda: {"reviews": []}, text=""))
    rc, _ = run(env, [], runner=ok_runner, session=session)
    assert rc == 0 and json.loads(env.state.read_text())["last_check"] == NOW.isoformat()
    env.state.unlink()
    rc, _ = run(env, ["--only", "worker-prod"])                    # a partial run never moves it
    assert not env.state.exists()
    rc, _ = run(env, [], runner=ok_runner, session=SimpleNamespace(
        get=lambda *a, **k: SimpleNamespace(status_code=403, json=lambda: {}, text="no")))   # play unread
    assert rc == 1 and not env.state.exists()                      # mutation: save-always makes this fail


def test_since_comes_from_the_last_check_unless_given(env):
    env.state.parent.mkdir(parents=True)
    env.state.write_text(json.dumps({"last_check": "2026-11-18T09:00:00+00:00"}))
    seen: list = []
    run(env, ["--only", "worker-prod", "--no-save"], seen=seen)
    assert all("since=2026-11-18" in u for _, u, _ in seen)
    seen.clear()
    run(env, ["--only", "worker-prod", "--no-save", "--since", "2026-11-01"], seen=seen)
    assert all("since=2026-11-01" in u for _, u, _ in seen)
    assert R.since_date(None, {}, NOW) == "2026-11-08"             # no state: 14 days back


def test_testflight_reads_the_two_scripts_and_marks_feedback(env):
    calls = []

    def runner(cmd, **k):
        calls.append(cmd)
        if "testflight-feedback.mjs" in cmd[1]:
            body = {"screenshots": [{"id": "tf-feed-0001", "attributes": {"createdDate": "2026-11-21T01:00:00Z",
                                     "deviceModel": "iPhone15,2", "osVersion": "18.1", "comment": "search is slow"}},
                                    {"id": "tf-old-0002", "attributes": {"createdDate": "2026-10-01T01:00:00Z"}}],
                    "crashes": []}
            return SimpleNamespace(returncode=0, stdout=json.dumps(body), stderr="")
        return SimpleNamespace(returncode=0, stdout="0.8.4 (43) -> \"Ext\": ok; external state WAITING_FOR_REVIEW\n", stderr="")
    sec = R.testflight_section(14, {}, datetime(2026, 11, 10, tzinfo=timezone.utc), runner)
    assert [i.id for i in sec.items] == ["tf-feed-0001"]           # the October one is older than the window
    assert any("WAITING_FOR_REVIEW" in n for n in sec.notes)
    assert ["--dry-run" in c for c in calls] == [False, True]      # the distribute script runs only as a dry run


def test_testflight_script_failure_is_said(env):
    bad = lambda cmd, **k: SimpleNamespace(returncode=1, stdout="", stderr="missing asc-api-key.json")
    sec = R.testflight_section(14, {}, NOW, bad)
    assert sec.error and "missing asc-api-key.json" in sec.error


def test_play_reviews_are_listed_with_a_get_and_old_ones_dropped(env):
    gets = []

    def get(url, params=None, timeout=0):
        gets.append(url)
        return SimpleNamespace(status_code=200, text="", json=lambda: {"reviews": [
            {"reviewId": "gp:review-0001", "comments": [{"userComment": {"text": "Great app", "starRating": 5,
             "appVersionName": "0.8.4", "lastModified": {"seconds": str(int(NOW.timestamp()) - 3600)}}}]},
            {"reviewId": "gp:review-0002", "comments": [{"userComment": {"text": "old", "starRating": 1,
             "lastModified": {"seconds": str(int(NOW.timestamp()) - 40 * 86400)}}}]}]})
    sec = R.play_section({"T9": "see gp:review-0001"}, datetime(2026, 11, 15, tzinfo=timezone.utc),
                         SimpleNamespace(get=get))
    assert [i.id for i in sec.items] == ["gp:review-0001"] and sec.items[0].cited_in == ["T9"]
    assert gets and gets[0].endswith("/applications/ca.canifishthis.app/reviews")
    assert any("closed-testing" in n.lower() for n in sec.notes)   # the part with no API is said, not skipped


def test_play_without_a_key_says_so(env, monkeypatch):
    monkeypatch.setattr(R, "PLAY_KEY", env.tmp / "absent.json")
    sec = R.play_section({}, NOW)
    assert sec.error and "no service-account key" in sec.error


def test_ids_too_short_never_match_prose():
    assert R.cited_by("T1", {"T5": "the T1 line"}) == []


def test_unknown_source_is_refused(env):
    assert R.main(["--only", "nope"], now=NOW, tickets_dir=env.tickets, state_path=env.state) == 2
