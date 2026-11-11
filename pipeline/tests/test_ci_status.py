"""T533: release / APK preflight read CI's result for HEAD (`scripts/ci_status.py`), with faked `gh` answers.

Recorded: CI was red from 10-29 to 11-09 and nobody saw it; no release or APK script asked GitHub."""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_status", ROOT / "scripts" / "ci_status.py")
CI = importlib.util.module_from_spec(spec)
spec.loader.exec_module(CI)

HEAD, OLD = "a" * 40, "b" * 40


def run(sha, status="completed", conclusion="success", n=1):
    return {"databaseId": n, "headSha": sha, "status": status, "conclusion": conclusion,
            "url": f"https://github.com/x/actions/runs/{n}", "createdAt": "2026-11-10T00:00:00Z", "event": "push"}


def fake(per_workflow):
    def runs_of(wf, branch):
        got = per_workflow[wf]
        if isinstance(got, Exception):
            raise got
        return got
    return runs_of


def test_green_on_head_passes():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(HEAD)], "pipeline-ci.yml": [run(HEAD, n=2)]}))
    assert rc == 0 and all(ln.startswith("ok") for ln in lines)


def test_red_on_head_stops_naming_the_run():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(HEAD)],
                                             "pipeline-ci.yml": [run(HEAD, conclusion="failure", n=7)]}))
    assert rc == 1
    assert "STOP  pipeline-ci.yml: RED on aaaaaaaaaaaa (failure): https://github.com/x/actions/runs/7" in lines


def test_running_warns_and_does_not_block():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(HEAD, status="in_progress", conclusion="")],
                                             "pipeline-ci.yml": [run(HEAD)]}))
    assert rc == 0 and lines[0].startswith("WARN  app-ci.yml: still in_progress on aaaaaaaaaaaa")


def test_no_run_for_head_warns_unless_the_newest_finished_run_is_red():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(OLD)], "pipeline-ci.yml": []}))
    assert rc == 0
    assert lines[0].startswith("WARN  app-ci.yml: no run for aaaaaaaaaaaa (path filter, or not pushed); the newest")
    assert lines[1].startswith("WARN  pipeline-ci.yml: no run for aaaaaaaaaaaa and no finished run")
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(OLD, status="in_progress", conclusion=""),
                                                            run(OLD, conclusion="failure", n=9)],
                                             "pipeline-ci.yml": [run(HEAD)]}))
    assert rc == 1 and "the newest finished run is RED (failure on bbbbbbbbbbbb): https://github.com/x/actions/runs/9" \
        in lines[0]


def test_gh_failing_is_a_stop_not_a_pass():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": CI.CannotTell("gh: not logged in"),
                                             "pipeline-ci.yml": [run(HEAD)]}))
    assert rc == 2 and lines[0] == "STOP  app-ci.yml: CANNOT TELL — gh: not logged in"


def test_the_escape_prints_its_reason_and_an_empty_one_is_refused():
    env = {k: v for k, v in os.environ.items() if k != "CI_CHECK_SKIP"}
    ok = subprocess.run([sys.executable, str(ROOT / "scripts/ci_status.py"), "--skip", "CI down for T999"], env=env,
                        capture_output=True, text=True)
    assert ok.returncode == 0 and "SKIPPED (not asked): CI down for T999" in ok.stdout
    bad = subprocess.run([sys.executable, str(ROOT / "scripts/ci_status.py")], env={**env, "CI_CHECK_SKIP": " "},
                         capture_output=True, text=True)
    assert bad.returncode == 2 and "needs a reason" in bad.stderr
