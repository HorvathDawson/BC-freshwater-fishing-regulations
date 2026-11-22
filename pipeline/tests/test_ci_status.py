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
    def runs_of(wf, branch=None, commit=None):
        got = per_workflow.get(wf, [])      # (T993) a workflow a test does not name has no runs
        if isinstance(got, Exception):
            raise got
        return [r for r in got if r["headSha"] == commit] if commit else got
    return runs_of


def test_green_on_head_passes():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(HEAD)], "pipeline-ci.yml": [run(HEAD, n=2)],
                                             "docs-ci.yml": [run(HEAD, n=3)], "feeds-worker-ci.yml": [run(HEAD, n=4)]}))
    assert rc == 0 and len(lines) == 4 and all(ln.startswith("ok") for ln in lines)


def test_every_workflow_that_gates_a_push_is_read():
    """(T993) app-ci's ledger guard and worker suite moved to their own workflows; the release gate must still see them."""
    assert set(CI.WORKFLOWS) == {"app-ci.yml", "pipeline-ci.yml", "docs-ci.yml", "feeds-worker-ci.yml"}
    for wf in CI.WORKFLOWS:
        assert (ROOT / ".github" / "workflows" / wf).is_file(), wf


def test_a_red_ledger_guard_stops_the_release_check():
    rc, lines = CI.check(HEAD, runs_of=fake({"app-ci.yml": [run(HEAD)], "pipeline-ci.yml": [run(HEAD)],
                                             "docs-ci.yml": [run(HEAD, conclusion="failure", n=5)]}))
    assert rc == 1 and any(ln.startswith("STOP  docs-ci.yml: RED") for ln in lines)


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


def test_runs_are_found_by_commit_first_then_by_branch():
    """(T995 review) a fresh release branch has no runs of its own: its cut commit's runs (main's) are found by commit."""
    asked = []

    def runs_of(wf, branch=None, commit=None):
        asked.append((wf, branch, commit))
        return [run(HEAD, conclusion="failure", n=3)] if commit == HEAD and wf == "pipeline-ci.yml" else []
    rc, lines = CI.check(HEAD, branch="release/0.9.0", runs_of=runs_of)
    assert rc == 1 and "STOP  pipeline-ci.yml: RED on aaaaaaaaaaaa (failure): https://github.com/x/actions/runs/3" in lines
    assert ("pipeline-ci.yml", None, HEAD) in asked and ("pipeline-ci.yml", "release/0.9.0", None) not in asked
    assert ("app-ci.yml", "release/0.9.0", None) in asked                  # no run at the sha: the branch's


def test_strict_needs_a_green_run_at_the_sha_or_an_unchanged_area_since_one():
    """(T995 review) production: "no run" is a stop, unless nothing in the workflow's area changed since a green run
    at an ancestor (a path-filtered push). Running is a stop too."""
    anc = "c" * 40
    git = {"is_ancestor": lambda a, b: a == anc, "changed": lambda a, b: changed}
    green = {wf: [run(HEAD, n=i)] for i, wf in enumerate(CI.WORKFLOWS)}
    assert CI.check(HEAD, runs_of=fake(green), strict=True, **git)[0] == 0
    per = {**green, "docs-ci.yml": [run(anc, n=9)]}                       # docs ran last on an ancestor
    changed = ["pipeline/release.py"]                                      # … and nothing of docs changed since
    rc, lines = CI.check(HEAD, runs_of=fake(per), strict=True, **git)
    assert rc == 0 and any("docs-ci.yml: no run on aaaaaaaaaaaa; nothing in its area changed since its green run on "
                           "cccccccccccc" in ln for ln in lines)
    changed = ["handoff/PLAN/STATUS.md"]                                   # mutation: docs changed, no run saw it
    rc, lines = CI.check(HEAD, runs_of=fake(per), strict=True, **git)
    assert rc == 1 and any("its area changed since its last run (cccccccccccc: handoff/PLAN/STATUS.md)" in ln
                           for ln in lines)
    assert CI.check(HEAD, runs_of=fake({**green, "docs-ci.yml": []}), strict=True, **git)[0] == 1     # none at all
    rc, _ = CI.check(HEAD, runs_of=fake({**green, "app-ci.yml": [run(HEAD, status="in_progress", conclusion="")]}),
                     strict=True, **git)
    assert rc == 1
    # the same "no run" without --strict (staging) only warns
    assert CI.check(HEAD, runs_of=fake({**green, "docs-ci.yml": []}))[0] == 0


def test_the_production_store_builds_ask_strictly():
    """(T993 plan 5) both store builds ask through the one gate (deploy/lib/build-links.sh build_gates), --strict for
    production; the iPhone build's is also run in test_staging_scripts_fail_safe.py."""
    assert '.venv/bin/python scripts/ci_status.py ${strict:+"$strict"}' in (ROOT / "deploy/lib/build-links.sh").read_text()
    for build in ("build-apk", "build-ios-testflight"):
        assert f"build_gates {build} ${{PROD:+--strict}}" in (ROOT / f"deploy/staging/{build}.sh").read_text(), build

