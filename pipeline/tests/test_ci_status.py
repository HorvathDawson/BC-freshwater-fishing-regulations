"""T533: release / APK preflight read CI's result for HEAD (`scripts/ci_status.py`), with faked `gh` answers.

Recorded: CI was red from 10-29 to 11-09 and nobody saw it; no release or APK script asked GitHub."""
from __future__ import annotations

import importlib.util
import json
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
    """gh as it answers: `--commit` the runs at it; `page=N` (T1059) that page of all runs; else the newest `limit`."""
    def runs_of(wf, branch=None, commit=None, limit=30, page=None):
        got = per_workflow.get(wf, [])      # (T993) a workflow a test does not name has no runs
        if isinstance(got, Exception):
            raise got
        if commit:
            return [r for r in got if r["headSha"] == commit][:limit]
        if page is not None:
            return {"total": len(got), "runs": got[(page - 1) * CI.PER_PAGE:page * CI.PER_PAGE]}
        return got[:limit]
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



# ── (T1059) the strict ancestor search reads every run, not the newest 30 of all branches ──────────────────────────
# Recorded 2026-11-24 from the live repo (read-only): docs-ci's runs, two pages of `gh api …/runs --jq PAGE_JQ`. The
# 0.9.0 build commit (defb62b4c) had no docs-ci run; its green ancestor run (15a257ca9, main) was number 109 of all
# branches' runs, so `build-apk.sh --production --dry-run` said "no finished run" and the build used CI_CHECK_SKIP.
REC = json.loads((ROOT / "pipeline/tests/fixtures/ci_status_docs_ci_runs_2026-11-24.json").read_text())
REC_SHA = REC["sha"]


#: the recording's page 2 is cut 6 runs after the ancestor (the fixture's note), so here the runs END there: GitHub's
#: total (375 that night) is read as the distinct runs recorded, 113 (one moved pages while recording)
REC_TOTAL = len({r["databaseId"] for pg in REC["pages"] for r in pg["runs"]})


# (T1081) the docs area grew (every markdown file, a package manifest: the size guard reads them), and two of the recorded
# paths are now in it; this test is about finding the run, so it keeps the recorded paths that were outside the area then
RECORDED_OUTSIDE_DOCS = [p for p in REC["changed_since_15a257ca9"] if p not in ("CHANGELOG.md", "app/apps/mobile/package.json")]


def recorded(ancestors, total=REC_TOTAL):
    """the recorded docs-ci answers through the real page reader; the other workflows green at the sha."""
    def runs_of(wf, branch=None, commit=None, limit=30, page=None):
        if wf != "docs-ci.yml":
            return [run(REC_SHA)] if commit == REC_SHA else []
        if commit:
            return []                                              # docs-ci never ran on the build commit
        if page is not None:
            pages = REC["pages"]
            pg = pages[page - 1] if page <= len(pages) else {"runs": []}
            return CI.parse_page(json.dumps({**pg, "total": total}), "rec")
        return [r for pg in REC["pages"] for r in pg["runs"]][:limit]  # what the old query (newest 30) saw
    git = {"is_ancestor": lambda a, b: b == REC_SHA and a in ancestors,
           "changed": lambda a, b: RECORDED_OUTSIDE_DOCS if a.startswith("15a257ca9") else ["handoff/x.md"]}
    return runs_of, git


def test_a_green_ancestor_run_past_the_newest_30_is_found():
    runs_of, git = recorded(set(REC["ancestors_of_sha"]))
    first = [r for pg in REC["pages"] for r in pg["runs"]].index(
        next(r for pg in REC["pages"] for r in pg["runs"] if r["headSha"].startswith("15a257ca9")))
    assert first > 100                                             # past gh's 30, and past the first page too
    rc, lines = CI.check(REC_SHA, runs_of=runs_of, strict=True, **git)
    assert rc == 0, lines
    assert ("ok    docs-ci.yml: no run on defb62b4c31f; nothing in its area changed since its green run on "
            "15a257ca9a78") in lines


def test_a_truly_missing_ancestor_run_is_still_a_stop_that_says_so():
    runs_of, git = recorded(set())                                 # no run anywhere is at an ancestor
    n = len({r["databaseId"] for pg in REC["pages"] for r in pg["runs"]})   # 113: one run moved pages while recording
    assert n == sum(len(pg["runs"]) for pg in REC["pages"]) - 1
    rc, lines = CI.check(REC_SHA, runs_of=runs_of, strict=True, **git)
    assert rc == 1
    assert (f"STOP  docs-ci.yml: no finished run on defb62b4c31f or any commit before it, among all {n} runs of the "
            "workflow (--strict)") in lines


def test_a_run_shifted_onto_the_next_page_is_read_once():
    """a run started while paging pushes page 1's last run onto page 2: it is one run, not two."""
    full = [run("d" * 40, n=i) for i in range(CI.PER_PAGE)]
    pages = {1: full, 2: [full[-1], run("e" * 40, n=500)]}
    rc, lines = CI.check(HEAD, runs_of=lambda wf, page=None, **k: {"total": CI.PER_PAGE + 1, "runs": pages.get(page, [])}
                         if wf == "docs-ci.yml" and page else ([run(HEAD)] if k.get("commit") == HEAD and wf != "docs-ci.yml" else []),
                         strict=True, is_ancestor=lambda a, b: False, changed=lambda a, b: [])
    assert rc == 1 and any(f"among all {CI.PER_PAGE + 1} runs" in ln for ln in lines), lines


def test_too_many_runs_to_reach_an_ancestor_is_cannot_tell_not_no_run():
    """(W46) MAX_PAGES full pages and no ancestor: said, exit 2 — never "no run"."""
    full = [run("d" * 40, n=i) for i in range(CI.PER_PAGE)]
    asked = []

    def runs_of(wf, branch=None, commit=None, limit=30, page=None):
        if wf != "docs-ci.yml":
            return [run(HEAD)] if commit == HEAD else []
        if page is not None:
            asked.append(page)
            return {"total": 5000, "runs": [{**r, "databaseId": page * 1000 + i} for i, r in enumerate(full)]}
        return []
    rc, lines = CI.check(HEAD, runs_of=runs_of, strict=True, is_ancestor=lambda a, b: False, changed=lambda a, b: [])
    assert rc == 2 and asked == list(range(1, CI.MAX_PAGES + 1))
    assert any(ln.startswith(f"STOP  docs-ci.yml: CANNOT TELL — read the newest {CI.PER_PAGE * CI.MAX_PAGES} runs")
               for ln in lines), lines


def test_gh_runs_pages_through_the_api_and_unreadable_pages_cannot_tell(monkeypatch):
    seen = []

    class Done:
        returncode, stderr = 0, ""
        stdout = json.dumps(REC["pages"][1])

    monkeypatch.setattr(CI.subprocess, "run", lambda args, **k: seen.append(args) or Done)
    got = CI.gh_runs("docs-ci.yml", page=2)
    assert got == {"total": REC["pages"][1]["total"], "runs": REC["pages"][1]["runs"]}
    assert got["runs"][0]["headSha"] and "databaseId" in got["runs"][0]
    assert seen[0][:4] == ["gh", "api", "-X", "GET"]
    assert "repos/{owner}/{repo}/actions/workflows/docs-ci.yml/runs" in seen[0]
    assert f"per_page={CI.PER_PAGE}" in seen[0] and "page=2" in seen[0] and CI.PAGE_JQ in seen[0]
    Done.stdout = "{}"
    try:
        CI.gh_runs("docs-ci.yml", page=1)
    except CI.CannotTell as e:
        assert "unreadable output" in str(e)
    else:
        raise AssertionError("an unreadable page must be CannotTell")


def test_runs_that_end_short_of_githubs_total_are_cannot_tell_not_no_run():
    """(review) "no run" only when every run GitHub counts was read; a list that ends early (a deleted run, a short
    answer) cannot say there is none."""
    runs_of, git = recorded(set(), total=REC_TOTAL + 1)
    rc, lines = CI.check(REC_SHA, runs_of=runs_of, strict=True, **git)
    assert rc == 2
    assert (f"STOP  docs-ci.yml: CANNOT TELL — the runs ended after {REC_TOTAL} of the {REC_TOTAL + 1} GitHub counts, "
            "none at defb62b4c31f") in "\n".join(lines), lines


def test_a_running_or_cancelled_run_at_an_ancestor_is_passed_over():
    """(review) only a finished run decides: an in-progress and a cancelled run at an ancestor, newer than its green
    one, are skipped; with only those, there is no finished run."""
    anc = "c" * 40
    git = {"is_ancestor": lambda a, b: a == anc, "changed": lambda a, b: []}
    green = {wf: [run(HEAD, n=i)] for i, wf in enumerate(CI.WORKFLOWS)}
    busy = [run(anc, status="in_progress", conclusion="", n=11), run(anc, conclusion="cancelled", n=12)]
    rc, lines = CI.check(HEAD, runs_of=fake({**green, "docs-ci.yml": [*busy, run(anc, conclusion="failure", n=13)]}),
                         strict=True, **git)
    assert rc == 1 and any("docs-ci.yml: RED on cccccccccccc (failure)" in ln for ln in lines), lines
    rc, lines = CI.check(HEAD, runs_of=fake({**green, "docs-ci.yml": busy}), strict=True, **git)
    assert rc == 1 and any("docs-ci.yml: no finished run on aaaaaaaaaaaa or any commit before it, among all 2 runs"
                           in ln for ln in lines), lines
