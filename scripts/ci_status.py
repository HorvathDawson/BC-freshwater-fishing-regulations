"""Is CI green on HEAD? (T533) — the newest `app-ci`, `pipeline-ci`, `docs-ci` and `feeds-worker-ci` runs for the branch, read with `gh`.

    python scripts/ci_status.py [--sha SHA] [--branch B] [--strict] [--skip REASON]

(T995 review) Runs are found BY COMMIT first (`gh run list --commit <sha>`, any branch: a fresh release/x has no runs
of its own, but its cut commit has main's), then by the branch HEAD is on (`--branch` names another).

`--strict` (production builds: `build-apk.sh --production`): a workflow with no FINISHED, GREEN run at the sha is a STOP
— unless its newest finished run at an ANCESTOR of the sha is green and nothing in its area (scripts/ci_paths.py AREAS)
changed since (a push to main / release/** is path-filtered, so an unchanged area has no run of its own). A running or
cancelled run is a STOP too. (T1059) The ancestor's run is searched for through EVERY run of the workflow, page by page
(`gh api …/workflows/<wf>/runs`, 100 a page), not the newest 30 of all branches: on a busy night docs-ci's green
ancestor was run 108 and the 0.9.0 production build had to skip its CI check. Reading MAX_PAGES pages without reaching
an ancestor is CANNOT TELL (said, exit 2), never "no run". Staging (release preflight, `build-apk.sh` without --production, upload.sh) keeps the rules
below: a stricter staging would stop every release whose branch tip left one area untouched by a path filter.

Recorded: CI was red from 10-29 to 11-09 and nobody saw it (29 pipeline tests and two app checks), and two more CI-only
reds came on 2026-11-09; no release or APK script asked GitHub. Release preflight (and its --dry-run), build-apk.sh
(before the heavy lock) and deploy/staging/upload.sh ask here.

Per workflow:
  - a run for the sha, finished and green: ok
  - a run for the sha, finished red (failure, timed out, startup failure): STOP, naming the run
  - a run for the sha still running, or cancelled (a newer push cancels it): WARN, does not block
  - no run for the sha (the workflow's path filter, or not pushed): WARN; then the NEWEST FINISHED run on the branch
    decides: red = STOP (the branch is red and this sha did not run to say otherwise), green = ok
`gh` failing (offline, not logged in) is a STOP that says so — never a pass. `--skip REASON` (CI_CHECK_SKIP=REASON)
prints the reason and passes; an empty reason is refused.
Exit: 0 ok (warnings printed), 1 red, 2 cannot tell / usage.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))      # beside it, however it is loaded (the tests: by path)
import ci_paths as CP  # noqa: E402  (T995) the trunk and the areas, typed once: scripts/ci_paths.py
from ci_paths import TRUNK  # noqa: E402

# (T993) app-ci split into jobs of one workflow (their conclusions roll up to the run) and two small workflows with
# their own path filters: the ticket ledger / docs guard (docs-ci.yml, on handoff/**) and the feeds worker's suites
# (feeds-worker-ci.yml). A workflow with no run for the sha falls back to its newest finished run, as below.
WORKFLOWS = ("app-ci.yml", "pipeline-ci.yml", "docs-ci.yml", "feeds-worker-ci.yml")
RED = {"failure", "timed_out", "startup_failure", "action_required"}
FIELDS = "databaseId,headSha,status,conclusion,url,createdAt,event"


class CannotTell(Exception):
    pass


#: (T1059) the strict ancestor search reads the workflow's runs (all branches) PER_PAGE at a time, at most MAX_PAGES
#: pages (2026-11-24: docs-ci had 375 runs in all; its green ancestor was number 108)
PER_PAGE, MAX_PAGES = 100, 10
#: `gh api …/runs` trimmed to `gh run list --json FIELDS`'s names, so both answers read the same
PAGE_JQ = ("{total: .total_count, runs: [.workflow_runs[] | {databaseId: .id, headSha: .head_sha, status, conclusion, "
           "url: .html_url, createdAt: .created_at, event, headBranch: .head_branch}]}")


def _gh(args: list[str], what: str) -> str:
    try:
        got = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise CannotTell(f"{what}: {e}") from e
    if got.returncode:
        raise CannotTell(f"{what} failed (rc={got.returncode}): {got.stderr.strip()[-300:]}")
    return got.stdout


def parse_page(text: str, what: str) -> list[dict]:
    """(T1059) one page of `gh api …/runs --jq PAGE_JQ`: its runs, newest first."""
    try:
        got = json.loads(text)
        runs = got["runs"]
    except (ValueError, KeyError, TypeError) as e:
        raise CannotTell(f"{what}: unreadable output ({e})") from e
    if not isinstance(runs, list):
        raise CannotTell(f"{what}: unreadable output (runs is {type(runs).__name__})")
    return runs


def gh_runs(workflow: str, branch: str | None = None, commit: str | None = None, limit: int = 30,
            page: int | None = None) -> list[dict]:
    """The workflow's newest runs, newest first: at a commit (any branch), on a branch, or (neither) anywhere; `page`
    (T1059): page N (from 1) of ALL its runs, PER_PAGE a page. The one place `gh` is called; tests replace it."""
    if page is not None:
        what = f"gh api actions/workflows/{workflow}/runs page {page}"
        return parse_page(_gh(["api", "-X", "GET", f"repos/{{owner}}/{{repo}}/actions/workflows/{workflow}/runs",
                               "-f", f"per_page={PER_PAGE}", "-f", f"page={page}", "--jq", PAGE_JQ], what), what)
    sel = [*(["--commit", commit] if commit else []), *(["--branch", branch] if branch else [])]
    what = f"gh run list --workflow {workflow}"
    text = _gh(["run", "list", "--workflow", workflow, *sel, "--limit", str(limit), "--json", FIELDS], what)
    try:
        return json.loads(text)
    except ValueError as e:
        raise CannotTell(f"{what}: unreadable output ({e})") from e


def judge(workflow: str, runs: list[dict], sha: str) -> tuple[str, str]:
    """('ok' | 'warn' | 'red', what to print) for one workflow."""
    mine = [r for r in runs if r.get("headSha") == sha]
    if mine:
        r = mine[0]
        if r.get("status") != "completed":
            return "warn", f"{workflow}: still {r.get('status')} on {sha[:12]} — not waited for ({r.get('url')})"
        if r.get("conclusion") == "success":
            return "ok", f"{workflow}: green on {sha[:12]}"
        if r.get("conclusion") in RED:
            return "red", f"{workflow}: RED on {sha[:12]} ({r.get('conclusion')}): {r.get('url')}"
        return "warn", f"{workflow}: {r.get('conclusion')} on {sha[:12]} — no verdict for this sha ({r.get('url')})"
    done = [r for r in runs if r.get("status") == "completed" and r.get("conclusion") in RED | {"success"}]
    if not done:
        return "warn", f"{workflow}: no run for {sha[:12]} and no finished run on the branch"
    last = done[0]
    if last.get("conclusion") in RED:
        return "red", (f"{workflow}: no run for {sha[:12]} (path filter, or not pushed), and the newest finished run is "
                       f"RED ({last.get('conclusion')} on {last.get('headSha', '')[:12]}): {last.get('url')}")
    return "warn", (f"{workflow}: no run for {sha[:12]} (path filter, or not pushed); the newest finished run "
                    f"({last.get('headSha', '')[:12]}) is green")


def current_branch() -> str:
    """(T995) the branch HEAD is on — a release branch, a lane's PR branch (its pull_request runs list under it) or the
    trunk; the trunk when HEAD is detached."""
    got = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True)
    name = got.stdout.strip()
    return TRUNK if name in ("", "HEAD") else name


def _git_ok(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True)


def git_is_ancestor(a: str, b: str) -> bool:
    return _git_ok("merge-base", "--is-ancestor", a, b).returncode == 0


def git_changed(a: str, b: str) -> list[str]:
    got = _git_ok("diff", "--name-only", "--no-renames", a, b)
    if got.returncode:
        raise CannotTell(f"git diff --name-only {a[:12]} {b[:12]} failed: {got.stderr.strip()[-200:]}")
    return [x for x in got.stdout.splitlines() if x]


AREA_OF = {wf: area for area, wf in CP.WORKFLOWS.items()}


def strict_judge(workflow: str, sha: str, runs_of, is_ancestor=git_is_ancestor, changed=git_changed) -> tuple[str, str]:
    """(T995 review) production's rule: a green finished run at the sha, or at its nearest ancestor with a finished run
    when nothing in the workflow's area changed since; anything else is red."""
    mine = runs_of(workflow, commit=sha)
    if mine:
        r = mine[0]
        if r.get("status") == "completed" and r.get("conclusion") == "success":
            return "ok", f"{workflow}: green on {sha[:12]}"
        return "red", (f"{workflow}: {r.get('status')}/{r.get('conclusion') or '-'} on {sha[:12]} — production builds "
                       f"only on a finished green run (--strict): {r.get('url')}")
    r, read = newest_finished_ancestor_run(workflow, sha, runs_of, is_ancestor)
    if r is None:
        return "red", (f"{workflow}: no finished run on {sha[:12]} or any commit before it, among all {read} runs of the "
                       f"workflow (--strict)")
    anc = r["headSha"]
    hit = [x for x in changed(anc, sha) if CP.matches(x, CP.AREAS[AREA_OF[workflow]])]
    if hit:
        return "red", (f"{workflow}: no run on {sha[:12]}, and its area changed since its last run ({anc[:12]}: "
                       f"{', '.join(hit[:4])}) — production needs a run that saw them (--strict)")
    if r.get("conclusion") == "success":
        return "ok", f"{workflow}: no run on {sha[:12]}; nothing in its area changed since its green run on {anc[:12]}"
    return "red", f"{workflow}: RED on {anc[:12]} ({r.get('conclusion')}), unchanged since: {r.get('url')}"


def newest_finished_ancestor_run(workflow: str, sha: str, runs_of, is_ancestor) -> tuple[dict | None, int]:
    """(T1059) (the newest finished run, red or green, at an ancestor of the sha, or None when there is none; how many
    runs were read). Pages through ALL the workflow's runs (every branch, newest first) until one is found or the runs
    end, so the answer does not hang on how many other branches ran since. MAX_PAGES full pages and still none:
    CannotTell, said — never "no run"."""
    seen: set = set()
    ancestor: dict[str, bool] = {}
    for page in range(1, MAX_PAGES + 1):
        got = runs_of(workflow, page=page)
        for r in got:
            seen.add(r.get("databaseId") or id(r))  # a run started while paging pushes one onto the next page: count once
            anc = r.get("headSha") or ""
            if not anc or r.get("status") != "completed" or r.get("conclusion") not in RED | {"success"}:
                continue
            if anc not in ancestor:
                ancestor[anc] = is_ancestor(anc, sha)
            if ancestor[anc]:
                return r, len(seen)
        if len(got) < PER_PAGE:                 # a short page is the last one: every run was read
            return None, len(seen)
    raise CannotTell(f"read the newest {len(seen)} runs ({MAX_PAGES} pages of {PER_PAGE}) and none is at "
                     f"{sha[:12]} or a commit before it; more runs exist (T1059: raise MAX_PAGES in scripts/ci_status.py, "
                     f"or --skip REASON)")


def check(sha: str, branch: str = TRUNK, runs_of=gh_runs, strict: bool = False, **git) -> tuple[int, list[str]]:
    """(exit code, lines) over every workflow. Runs at the sha (any branch) first, then the branch's (T995 review);
    `strict`: production's rule (`strict_judge`)."""
    lines, worst = [], 0
    for wf in WORKFLOWS:
        try:
            if strict:
                verdict, said = strict_judge(wf, sha, runs_of, **git)
            else:
                verdict, said = judge(wf, runs_of(wf, commit=sha) or runs_of(wf, branch=branch), sha)
        except CannotTell as e:
            verdict, said = "cannot", f"{wf}: CANNOT TELL — {e}"
        lines.append(("ok    " if verdict == "ok" else "WARN  " if verdict == "warn" else "STOP  ") + said)
        worst = max(worst, {"ok": 0, "warn": 0, "red": 1, "cannot": 2}[verdict])
    return worst, lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sha", default=None, help="the commit to judge (default HEAD)")
    ap.add_argument("--branch", default=None, help="whose runs to read (default: the branch HEAD is on, T995)")
    ap.add_argument("--strict", action="store_true",
                    help="(T995 review) production: no finished green run at the sha (or unchanged since one) stops")
    ap.add_argument("--skip", default=os.environ.get("CI_CHECK_SKIP"), metavar="REASON",
                    help="pass without asking, printing the reason (also CI_CHECK_SKIP)")
    a = ap.parse_args(argv)
    if a.skip is not None:
        if not a.skip.strip():
            print("ci-status: --skip / CI_CHECK_SKIP needs a reason, which is printed", file=sys.stderr)
            return 2
        print(f"ci-status: SKIPPED (not asked): {a.skip}")
        return 0
    sha = a.sha or subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
    rc, lines = check(sha, a.branch or current_branch(), strict=a.strict)
    for ln in lines:
        print(f"ci-status: {ln}")
    if rc:
        print("ci-status: " + ("CI is red: fix it (or read the run) first; --skip REASON / CI_CHECK_SKIP=REASON goes on"
                               if rc == 1 else "could not read CI (gh): fix gh, or --skip REASON / CI_CHECK_SKIP=REASON"),
              file=sys.stderr)
    return rc


if __name__ == "__main__":
    sys.exit(main())
