"""Is CI green on HEAD? (T533) — the newest `app-ci` and `pipeline-ci` runs for the branch, read with `gh`.

    python scripts/ci_status.py [--sha SHA] [--branch B] [--skip REASON]

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

WORKFLOWS = ("app-ci.yml", "pipeline-ci.yml")
BRANCH = "redesign/stream-sections"
RED = {"failure", "timed_out", "startup_failure", "action_required"}
FIELDS = "databaseId,headSha,status,conclusion,url,createdAt,event"


class CannotTell(Exception):
    pass


def gh_runs(workflow: str, branch: str, limit: int = 30) -> list[dict]:
    """The workflow's newest runs on the branch, newest first (the one place `gh` is called; tests replace it)."""
    try:
        got = subprocess.run(["gh", "run", "list", "--workflow", workflow, "--branch", branch, "--limit", str(limit),
                              "--json", FIELDS], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise CannotTell(f"gh run list --workflow {workflow}: {e}") from e
    if got.returncode:
        raise CannotTell(f"gh run list --workflow {workflow} failed (rc={got.returncode}): {got.stderr.strip()[-300:]}")
    try:
        return json.loads(got.stdout)
    except ValueError as e:
        raise CannotTell(f"gh run list --workflow {workflow}: unreadable output ({e})") from e


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


def check(sha: str, branch: str = BRANCH, runs_of=gh_runs) -> tuple[int, list[str]]:
    """(exit code, lines) over every workflow."""
    lines, worst = [], 0
    for wf in WORKFLOWS:
        try:
            verdict, said = judge(wf, runs_of(wf, branch), sha)
        except CannotTell as e:
            verdict, said = "cannot", f"{wf}: CANNOT TELL — {e}"
        lines.append(("ok    " if verdict == "ok" else "WARN  " if verdict == "warn" else "STOP  ") + said)
        worst = max(worst, {"ok": 0, "warn": 0, "red": 1, "cannot": 2}[verdict])
    return worst, lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sha", default=None, help="the commit to judge (default HEAD)")
    ap.add_argument("--branch", default=BRANCH)
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
    rc, lines = check(sha, a.branch)
    for ln in lines:
        print(f"ci-status: {ln}")
    if rc:
        print("ci-status: " + ("CI is red: fix it (or read the run) first; --skip REASON / CI_CHECK_SKIP=REASON goes on"
                               if rc == 1 else "could not read CI (gh): fix gh, or --skip REASON / CI_CHECK_SKIP=REASON"),
              file=sys.stderr)
    return rc


if __name__ == "__main__":
    sys.exit(main())
