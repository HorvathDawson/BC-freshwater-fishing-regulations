"""`scripts/before_pr.sh` (T1041): the whole app suite and the no-data tier, once before the PR, both through heavy.sh,
ending in ONE verdict line with both exit codes. The suites are stood in for (BEFORE_PR_SELFTEST) — each is a real run
of minutes; the commands it runs are pinned from the script's text."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

SCRIPT = Path(REPO_ROOT) / "scripts" / "before_pr.sh"


def title_of(n: int) -> str:
    """A Conventional Commits title of exactly n characters."""
    t = "ci: " + "a" * (n - 4)
    assert len(t) == n
    return t


def _run(codes: str, title: str = "ci: a title", *extra: str, env: dict | None = None) -> subprocess.CompletedProcess:
    env = {**os.environ, "BEFORE_PR_SELFTEST": codes, **(env or {})}
    return subprocess.run(["bash", str(SCRIPT), "--title", title, *extra], cwd=REPO_ROOT, env=env,
                          capture_output=True, text=True, timeout=60)


def _fake_gh(tmp_path: Path, pr: str = "265", issue: str = "262", rc: int = 0) -> dict:
    """(T1144) A `gh` on PATH that answers `pr list` / `issue list` with these numbers (or fails with rc): the real gh
    is never called."""
    d = tmp_path / "bin"
    d.mkdir(exist_ok=True)
    gh = d / "gh"
    gh.write_text(f'#!/bin/sh\n[ {rc} -ne 0 ] && {{ echo "gh: no network" >&2; exit {rc}; }}\n'
                  f'case "$1" in pr) echo {pr};; issue) echo {issue};; esac\n')
    gh.chmod(0o755)
    return {"PATH": f"{d}{os.pathsep}{os.environ['PATH']}"}


def test_one_line_with_both_exit_codes_and_a_failure_fails():
    """Mutation: drop either rc from the PASS condition -> the matching FAIL case passes and this fails."""
    ok = _run("0 0", "ci: a title", "--number", "100")
    assert ok.returncode == 0 and ok.stdout.rstrip().splitlines()[-1].startswith(
        "before-pr: PASS (title 11/93 ok, app suite rc=0, pipeline tier rc=0, ")
    for codes, said in (("1 0", "app suite rc=1, pipeline tier rc=0"), ("0 5", "app suite rc=0, pipeline tier rc=5"),
                        ("2 3", "app suite rc=2, pipeline tier rc=3")):
        got = _run(codes, "ci: a title", "--number", "100")
        assert got.returncode == 1, codes
        assert f"before-pr: FAIL ({said}, " in got.stderr, (codes, got.stderr)
        # the failed suite's own output is shown, not only its code
        assert "FAILED (rc=" in got.stdout and "selftest" in got.stdout, codes


def test_it_runs_the_whole_suites_through_heavy_sh():
    text = SCRIPT.read_text()
    assert 'bash scripts/heavy.sh\n           "before-pr-app-$here" 30 -- pnpm -C app exec vitest run)' in text
    assert ('tier_cmd=(bash scripts/heavy.sh "before-pr-tier-$here" 30 -- bash scripts/pytest-chunked.sh -- -q -m '
            '"$markers")') in text
    assert "markers=$(no_data_markers) || exit 1" in text            # CI's own marker expression
    assert os.access(SCRIPT, os.X_OK)


def test_the_app_suite_takes_half_the_cores_on_a_laptop():
    """(T1041) with cores - 1 workers the whole suite exited 1 with every test passed when other lanes shared the Mac
    ("[vitest-worker]: Timeout calling onTaskUpdate", L21/L22/L23 2026-11-23); half the cores ran clean under the same
    load and heavier. CI keeps its 2. Mutation: back to the default (`{}` off CI) -> fails."""
    text = (Path(REPO_ROOT) / "app" / "vitest.config.ts").read_text()
    assert "maxWorkers: process.env.CI ? 2 : Math.max(2, Math.floor(availableParallelism() / 2))," in text
    assert 'import { availableParallelism } from "node:os";' in text
    assert "dangerouslyIgnoreUnhandledErrors" not in text            # an unhandled error still fails the run


def test_the_title_is_checked_with_its_suffix_at_the_limit(tmp_path):
    """(T1144) CI checks the merge commit "<title> (#N)" at 100: 93 characters for a 3-digit number, 92 for 4 digits.
    Exactly at the limit passes; one over fails, says by how many, and runs nothing else (the selftest suites would
    otherwise print their lines). Mutation: check the bare title (drop --number) -> the over-limit cases pass, fail."""
    for number, limit in (("266", 93), ("1000", 92)):
        ok = _run("0 0", title_of(limit), "--number", number)
        assert ok.returncode == 0, (number, ok.stderr, ok.stdout)
        assert f"title {limit}/{limit} ok" in ok.stdout
        bad = _run("0 0", title_of(limit + 1), "--number", number)
        assert bad.returncode == 1, (number, bad.stdout)
        assert f"1 over the limit of {limit} for PR #{number}" in bad.stderr, bad.stderr
        assert "selftest" not in bad.stdout and "rc=" not in bad.stderr        # nothing heavy started


def test_the_next_number_is_the_highest_pr_or_issue_plus_one(tmp_path):
    env = _fake_gh(tmp_path, pr="998", issue="999")               # #1000 is the next: the 4-digit limit, 92
    assert _run("0 0", title_of(92), env=env).returncode == 0
    assert _run("0 0", title_of(93), env=env).returncode == 1
    env = _fake_gh(tmp_path, pr="265", issue="262")               # #266: 93
    got = _run("0 0", title_of(93), env=env)
    assert got.returncode == 0 and "title 93/93 ok" in got.stdout, got.stderr


def test_gh_cannot_answer_and_no_number_stops_saying_so(tmp_path):
    got = _run("0 0", "ci: a title", env=_fake_gh(tmp_path, rc=1))
    assert got.returncode == 2 and "gh could not tell the next PR number: pass --number N" in got.stderr
    assert "PASS" not in got.stdout
    assert _run("0 0", "ci: a title", "--number", "100", env=_fake_gh(tmp_path, rc=1)).returncode == 0   # --number needs no gh
    missing = subprocess.run(["bash", str(SCRIPT)], cwd=REPO_ROOT, capture_output=True, text=True, timeout=60,
                             env={**os.environ, "BEFORE_PR_SELFTEST": "0 0"})
    assert missing.returncode == 2 and "--title" in missing.stderr and "required" in missing.stderr
