"""`scripts/before_pr.sh` (T1041): the whole app suite and the no-data tier, once before the PR, both through heavy.sh,
ending in ONE verdict line with both exit codes. The suites are stood in for (BEFORE_PR_SELFTEST) — each is a real run
of minutes; the commands it runs are pinned from the script's text."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

SCRIPT = Path(REPO_ROOT) / "scripts" / "before_pr.sh"


def _run(codes: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "BEFORE_PR_SELFTEST": codes}
    return subprocess.run(["bash", str(SCRIPT)], cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=60)


def test_one_line_with_both_exit_codes_and_a_failure_fails():
    """Mutation: drop either rc from the PASS condition -> the matching FAIL case passes and this fails."""
    ok = _run("0 0")
    assert ok.returncode == 0 and ok.stdout.rstrip().splitlines()[-1].startswith(
        "before-pr: PASS (app suite rc=0, pipeline tier rc=0, ")
    for codes, said in (("1 0", "app suite rc=1, pipeline tier rc=0"), ("0 5", "app suite rc=0, pipeline tier rc=5"),
                        ("2 3", "app suite rc=2, pipeline tier rc=3")):
        got = _run(codes)
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
