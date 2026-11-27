"""(T1193, owner 2026-11-26 "yes with loud default") A PLAIN `pytest` IS CI'S NO-DATA TIER, AND SAYS SO: with no `-m`
the root `conftest.py` deselects what CI's "Tests" step deselects (`tiers.ci_markers`, read from the workflow), and the
run's last line counts the data tests it left out and names `--with-data`. They are deselected, never skipped
(test_no_silent_skips.py). `--with-data` is `-m "not slow"` (the default before); an explicit `-m` is left as given.
Real pytest runs over four planted tests (`fixtures/default_tier/planted.py`: no-data, needs_tiles, slow, slow+needs_cache)."""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from pipeline.tests import tiers

ROOT = Path(__file__).resolve().parents[2]
PLANTED = "pipeline/tests/fixtures/default_tier/planted.py"
LOUD = re.compile(r"^!+ (.*NOT RUN.*) !+$")


def _ci_markers_from_the_shell() -> str:
    out = subprocess.run(["bash", "-c", ". scripts/test-env.sh && no_data_markers"], cwd=ROOT, capture_output=True,
                         text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def _run(*args: str) -> tuple[int, set[str], list[str], str]:
    """(exit code, the planted tests that RAN, the loud lines, the whole output) of one real pytest run."""
    out = subprocess.run([sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider", *args, PLANTED], cwd=ROOT,
                         capture_output=True, text=True, timeout=120)
    text = out.stdout + out.stderr
    ran = set(re.findall(r"planted\.py::(\w+) (?:PASSED|FAILED|ERROR)", text))
    return out.returncode, ran, [m.group(1) for ln in text.splitlines() if (m := LOUD.match(ln))], text


def test_the_default_expression_is_ci_s():
    """The pin: what a plain run deselects by is CI's "Tests" step expression, the one the shell runners read too, and
    it is a no-data tier (it leaves out slow and every needs_* kind)."""
    expr = tiers.ci_markers()
    assert expr == _ci_markers_from_the_shell()
    assert tiers.is_no_data(expr, tiers.data_markers()), expr
    assert set(re.findall(r"not (\w+)", expr)) == set(tiers.data_markers())


def test_a_plain_run_is_the_no_data_tier_and_counts_what_it_left_out():
    rc, ran, loud, text = _run()
    assert rc == 0, text
    assert ran == {"test_no_data"}, text
    assert loud == ["2 data tests NOT RUN (they need built data): add --with-data (and 1 slow: -m slow)"], text
    assert "3 deselected" in text and "skipped" not in text, text           # deselected and counted, never a skip
    assert tiers.HIDDEN in text, text


def test_with_data_runs_the_data_tests_and_is_quiet():
    rc, ran, loud, text = _run("--with-data")
    assert rc == 0, text
    assert ran == {"test_no_data", "test_needs_data"}, text                 # -m "not slow": today's data tier
    assert loud == [] and tiers.VISIBLE in text, text


def test_an_explicit_m_is_left_alone():
    """A script's or CI's `-m` decides alone: no default applied on top, no loud line."""
    rc, ran, loud, text = _run("-m", tiers.ci_markers())
    assert (rc, ran, loud) == (0, {"test_no_data"}, []), text
    rc, ran, loud, text = _run("-m", "needs_cache")
    assert (rc, ran, loud) == (0, {"test_slow_and_needs_data"}, []), text
    assert tiers.VISIBLE in text, text


def test_minus_k_narrows_first_so_only_the_tier_s_leavings_are_counted():
    rc, ran, loud, text = _run("-k", "needs or no_data")                    # -k drops test_slow before the tier runs
    assert rc == 0 and ran == {"test_no_data"}, text
    assert loud == ["2 data tests NOT RUN (they need built data): add --with-data"], text


def test_with_data_and_minus_m_together_is_refused():
    rc, ran, _, text = _run("--with-data", "-m", "slow")
    assert rc == 4 and not ran, text
    assert "--with-data and -m 'slow' together" in text, text
