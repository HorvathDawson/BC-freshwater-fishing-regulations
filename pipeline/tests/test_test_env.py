"""THE TEST-ENVIRONMENT CONTRACT (T667 slice 7): the tier, not the runner, decides what a test sees
(`pipeline/tests/tiers.py`, the repo-root `conftest.py`), and the shell half has one home (`scripts/test-env.sh`)."""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.tests import tiers
from pipeline.tests.conftest import KINDS

ROOT = Path(__file__).resolve().parents[2]
RUNNERS = ("scripts/test-smoke.sh", "scripts/test-full.sh", "scripts/pytest-chunked.sh", "scripts/fast-fail.sh",
           "scripts/fresh-copy-tests.sh")


def _ci_markers() -> str:
    out = subprocess.run(["bash", "-c", ". scripts/test-env.sh && no_data_markers"], cwd=ROOT, capture_output=True,
                         text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def _source(env_py: str | None, cwd: Path) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "PIPELINE_PY"} | ({"PIPELINE_PY": env_py} if env_py else {})
    return subprocess.run(["bash", "-c", f'. "{ROOT}/scripts/test-env.sh" && echo "PY=$PY"'], cwd=cwd, env=env,
                          capture_output=True, text=True)


def test_the_interpreter_is_named_one_way_and_a_missing_one_stops(tmp_path):
    """PIPELINE_PY names it (CI sets it); unset, the checkout's .venv; neither runnable stops, naming the file and what
    to set — never whatever python is on PATH (CI red on b07996827: a hosted runner has no .venv)."""
    assert _source(sys.executable, tmp_path).stdout.strip() == f"PY={sys.executable}"
    if (ROOT / ".venv/bin/python").exists():
        assert _source(None, ROOT).stdout.strip() == "PY=.venv/bin/python"
    for py in (None, str(tmp_path / "no-such-python")):           # tmp_path has no .venv
        out = _source(py, tmp_path)
        assert out.returncode != 0 and "PY=" not in out.stdout, out.stdout
        assert "scripts/test-env.sh" in out.stderr and "PIPELINE_PY" in out.stderr, out.stderr


def test_ci_names_its_interpreter():
    import yaml
    jobs = yaml.safe_load((ROOT / ".github/workflows/pipeline-ci.yml").read_text(encoding="utf-8"))["jobs"]
    assert jobs["check"]["env"]["PIPELINE_PY"] == "python"


def test_the_data_markers_are_slow_and_every_needs_kind():
    assert set(tiers.data_markers()) == {"slow"} | {f"needs_{k}" for k in KINDS}


@pytest.mark.parametrize("expr, no_data", [
    ("smoke", True),                                   # the smoke tier: a pick never carries needs_* / slow (smoke.py)
    ("not slow", False), ("", False), ("slow", False), ("needs_bundle", False), ("not slow or needs_atlas", False),
    ("not slow and not needs_bundle", False),          # mutation: one data kind short of CI's tier is a data run
])
def test_a_run_is_no_data_only_when_it_leaves_out_every_data_marker(expr, no_data):
    assert tiers.is_no_data(expr, tiers.data_markers()) is no_data


def test_ci_s_tier_is_a_no_data_run():
    expr = _ci_markers()
    assert tiers.is_no_data(expr, tiers.data_markers()), expr
    dropped = expr.replace(" and not needs_cache", "")
    assert dropped != expr and not tiers.is_no_data(dropped, tiers.data_markers())


@pytest.mark.smoke
def test_the_tier_not_the_runner_decides_whether_builds_are_hidden():
    """Real pytest runs (collection only): CI's tier and the smoke tier hide the builds with no plugin flag; the default
    run sees them. The header line is what the root conftest decided."""
    me = Path(__file__).relative_to(ROOT).as_posix()

    def header(*m):
        out = subprocess.run([sys.executable, "-m", "pytest", "--collect-only", "-p", "no:cacheprovider", *m, me],
                             cwd=ROOT, capture_output=True, text=True)
        assert out.returncode == 0, out.stdout + out.stderr
        return [l for l in out.stdout.splitlines() if l.startswith("test tier:")]

    assert header("-m", _ci_markers()) == [tiers.HIDDEN]
    assert header("-m", "smoke") == [tiers.HIDDEN]
    assert header() == [tiers.VISIBLE]


def test_every_runner_takes_its_environment_from_the_one_script():
    for f in RUNNERS:
        sh = (ROOT / f).read_text(encoding="utf-8")
        code = "\n".join(l for l in sh.splitlines() if not l.lstrip().startswith("#"))
        assert re.search(r"^\s*\. scripts/test-env\.sh\b", code, re.M), f"{f} does not source scripts/test-env.sh"
        assert "pipeline.tests.ci_no_build" not in code, f"{f} passes the no-build plugin: the tier decides (tiers.py)"
        assert "PYTHONPYCACHEPREFIX=$(mktemp" not in code, f"{f} makes its own bytecode prefix: test_env_pycache"
        assert '"name") == "Tests"' not in code, f"{f} parses CI's Tests step itself: no_data_markers"
