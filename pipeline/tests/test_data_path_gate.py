"""(T1021) pipeline-ci's "No hardcoded curated or source paths" gate, run as CI runs it (the step's own `run`, read
from the workflow) on a planted tree and on this one.

The gate matched `"data/<one segment>.<ext>"` and two allowlisted forms, so `Path("data/source/hydat.sqlite3")` and
`Path("cache/dfo_salmon/history")` lived (T994 finding 7). It now refuses any quoted string that starts a path under
data/ or cache/, with no file allowlisted: a string that only names a path carries `# path-label: <why>` on its line.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
STEP = "No hardcoded curated or source paths"


def _step() -> str:
    jobs = yaml.safe_load((REPO / ".github/workflows/pipeline-ci.yml").read_text())["jobs"]
    runs = [s["run"] for j in jobs.values() for s in j["steps"] if s.get("name") == STEP]
    assert len(runs) == 1, f"pipeline-ci.yml: step {STEP!r} found {len(runs)} times"
    return runs[0]


def _gate(cwd: Path) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(["bash", "-e", "-c", _step()], cwd=cwd, env=env, capture_output=True, text=True)


def _tree(tmp_path: Path, files: dict[str, str]) -> Path:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True, env=env)
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, env=env)        # the gate reads tracked files only
    return tmp_path


#: each a line the gate must refuse in pipeline code (the T994 7 forms, and the ones the old allowlist let through)
PLANTED = [
    'X = Path("data/source/x.json")',
    'ap.add_argument("--source", type=Path, default=Path("data/source/bc_fisheries_data.gpkg"))',
    'HISTORY = Path("cache/dfo_salmon/history")',
    'paths = glob.glob(str(repo / "cache/dfo_salmon/raw/region*-eng.html"))',
    'ap.add_argument("--releases", type=Path, default=Path("data/release"))',
    "out = f'data/generated/side/{name}'",
    'X = Path("data/curated/waters/cuts.json")  # path-label: short',          # a reason under 10 characters
    'X = Path("data/curated/waters/cuts.json")  # path-label:',
]


@pytest.mark.parametrize("line", PLANTED)
def test_the_gate_refuses_a_planted_data_or_cache_literal(tmp_path, line):
    """Mutation: narrow the pattern back to `data/(curated|generated|source)/` as a value, or exempt a file, or take
    a marker without a reason: one of these passes the gate and this fails."""
    tree = _tree(tmp_path, {"pipeline/x.py": f"from pathlib import Path\n{line}\n"})
    got = _gate(tree)
    assert got.returncode == 1, got.stdout + got.stderr
    assert f"pipeline/x.py:2:{line}" in got.stdout


def test_the_gate_takes_comments_docs_tests_labels_and_other_words(tmp_path):
    """What the gate leaves alone: a comment line, a backticked docstring line, a test file, a line that says why it
    only names a path, and a string that is not a path under data/ or cache/."""
    tree = _tree(tmp_path, {
        "pipeline/ok.py": "\n".join([
            "# reads data/source/x.json through SOURCE",
            '"""`data/curated/waters/cuts.json` is CURATED.waters.cuts."""',
            'HELP = ("the folder (default "',
            '        "data/release/LATEST)")  # path-label: words in a help text',
            'X = "metadata/source/x.json"',
            'Y = "app/packages/data/src/pack.generated.ts"',
            'Z = Path("cached/dfo_salmon")',
        ]) + "\n",
        "pipeline/tests/test_x.py": 'X = Path("data/source/x.json")\n',
    })
    got = _gate(tree)
    assert got.returncode == 0, got.stdout + got.stderr
    assert "quoted data/… or cache/… literal" in got.stdout


def test_the_live_tree_passes_the_gate():
    got = _gate(REPO)
    assert got.returncode == 0, got.stdout + got.stderr


def test_the_dfo_cache_has_one_name():
    """(T1021) the DFO page cache was `Path("cache/dfo_salmon")` (the cwd's) in the scrape and `parents[N] / "cache" /
    "dfo_salmon"` in the review readers: from the repo root one directory, now one config key for all of them."""
    from project_config import get_config
    from pipeline.regs.dfo_salmon import churn, fetch, splitwork
    from pipeline.regs.review import dfo as RD
    cache = get_config().dfo_salmon_cache
    assert cache.is_absolute() and cache == get_config().project_root / "cache" / "dfo_salmon"
    assert fetch.DEFAULT_CACHE == cache and RD.default_history_dir() == cache / "history"
    assert churn._HISTORY == splitwork.HISTORY == cache / "history" and splitwork.RAW == cache / "raw"
