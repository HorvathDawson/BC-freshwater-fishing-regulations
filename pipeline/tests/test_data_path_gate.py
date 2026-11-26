"""(T1021) pipeline-ci's "No hardcoded curated or source paths" gate, run as CI runs it (the step's own `run`, read
from the workflow) on a planted tree and on this one.

The gate matched `"data/<one segment>.<ext>"` and two allowlisted forms, so `Path("data/source/hydat.sqlite3")` and
`Path("cache/dfo_salmon/history")` lived (T994 finding 7). It now refuses (scripts/check_data_paths.py, by the AST) any
string that starts a path under data/ or cache/, with no file allowlisted and only docstrings and comments exempt: a
string that only names a path carries `# path-label: <why>` on its line, and the labels let through are pinned here.
"""
from __future__ import annotations

import os
import subprocess
import sys
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
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"     # `python`, as CI's setup-python
    return subprocess.run(["bash", "-e", "-c", _step()], cwd=cwd, env=env, capture_output=True, text=True)


def _tree(tmp_path: Path, files: dict[str, str]) -> Path:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True, env=env)
    files = {"scripts/check_data_paths.py": (REPO / "scripts/check_data_paths.py").read_text(), **files}
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, env=env)        # the gate reads tracked files only
    return tmp_path


#: each a line the gate must refuse in pipeline code: the T994 7 forms, the ones the old allowlist let through, and
#: (the #97 review) the forms the grep missed: a bare tree name starting a path, `./`, a concatenation, another case,
#: and a code line holding a backtick
PLANTED = [
    'X = Path("data/source/x.json")',
    'ap.add_argument("--source", type=Path, default=Path("data/source/bc_fisheries_data.gpkg"))',
    'HISTORY = Path("cache/dfo_salmon/history")',
    'paths = glob.glob(str(repo / "cache/dfo_salmon/raw/region*-eng.html"))',
    'ap.add_argument("--releases", type=Path, default=Path("data/release"))',
    "out = f'data/generated/side/{name}'",
    'PDF = os.path.join("data", "source", "fishing_synopsis.pdf")',
    'sys.path.insert(0, str(Path("data").resolve()))',
    'X = Path("data") / "source" / "x.json"',
    'X = Path("./data/source/x.json")',
    'X = Path("data/" + "source/x.json")',
    'X = Path("Cache/Dfo_salmon")',
    'X = Path("data/source/x.json")  # see `SOURCE`',
    'X = Path("data/curated/waters/cuts.json")  # path-label: short',          # a reason under 10 characters
    'X = Path("data/curated/waters/cuts.json")  # path-label:',
]


@pytest.mark.parametrize("where", ["pipeline", "pipeline/regs/dfo_salmon", "tools/curation-review/backend"])
def test_the_gate_refuses_a_planted_data_or_cache_literal(tmp_path, where):
    """Each planted line in a file of its own, under a top folder and nested. Mutations: narrow the pattern, exempt a
    file or a folder, drop the bare-name rule, take a marker without a reason, or exempt backtick lines: one plant
    passes the gate and this fails."""
    tree = _tree(tmp_path, {f"{where}/p{i}.py": f"import os, sys\nfrom pathlib import Path\n{line}\n"
                            for i, line in enumerate(PLANTED)})
    got = _gate(tree)
    assert got.returncode == 1, got.stdout + got.stderr
    refused = got.stdout.split("data/… or cache/… string in code")[0]          # the offenders, before the labels
    missed = [line for i, line in enumerate(PLANTED) if f"{where}/p{i}.py:3:" not in refused]
    assert missed == [], got.stdout


def test_the_gate_takes_comments_docs_tests_labels_and_other_words(tmp_path):
    """What the gate leaves alone: a comment, a docstring, a test file, a string that says why it only names a path
    (listed), a help text whose path is not where its string starts, and strings that are not a path under data/ or
    cache/ (`ROOT / "data" / …` is anchored at the root: the step's older checks hold that form). A label on a line with
    no such string is stale and refused."""
    tree = _tree(tmp_path, {
        "pipeline/ok.py": "\n".join([
            '"""`data/curated/waters/cuts.json` is CURATED.waters.cuts."""',
            "# reads data/source/x.json through SOURCE",
            "from pathlib import Path",
            'HELP = ("the folder (default "',
            '        "data/release/LATEST)")',
            'LABEL = "data/source/x.json"  # path-label: provenance written into an output',
            'X = "metadata/source/x.json"',
            'Y = "app/packages/data/src/pack.generated.ts"',
            'Z = Path("cached/dfo_salmon")',
            'W = {"data": 1}["data"]',
            'def f():',
            '    """data/source/x.json in a function docstring."""',
        ]) + "\n",
        "pipeline/tests/test_x.py": 'X = Path("data/source/x.json")\n',
    })
    got = _gate(tree)
    assert got.returncode == 0, got.stdout + got.stderr
    assert "path-labels let through                  1" in got.stdout
    assert "pipeline/ok.py:6: 'data/source/x.json'  (provenance written into an output)" in got.stdout
    stale = _tree(tmp_path / "stale", {"pipeline/s.py": 'X = 1  # path-label: nothing here to name a path\n'})
    got = _gate(stale)
    assert got.returncode == 1 and "pipeline/s.py:1: a path-label on no data/cache string" in got.stdout


#: (the #97 review) EVERY string the gate lets through on its label, by file and text (line numbers move with edits):
#: a new label is a diff here, read in review
LABELS = {
    ("pipeline/atlas/build.py", "data/source/municipal/osm_streams.geojson"),
    ("pipeline/atlas/build.py", "data/source/municipal/manual_streams.geojson"),
    ("pipeline/atlas/build.py", "data/source/municipal ("),
    ("pipeline/common/licence.py", "data/LICENSE-ODbL.md"),
    ("pipeline/deliver/release/regs_fixture.py", "data/release/r-*"),
    ("pipeline/gauges/feed/publish.py", "data/source/bc_hydrometric_stations.json"),
    ("pipeline/regs/review/split_suggest.py", "data/curated/waters/cuts.json anchor: "),
    ("pipeline/regs/review/tidal.py", "data/source/dfo_tidal_areas/2026-10-28/fm-gp/rec/definitions-eng.html"),
    ("pipeline/regs/steelhead/known_waters.py", "data/generated/steelhead/steelhead_waters.json"),
    ("pipeline/release.py", "data/curated"),
    ("pipeline/release.py", "data/"),
    ("pipeline/release.py", "data/<r-id> only (a data-only release, app "),
    ("pipeline/release.py", "data/<r-id> + v"),
    ("pipeline/tools/audit/tidal_candidates.py", "data/curated/gauges/tidal_stations.json + HYDAT remarks"),
}


def test_the_labels_let_through_are_the_pinned_list():
    import importlib.util
    spec = importlib.util.spec_from_file_location("check_data_paths", REPO / "scripts" / "check_data_paths.py")
    cdp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cdp)
    got = []
    for rel in cdp.files(REPO):
        bad, let, stale = cdp.check(rel, (REPO / rel).read_text(encoding="utf-8"))
        assert not bad and not stale, (bad, stale)
        got += [(rel, eval(x.split(": ", 1)[1].rsplit("  (", 1)[0])) for x in let]   # noqa: S307 (its own repr)
    assert set(got) == LABELS and len(got) == 15


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
