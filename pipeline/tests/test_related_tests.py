"""`scripts/related_tests.py` (T1041): the tests a change reaches, for the pre-push hook and the mutation report. Over a
made-up tree: a test that imports a changed module DIRECTLY is chosen (absolute, `from … import`, relative); one that
reaches it only through another module is not; a changed data file counts as a change to the module naming it; a
conftest, a deletion and the cap are said, never dropped quietly."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pipeline.common.curated import REPO_ROOT

spec = importlib.util.spec_from_file_location("related_tests", Path(REPO_ROOT) / "scripts" / "related_tests.py")
RT = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = RT
spec.loader.exec_module(RT)


def _tree(tmp_path: Path) -> Path:
    files = {
        "pytest.ini": "[pytest]\ntestpaths = pipeline/tests\n",
        "pipeline/__init__.py": "",
        "pipeline/a/__init__.py": "",
        "pipeline/a/leaf.py": "X = 1\n",
        "pipeline/a/mid.py": "from pipeline.a.leaf import X\n",
        "pipeline/a/near.py": "from . import leaf\nPIN = 'pin.json'\n",
        "pipeline/a/pin.json": "{}",
        "pipeline/tests/__init__.py": "",
        "pipeline/tests/test_direct.py": "import pipeline.a.leaf\n",
        "pipeline/tests/test_from.py": "from pipeline.a import leaf\n",
        "pipeline/tests/test_through_mid.py": "from pipeline.a.mid import X\n",
        "pipeline/tests/test_near.py": "from pipeline.a.near import PIN\n",
        "pipeline/tests/test_other.py": "x = 1\n",
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    return tmp_path


def test_direct_importers_only_and_changed_tests_first(tmp_path):
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["pipeline/a/leaf.py"], root)
    # test_through_mid reaches leaf only through mid: not chosen (the transitive closure is the whole tier)
    assert got == ["pipeline/tests/test_direct.py", "pipeline/tests/test_from.py"]
    assert said == ["1 changed pipeline module(s): 2 test file(s) import one directly"]
    got, _ = RT.pytest_related(["pipeline/a/mid.py", "pipeline/tests/test_other.py"], root)
    assert got == ["pipeline/tests/test_other.py", "pipeline/tests/test_through_mid.py"]
    # relative import: `from . import leaf` in near.py names pipeline.a.leaf (near's own test is not leaf's)
    mods, rev = RT.import_graph(root)
    assert "pipeline.a.near" in rev["pipeline.a.leaf"] and "pipeline.a.mid" in rev["pipeline.a.leaf"]


def test_a_data_file_counts_as_a_change_to_the_module_that_names_it(tmp_path):
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["pipeline/a/pin.json"], root)
    assert got == ["pipeline/tests/test_near.py"] and said[0] == "pipeline/a/pin.json: read by pipeline.a.near"


def test_what_is_not_followed_is_said(tmp_path):
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["conftest.py", "pipeline/tests/conftest.py", "pipeline/gone.py",
                                   "pipeline/a/gone.json", "app/x.ts", "README.md"], root)
    assert got == []
    assert [s.split(":")[0] for s in said] == ["conftest.py", "pipeline/tests/conftest.py", "pipeline/gone.py",
                                               "pipeline/a/gone.json"]
    assert all(("conftest" in s) or ("deleted" in s) for s in said)


def test_the_cap_names_what_it_leaves(tmp_path):
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["pipeline/a/leaf.py", "pipeline/tests/test_other.py"], root, cap=2)
    assert got == ["pipeline/tests/test_other.py", "pipeline/tests/test_direct.py"]
    assert said[-1] == "CAPPED at 2 of 3 test files: 1 left to scripts/before_pr.sh and the PR's tier: test_from.py"


def test_the_app_side_hands_vitest_the_code_and_says_the_rest(tmp_path):
    root = tmp_path
    for rel in ("app/packages/core/src/a.ts", "app/packages/ui-native/src/B.tsx", "app/workers/feeds/src/c.ts",
                "app/packages/data/dev/mini-release/LATEST"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text("x")
    got, said = RT.app_related(["app/packages/ui-native/src/B.tsx", "app/packages/core/src/a.ts",
                                "app/workers/feeds/src/c.ts", "app/packages/data/dev/mini-release/LATEST",
                                "app/packages/core/src/gone.ts", "pipeline/x.py"], root)
    assert got == ["packages/core/src/a.ts", "packages/ui-native/src/B.tsx"]        # the worker: its own step
    assert len(said) == 2 and "deleted" in said[0] and "gone.ts" in said[0]
    assert "not code" in said[1] and "mini-release/LATEST" in said[1]


def test_only_regex_matches_exactly_those_paths():
    import re
    rx = re.compile(RT.only_regex(["pipeline/tests/test_a.py", "pipeline/tests/test_b.py"]))
    assert rx.search("pipeline/tests/test_a.py") and not rx.search("pipeline/tests/test_aXpy")
    assert not rx.search("pipeline/tests/test_a.py.bak") and not rx.search("x/pipeline/tests/test_b.py")


def test_on_this_repo_decided_reaches_the_test_that_failed_on_67():
    """The live tree: #67's change (decided.py, passage_boxes.py, passage_unlocated.json) reaches test_local_closures
    (red on CI 45239324023) and the T1032 tests, well under the cap."""
    got, _ = RT.pytest_related(["pipeline/deliver/answers/display/decided.py", "pipeline/regs/extraction/passage_boxes.py",
                                "pipeline/regs/extraction/passage_unlocated.json"])
    assert "pipeline/tests/test_local_closures.py" in got and "pipeline/tests/test_t1032_passage_boxes.py" in got
    assert len(got) < RT.PY_CAP
