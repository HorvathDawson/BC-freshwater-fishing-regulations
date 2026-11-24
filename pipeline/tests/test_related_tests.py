"""`scripts/related_tests.py` (T1041): the tests a change reaches, for the pre-push hook and the mutation report. Over a
made-up tree: a test that imports a changed module DIRECTLY is chosen (absolute, `from … import`, relative); then
(T1041 review, 0.9.1) one level up, a test of a module that imports it, at most PY_UP_CAP, never through a package
`__init__`; a changed data file counts as a change to the module naming it; a conftest, a deletion, an `__init__` not
followed and both caps are said, never dropped quietly."""
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
        "pipeline/tests/test_script.py": "SCRIPT = ROOT / 'scripts' / 'tool.py'\n",
        "scripts/tool.py": "print(1)\n",
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text)
    return tmp_path


def test_direct_importers_only_and_changed_tests_first(tmp_path):
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["pipeline/a/leaf.py"], root, depth=1)
    # depth 1 (the mutation report): test_through_mid reaches leaf only through mid, not chosen
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
    assert said[-1] == ("CAPPED at 2 of 5 test files (direct importers first): 3 left to scripts/before_pr.sh and the "
                        "PR's tier: test_from.py, test_near.py, test_through_mid.py")


def test_one_level_up_reaches_the_tests_of_a_module_that_imports_the_change(tmp_path):
    """(T1041 review, 0.9.1) the hook's default: after the direct importers, the tests of each module that imports a
    changed one (mid and near import leaf), never a test twice. Mutation: drop the level-up -> fails."""
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["pipeline/a/leaf.py"], root)
    assert got == ["pipeline/tests/test_direct.py", "pipeline/tests/test_from.py",       # direct first
                   "pipeline/tests/test_near.py", "pipeline/tests/test_through_mid.py"]
    assert said == ["1 changed pipeline module(s): 2 test file(s) import one directly",
                    "one level up: 2 more test file(s) import one of the 2 module(s) that import a changed one"]
    # a test that imports both the change and a middle is a direct one, listed once
    (root / "pipeline/tests/test_direct.py").write_text("import pipeline.a.leaf\nimport pipeline.a.mid\n")
    got, _ = RT.pytest_related(["pipeline/a/leaf.py"], root)
    assert got.count("pipeline/tests/test_direct.py") == 1 and len(got) == 4


def test_one_level_up_has_its_own_cap_and_never_goes_through_an_init(tmp_path):
    """At most `up_cap` level-up files, the rest named; a package `__init__` that imports the change is not followed
    (it re-exports into most of the tree), and is named. Mutations: ignore up_cap, or follow an __init__ -> fails."""
    root = _tree(tmp_path)
    (root / "pipeline/a/__init__.py").write_text("from . import leaf\n")
    (root / "pipeline/tests/test_pkg.py").write_text("import pipeline.a\n")
    got, said = RT.pytest_related(["pipeline/a/leaf.py"], root, up_cap=1)
    assert got == ["pipeline/tests/test_direct.py", "pipeline/tests/test_from.py", "pipeline/tests/test_near.py"]
    assert said[1] == ("one level up: 2 more test file(s) import one of the 2 module(s) that import a changed one; 1 "
                       "taken (at most 1), 1 left to scripts/before_pr.sh and the PR's tier: test_through_mid.py")
    assert said[2] == ("one level up, not followed through 1 package __init__(s) (they re-export into most of the tree; "
                       "the tier's job): pipeline.a")
    assert "pipeline/tests/test_pkg.py" not in RT.pytest_related(["pipeline/a/leaf.py"], root)[0]


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


def test_a_changed_script_reaches_the_tests_that_name_it(tmp_path):
    """A scripts/*.py (or .sh) is loaded by path or run, never imported: the tests naming its file name. Mutation:
    drop the `named` set -> fails."""
    root = _tree(tmp_path)
    got, said = RT.pytest_related(["scripts/tool.py", "scripts/gone.sh"], root)
    assert got == ["pipeline/tests/test_script.py"]
    assert said == ["scripts/tool.py: named by 1 test file(s)",
                    "scripts/gone.sh: deleted — its tests run in the tier (scripts/before_pr.sh, CI)"]
