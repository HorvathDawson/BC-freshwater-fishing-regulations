"""T777 / T993 item 6: TESTS NEVER READ MACHINE STATE (`pipeline/tests/machine_state.py`), pinned by mutation.

A planted test file runs in a child pytest with the plugin. Each planted case is one way a no-data test could reach
this machine's trees (the real curated tree read directly, read and the error swallowed, written; an untracked
generated path), each must FAIL naming the marker to add, and its twins must pass: the same read with the marker, the
curated data read through `CURATED` (the session sandbox), a tracked generated file.
"""
from __future__ import annotations

import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from pipeline.common import curated as C
from pipeline.tests import machine_state as M

REPO = Path(__file__).resolve().parents[2]

INI = """[pytest]
markers =
    slow: slow
    needs_bundle: bundle
    needs_atlas: atlas
addopts = --strict-markers -p no:cacheprovider
"""

PLANTED = '''
import os
from pathlib import Path

import pytest

REPO = Path(os.environ["PLANT_REPO"])
CUTS = REPO.joinpath("data", "curated", "waters", "cuts.json")


def test_reads_the_real_curated_tree():
    CUTS.read_text(encoding="utf-8")


def test_swallows_the_refusal():
    try:
        CUTS.read_text(encoding="utf-8")
    except BaseException:
        pass


def test_writes_the_real_curated_tree():
    REPO.joinpath("data", "curated", os.environ["PLANT_NAME"]).write_text("x")


def test_reads_an_untracked_generated_path():
    REPO.joinpath("data", "generated", "atlas", "full", "registry.json").open()


def test_reads_the_release_pointer():
    REPO.joinpath("data", "release", "LATEST").read_text()


def test_a_listing_reads_no_content():
    os.listdir(REPO.joinpath("data", "curated"))


@pytest.mark.needs_bundle
def test_a_data_test_may_not_write_the_real_curated_tree_either():
    REPO.joinpath("data", "curated", os.environ["PLANT_NAME"]).write_text("x")


@pytest.mark.needs_bundle
def test_a_data_test_copies_the_real_tree_out(tmp_path):
    import shutil
    shutil.copytree(REPO.joinpath("data", "curated", "waters"), tmp_path / "waters")


@pytest.mark.needs_bundle
def test_the_marked_twin_reads_the_real_tree():
    CUTS.read_text(encoding="utf-8")
    from pipeline.common.curated import CURATED
    assert Path(CURATED.base).resolve() == REPO.joinpath("data", "curated").resolve()


def test_curated_reads_go_to_the_sandbox():
    from pipeline.common.curated import CURATED
    assert not Path(CURATED.base).resolve().is_relative_to(REPO)
    assert Path(CURATED.waters.cuts).read_text(encoding="utf-8")


def test_the_sandbox_review_is_empty():
    from pipeline.common.curated import CURATED
    assert Path(CURATED.review.marks).read_text(encoding="utf-8") == ""
    assert not Path(CURATED.review.closed).exists()


def test_reads_a_tracked_generated_file():
    REPO.joinpath(os.environ["PLANT_TRACKED"]).read_bytes()


@pytest.fixture(scope="module")
def own_root(tmp_path_factory):
    """(T777 review) a module-scoped fixture's own curated root holds for EVERY test of the module, not only the first"""
    from pipeline.tests.conftest import curated_root
    mp = pytest.MonkeyPatch()
    root = curated_root(mp, tmp_path_factory.mktemp("own") / "curated")
    yield str(root)
    mp.undo()


def test_zz_a_module_root_holds_for_its_first_test(own_root):
    assert os.environ.get("CURATED_ROOT") == own_root


def test_zz_a_module_root_holds_for_a_later_test(own_root):
    assert os.environ.get("CURATED_ROOT") == own_root


@pytest.mark.needs_bundle
def test_zz_a_module_root_holds_for_a_data_test(own_root):
    assert os.environ.get("CURATED_ROOT") == own_root
'''

#: planted test -> what its failure must say (None: it must pass)
WANT = {
    "test_reads_the_real_curated_tree": ("the real curated tree", "data/curated/waters/cuts.json", "needs_bundle"),
    "test_swallows_the_refusal": ("the real curated tree", "needs_bundle"),
    "test_writes_the_real_curated_tree": ("writes the real curated tree", "never writes"),
    "test_a_data_test_may_not_write_the_real_curated_tree_either": ("writes the real curated tree", "never writes"),
    "test_a_data_test_copies_the_real_tree_out": None,
    "test_reads_an_untracked_generated_path": ("untracked generated", "registry.json", "needs_atlas"),
    "test_reads_the_release_pointer": ("untracked generated", "data/release/LATEST", "needs_bundle"),
    "test_a_listing_reads_no_content": None,
    "test_the_marked_twin_reads_the_real_tree": None,
    "test_curated_reads_go_to_the_sandbox": None,
    "test_the_sandbox_review_is_empty": None,
    "test_reads_a_tracked_generated_file": None,
    "test_zz_a_module_root_holds_for_its_first_test": None,
    "test_zz_a_module_root_holds_for_a_later_test": None,
    "test_zz_a_module_root_holds_for_a_data_test": None,
}


@pytest.fixture(scope="module")
def planted(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("planted")
    (tmp / "pytest.ini").write_text(INI)
    (tmp / "test_planted.py").write_text(PLANTED)
    tracked = M._git_files(Path(os.path.abspath(C.load_generated().base)).relative_to(REPO).as_posix())
    assert tracked, "git tracks no generated file: the tracked-file twin has nothing to read"
    name = f"zz-planted-{os.getpid()}.txt"
    env = os.environ | {"PLANT_REPO": str(REPO), "PLANT_NAME": name, "PLANT_TRACKED": tracked[0],
                        "PYTHONPATH": os.pathsep.join([str(REPO), os.environ.get("PYTHONPATH", "")])}
    env.pop(C.ROOT_ENV, None)                    # the child makes its own sandbox from the live tree
    xml = tmp / "out.xml"
    run = subprocess.run([sys.executable, "-m", "pytest", "-p", "pipeline.tests.machine_state", "-c",
                          str(tmp / "pytest.ini"), "--rootdir", str(tmp), f"--junitxml={xml}", "-q",
                          str(tmp / "test_planted.py")], cwd=tmp, env=env, capture_output=True, text=True,
                         timeout=300)
    assert xml.is_file(), run.stdout + run.stderr
    got = {}
    for case in ET.parse(xml).getroot().iter("testcase"):
        bad = [e for e in case if e.tag in ("failure", "error")]
        got.setdefault(case.get("name"), []).extend(f"{e.get('message', '')}\n{e.text or ''}" for e in bad)
    leaked = REPO / "data" / "curated" / name     # a stat, not a read: the guard sees no event (and must not)
    return {"got": got, "leaked": leaked if leaked.exists() else None, "out": run.stdout + run.stderr}


@pytest.mark.parametrize("name", sorted(WANT))
def test_each_planted_case(planted, name):
    got, want = planted["got"], WANT[name]
    assert name in got, planted["out"][-3000:]
    if want is None:
        assert not got[name], f"{name} must pass:\n" + "\n".join(got[name])[-2000:]
    else:
        text = "\n".join(got[name])
        assert got[name], f"{name} must FAIL (the guard let it through)"
        for part in want:
            assert part in text, f"{name}: the failure does not say {part!r}:\n{text[-2000:]}"


def test_the_refused_write_never_reached_the_tree(planted):
    assert planted["leaked"] is None, f"the planted write reached the real tree: delete {planted['leaked']}"


def test_this_run_reads_the_session_sandbox():
    """The suite itself runs on the sandbox: `CURATED` is the copy, its review empty, and the live tree untouched."""
    live = C.load_repo()
    assert Path(C.CURATED.base).resolve() != Path(live.base).resolve()
    assert Path(C.CURATED.review.marks).read_text(encoding="utf-8") == ""
    assert Path(C.CURATED.waters.cuts).is_file()


def test_the_marker_named_follows_the_tree():
    at = lambda rel: M.marker_for(str(REPO.joinpath("data", rel)))           # noqa: E731
    assert at("generated/atlas/full/graph.pkl") == "needs_atlas"
    assert at("generated/reaches/full/x") == "needs_reach_run"
    assert at("generated/tiles/layers") == "needs_tiles"
    assert at("generated/regs/extraction/synopsis_raw_data.json") == "needs_source"
    assert at("generated/inseason/resolved.json") == "needs_bundle"
    assert at("release/LATEST") == "needs_bundle"


@pytest.mark.parametrize("event,args,write", [
    ("open", ("p", "r", 0), False), ("open", ("p", "rb", 0), False), ("open", ("p", "w", 0), True),
    ("open", ("p", "a", 0), True), ("open", ("p", "r+", 0), True), ("open", ("p", None, os.O_RDONLY), False),
    ("open", ("p", None, os.O_WRONLY | os.O_CREAT), True), ("os.listdir", ("p",), False),
    ("os.remove", ("p", None), True), ("os.rename", ("p", "q", None, None), True)])
def test_what_counts_as_a_write(event, args, write):
    assert M._is_write(event, args) is write


def test_a_copy_reads_its_source_and_writes_its_destination():
    a, b = os.path.abspath("a"), os.path.abspath("b")
    assert M._paths("shutil.copytree", ("a", "b")) == [(a, False), (b, True)]
    assert M._paths("os.rename", ("a", "b", None, None)) == [(a, True), (b, True)]
    assert M._paths("sqlite3.connect", ("file:a?mode=ro",)) == [(a, False)]
    assert M._paths("sqlite3.connect", (":memory:",)) == []
