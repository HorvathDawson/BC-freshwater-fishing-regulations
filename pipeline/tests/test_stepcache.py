"""T522: the recorder (`pipeline/_trace.py`) and the input-keyed step cache (`pipeline.common.stepcache`).

A reused step must be EXACTLY a run step: every input it read (code as imported, files by content, listings, probed
paths, its staging files, its declared inputs) is in the key, and any one of them moving reruns it. The scratch
directory is repo-local (the recorder ignores the temp dir by design)."""
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline.common import stepcache as SC
from pipeline.common.curated import REPO_ROOT


@pytest.fixture
def scratch():
    d = REPO_ROOT / "data" / "generated" / f".stepcache-test-{uuid.uuid4().hex[:8]}"
    d.mkdir(parents=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


STEP = """
import os, sys, pathlib
import pipeline.common.entry_id                      # a repo module: must be in the trace
src, staging, probe, listed = map(pathlib.Path, sys.argv[1:5])
text = src.read_text()
extra = "+" if probe.exists() else "-"
names = ",".join(sorted(os.listdir(listed)))
inp = (staging / "in.txt").read_text() if (staging / "in.txt").exists() else ""
(staging / "out.txt").write_text(text + extra + names + inp)
"""


def _run(trace_dir, *args):
    subprocess.run([sys.executable, "-c", STEP, *map(str, args)], check=True, cwd=REPO_ROOT,
                   env={**os.environ, "PIPELINE_TRACE": str(trace_dir)})


def test_the_recorder_sees_modules_reads_listings_and_probes(scratch):
    src, probe, listed = scratch / "src.txt", scratch / "probe", scratch / "listed"
    src.write_text("a")
    listed.mkdir()
    staging = scratch / "staging"
    staging.mkdir()
    _run(scratch / "tr", src, staging, probe, listed)
    t = SC.Trace(scratch / "tr")
    rel = lambda p: str(p.relative_to(REPO_ROOT))     # noqa: E731
    assert "pipeline/common/entry_id.py" in t.modules and "pipeline/__init__.py" in t.modules
    assert rel(src) in t.reads and rel(listed) in t.lists and rel(probe) in t.probes
    assert not t.conflicts


def test_no_trace_is_an_error_not_an_empty_key(scratch):
    with pytest.raises(FileNotFoundError):
        SC.Trace(scratch / "none")


def _step(cache, scratch, d, declared=None):
    """Lookup, else run + record; returns 'reused' or 'ran'."""
    argv = ["step", "<staging>"]
    rec, _why = cache.lookup("s", argv, d, declared or {})
    if rec is not None:
        cache.restore("s", rec, d)
        return "reused"
    before = SC.snapshot(d)
    tr = scratch / f"tr-{uuid.uuid4().hex[:6]}"
    _run(tr, scratch / "src.txt", d, scratch / "probe", scratch / "listed")
    cache.record("s", argv, d, declared or {}, before, SC.Trace(tr))
    return "ran"


def _fresh(scratch, name):
    d = scratch / name
    d.mkdir()
    return d


def test_a_step_is_reused_only_while_every_input_is_unchanged(scratch):
    (scratch / "src.txt").write_text("a")
    (scratch / "listed").mkdir()
    cache = SC.StepCache(scratch / "cache")
    assert _step(cache, scratch, _fresh(scratch, "d1")) == "ran"
    d2 = _fresh(scratch, "d2")
    assert _step(cache, scratch, d2) == "reused"
    assert (d2 / "out.txt").read_text() == (scratch / "d1" / "out.txt").read_text()
    # a file it read changes by content (same size): rerun
    (scratch / "src.txt").write_text("b")
    assert _step(cache, scratch, _fresh(scratch, "d3")) == "ran"
    assert _step(cache, scratch, _fresh(scratch, "d4")) == "reused"
    # an optional input appears (it was only probed): rerun
    (scratch / "probe").write_text("")
    assert _step(cache, scratch, _fresh(scratch, "d5")) == "ran"
    # a listed directory gains a name: rerun
    (scratch / "listed" / "new").write_text("")
    assert _step(cache, scratch, _fresh(scratch, "d6")) == "ran"
    # a staging input from an earlier step changes: rerun
    d7 = _fresh(scratch, "d7")
    (d7 / "in.txt").write_text("x")
    assert _step(cache, scratch, d7) == "ran"
    d8 = _fresh(scratch, "d8")
    (d8 / "in.txt").write_text("x")
    assert _step(cache, scratch, d8) == "reused"
    # a declared input moves: rerun
    d9 = _fresh(scratch, "d9")
    (d9 / "in.txt").write_text("x")
    assert _step(cache, scratch, d9, declared={"gpkg": [1, 2]}) == "ran"
    # the kept output was changed behind the cache's back: rerun, never serve it
    (scratch / "cache" / "s" / "out" / "out.txt").write_text("tampered")
    d10 = _fresh(scratch, "d10")
    (d10 / "in.txt").write_text("x")
    assert _step(cache, scratch, d10, declared={"gpkg": [1, 2]}) == "ran"


def test_code_is_keyed_as_imported(scratch, monkeypatch):
    """A module recorded with one content and on disk with another (edited while the step ran) is not kept."""
    (scratch / "src.txt").write_text("a")
    (scratch / "listed").mkdir()
    d = _fresh(scratch, "d")
    before = SC.snapshot(d)
    tr = scratch / "tr"
    _run(tr, scratch / "src.txt", d, scratch / "probe", scratch / "listed")
    t = SC.Trace(tr)
    t.modules["pipeline/common/entry_id.py"] = "0" * 64
    msg = SC.StepCache(scratch / "cache").record("s", ["x"], d, {}, before, t)
    assert msg.startswith("not kept")
    assert not (scratch / "cache" / "s" / "record.json").exists()


def test_a_reused_step_still_names_its_code_to_the_stage(scratch):
    mods = {"pipeline/common/entry_id.py": "ab" * 32}
    SC.write_modules_trace(scratch / "stage", mods, "bundle")
    assert SC.Trace(scratch / "stage").modules == mods


def test_a_traced_stage_keys_on_the_files_it_imported(monkeypatch):
    """`pipeline build`: with `code_files` recorded, the reach/deliver code key is those files now — an edit elsewhere
    (the review app) leaves it alone; before the first recorded run it is the broad roots, as before."""
    f = "pipeline/common/entry_id.py"
    m = {"stages": {"reach": {"code_files": {f: "x"}}}, "runs": []}
    assert B.stage_code("reach", m) == {"traced": SC.code_digest(SC.code_now([f]))}
    assert B.stage_code("deliver", {"stages": {}, "runs": []}) == B.code_digest(B.CODE["deliver"])
    assert B.stage_code("tiles", m) == B.code_digest(B.CODE["tiles"])        # tiles keep their narrow roots
