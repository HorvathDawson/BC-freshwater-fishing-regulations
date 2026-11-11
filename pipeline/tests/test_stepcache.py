"""T522: the recorder (`pipeline/_trace.py`) and the input-keyed step cache (`pipeline.common.stepcache`).

A reused step must be EXACTLY a run step. Every test here is a mutation: move one input (a file's bytes, a probed path,
a listing, a staging file, a declared stamp, the installed packages, a module, an input changed WHILE the step ran)
and the step must run again — or not be kept at all. The scratch directory is repo-local (the recorder ignores the
temp dir by design)."""
import json
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from pathlib import Path

import pytest

from pipeline import build as B
from pipeline.common import stepcache as SC
from pipeline.common.curated import REPO_ROOT
from pipeline.tests import under_test

PY = sys.executable


@pytest.fixture
def scratch():
    d = REPO_ROOT / "data" / "generated" / f".stepcache-test-{uuid.uuid4().hex[:8]}"
    d.mkdir(parents=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


def rel(p: Path) -> str:
    return str(p.relative_to(REPO_ROOT))


#: a stand-in step: reads src, probes `probe`, lists `listed`, reads <staging>/in.txt when there; writes out.txt.
#: MODE "mutate" rewrites src after reading it (an input that moved while the step ran); "worker" reads src in a
#: spawned multiprocessing worker only.
STEP = textwrap.dedent("""
    import os, sys, pathlib
    import pipeline.common.entry_id                      # a repo module: must be in the trace
    def read(p):
        return pathlib.Path(p).read_text()
    if __name__ == "__main__":
        src, staging, probe, listed, mode = sys.argv[1:6]
        src, staging = pathlib.Path(src), pathlib.Path(staging)
        if mode == "worker":
            import multiprocessing as mp
            with mp.get_context("spawn").Pool(1) as pool:
                text = pool.apply(read, (str(src),))
        else:
            text = src.read_text()
        if mode == "mutate":
            src.write_text(text + "!")
        extra = "+" if pathlib.Path(probe).exists() else "-"
        names = ",".join(sorted(os.listdir(listed)))
        inp = (staging / "in.txt").read_text() if (staging / "in.txt").exists() else ""
        (staging / "out.txt").write_text(text + extra + names + inp)
""")


@pytest.fixture
def world(scratch):
    (scratch / "src.txt").write_text("a")
    (scratch / "listed").mkdir()
    (scratch / "step.py").write_text(STEP)
    return scratch


def _run(world, trace_dir, d, mode="plain"):
    subprocess.run([PY, str(world / "step.py"), str(world / "src.txt"), str(d), str(world / "probe"),
                    str(world / "listed"), mode], check=True, cwd=REPO_ROOT,
                   env={**os.environ, "PIPELINE_TRACE": str(trace_dir), "PYTHONPATH": str(REPO_ROOT)})


def _step(cache, world, d, declared=None, mode="plain"):
    """reuse, else run + record; returns ('reused' | 'ran', message)."""
    argv = ["step", "<staging>"]
    rec, why = cache.reuse("s", argv, d, declared or {})
    if rec is not None:
        return "reused", why
    before = SC.snapshot(d)
    tr = world / f"tr-{uuid.uuid4().hex[:6]}"
    _run(world, tr, d, mode)
    return "ran", cache.record("s", argv, d, declared or {}, before, SC.Trace(tr))


def _fresh(world, name, staged=None):
    d = world / name
    d.mkdir()
    if staged is not None:
        (d / "in.txt").write_text(staged)
    return d


# ---------------------------------------------------------------------------------------------------- the recorder

def test_the_recorder_sees_modules_reads_listings_and_probes_with_their_stamps(world):
    d = _fresh(world, "d")
    _run(world, world / "tr", d)
    t = SC.Trace(world / "tr")
    assert "pipeline/common/entry_id.py" in t.modules and "pipeline/__init__.py" in t.modules
    src = world / "src.txt"
    st = src.stat()
    assert t.reads[rel(src)] == f"{st.st_size}:{st.st_mtime_ns}"
    assert rel(world / "listed") in t.lists and t.probes[rel(world / "probe")] == "missing"
    assert t.interpreters and not (t.conflicts or t.moved or t.unkeyable)


PROBES = textwrap.dedent("""
    import os, sys, shutil, pathlib
    import pipeline
    w = pathlib.Path(sys.argv[1])
    os.access(w / "acc", os.R_OK)
    os.path.lexists(w / "lex")
    if sys.argv[2] == "dirfd":
        fd = os.open(w, os.O_RDONLY)
        try:
            os.stat("rel", dir_fd=fd)
        except FileNotFoundError:
            pass
    if sys.argv[2] == "scandirfd":
        fd = os.open(w, os.O_RDONLY)
        list(os.scandir(fd))
    if sys.argv[2] == "rmtree":
        (w / "gone").mkdir(); (w / "gone" / "f").write_text("x")
        shutil.rmtree(w / "gone")
""")


@pytest.mark.parametrize("case,unkeyable", [("plain", False), ("dirfd", True), ("scandirfd", True),
                                            ("rmtree", False)])
def test_access_lexists_are_probes_and_fd_relative_events_are_refused_loudly(world, case, unkeyable):
    (world / "probes.py").write_text(PROBES)
    subprocess.run([PY, str(world / "probes.py"), str(world), case], check=True, cwd=REPO_ROOT,
                   env={**os.environ, "PIPELINE_TRACE": str(world / "tr"), "PYTHONPATH": str(REPO_ROOT)})
    t = SC.Trace(world / "tr")
    assert t.probes[rel(world / "acc")] == "missing"          # os.access
    assert t.probes[rel(world / "lex")] == "missing"          # os.path.lexists (os.lstat)
    assert bool(t.unkeyable) is unkeyable, t.unkeyable
    if unkeyable:
        d = _fresh(world, "d")
        msg = SC.StepCache(world / "cache").record("s", ["x"], d, {}, {}, t)
        assert msg.startswith("not kept: the recorder could not key")


def test_a_dangling_symlink_is_a_kind_of_its_own():
    """`lexists` sees a symlink to nothing; its appearing must move the key though `exists` stays False."""
    from pipeline import _trace
    p = REPO_ROOT / "data" / "generated" / f".dangling-{uuid.uuid4().hex[:6]}"
    try:
        assert _trace.kind(str(p)) == "missing"
        p.symlink_to(p.with_name("nowhere-" + p.name))
        assert _trace.kind(str(p)) == "dangling"
    finally:
        p.unlink(missing_ok=True)


def test_no_trace_is_an_error_not_an_empty_key(scratch):
    with pytest.raises(FileNotFoundError):
        SC.Trace(scratch / "none")


# ---------------------------------------------------------------------------------------------------- the cache

def test_a_step_is_reused_only_while_every_input_is_unchanged(world):
    cache = SC.StepCache(world / "cache")
    assert _step(cache, world, _fresh(world, "d1"))[0] == "ran"
    d2 = _fresh(world, "d2")
    assert _step(cache, world, d2)[0] == "reused"
    assert (d2 / "out.txt").read_text() == (world / "d1" / "out.txt").read_text()
    (world / "src.txt").write_text("b")                                   # a read file, new bytes, same size
    assert _step(cache, world, _fresh(world, "d3"))[0] == "ran"
    assert _step(cache, world, _fresh(world, "d4"))[0] == "reused"
    (world / "probe").write_text("")                                      # an optional input appears
    assert _step(cache, world, _fresh(world, "d5"))[0] == "ran"
    (world / "listed" / "new").write_text("")                             # a listed directory gains a name
    assert _step(cache, world, _fresh(world, "d6"))[0] == "ran"
    assert _step(cache, world, _fresh(world, "d7", staged="x"))[0] == "ran"       # a staging input
    assert _step(cache, world, _fresh(world, "d8", staged="x"))[0] == "reused"
    assert _step(cache, world, _fresh(world, "d9", staged="x"), {"gpkg": [1, 2]})[0] == "ran"   # declared
    (world / "cache" / "s" / "out" / "out.txt").write_text("tampered")   # the kept output, behind its back
    state, why = _step(cache, world, _fresh(world, "d10", staged="x"), {"gpkg": [1, 2]})
    assert state == "ran"


def test_an_input_that_moves_while_the_step_runs_is_never_kept(world):
    """The output came from the bytes read; the bytes on disk at the end are others. No record; the next run runs."""
    cache = SC.StepCache(world / "cache")
    assert _step(cache, world, _fresh(world, "d1"))[0] == "ran"          # an honest record first
    state, msg = _step(cache, world, _fresh(world, "d2", staged="y"), mode="mutate")
    assert state == "ran" and msg.startswith("not kept") and "src.txt" in msg and "changed while the step ran" in msg
    assert not (world / "cache" / "s" / "record.json").exists()          # the older record is dropped too
    rec, why = cache.reuse("s", ["step", "<staging>"], _fresh(world, "d3", staged="y"), {})
    assert rec is None and why == "no record"


def test_a_package_or_interpreter_change_reruns(world, monkeypatch):
    cache = SC.StepCache(world / "cache")
    assert _step(cache, world, _fresh(world, "d1"))[0] == "ran"
    assert _step(cache, world, _fresh(world, "d2"))[0] == "reused"
    real = SC.env_fingerprint()
    monkeypatch.setattr(SC, "env_fingerprint", lambda: dict(real, distributions="bumped"))
    rec, why = cache.reuse("s", ["step", "<staging>"], _fresh(world, "d3"), {})
    assert rec is None and why == "the interpreter or an installed package changed"


def test_another_interpreter_is_not_kept(world):
    d = _fresh(world, "d")
    _run(world, world / "tr", d)
    t = SC.Trace(world / "tr")
    t.interpreters = {("3.0.0 (fake)", "/nowhere/python")}
    assert SC.StepCache(world / "cache").record("s", ["x"], d, {}, {}, t).startswith("not kept: it ran under another")


def test_a_worker_process_s_reads_are_in_the_key(world):
    cache = SC.StepCache(world / "cache")
    assert _step(cache, world, _fresh(world, "d1"), mode="worker")[0] == "ran"
    rec = json.loads((world / "cache" / "s" / "record.json").read_text())
    assert rel(world / "src.txt") in rec["paths"]["reads"]          # read only in the spawned worker
    assert _step(cache, world, _fresh(world, "d2"), mode="worker")[0] == "reused"
    (world / "src.txt").write_text("z")
    assert _step(cache, world, _fresh(world, "d3"), mode="worker")[0] == "ran"


def test_code_is_keyed_as_imported(world):
    """A module recorded with one content and on disk with another (edited while the step ran) is not kept."""
    d = _fresh(world, "d")
    _run(world, world / "tr", d)
    t = SC.Trace(world / "tr")
    t.modules["pipeline/common/entry_id.py"] = "0" * 64
    msg = SC.StepCache(world / "cache").record("s", ["x"], d, {}, {}, t)
    assert msg.startswith("not kept")
    assert not (world / "cache" / "s" / "record.json").exists()


def test_reuse_waits_for_the_lock_and_reverifies_what_it_copied(world, monkeypatch):
    cache = SC.StepCache(world / "cache")
    assert _step(cache, world, _fresh(world, "d1"))[0] == "ran"
    import fcntl
    held = open(world / "cache" / "s.lock", "a")
    fcntl.flock(held, fcntl.LOCK_EX)
    out = {}
    t = threading.Thread(target=lambda: out.update(r=cache.reuse("s", ["step", "<staging>"], world / "d2", {})))
    (world / "d2").mkdir()
    t.start()
    time.sleep(0.4)
    assert t.is_alive() and not (world / "d2" / "out.txt").exists()      # blocked on the record's lock
    fcntl.flock(held, fcntl.LOCK_UN)
    held.close()
    t.join(10)
    assert out["r"][0] is not None
    # a copy that does not arrive intact is not served, and leaves nothing behind
    real = SC._place
    monkeypatch.setattr(SC, "_place", lambda s, dst: (real(s, dst), dst.write_text("torn")))
    d3 = _fresh(world, "d3")
    rec, why = cache.reuse("s", ["step", "<staging>"], d3, {})
    assert rec is None and "did not copy back intact" in why and not (d3 / "out.txt").exists()


def test_a_reused_step_still_names_its_code_to_the_stage(scratch):
    mods = {"pipeline/common/entry_id.py": "ab" * 32}
    SC.write_modules_trace(scratch / "stage", mods, "bundle")
    assert SC.Trace(scratch / "stage").modules == mods


# ---------------------------------------------------------------------------------------------------- the wiring

FAKE = textwrap.dedent("""
    import sys, pathlib
    import pipeline.common.entry_id
    name, src, out = sys.argv[1:4]
    pathlib.Path(out).write_text(name + pathlib.Path(src).read_text())
""")


def test_the_deliver_step_wiring_runs_then_reuses_and_feeds_the_stage_trace(world, monkeypatch, capsys):
    from pipeline.deliver import __main__ as D
    (world / "fake.py").write_text(FAKE)
    monkeypatch.setattr(D, "STEP_CMD", [PY, str(world / "fake.py")])
    monkeypatch.setattr(D, "CACHED_STEPS", ("fake",))
    monkeypatch.setenv("PYTHONPATH", str(REPO_ROOT))
    stage = world / "stage"
    monkeypatch.setenv("PIPELINE_TRACE", str(stage))     # the enclosing build stage's recorder

    def once(name):
        d = _fresh(world, name)
        steps = D._Steps(d, SC.StepCache(world / "cache"), {"src": [1]}, {})
        D._step(["fake", str(world / "src.txt"), str(d / "o.txt")], steps)
        return d, capsys.readouterr().out

    d1, out1 = once("d1")
    assert "fake runs: no record" in out1 and "fake kept" in out1 and (d1 / "o.txt").read_text() == "fakea"
    d2, out2 = once("d2")
    assert "fake REUSED" in out2 and (d2 / "o.txt").read_text() == "fakea"
    assert "pipeline/common/entry_id.py" in SC.Trace(stage).modules          # run AND reused steps name their code
    assert any(p.name == "reused-fake.trace" for p in stage.iterdir())
    (world / "src.txt").write_text("b")
    d3, out3 = once("d3")
    assert "fake runs: reads changed" in out3 and (d3 / "o.txt").read_text() == "fakeb"


def test_deliver_all_passes_the_cache_unless_told_not_to(monkeypatch, tmp_path):
    from pipeline.deliver import __main__ as D
    seen = []

    class Stop(Exception):
        pass

    def fake_step(args, steps=None):
        seen.append(steps)
        raise Stop
    monkeypatch.setattr(D, "_step", fake_step)
    monkeypatch.setattr(D, "step_declared", lambda b, r=None: ({"a": 1}, {"a": 1, "b": 2}))
    import argparse
    for no_cache in (False, True):
        a = argparse.Namespace(out_dir=tmp_path / f"o{no_cache}", build=tmp_path, reaches=None, entries=None,
                               tiles=None, debug_json=False, hold_latest=False, no_step_cache=no_cache)
        monkeypatch.setattr(D, "out_dir_release_problems", lambda *x, **k: [])
        with pytest.raises(Stop):
            D._all(a)
    assert isinstance(seen[0], D._Steps) and seen[0].bundle_declared == {"a": 1, "b": 2}
    assert seen[1] is None


def test_step_declared_keys_c_level_inputs_and_the_reach_pick(scratch, monkeypatch):
    from pipeline.common import curated as C
    from pipeline.deliver import __main__ as D
    src, atlas, reaches = scratch / "source", scratch / "atlas", scratch / "reaches"
    for p in (src, atlas, reaches / "full", reaches / "full_prev"):
        p.mkdir(parents=True)
    (src / "boundary.geojson").write_text("{}")
    (atlas / "area_catalog.gpkg").write_text("x")
    for r in ("full", "full_prev"):
        (reaches / r / "report.json").write_text("{}")
    monkeypatch.setattr(C, "SOURCE", src)
    monkeypatch.setattr(C, "GENERATED", type("G", (), {"reaches": reaches})())
    every, bundle = D.step_declared(atlas)
    assert rel(src / "boundary.geojson") in every and rel(atlas / "area_catalog.gpkg") in every
    marks = rel(Path(B.CURATED.review.marks))
    assert marks not in bundle and any(k.startswith(rel(under_test.curated().base) + "/") for k in bundle)
    pick = sorted(k for k in bundle if k.startswith("reach-pick:"))
    assert pick == [f"reach-pick:{rel(reaches / r / 'report.json')}" for r in ("full", "full_prev")]
    # the bundle picks the NEWEST run by mtime: a touch changes the pick, so it must change the key
    st = (reaches / "full_prev" / "report.json").stat()
    os.utime(reaches / "full_prev" / "report.json", ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))
    assert D.step_declared(atlas)[1] != bundle
    # a named run (`--reaches`) is no pick
    assert not any(k.startswith("reach-pick:") for k in D.step_declared(atlas, reaches / "full")[1])
    # the source tree changes by a stamp only (GDAL reads it in C): key moves
    (src / "boundary.geojson").write_text("{ }")
    assert D.step_declared(atlas)[0] != every


# ---------------------------------------------------------------------------------------------------- the stages

def test_a_traced_stage_keys_on_the_files_it_imported(scratch):
    """`pipeline build`: with `code_files` recorded, the reach/deliver code key is those files now (and the env) —
    an edit to a module it imported moves it, an edit elsewhere does not; before the first recorded run it is the
    broad roots, as before; tiles keep their narrow roots."""
    mod = scratch / "mod.py"
    mod.write_text("x = 1\n")
    other = scratch / "other.py"
    other.write_text("y = 1\n")
    m = {"stages": {"reach": {"code_files": {rel(mod): "x"}}}, "runs": []}
    k1 = B.stage_code("reach", m)
    assert k1["env"] == SC.env_fingerprint()
    other.write_text("y = 2\n")
    assert B.stage_code("reach", m) == k1
    mod.write_text("x = 2\n")
    assert B.stage_code("reach", m) != k1
    assert B.stage_code("deliver", {"stages": {}, "runs": []}) == B.code_digest(B.CODE["deliver"])
    assert B.stage_code("tiles", m) == B.code_digest(B.CODE["tiles"])


# ---------------------------------------------------------------------------------------------------- the read set

def _trace_import(world, tag, script):
    tr = world / f"tr-{tag}"
    # T661: the chunked runner exports PYTHONPYCACHEPREFIX, which sends every .pyc away from the module; the cold/warm
    # pair below needs the .pyc beside it, so the child runs without the prefix.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPYCACHEPREFIX"}
    subprocess.run([PY, "-c", script], check=True, cwd=REPO_ROOT,
                   env={**env, "PIPELINE_TRACE": str(tr), "PYTHONPATH": f"{REPO_ROOT}{os.pathsep}{world}"})
    return SC.Trace(tr)


def test_the_read_set_does_not_depend_on_whether_a_pyc_exists(world):
    """T522 review: the bundle recorded 198 / 142 / 82 reads on one input set — the import system reading module
    SOURCES on a .pyc miss (a fresh worktree) was recorded as reads. A module is keyed by its M line; the read set must
    be the same cold and warm. Also: a repo module outside `pipeline` is keyed (M), and a file a module reads at
    IMPORT time (under the import system's frames) is a read."""
    pkg = world / "t522mod"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    data = world / "data.txt"
    data.write_text("d")
    (pkg / "m.py").write_text(f"import pathlib\nDATA = pathlib.Path({str(data)!r}).read_text()\n")
    script = "import pipeline, t522mod.m"
    for c in pkg.rglob("__pycache__"):
        shutil.rmtree(c)
    cold = _trace_import(world, "cold", script)
    assert list(pkg.rglob("*.pyc")), "the cold run should have written the .pyc"
    warm = _trace_import(world, "warm", script)
    assert cold.reads == warm.reads and cold.lists == warm.lists
    assert not any(r.endswith(".py") for r in cold.reads), sorted(cold.reads)
    assert rel(pkg / "m.py") in cold.modules and rel(pkg / "m.py") in warm.modules
    assert rel(data) in cold.reads and rel(data) in warm.reads
