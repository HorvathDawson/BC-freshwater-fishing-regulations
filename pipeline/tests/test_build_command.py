"""P2: `python -m pipeline build` — a stage runs when its key (what it reads) changed, its outputs
are missing, or it is forced, and a stage that runs makes every later stage run."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import build as B


@pytest.fixture
def world(tmp_path: Path, monkeypatch):
    outs = {s: [tmp_path / f"{s}.out"] for s in B.STAGES}
    for ps in outs.values():
        ps[0].write_text("x")
    k = {s: {"in": s} for s in B.STAGES}
    monkeypatch.setattr(B, "MANIFEST", tmp_path / "build-manifest.json")
    monkeypatch.setattr(B, "keys", lambda _b, missing_ok=False: k)
    monkeypatch.setattr(B, "outputs", lambda _b: outs)

    def seal():
        B.save_manifest({"stages": {s: {"key": B._sha(k[s])} for s in B.STAGES}, "runs": []})
    return k, outs, seal


def _runs(build_dir=Path("atlas"), force=()):
    return [s for s, _key, go in B.plan(build_dir, set(force)) if go]


def test_no_manifest_runs_everything(world):
    assert _runs() == list(B.STAGES)


def test_an_unchanged_world_is_up_to_date(world):
    _k, _o, seal = world
    seal()
    assert _runs() == []


def test_a_changed_input_reruns_its_stage_and_what_depends_on_it(world):
    k, _o, seal = world
    seal()
    k["tiles"]["in"] = "edited"
    assert _runs() == ["tiles", "deliver"]           # deliver reads the tiles' polygons for section_access (T54)
    k["tiles"]["in"] = "tiles"
    k["reach"]["in"] = "edited"
    assert _runs() == ["reach", "deliver"]           # tiles do not read the reach run
    k["reach"]["in"] = "reach"
    k["deliver"]["in"] = "edited"
    assert _runs() == ["deliver"]


def test_a_missing_output_reruns_the_stage(world):
    _k, outs, seal = world
    seal()
    outs["deliver"][0].unlink()
    assert _runs() == ["deliver"]


def test_force_reruns_from_that_stage(world):
    _k, _o, seal = world
    seal()
    assert _runs(force=["reach"]) == ["reach", "deliver"]


def test_a_failed_stage_is_recorded_and_not_marked_done(world, monkeypatch, tmp_path):
    monkeypatch.setattr(B, "_run", lambda *a, **kw: (3, 0))
    m, run = {"stages": {}, "runs": []}, {"started": "t", "stages": [], "status": "running"}
    m["runs"].append(run)
    with pytest.raises(SystemExit, match="reach failed"):
        B.run_stage("reach", ["x"], "k", run, m)
    got = json.loads(B.MANIFEST.read_text())
    assert got["stages"] == {} and got["runs"][0]["status"] == "failed at reach"
    assert got["runs"][0]["stages"][0]["status"] == "rc=3"


def test_the_curated_digest_sees_every_curated_file():
    """No literal path: every path the CURATED model names (T331: not the whole tree under `base`)."""
    assert len(B.curated_digest()) == 16


def test_the_deliver_key_reads_only_what_deliver_can_read(monkeypatch):
    """T331: a review-only or undeclared curated file never reruns deliver; every declared deliver input is hashed."""
    from pipeline.tests import under_test
    opened = []
    real = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: (opened.append(Path(self)), real(self))[1])
    B.curated_digest()
    got = set(opened)
    for p in (under_test.curated().waters.slough_page_calls, under_test.curated().review.proposal_overrides,
              under_test.curated().waters.found_name_calls, under_test.curated().waters.unmapped_waters,
              under_test.curated().base / "README.md"):
        assert Path(p) not in got, p
    for p in (under_test.curated().waters.cuts, under_test.curated().waters.found_names, under_test.curated().gauges.matches, under_test.curated().dfo_salmon.wording,
              under_test.curated().runtiming.run_calls, under_test.curated().inseason.notice_waters):
        assert Path(p) in got, p
    assert any(under_test.curated().regulations.synopsis in p.parents for p in got)
    assert all(under_test.curated().base in p.parents for p in got)


# ------------------------------------------------------------------ N7: the deliver key covers every input

@pytest.fixture
def inputs(tmp_path: Path, monkeypatch):
    """The files the deliver step reads from outside the atlas and the corpus, as tmp files."""
    src = {k: tmp_path / f"{k}.bin" for k in ("gpkg", "stations", "places", "hydat")}
    for p in src.values():
        p.write_text("v1")
    clim = tmp_path / "clim.json"
    clim.write_text('{"release": "2026-01"}')
    monkeypatch.setattr(B, "sources", lambda: src)
    monkeypatch.setattr(B, "CLIM_PATH", clim)
    return src, clim


def test_a_refreshed_gpkg_changes_the_deliver_key(inputs):
    """The bundle reads the RAW gpkg (the catchment-area fit, the drainage groups); a fetch that
    refreshed it left the bundle 'up to date' (F06)."""
    src, _ = inputs
    before = B._sha(B.deliver_sources())
    src["gpkg"].write_text("v2, refreshed")
    assert B._sha(B.deliver_sources()) != before


def test_a_new_envelope_changes_the_deliver_key_even_at_the_same_size_and_time(inputs):
    """clim.json is the `clim` stage's output, and the deliver key holds its CONTENT, so a rewrite that kept
    size and mtime still reruns the bundle."""
    import os
    _, clim = inputs
    before = B._sha(B.deliver_sources())
    st = clim.stat()
    clim.write_text('{"release": "2026-02"}')
    os.utime(clim, ns=(st.st_atime_ns, st.st_mtime_ns))
    assert clim.stat().st_size == st.st_size
    assert B._sha(B.deliver_sources()) != before


def test_a_missing_envelope_is_refused_with_the_command_that_makes_it(inputs):
    _, clim = inputs
    clim.unlink()
    with pytest.raises(SystemExit, match="pipeline.gauges.feed.climatology"):
        B.deliver_sources()


def test_a_missing_envelope_is_only_a_placeholder_in_the_plan(inputs):
    """The plan runs before `clim` has made it; deliver is keyed again once it has (`execute`)."""
    _, clim = inputs
    clim.unlink()
    assert B.deliver_sources(missing_ok=True)["clim"] is None


# ------------------------------------------------------------------ N9: the envelope is a stage

def test_the_envelope_is_made_first_from_hydat(tmp_path, monkeypatch):
    assert B.STAGES[0] == "clim" and B.STAGES.index("clim") < B.STAGES.index("deliver")
    cmd = B.commands(Path("atlas"))["clim"]
    assert cmd[2] == "pipeline.gauges.feed.climatology" and str(B.CLIM_PATH) in cmd and "--hydat" in cmd
    assert B.outputs(Path("atlas"))["clim"] == [Path(B.CLIM_PATH)]
    # the key stamps the fetched HYDAT archive and station roster (not in git): tmp stand-ins here
    for name in ("hydat.sqlite3", "bc_hydrometric_stations.json"):
        (tmp_path / name).write_text("v1")
    monkeypatch.setattr(B, "SOURCE", tmp_path)
    assert set(B.clim_key()) == {"hydat", "release", "stations", "year", "code"}


def test_a_new_hydat_reruns_the_envelope_only(world):
    k, _o, seal = world
    seal()
    k["clim"]["in"] = "HYDAT 2027"
    assert _runs() == ["clim"]           # deliver is re-keyed after it runs, never by dependency


def _execute(world, monkeypatch, envelope_moves: bool):
    k, _o, seal = world
    seal()
    k["clim"]["in"] = "HYDAT 2027"
    ran: list[str] = []
    monkeypatch.setattr(B, "run_stage", lambda s, *_a: ran.append(s))

    def keys_after(_b, missing_ok=False):
        if envelope_moves and "clim" in ran:
            return dict(k, deliver={"in": "deliver", "clim": "new bytes"})
        return k
    monkeypatch.setattr(B, "keys", keys_after)
    rows = B.plan(Path("atlas"), set())
    B.execute(Path("atlas"), rows, {s: [s] for s in B.STAGES}, {}, B.load_manifest())
    return ran


def test_an_envelope_rewritten_with_the_same_bytes_does_not_rebuild_the_bundle(world, monkeypatch):
    assert _execute(world, monkeypatch, envelope_moves=False) == ["clim"]


def test_an_envelope_that_moved_rebuilds_the_bundle(world, monkeypatch):
    assert _execute(world, monkeypatch, envelope_moves=True) == ["clim", "deliver"]


def test_a_bare_python_m_pipeline_refuses():
    import subprocess
    import sys
    r = subprocess.run([sys.executable, "-m", "pipeline"], capture_output=True, text=True,
                       cwd=Path(B.__file__).resolve().parents[1])
    assert r.returncode != 0 and "python -m pipeline build" in r.stderr


# ------------------------------------------------------------------ T295 / T297: parallel stages, reasons, side runs

def test_independent_stages_run_side_by_side_and_deliver_waits_for_them(world, monkeypatch):
    """--jobs 2: clim, reach and tiles read only the atlas, so they overlap; deliver starts only after reach AND
    tiles (DEPENDS) and clim (READS_CONTENT) are done."""
    import threading
    import time as _t
    events, lock = [], threading.Lock()

    def fake(s, *_a, **_kw):
        with lock:
            events.append(("start", s))
        _t.sleep(0.05)
        with lock:
            events.append(("end", s))
    monkeypatch.setattr(B, "run_stage", fake)
    rows = B.plan(Path("atlas"), set())
    ran = B.execute(Path("atlas"), rows, {s: [s] for s in B.STAGES}, {}, B.load_manifest(), jobs=2)
    assert ran == set(B.STAGES)
    starts = [s for e, s in events if e == "start"]
    assert starts[:2] == ["clim", "reach"]                       # two at once, in run order
    i_deliver = events.index(("start", "deliver"))
    assert all(events.index(("end", s)) < i_deliver for s in ("clim", "reach", "tiles"))
    first_end = min(i for i, e in enumerate(events) if e[0] == "end")
    assert events.index(("start", "reach")) < first_end          # reach began before clim finished


def test_a_failed_stage_stops_new_launches(world, monkeypatch):
    started = []

    def fake(s, *_a, **_kw):
        started.append(s)
        if s == "clim":
            raise SystemExit("build: clim failed")
    monkeypatch.setattr(B, "run_stage", fake)
    rows = B.plan(Path("atlas"), set())
    with pytest.raises(SystemExit, match="clim failed"):
        B.execute(Path("atlas"), rows, {s: [s] for s in B.STAGES}, {}, B.load_manifest(), jobs=1)
    assert started == ["clim"]


def test_the_plan_says_which_input_moved(world):
    k, _o, _seal = world
    B.save_manifest({"stages": {s: {"key": B._sha(k[s]), "inputs": B.input_digests(k[s])} for s in B.STAGES},
                     "runs": []})
    k["reach"]["in"] = "edited"
    why: dict = {}
    B.plan(Path("atlas"), set(), why)
    assert why["reach"] == "inputs changed: in" and why["deliver"] == "after reach"


def test_a_side_run_writes_nothing_live(monkeypatch, tmp_path):
    """--side DIR: every stage's command and output names DIR; the live homes and LATEST are not in them."""
    from pipeline.tests import under_test
    monkeypatch.setattr(B, "SIDE", tmp_path / "side")
    build = under_test.generated().build()
    cmds = B.commands(build)
    outs = B.outputs(build)
    for s in B.STAGES:
        assert all(str(tmp_path / "side") in str(p) for p in outs[s]), s
    assert cmds["deliver"][cmds["deliver"].index("--out-dir") + 1] == str(tmp_path / "side" / "deliver")
    assert cmds["tiles"][cmds["tiles"].index("--release-root") + 1] == str(tmp_path / "side" / "deliver" / "release")
    assert cmds["reach"][cmds["reach"].index("--out") + 1] == str(tmp_path / "side" / "reaches" / "full")
    assert cmds["clim"][cmds["clim"].index("--out") + 1] == str(tmp_path / "side" / "clim.json")


def test_the_marks_are_in_no_stage_key(monkeypatch):
    """T266: the review log (and its closed list) feed the apply paths, never a stage — not even through `base`, the
    whole curated tree: the digest never opens them."""
    from pipeline.tests import under_test
    nb = B.not_build_inputs()
    assert Path(under_test.curated().review.marks) in nb and Path(under_test.curated().review.closed) in nb
    assert not hasattr(under_test.curated(), "decided")                 # T408: no decided-data dir
    opened = []
    real = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda self: (opened.append(self), real(self))[1])
    B.curated_digest()
    assert opened and not [p for p in opened if any(p == x or x in p.parents for x in nb)]


def test_the_curated_tree_loads_without_the_marks_store(tmp_path):
    """T266: a build runs with marks.jsonl absent — the curated model no longer requires it to exist."""
    import yaml
    from pipeline.common import curated as C
    blob = yaml.safe_load(C.CONFIG.read_text(encoding="utf-8"))
    gone = tmp_path / "no-marks.jsonl"
    blob["curated"]["review"]["marks"] = str(gone)
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.safe_dump(blob))
    got = C.load(cfg)
    assert got.review.marks == gone and not gone.exists()


def test_a_live_build_keeps_latests_reach_run_before_reach_reruns(tmp_path, monkeypatch, capsys):
    """caught (2026-11-09): lanes ran `pipeline build` (reach) outside a release, the reach run was overwritten, and
    `release --staging` had no "before" for its binding diff. The live build now keeps the copy release makes."""
    from pipeline import release as R
    root, reach = tmp_path / "release", tmp_path / "reaches" / "full"
    (root / "r-live").mkdir(parents=True)
    (root / "r-live" / "manifest.json").write_text(json.dumps({"bundle": {"reach_digest": "d-live"}}))
    (root / "LATEST").write_text("r-live\n")
    reach.mkdir(parents=True)
    (reach / "report.json").write_text(json.dumps({"digest": "d-live"}))
    (reach / "rule_section.jsonl.gz").write_bytes(b"rs")
    monkeypatch.setattr(R, "release_root", lambda: root)
    monkeypatch.setattr(R, "reach_dir", lambda: reach)
    monkeypatch.setattr(R, "HOME", tmp_path / "home")
    msg = B.keep_live_baseline()
    kept = tmp_path / "home" / "baseline" / "r-live"
    assert "copied" in msg and (kept / "rule_section.jsonl.gz").read_bytes() == b"rs"
    (reach / "report.json").write_text(json.dumps({"digest": "d-new"}))        # the reach rerun
    assert "kept:" in B.keep_live_baseline()
    import shutil
    shutil.rmtree(kept)
    assert "WARNING" in B.keep_live_baseline() and "--baseline" in B.keep_live_baseline()


def test_the_build_keeps_the_baseline_only_when_reach_will_run():
    src = Path(B.__file__).read_text()
    i = src.index("keep_live_baseline()", src.index("def main"))
    assert 'SIDE is None and dict((s, go) for s, _k, go in rows).get("reach")' in src[i - 120:i]
    assert i < src.index("execute(build_dir, rows", i)
