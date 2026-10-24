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
    monkeypatch.setattr(B.subprocess, "run", lambda *a, **kw: type("R", (), {"returncode": 3})())
    m, run = {"stages": {}, "runs": []}, {"started": "t", "stages": [], "status": "running"}
    m["runs"].append(run)
    with pytest.raises(SystemExit, match="reach failed"):
        B.run_stage("reach", ["x"], "k", run, m)
    got = json.loads(B.MANIFEST.read_text())
    assert got["stages"] == {} and got["runs"][0]["status"] == "failed at reach"
    assert got["runs"][0]["stages"][0]["status"] == "rc=3"


def test_the_curated_digest_sees_every_curated_file():
    """No literal path: every path the CURATED model names, the whole tree under `base`."""
    assert len(B.curated_digest()) == 16


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

def test_the_envelope_is_made_first_from_hydat():
    assert B.STAGES[0] == "clim" and B.STAGES.index("clim") < B.STAGES.index("deliver")
    cmd = B.commands(Path("atlas"))["clim"]
    assert cmd[2] == "pipeline.gauges.feed.climatology" and str(B.CLIM_PATH) in cmd and "--hydat" in cmd
    assert B.outputs(Path("atlas"))["clim"] == [Path(B.CLIM_PATH)]
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
