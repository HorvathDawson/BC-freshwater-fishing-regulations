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
    monkeypatch.setattr(B, "keys", lambda _b: k)
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


def test_a_changed_input_reruns_its_stage_and_every_later_one(world):
    k, _o, seal = world
    seal()
    k["deliver"]["in"] = "edited"
    assert _runs() == ["deliver", "tiles"]


def test_a_missing_output_reruns_the_stage(world):
    _k, outs, seal = world
    seal()
    outs["tiles"][0].unlink()
    assert _runs() == ["tiles"]


def test_force_reruns_from_that_stage(world):
    _k, _o, seal = world
    seal()
    assert _runs(force=["reach"]) == list(B.STAGES)


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
