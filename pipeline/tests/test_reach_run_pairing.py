"""Which reach run belongs to which atlas — `pipeline.deliver.bundle.build._reach_run`.

A bundle built from one atlas against another atlas's reaches binds rules to section ids that
no longer exist. `rules.write` catches it, but only after a full rebuild has been spent; this
is the pairing that should make it impossible in the first place.
"""

import json
from pathlib import Path

import pytest

# `pipeline.deliver.bundle.build` is shadowed by a FUNCTION of the same name in the
# package's __init__, so `from ... import build` hands back the function.
import importlib

B = importlib.import_module("pipeline.deliver.bundle.build")


@pytest.fixture
def world(tmp_path, monkeypatch):
    """Two atlases and two reach runs, each run built from the atlas of the same name."""
    reaches = tmp_path / "reaches"
    atlas = tmp_path / "atlas"
    for name, body in (("full", "a:0\na:1\n"), ("scratch", "b:0\nb:1\n")):
        (atlas / name).mkdir(parents=True)
        (atlas / name / "section_handles.txt").write_text(body)
    monkeypatch.setattr(B.GENERATED, "reaches", reaches, raising=False)

    def run(run_name, built_from, *, with_digest=True):
        from pipeline.common.section_handles import digest_for
        d = reaches / run_name
        d.mkdir(parents=True, exist_ok=True)
        rep = {"build": built_from}
        if with_digest:
            rep["handles"] = digest_for(atlas / built_from)
        (d / "report.json").write_text(json.dumps(rep))
        return d

    return atlas, reaches, run


def test_a_promotion_rename_cannot_hand_the_bundle_a_stale_run(world, tmp_path):
    """**The failure this exists to stop, reproduced.**

    Reaches are built against a scratch atlas (`scratch`), then the atlas is promoted by
    renaming it to `full` and the old one to `full.prev`. Matched on the folder NAME, the new
    run records `build: "scratch"` and matches nothing, while the stale run renamed alongside
    still says `build: "full"` and wins — so the bundle is handed the previous run's
    rule->section rows. Matched on the handle digest, the rename is invisible.
    """
    atlas, reaches, run = world
    stale = run("full.prev", "full")                 # the old pairing, renamed
    fresh = run("full_next", "scratch")              # built against the scratch path
    # promote: the scratch atlas becomes `full`
    (atlas / "full").rename(atlas / "full.was")
    (atlas / "scratch").rename(atlas / "full")

    got = B._reach_run(atlas / "full")
    assert got == fresh, f"paired with {got.name if got else None}, not the run for this atlas"
    assert got != stale


def test_the_newest_still_wins_among_runs_of_the_same_atlas(world):
    """Re-running the builder is how its output is corrected, so the later run is the live one
    — but only among runs that are actually for THIS atlas."""
    import os, time
    atlas, reaches, run = world
    old = run("a", "full")
    new = run("b", "full")
    os.utime(old / "report.json", (time.time() - 600, time.time() - 600))
    assert B._reach_run(atlas / "full") == new


def test_a_run_written_before_the_digest_existed_still_pairs(world):
    """The field is new. A run without it must keep pairing by name rather than disappearing,
    or every bundle stops building the day this lands."""
    atlas, reaches, run = world
    legacy = run("legacy", "full", with_digest=False)
    assert B._reach_run(atlas / "full") == legacy


def test_a_digested_run_beats_a_legacy_one_for_the_same_name(world):
    """Content is the better evidence, so it wins even when a name-matched run is newer."""
    import os, time
    atlas, reaches, run = world
    legacy = run("legacy", "full", with_digest=False)
    digested = run("digested", "full")
    os.utime(digested / "report.json", (time.time() - 600, time.time() - 600))
    assert B._reach_run(atlas / "full") == digested


def test_no_run_for_this_atlas_returns_nothing_rather_than_a_guess(world):
    atlas, reaches, run = world
    run("elsewhere", "scratch")
    (atlas / "empty").mkdir()
    (atlas / "empty" / "section_handles.txt").write_text("z:0\n")
    assert B._reach_run(atlas / "empty") is None
