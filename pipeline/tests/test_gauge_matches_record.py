"""THE ATLAS RECORDS WHICH GAUGE MATCHES IT CUT AT, AND THE BUNDLE REFUSES ANOTHER (N8, audit F28).

The atlas cuts rivers at their gauges from `data/curated/gauges/matches.json`; the bundle places
each station on a section from the same file. Read at two different times, the two could come from
two versions of it — a gauge cut where the bundle no longer puts the station — and nothing noticed.
Now the atlas writes `gauge_matches.json` (the file's sha256, and whether it cut at all) and the
bundle compares it with the file it is about to read.

No data: tmp files only.
"""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path

import pytest

from pipeline.gauges import matches as M


@pytest.fixture
def world(tmp_path: Path):
    matches = tmp_path / "matches.json"
    matches.write_text(json.dumps({"stations": [{"station": "08AA001", "status": "matched"}]}))
    atlas = tmp_path / "atlas"
    atlas.mkdir()
    return matches, atlas


def test_the_same_file_passes(world):
    matches, atlas = world
    M.write_record(atlas, cut=True, sha=M.matches_digest(matches))
    M.check_record(atlas, matches)                         # no exception


def test_a_changed_file_is_refused_with_both_digests_and_the_fix(world):
    matches, atlas = world
    M.write_record(atlas, cut=True, sha=M.matches_digest(matches))
    matches.write_text(json.dumps({"stations": [{"station": "08AA001", "status": "no_match"}]}))
    with pytest.raises(SystemExit) as e:
        M.check_record(atlas, matches)
    msg = str(e.value)
    assert "pipeline.atlas.build" in msg and M.matches_digest(matches) in msg


def test_an_atlas_without_the_record_is_refused_with_the_stamp_command(world):
    matches, atlas = world
    with pytest.raises(SystemExit, match="--stamp-atlas"):
        M.check_record(atlas, matches)


def test_an_atlas_that_cut_at_no_gauge_accepts_any_matches(world):
    """`--no-gauge-splits`: the graph holds no gauge cut, so no version of the file can disagree with it."""
    matches, atlas = world
    M.write_record(atlas, cut=False, sha=None)
    matches.write_text("{}")
    M.check_record(atlas, matches)


def test_stamping_an_older_atlas_needs_the_file_to_predate_it(world):
    """An atlas built before N8 carries no record. It may be stamped with today's file ONLY when that file
    is older than the atlas build — otherwise the atlas may have cut at an earlier version."""
    matches, atlas = world
    (atlas / "graph.pkl.meta.json").write_text(json.dumps({"build_ts": 2_000_000_000.0}))
    os.utime(matches, (1_900_000_000, 1_900_000_000))
    M.stamp_atlas(atlas, matches)
    rec = json.loads((atlas / M.RECORD).read_text())
    assert rec["matches_sha256"] == M.matches_digest(matches) and rec["cut"] is True
    assert rec["stamped_after_build"] is True
    M.check_record(atlas, matches)
    (atlas / M.RECORD).unlink()
    os.utime(matches, (2_100_000_000, 2_100_000_000))
    with pytest.raises(SystemExit, match="newer than the atlas"):
        M.stamp_atlas(atlas, matches)
    assert not (atlas / M.RECORD).exists()


def test_the_atlas_build_writes_the_record_and_the_bundle_checks_it_first():
    """Both ends are wired (the builds themselves are heavy; their source is read)."""
    import importlib
    atlas_build = importlib.import_module("pipeline.atlas.build")
    bundle_build = importlib.import_module("pipeline.deliver.bundle.build")
    a = inspect.getsource(atlas_build)
    assert "write_record(" in a and "matches_digest(" in a
    g = inspect.getsource(bundle_build._gauges)
    assert g.index("check_record(") < g.index("read_match(")
