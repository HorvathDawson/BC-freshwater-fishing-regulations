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
from pathlib import Path

import pytest

from pipeline.gauges import matches as M


@pytest.fixture
def world(tmp_path: Path):
    matches = tmp_path / "matches.json"
    matches.write_text(json.dumps({"stations": [{"station": "08AA001", "status": "matched", "resolved_by": "t", "distance_m": 0}]}))
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
    matches.write_text(json.dumps({"stations": [{"station": "08AA001", "status": "no_match", "resolved_by": "t", "distance_m": 0}]}))
    with pytest.raises(SystemExit) as e:
        M.check_record(atlas, matches)
    msg = str(e.value)
    assert "pipeline.atlas.build" in msg and M.matches_digest(matches) in msg


def test_an_atlas_without_the_record_is_refused_with_the_rebuild(world):
    matches, atlas = world
    with pytest.raises(SystemExit, match="Rebuild the atlas"):
        M.check_record(atlas, matches)


def test_an_atlas_that_cut_at_no_gauge_accepts_any_matches(world):
    """`--no-gauge-splits`: the graph holds no gauge cut, so no version of the file can disagree with it."""
    matches, atlas = world
    M.write_record(atlas, cut=False, sha=None)
    matches.write_text("{}")
    M.check_record(atlas, matches)


def test_no_file_time_decides_staleness():
    """(T435) the record is content: no mtime / build_ts comparison is left (a copied atlas refused falsely, a touched
    file passed), and the tools that used one are gone."""
    src = (Path(M.__file__)).read_text()
    assert "st_mtime" not in src and "build_ts" not in src
    assert not hasattr(M, "stamp_atlas") and not hasattr(M, "record_cuts")


def test_the_atlas_build_writes_the_record_and_the_bundle_checks_it_first():
    """Both ends are wired (the builds themselves are heavy; their source is read)."""
    import importlib
    atlas_build = importlib.import_module("pipeline.atlas.build")
    bundle_build = importlib.import_module("pipeline.gauges.consume.bundle_tables")
    a = inspect.getsource(atlas_build)
    assert "write_record(" in a and "matches_digest(" in a
    g = inspect.getsource(bundle_build.write)
    assert g.index("check_record(") < g.index("read_match(")


# ---------------------------------------------------------------- T159: the CUT SET, not the bytes

def _rows(stream2: dict) -> dict:
    return {"stations": [
        {"station": "08AA001", "status": "matched", "resolved_by": "t", "distance_m": 0, "lon": -122.0, "lat": 49.0, "wsc": "100-1"},
        {"station": "08AA002", "resolved_by": "t", "distance_m": 0, **stream2}]}


def test_a_change_that_moves_no_cut_passes_and_one_that_does_is_refused(tmp_path, monkeypatch):
    """T159 promoted 8 stations — 6 lake gauges, 2 not transmitting — and the byte digest refused the
    bundle though no boundary moved. A non-cut row passes; a row that becomes a cut is still refused."""
    from pipeline.gauges.consume import cuts
    monkeypatch.setattr(cuts, "may_speak", lambda s: s["station"] == "08AA001")
    roster = tmp_path / "roster.json"
    roster.write_text("[]")
    from pipeline.gauges.consume import shed
    monkeypatch.setattr(shed, "load_stations", lambda p: [
        {"station": "08AA001", "name": "a"}, {"station": "08AA002", "name": "b"}])
    matches, atlas = tmp_path / "m.json", tmp_path / "atlas"
    atlas.mkdir()
    matches.write_text(json.dumps(_rows({"status": "no_match"})))
    M.write_record(atlas, cut=True, sha=M.matches_digest(matches),
                   cuts_sha=M.cuts_digest(matches, roster))
    # a lake gauge / a retired station joins: no cut
    matches.write_text(json.dumps(_rows({"status": "matched", "lon": -122.1, "lat": 49.1, "wsc": "100-2"})))
    M.check_record(atlas, matches, roster)
    # ECCC calls it Active: now it IS a cut the atlas never made
    monkeypatch.setattr(cuts, "may_speak", lambda s: True)
    with pytest.raises(SystemExit, match="did not cut"):
        M.check_record(atlas, matches, roster)
