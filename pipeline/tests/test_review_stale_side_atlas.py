"""T1040: a side atlas vouches for a proposal only when it was built with the proposal's cuts WHERE THEY LAND NOW.

`side_dir` is keyed by the proposal's id and digest, and the digest of a place naming a new cut does not cover where the
cut sits (its id comes from water, name, anchor and offset). Moving the point on the same anchor kept the old side
atlas "ready", and the place was bound at the old point. The atlas's own record (`split_anchors.json`) says where each
cut it was built with is; `proposals.stale_cuts` compares it with the cut file as the chain would leave it."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.atlas.splits.splits import ANCHORS_FILE, anchor_digests
from pipeline.regs.review import apply_proposals as AP
from pipeline.regs.review import proposals as P

CUT = "x_river__the_bridge"
WB = {"name": "X RIVER", "applies_to": {"gnis_id": "1"}}


def _split(lon: float) -> dict:
    return {"id": CUT, "label": "the bridge", "anchor": {"type": "point", "lon": lon, "lat": 50.0}}


def _src(tmp_path: Path, cuts: list[dict]) -> P.Sources:
    for d in ("cat", "dfo"):
        (tmp_path / d).mkdir(exist_ok=True)
    path = tmp_path / "cuts.json"
    path.write_text(json.dumps({"waterbodies": [{**WB, "splits": cuts}]}))
    return P.Sources(tmp_path / "cat", tmp_path / "dfo", path, None)


def _chain(lon: float) -> tuple[dict, list[dict]]:
    """a new cut (`new_split`) and the DFO place that names it (it depends on it)"""
    cut = {"id": "split-n1", "kind": "new_split", "target": "gnis:1", "scope": "split", "depends_on": [],
           "edits": [{"file": "cuts.json", "kind": "split", "waterbody": WB, "split": _split(lon)}], "digest": "c" * 64}
    place = {"id": "dfo-1", "kind": "extent", "target": "8:x-river:a", "scope": "dfo", "depends_on": ["split-n1"],
             "edits": [{"file": "region-8.json", "kind": "dfo", "scope": "entry", "field": "extents",
                        "after": [{"op": "upstream_of", "splits": [CUT]}]}], "digest": "d" * 64}
    return place, [cut, place]


def _atlas(tmp_path: Path, cuts_doc: dict) -> Path:
    """a built atlas's record of where its cuts are (what the atlas build writes, `write_anchor_record`)"""
    a = tmp_path / P.side_dir({"id": "dfo-1", "digest": "d" * 64}) / "atlas"
    a.mkdir(parents=True, exist_ok=True)
    (a / ANCHORS_FILE).write_text(json.dumps(anchor_digests(cuts_doc)))
    return a


def test_a_side_atlas_built_before_its_new_cut_moved_is_not_ready(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "REPO", tmp_path)
    src = _src(tmp_path, [])
    place, props = _chain(-120.0)
    _atlas(tmp_path, {"waterbodies": [{**WB, "splits": [_split(-120.0)]}]})       # built with the cut at -120.0
    assert P.chain_cuts(place, props) == {CUT}
    assert P.stale_cuts(place, props, tmp_path / P.side_dir(place) / "atlas", src) == []
    assert P.side_ready(place, props, src) is True
    moved, props2 = _chain(-120.01)                                                # the point moved, same id/digest
    assert P.side_dir(moved) == P.side_dir(place)
    assert P.stale_cuts(moved, props2, tmp_path / P.side_dir(moved) / "atlas", src) == [CUT]
    assert P.side_ready(moved, props2, src) is False


def test_a_cut_that_already_landed_and_moved_stales_the_side_atlas_too(tmp_path, monkeypatch):
    """the cut is in the cut file now (landed in an earlier run) and the place names it: the side atlas must hold
    the cut as the FILE has it"""
    monkeypatch.setattr(P, "REPO", tmp_path)
    place, _ = _chain(-120.0)
    place = {**place, "depends_on": []}
    doc = {"waterbodies": [{**WB, "splits": [_split(-120.0)]}]}
    _atlas(tmp_path, doc)
    assert P.side_ready(place, [place], _src(tmp_path, [_split(-120.0)])) is True
    assert P.side_ready(place, [place], _src(tmp_path, [_split(-119.9)])) is False
    # a label edit moves no cut: still ready
    relabelled = dict(_split(-120.0), label="the old bridge")
    assert P.side_ready(place, [place], _src(tmp_path, [relabelled])) is True


def test_an_atlas_with_no_cut_record_is_not_trusted(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "REPO", tmp_path)
    place, props = _chain(-120.0)
    (tmp_path / P.side_dir(place) / "atlas").mkdir(parents=True)
    assert P.stale_cuts(place, props, tmp_path / P.side_dir(place) / "atlas", _src(tmp_path, [])) is None
    assert P.side_ready(place, props, _src(tmp_path, [])) is False


def test_an_atlas_named_by_build_is_used_only_when_it_holds_the_cut_where_it_lands(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "REPO", tmp_path)
    monkeypatch.delenv("CURATION_BUILD", raising=False)
    src = _src(tmp_path, [])
    monkeypatch.setattr(P.Sources, "live", classmethod(lambda cls, build=None: src))
    place, props = _chain(-120.01)
    named = tmp_path / "elsewhere"
    named.mkdir()
    (named / ANCHORS_FILE).write_text(json.dumps(anchor_digests({"waterbodies": [{**WB, "splits": [_split(-120.0)]}]})))
    assert AP.side_atlas(place, props, named) is None                               # built with the old point
    (named / ANCHORS_FILE).write_text(json.dumps(anchor_digests({"waterbodies": [{**WB, "splits": [_split(-120.01)]}]})))
    assert AP.side_atlas(place, props, named) == named
