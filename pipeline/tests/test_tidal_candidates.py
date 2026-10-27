"""T212: the proposed tidal boundaries — the census tool's geometry, the "tidal" mark scope and its
staleness, and that accepting one is a mark (a ticket), never a curated write. No generated data."""
from __future__ import annotations

import json

import pytest
from shapely.geometry import LineString, Polygon

from pipeline.regs.review import marks as M
from pipeline.regs.review import tidal as T
from pipeline.regs.review import tickets
from pipeline.tools.audit import tidal_candidates as TC

SHA = "0" * 64


def _row(cid="tidal:1", measure=120.0) -> dict:
    return {"id": cid, "water": "Test Creek", "blk": "1", "wsc": "920-1", "enters": "ocean",
            "items": ["blk:1"], "item_names": ["Test Creek"], "zones": ["1-1"],
            "proposed": {"measure": measure, "coord": [-123.5, 49.5], "above_mouth_m": measure,
                         "basis": "the polygon's crossing", "split_label": TC.MOUTH_LABEL,
                         "split_name": f"Test Creek: {TC.MOUTH_LABEL}", "aliases": ["tidal boundary"]},
            "polygon": {"crossing": "clean", "measure": measure, "coord": [-123.5, 49.5], "n_cuts": 1,
                        "inside_pieces": 1, "outside_below_m": 0.0, "mouth_gap_m": 0.0},
            "mouth": {"measure": 0.0, "coord": [-123.51, 49.5]}, "sources": [],
            "confidence": "medium", "why": "", "agreement": "polygon only", "words_disagree": False}


@pytest.fixture
def cands(tmp_path, monkeypatch):
    p = tmp_path / "tidal-candidates.json"

    def write(*rows):
        p.write_text(json.dumps({"coverage": {}, "candidates": list(rows)}))
        T._load.cache_clear()
    write(_row())
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(p))
    return write


def _split(**kw) -> dict:
    return {"ref": "tidal-1", "anchor": "coord", "name": f"Test Creek: {TC.MOUTH_LABEL}",
            "on_item": "blk:1", "coord": [-123.5, 49.5], **kw}


def _mark(verdict="verified", answers=None, **kw) -> M.Mark:
    return M.Mark.model_validate({
        "scope": "tidal", "target": "tidal:1", "verdict": verdict, "answers": answers or {"P": "y"},
        "reviewer": "owner", "date": "2026-10-27", "pdf_sha256": SHA,
        "prints": T.prints_of("tidal:1"), **kw})


# --------------------------------------------------------------------------- the mark scope

def test_accepting_is_place_y_with_exactly_its_one_cut(cands):
    m = _mark(new_splits=[_split()])
    assert m.scope == "tidal" and m.verdict == "verified" and len(m.new_splits) == 1
    with pytest.raises(ValueError, match="exactly its one cut"):
        _mark()
    with pytest.raises(ValueError, match="exactly its one cut"):
        _mark(new_splits=[_split(), _split(ref="tidal-2", coord=[-123.4, 49.5])])


def test_a_tidal_mark_answers_place_only_and_proposes_no_extent(cands):
    with pytest.raises(ValueError, match="answers"):
        _mark(answers={"P": "y", "T": "y"}, new_splits=[_split()])
    with pytest.raises(ValueError, match="no extent fix"):
        _mark(verdict="unsure", answers={"P": "?"},
              extent_fix=[{"scope": "entry", "op": "whole"}])
    # a misplaced proposal: P n, the reviewer's own point as the cut
    m = _mark(verdict="wrong", answers={"P": "n"}, problem="split misplaced", note="it is at the bridge",
              new_splits=[_split(coord=[-123.49, 49.5])])
    assert m.verdict == "wrong"


def test_a_tidal_mark_holds_exactly_its_candidates_prints(cands):
    bad = {"text": "x", "records": [{"key": "entry", "meaning": "a", "binding": "b"}]}
    with pytest.raises(ValueError, match="candidate's prints"):
        _mark(new_splits=[_split()], prints=bad)


def test_a_moved_proposal_makes_the_mark_stale(cands):
    m = _mark(new_splits=[_split()])
    now = M.target_prints(m, {}, None)
    assert M.status(m, now)["status"] == "verified"
    cands(_row(measure=900.0))                         # the regenerated file moved the cut
    st = M.status(m, M.target_prints(m, {}, None))
    assert st["status"] == "stale" and M.StaleReason.rule_changed.value in st["reasons"]
    cands()                                            # the candidate is gone
    T._load.cache_clear()
    assert M.target_prints(m, {}, None) is None


def test_an_accepted_tidal_boundary_becomes_a_ticket_for_t185(cands):
    m = _mark(new_splits=[_split()])
    assert tickets.ticketed(m)
    text = tickets.ticket_text(m)
    assert "tidal" in text and TC.MOUTH_LABEL in text
    assert any("mouth (tidal boundary)" in ln for ln in tickets._work(m)) or "splits.json" in "".join(tickets._work(m))


# --------------------------------------------------------------------------- the census geometry

def _seg(fid, down, coords):
    g = LineString(coords)
    return {"fid": fid, "blk": 1, "name": "Test Creek", "gnis": 1, "wsc": "920-1", "down": down,
            "length": g.length, "edge": 1000, "ocean": True}, g


def test_the_candidate_is_where_the_line_leaves_the_polygon_going_upstream():
    sea = Polygon([(0, -100), (300, -100), (300, 100), (0, 100)])          # the sea: x < 300
    s1, g1 = _seg(1, 0.0, [(0, 0), (500, 0)])                               # mouth at x=0
    s2, g2 = _seg(2, 500.0, [(500, 0), (1000, 0)])
    a = TC.analyse([s1, s2], {1: g1, 2: g2}, sea)
    assert a["top"] == pytest.approx(300.0, abs=1) and a["n_inside"] == 1 and a["mouth_m"] == 0.0


def test_a_separate_upstream_patch_is_no_boundary():
    """A polygon piece the polygon does not join to the run along the river (a patch elsewhere) is
    ignored: the boundary stays at the top of the run connected to the mouth."""
    sea = Polygon([(0, -100), (300, -100), (300, 100), (0, 100)]).union(
        Polygon([(800, -20), (830, -20), (830, 20), (800, 20)]))
    s1, g1 = _seg(1, 0.0, [(0, 0), (1000, 0)])
    a = TC.analyse([s1], {1: g1}, sea)
    assert a["n_inside"] == 1 and a["top"] == pytest.approx(300.0, abs=1)
    assert a["ignored"] == [(800.0, 830.0)] and a["gaps_filled"] == []


def test_in_out_in_along_the_corridor_is_one_run_up_to_the_top():
    """(owner, T212 follow-up 2, Mill Stream): the polygon is drawn up the channel; where the FWA line
    wanders out of it and back in, the gap is filled — ONE run from the mouth to the uppermost exit.
    A far patch the corridor polygon does not reach stays ignored."""
    # the channel polygon runs y in [20, 60] from x 300 to 700, joined to the sea; the line dips in
    # and out of it: inside 0-300 (sea), out 300-400 (y=0), in 400-600 (y=40), out above 600
    channel = Polygon([(250, 20), (700, 20), (700, 60), (250, 60)])
    sea = Polygon([(0, -100), (300, -100), (300, 100), (0, 100)])
    patch = Polygon([(1500, 30), (1540, 30), (1540, 50), (1500, 50)])
    poly = sea.union(channel).union(patch)
    s1, g1 = _seg(1, 0.0, [(0, 0), (400, 0), (400, 40), (600, 40), (600, 0), (1600, 0), (1600, 40),
                           (1450, 40)])
    a = TC.analyse([s1], {1: g1}, poly)
    assert a["n_inside"] == 2, a
    assert a["top"] == pytest.approx(400 + 40 + 200 + 20, abs=2)           # leaves at (600, 20)
    assert len(a["gaps_filled"]) == 1 and a["gap_below_m"] == pytest.approx(120, abs=2)
    assert len(a["ignored"]) == 1                                           # the patch, crossed at the far end
    assert len(a["cuts"]) >= 4


def test_a_named_bridge_is_located_where_it_crosses_the_line():
    s1, g1 = _seg(1, 0.0, [(0, 0), (2000, 0)])
    ways = [{"id": 1, "kind": "rail", "tags": {"railway": "rail", "bridge": "yes"},
             "geom": LineString([(250, -50), (250, 50)])},
            {"id": 2, "kind": "road", "tags": {"highway": "primary", "ref": "19", "bridge": "yes"},
             "geom": LineString([(1200, -50), (1200, 50)])},
            {"id": 3, "kind": "road", "tags": {"highway": "residential", "name": "Mill Rd"},
             "geom": LineString([(600, -50), (600, 50)])}]
    ll = lambda x, y: (x, y)                                                # noqa: E731
    rail = TC.locate_marker([s1], {1: g1}, ways, "rail", 300.0, ll)
    assert rail["measure"] == pytest.approx(250, abs=1)
    hwy = TC.locate_marker([s1], {1: g1}, ways, "road", 300.0, ll, match="19", offset_m=100.0)
    assert hwy["measure"] == pytest.approx(1300, abs=1) and hwy["named_match"] is True
    first = TC.locate_marker([s1], {1: g1}, ways, "road", 1500.0, ll, pick="lowest")
    assert first["crossing_measure"] == pytest.approx(600, abs=1)


def test_pmtiles_tile_ids_follow_the_hilbert_order():
    tid = TC.Basemap._tile_id
    assert [tid(0, 0, 0), tid(1, 0, 0), tid(1, 0, 1), tid(1, 1, 1), tid(1, 1, 0)] == [0, 1, 2, 3, 4]
    assert tid(2, 0, 0) == 5


def test_every_proposal_is_labelled_mouth_tidal_boundary():
    assert TC.MOUTH_LABEL == "mouth (tidal boundary)"


# --------------------------------------------------------------------------- reusing a curated cut

CAMPBELL_SIGN = {"split_id": "campbell_river__cement_block_boundary_sign", "label": "cement block boundary sign",
                 "measure": 1630.67}


def test_a_curated_split_within_reach_on_the_line_is_reused_not_cut_again():
    # Campbell River: the cement block sign at 1631 m, the polygon's crossing at 1625 m
    ru = TC.pick_reuse(1625.0, [{"split_id": "campbell_river__mouth", "label": "mouth", "measure": 0.0},
                                CAMPBELL_SIGN])
    assert ru["split_id"] == CAMPBELL_SIGN["split_id"] and ru["measured"] == "along the line"
    assert ru["distance_m"] == pytest.approx(5.7, abs=0.1) and ru["split_measure"] == pytest.approx(1630.7)
    assert ru["alias"] == "tidal boundary" and ru["tidal_measure"] == 1625.0


def test_a_split_further_than_reach_or_off_the_line_is_not_reused():
    far = {**CAMPBELL_SIGN, "measure": 1625.0 + TC.REUSE_M + 1}
    assert TC.pick_reuse(1625.0, [far]) is None
    # an unresolved point close on the ground but on ANOTHER line (far from this one): not reused
    other = {"split_id": "x__trib", "label": "trib", "measure": 1625.0, "line_m": 80.0, "ground_m": 10.0}
    assert TC.pick_reuse(1625.0, [], [other]) is None
    # the same point on this line: reused, measured on the ground
    near = {**other, "line_m": 3.0}
    assert TC.pick_reuse(1625.0, [], [near])["measured"] == "ground distance"
    # a resolved split on the line wins over an unresolved point
    assert TC.pick_reuse(1625.0, [CAMPBELL_SIGN], [near])["split_id"] == CAMPBELL_SIGN["split_id"]


def test_accepting_a_reuse_candidate_names_the_existing_split(cands):
    ns = _split(anchor="split", split_id=CAMPBELL_SIGN["split_id"], note="aliases: tidal boundary")
    m = _mark(new_splits=[ns])
    assert m.new_splits[0].anchor == "split" and m.new_splits[0].split_id == CAMPBELL_SIGN["split_id"]
    with pytest.raises(ValueError, match="names that split"):
        _mark(new_splits=[_split(anchor="split")])


def test_a_dfo_line_between_two_landmarks_is_located_where_it_meets_the_stream():
    s1, g1 = _seg(1, 0.0, [(0, 0), (2000, 0)])
    across = TC.locate_line([s1], {1: g1}, [(700, -300), (700, 300)])
    assert across["measure"] == pytest.approx(700, abs=1)
    seaward = TC.locate_line([s1], {1: g1}, [(-600, -300), (-600, 300)])      # out in the bay
    assert seaward["measure"] == 0.0 and "seaward" in seaward["how"]
    assert TC.locate_line([s1], {1: g1}, [(-9000, -300), (-9000, 300)]) is None
