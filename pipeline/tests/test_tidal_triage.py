"""T212 follow-up: where the owner hand-edited the DFO tidal polygon (the 1 cm vertex-grid
fingerprint) and the triage of each tidal candidate. Synthetic geometry + the committed proposal
files; no generated data, no gpkg."""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
from shapely.geometry import Polygon

from pipeline.tools.audit import tidal_triage as TT

REPO = Path(__file__).resolve().parents[2]
TIDAL = REPO / "handoff/PLAN/curation-review/tidal"


def _ring(n=1000, r=5000.0, x0=1_000_000.0, y0=500_000.0):
    """A DFO-like ring: every vertex on the 1 cm grid."""
    t = np.linspace(0, 2 * math.pi, n, endpoint=False)
    return np.round(np.c_[x0 + r * np.cos(t), y0 + r * np.sin(t)], 2)


def test_dfo_vertices_are_on_the_grid_and_hand_vertices_are_not():
    c = _ring(10)
    assert not TT.off_grid(c).any()
    assert not TT.off_grid(c + 3e-5).any()                       # float noise stays on the grid
    assert TT.off_grid(c + np.array([0.0037, 0.0])).all()        # a hand-placed vertex is off it


def test_an_off_grid_run_among_on_grid_vertices_is_one_edit_site():
    c = _ring()
    c[400:402] += np.array([0.0041, 0.0063])                     # a two-vertex cut, like edit:7
    runs = TT.ring_runs(np.vstack([c, c[:1]]))
    assert len(runs) == 1 and runs[0]["k"] == 2 and runs[0]["on_share"] == 1.0
    assert len(runs[0]["line"]) == 4                             # the run + one DFO vertex each side
    sites, blind = TT.find_edits([({"NAME": "Subarea 1-1"}, Polygon(c))])
    assert [s["kind"] for s in sites] == ["cut"] and blind is None


def test_a_run_that_wraps_the_rings_start_is_one_run():
    c = _ring()
    c[:2] += 0.0041
    c[-2:] += 0.0041
    runs = TT.ring_runs(c)
    assert len(runs) == 1 and runs[0]["k"] == 4


def test_an_off_grid_stretch_of_dfo_data_is_blind_not_an_edit():
    c = _ring()
    off = np.arange(len(c)) % 20 != 0                            # off the grid but for a few DFO vertices
    c[off] += 0.0041
    sites, blind = TT.find_edits([({"NAME": "Subarea 19-1"}, Polygon(c))])
    assert sites == [] and blind is not None and not blind.is_empty


def test_a_row_without_dfo_attributes_is_hand_drawn():
    sq = Polygon([(0.123, 0.456), (100.789, 0.1), (100.2, 100.3), (0.5, 100.77)])
    sites, _ = TT.find_edits([({"NAME": None, "SQ_KM": float("nan")}, sq)])
    assert [s["kind"] for s in sites] == ["drawn"]


def _c(sources=(), measure=500.0):
    return {"id": "tidal:1", "polygon": {"measure": measure, "coord": [-123.5, 49.5]},
            "proposed": {"coord": [-123.5, 49.5]}, "sources": list(sources)}


def test_the_five_classes():
    no = {"kind": "synopsis_box", "agrees": False}
    yes = {"kind": "synopsis_box", "agrees": True}
    assert TT.classify(_c([no]), False, [])[0] == "missed"
    assert TT.classify(_c([no]), None, [])[0] == "missed"         # can't see an edit: the owner looks
    assert TT.classify(_c([no]), True, [])[0] == "sanity"
    assert TT.classify(_c([yes]), True, [])[0] == "owner-edited"
    assert TT.classify(_c(), True, [])[0] == "owner-edited"
    assert TT.classify(_c([yes]), False, [])[0] == "book agrees"
    assert TT.classify(_c(), False, [])[0] == "no claim"
    assert TT.classify(_c([{"kind": "dfo_notice", "agrees": None}]), False, [])[0] == "no claim"
    assert TT.classify(_c(), False, [{"agrees": False}])[0] == "missed"   # the web is a source too
    assert TT.classify(_c(), False, [{"agrees": True}])[0] == "book agrees"
    assert TT.classify(_c([{"kind": "tidal_gauge", "agrees": False}]), False, [])[0] == "no claim"


def test_a_located_web_finding_agrees_within_300_m_along_the_line():
    assert TT.web_verdict({"located": {"measure": 750.0}}, _c(measure=500.0)) is True
    assert TT.web_verdict({"located": {"measure": 900.0}}, _c(measure=500.0)) is False
    assert TT.web_verdict({"located": None}, _c()) is None
    # the polygon misses the stream: held against the mouth
    assert TT.web_verdict({"located": {"measure": 1000.0}},
                          {"polygon": {"measure": None}, "proposed": {}}) is False


def test_the_committed_triage_covers_every_candidate():
    doc = json.loads((TIDAL / "tidal-candidates.json").read_text())
    cs = doc["candidates"]
    assert all(c.get("triage", {}).get("class") in TT.CLASSES for c in cs)
    by = doc["coverage"]["by_triage"]
    assert sum(by.values()) == len(cs)
    pitt = next(c for c in cs if c["id"] == "tidal:356360869")
    assert pitt["triage"]["class"] == "owner-edited" and pitt["triage"]["edit_sites"]
    edits = json.loads((TIDAL / "tidal-edits.json").read_text())
    named = {st["name"] for s in edits["sites"] for st in s["streams"] if st["name"]}
    assert {"Pitt River", "Coquitlam River", "Somass River", "Campbell River"} <= named


def test_a_long_straight_edge_across_a_stream_is_an_edit_even_on_the_grid():
    """Follow-up 2: DFO's outline follows the shore vertex by vertex; a >= 150 m edge with no vertex
    between that crosses a stream is a hand cut — seen even where the grid test is blind, and when
    both ends sit on the grid (a cut made by deleting vertices)."""
    c = _ring(n=2000)                                            # ~16 m edges, all on the grid
    c = np.delete(c, np.arange(100, 110), axis=0)                # 10 vertices deleted: one ~170 m edge
    rows = [({"NAME": "Subarea 1-1"}, Polygon(c))]
    sites, _ = TT.find_edits(rows)
    assert sites == []                                           # the grid sees nothing
    longs = TT.long_segments(rows)
    assert len(longs) == 1 and np.hypot(*(longs[0][1] - longs[0][0])) > TT.STRAIGHT_M
    stream = {"blk": "9", "name": "Test Creek"}
    hit = TT.straight_sites(longs, lambda ln: [stream])
    assert [s["kind"] for s in hit] == ["straight"] and hit[0]["crosses"] == [stream]
    assert TT.straight_sites(longs, lambda ln: []) == []         # crosses no stream: DFO's own shore


def test_a_multi_crossing_stream_is_its_own_class_unless_a_source_disagrees():
    multi = {"multi_crossing": True, "n_cuts": 3, "cuts": [100.0, 200.0, 400.0],
             "gaps_filled": [[100.0, 200.0]], "ignored_pieces": []}
    c = _c()
    c["polygon"].update(multi)
    cls, why = TT.classify(c, True, [])
    assert cls == "multi-crossing" and "gaps filled 100-200" in why
    c["sources"] = [{"kind": "synopsis_box", "agrees": False}]
    assert TT.classify(c, False, [])[0] == "missed"
    assert "multi-crossing" in TT.REVIEW


def test_straight_cut_threshold_is_150_m():
    """Owner 2026-10-27: 50 m caught too much of the generic coastline; 150 m keeps the owner's own cuts."""
    assert TT.STRAIGHT_M == 150.0


def test_an_owner_decision_wins_over_every_other_class():
    """decisions.md 2026-10-27: Capilano at the source text, Cowichan at the river mouth."""
    dec = json.loads((TIDAL / TT.OWNER_DECISIONS).read_text())["decisions"]
    assert {d["water"]: d["boundary"] for d in dec} == {"Capilano River": "source_text", "Cowichan River": "river_mouth"}
    no = {"kind": "synopsis_box", "agrees": False}
    c = _c([no])
    c.update(id=dec[0]["id"], blk=dec[0]["blk"])
    doc = {"candidates": [c]}
    assert TT.triage(doc, {"sites": []}, None, {"decisions": dec}) == {"owner-decided": 1}
    assert c["triage"]["class"] == "owner-decided" and c["triage"]["decision"]["boundary"] == "source_text"
    assert TT.triage({"candidates": [{**_c([no]), "blk": "1"}]}, {"sites": []}, None, None) == {"missed": 1}


def test_the_committed_owner_decided_candidates():
    cs = {c["water"]: c for c in json.loads((TIDAL / "tidal-candidates.json").read_text())["candidates"]}
    for w, b in (("Capilano River", "source_text"), ("Cowichan River", "river_mouth")):
        assert cs[w]["triage"]["class"] == "owner-decided" and cs[w]["triage"]["decision"]["boundary"] == b
    assert cs["Capilano River"]["proposed"]["above_mouth_m"] == 37.0      # the CN bridge, not the polygon
    assert cs["Cowichan River"]["proposed"]["above_mouth_m"] == 0.0       # the mouth
