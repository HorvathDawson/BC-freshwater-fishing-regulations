"""T174: no "answered -> grey -> answered" gap on a river without a reason (owner sanity rule, T169).

Q153 `ADOPT_ANSWERED` — a side piece whose LARGEST river section in reach is silent adopts the largest one that answers.
Q155 `WATER_LINE_GAPS` — a stream water's own unanswered lines between pieces of it answering ALIKE take that answer.
Whole values on miniatures, the narrow cases that must NOT move, and a switch-off mutation pin for each.
"""
from __future__ import annotations

import pytest

import pipeline.gauges.consume.panels as bp
from pipeline.gauges.consume.panels import build, mainstem_candidates
from pipeline.tests.test_build_panels import AREA, MODEL, E, N, g

R = "100-555555"
SLOUGH, CREEK, LAKE = "gnis:7", "gnis:8", "wbk:9"
KINDS = {SLOUGH: "stream", CREEK: "stream", LAKE: "lake"}


@pytest.fixture
def switches(monkeypatch):
    def set_(**kw):
        for k, v in kw.items():
            monkeypatch.setattr(bp, k, v)
    return set_


def _stations(panels, sec):
    return sorted(m.station for m in panels.members[panels.by_section[sec][0]])


# ------------------------------------------------------------------ Q153: adopt the largest ANSWERED section

def _silent_largest():
    """A side piece (1350) between an answered river piece and a far larger one no gauge can speak for (area ratio
    past MAX_AREA_RATIO): Elk / Bonanza / Lillooet's side pieces in miniature."""
    graph = g([N("riv_a", 500, wsc=R), N("side", 2, wsc=R, edge_types=("1350",)), N("big", 1_000_000, wsc=R)],
              [E("riv_a", "side"), E("side", "big"), E("riv_a", "big")])
    return graph, [("A", "riv_a", 40, False)]


def test_candidates_are_largest_first_and_the_first_is_mainstem_of():
    graph, _ = _silent_largest()
    assert mainstem_candidates(graph, AREA) == {"side": ["big", "riv_a"]}
    assert bp.mainstem_of(graph, AREA) == {"side": "big"}


def test_a_side_piece_whose_largest_section_is_silent_takes_the_largest_answered(switches):
    graph, donors = _silent_largest()
    panels = build(graph, MODEL, donors)
    assert "big" not in panels.by_section
    assert panels.by_section["side"] == panels.by_section["riv_a"]
    assert _stations(panels, "side") == ["A"]
    assert panels.by_section["side"][1] == 500


def test_an_answered_largest_section_is_still_the_one_adopted(switches):
    """Narrow: where the largest section answers, nothing changes (the 1,000s of ordinary adoptions)."""
    graph = g([N("riv_a", 500, wsc=R), N("side", 2, wsc=R, edge_types=("1350",)), N("big", 900, wsc=R)],
              [E("riv_a", "side"), E("side", "big"), E("riv_a", "big")])
    panels = build(graph, MODEL, [("A", "riv_a", 40, False), ("B", "big", 40, False)])
    assert panels.by_section["side"] == panels.by_section["big"]


def test_no_answered_section_in_reach_leaves_the_side_piece_silent(switches):
    graph = g([N("side", 2, wsc=R, edge_types=("1350",)), N("big", 1_000_000, wsc=R)], [E("side", "big")])
    panels = build(graph, MODEL, [])
    assert "side" not in panels.by_section


def test_mutation_pin_adopt_answered(switches):
    switches(ADOPT_ANSWERED=False)
    graph, donors = _silent_largest()
    assert "side" not in build(graph, MODEL, donors).by_section


# ------------------------------------------------------------------ Q155: a water's own lines between alike pieces

def poly(node_id: str) -> N:
    return N(node_id, stream_magnitude=None, kind="lake")


def _gravel(extra_donor: bool = False):
    """Gravel Slough in miniature: polygons p1, p2 with connector lines c1, c2 between them, answered from below
    (the polygons by the fallback); a creek joins c1 from outside the water.

        p1 -> c1 -> c2 -> p2 -> out   (all SLOUGH; gauge on `out`)    creek -> c1 (CREEK, gauge far up)
    """
    nodes = [poly("p1"), N("c1", 1), N("c2", 1), poly("p2"), N("out", 400), N("creek", 30, wsc="100-555555-1")]
    edges = [E("p1", "c1"), E("c1", "c2"), E("c2", "p2"), E("p2", "out"), E("creek", "c1")]
    water_of = {"p1": SLOUGH, "c1": SLOUGH, "c2": SLOUGH, "p2": SLOUGH, "out": SLOUGH, "creek": CREEK}
    donors = [("OUT", "out", 40, False)]
    if extra_donor:
        donors.append(("MID", "c2", 40, False))
    return g(nodes, edges), water_of, donors


def test_a_water_s_connector_lines_between_alike_pieces_take_that_answer(switches):
    graph, water_of, donors = _gravel()
    panels = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    for sec in ("p1", "p2", "c1", "c2"):
        assert panels.by_section[sec] == panels.by_section["out"], sec
    assert "creek" not in panels.by_section            # another water is never touched


def test_a_run_with_its_own_answer_is_left_alone(switches):
    """A gauge inside the run (c2) is a reason: the run is not a gap, and nothing overrides it."""
    graph, water_of, donors = _gravel(extra_donor=True)
    panels = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    assert "MID" in _stations(panels, "c2")


def test_two_different_answers_at_the_ends_leave_the_run_grey(switches):
    """Above answered by one source, below by another (a confluence, a gauge between): no single answer to take."""
    nodes = [N("top", 400), N("c1", 1), N("bot", 400)]
    graph = g(nodes, [E("top", "c1"), E("c1", "bot")])
    water_of = {"top": SLOUGH, "c1": SLOUGH, "bot": SLOUGH}
    by_section = {"top": (1, 400.0), "bot": (2, 400.0)}
    assert bp.water_line_gaps(graph, water_of, KINDS, by_section) == {}
    assert bp.water_line_gaps(graph, water_of, KINDS, {"top": (1, 400.0), "bot": (1, 400.0)}) == {"c1": "bot"}


def test_a_one_sided_run_or_a_lake_s_line_is_not_a_gap(switches):
    graph = g([N("top", 400), N("c1", 1)], [E("top", "c1")])
    assert bp.water_line_gaps(graph, {"top": SLOUGH, "c1": SLOUGH}, KINDS, {"top": (1, 400.0)}) == {}
    graph = g([N("top", 400), N("c1", 1), N("bot", 400)], [E("top", "c1"), E("c1", "bot")])
    wo = {"top": LAKE, "c1": LAKE, "bot": LAKE}
    assert bp.water_line_gaps(graph, wo, KINDS, {"top": (1, 1.0), "bot": (1, 1.0)}) == {}


def test_mutation_pin_water_line_gaps(switches):
    switches(WATER_LINE_GAPS=False)
    graph, water_of, donors = _gravel()
    panels = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    assert "c1" not in panels.by_section or panels.by_section["c1"] != panels.by_section["out"]


# ------------------------------------------------------------------ the real bundle

@pytest.mark.needs_bundle
def test_gravel_slough_s_connectors_answer_in_the_bundle():
    """Measured 2026-10-27 (T174): Gravel Slough (gnis:8009, 11 sections) had 3 unanswered 1425 connector lines between
    its answered polygons; two sit between pieces answering alike (Elk Creek's 08MF048/08MF018) and now answer. The
    third (356292469:9077) has two different answers at its ends (08MF064 on lake:329083360) — a reason — and stays."""
    import sqlite3

    from pipeline.tests.conftest import predates
    from pipeline.tests.test_stream_polygon_panels import _bundle
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    total, silent = db.execute(
        "SELECT count(*), sum(p.panel_id IS NULL) FROM item i JOIN item_section s ON s.ord = i.ord "
        "LEFT JOIN section_panel p ON p.sid = s.sid WHERE i.item_id = 'gnis:8009'").fetchone()
    assert total == 11
    if silent == 3:
        predates(f"{_bundle()} predates T174 (water_line_gaps) — rerun pipeline.deliver")
    # (T228, a757756b) a DORMANT station (discontinued, not real-time) attaches to nothing: Elk Creek's 08MF048 and
    # 08MF018 and the Fraser's 08MF064 are all dormant, so no donor is left and every section is silent — for that reason
    dormant = {st for st, sid in db.execute(
        "SELECT station, sid FROM gauge WHERE station IN ('08MF048', '08MF018', '08MF064')") if sid is None}
    assert dormant == {"08MF048", "08MF018", "08MF064"} and silent == total
