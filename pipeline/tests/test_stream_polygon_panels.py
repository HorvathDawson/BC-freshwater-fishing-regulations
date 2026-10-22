"""A stream water's polygons take that water's flow answer — OWNER 2026-10-22 (late), "sloughs and canals look
like streams in Conditions" (handoff/PLAN/app-integration/decisions.md).

The colouring follows the WATER'S KIND, never the geometry type. Nicomen Slough is a stream item with four polygon
sections on its stem; Vedder River carries 4.7 km of Vedder Canal as one polygon section. The panel walk sizes a
reach by its stream magnitude, which a polygon does not have, and the old code read the graph's `NodeKind` (a polygon
node is `lake:<wbk>`) as if it were the water's kind — so every one of the 146 stream-item polygons had no panel and
drew grey between coloured lines of the same water.
"""

from __future__ import annotations

import inspect
import sqlite3
import textwrap
from pathlib import Path

import pytest

import pipeline.atlas.gauges.build_panels as bp
from pipeline.atlas.gauges.build_panels import build, stream_polygons
from pipeline.tests.test_build_panels import MODEL, E, N, g

SLOUGH, RIVER, LAKE = "gnis:21105", "gnis:2", "wbk:9"
KINDS = {SLOUGH: "stream", RIVER: "stream", LAKE: "lake"}


def poly(node_id: str) -> N:
    """A polygon section: a `lake:<wbk>` node with no stream magnitude, as the real graph has it."""
    return N(node_id, stream_magnitude=None, kind="lake")


def _nicomen():
    """Nicomen Slough in miniature, flowing left to right (upper → lower):

        top ─▶ p_up ─▶ mid ─▶ p_low ─▶ foot          (all the slough; gauges on `top` and `foot`)
        lake ─▶ narrows ─▶ outlet                    (a TRUE lake item owning a line of its own, then a river)

    `mid` sits between two polygons and is reached by neither gauge, as 59620 is on the real slough. `narrows` is a
    line the LAKE item owns, answered by the river's gauge walking up — the case where only the water-kind gate keeps
    the lake's polygon off a flow answer.
    """
    nodes = [N("top", 100), poly("p_up"), N("mid", 100), poly("p_low"), N("foot", 100),
             poly("lake"), N("narrows", 100), N("outlet", 100)]
    edges = [E("top", "p_up"), E("p_up", "mid"), E("mid", "p_low"), E("p_low", "foot"),
             E("lake", "narrows"), E("narrows", "outlet")]
    water_of = {"top": SLOUGH, "p_up": SLOUGH, "mid": SLOUGH, "p_low": SLOUGH, "foot": SLOUGH,
                "lake": LAKE, "narrows": LAKE, "outlet": RIVER}
    donors = [("TOP", "top", 40, False), ("FOOT", "foot", 40, False), ("OUT", "outlet", 40, False)]
    return g(nodes, edges), water_of, donors


def _answers(panels) -> dict[str, tuple[tuple[str, ...], float | None]]:
    """`section -> (its panel's stations, its target area)` — the whole colour input, by value not by panel id."""
    return {sec: (tuple(m.station for m in panels.members[pid]), area)
            for sec, (pid, area) in panels.by_section.items()}


def test_a_stream_water_s_polygons_take_the_answer_of_its_line_below():
    """Whole value. The polygons paint like the slough; the lake stays a lake; `mid` keeps the walk's (non-)answer —
    it is a line, and why the walk cannot reach it is the separate, postponed question (T55)."""
    graph, water_of, donors = _nicomen()
    panels = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    assert _answers(panels) == {
        "top": (("TOP",), 100.0),
        "foot": (("FOOT",), 100.0),
        "outlet": (("OUT",), 100.0),
        "narrows": (("OUT",), 100.0),
        # Downstream first: the water leaving a polygon is the water that went through it. `p_up` walks on through
        # the unpanelled `mid` and the polygon below it, all the same water, to the first line that has an answer.
        "p_low": (("FOOT",), 100.0),
        "p_up": (("FOOT",), 100.0),
    }
    # The SAME panel, not an equal copy: the polygon and its line are one answer.
    assert panels.by_section["p_low"] == panels.by_section["foot"]
    assert panels.by_section["p_up"] == panels.by_section["foot"]


def test_with_no_line_below_a_polygon_takes_the_line_above():
    graph = g([N("top", 100), poly("p")], [E("top", "p")])
    panels = build(graph, MODEL, [("TOP", "top", 40, False)],
                   water_of={"top": SLOUGH, "p": SLOUGH}, kind_of=KINDS)
    assert _answers(panels) == {"top": (("TOP",), 100.0), "p": (("TOP",), 100.0)}


def test_a_true_lake_keeps_lake_treatment():
    """A lake item's polygon between two panelled lines gets nothing: its answer is `lake_gauge` (level), never a
    river's flow."""
    graph = g([N("inflow", 100), poly("lake"), N("outflow", 100)],
              [E("inflow", "lake"), E("lake", "outflow")])
    panels = build(graph, MODEL, [("IN", "inflow", 40, False), ("OUT", "outflow", 40, False)],
                   water_of={"inflow": RIVER, "lake": LAKE, "outflow": RIVER}, kind_of=KINDS)
    assert _answers(panels) == {"inflow": (("IN",), 100.0), "outflow": (("OUT",), 100.0)}


def test_a_polygon_never_borrows_another_water_s_answer():
    """The walk keeps to the polygon's own water: a canal-only item (Seton Canal, Bowman, Lewis …) with no line of
    its own is honestly unmeasured, however close a gauged river is."""
    graph = g([poly("canal"), N("river", 100)], [E("canal", "river")])
    panels = build(graph, MODEL, [("R", "river", 40, False)],
                   water_of={"canal": SLOUGH, "river": RIVER}, kind_of=KINDS)
    assert _answers(panels) == {"river": (("R",), 100.0)}


def test_a_stream_water_whose_lines_have_no_answer_leaves_its_polygons_without_one():
    graph = g([N("top", 100), poly("p"), N("foot", 100)], [E("top", "p"), E("p", "foot")])
    panels = build(graph, MODEL, [], water_of={"top": SLOUGH, "p": SLOUGH, "foot": SLOUGH}, kind_of=KINDS)
    assert _answers(panels) == {}


def test_stream_polygons_names_each_polygon_s_line():
    graph, water_of, donors = _nicomen()
    panelled = {"top": (0, 100.0), "foot": (1, 100.0), "outlet": (2, 100.0), "narrows": (2, 100.0)}
    assert stream_polygons(graph, water_of, KINDS, panelled) == {"p_low": "foot", "p_up": "foot"}


def test_the_water_kind_gate_is_what_holds_these_tests():
    """MUTATION PIN: gate on the GEOMETRY again (the graph's NodeKind) instead of the water's kind and the slough's
    polygons go grey; drop the gate and the true lake takes a river's flow. Both mutants must fail."""
    src = textwrap.dedent(inspect.getsource(bp.stream_polygons))
    gate = 'kind_of.get(water) != "stream"'
    assert src.count(gate) == 1, "the water-kind gate moved; update this pin to match"

    def mutant(replacement: str):
        ns = dict(vars(bp))
        exec(compile(src.replace(gate, replacement), bp.__file__, "exec"), ns)
        return ns["stream_polygons"]

    graph, water_of, donors = _nicomen()
    panelled = {"top": (0, 100.0), "foot": (1, 100.0), "outlet": (2, 100.0), "narrows": (2, 100.0)}
    assert bp.stream_polygons(graph, water_of, KINDS, panelled) == {"p_low": "foot", "p_up": "foot"}
    # Keyed on geometry: a polygon node is `lake`, so no slough polygon is ever a stream.
    assert mutant('_kind(node) != "stream"')(graph, water_of, KINDS, panelled) == {}
    # No gate at all: the lake borrows the river below it.
    assert mutant("False")(graph, water_of, KINDS, panelled) == {
        "p_low": "foot", "p_up": "foot", "lake": "narrows"}


# ------------------------------------------------------------------ on the real bundle

def _bundle() -> Path:
    import os

    from pipeline.deliver.bundle import read as R
    return Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)


def _stem(db, item_id):
    """`[(sid, shape)]` of a water's on-stem sections, upstream end first, and its sections' panels."""
    rows = db.execute(
        "SELECT sp.sid, sp.shape FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_span sp ON sp.sid = s.sid WHERE i.item_id = ? AND sp.off_stem = 0 "
        "ORDER BY sp.lo_m DESC", (item_id,)).fetchall()
    panels = {sid: (pid, area) for sid, pid, area in db.execute(
        f"SELECT sid, panel_id, area_km2 FROM section_panel WHERE sid IN ({','.join('?' * len(rows))})",
        [r[0] for r in rows])}
    return rows, panels


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id, want", [
    # Nicomen Slough: four stem polygons; 1265716 walks on through the unpanelled 59620 and polygon 1453408 to 59619.
    ("gnis:21105", {1453542: 59618, 1453408: 59619, 1265716: 59619, 1265712: 59621}),
    # Vedder River: the Vedder Canal polygon (4.7 km) takes the line at the mouth.
    ("gnis:3062", {1957095: 313837}),
])
def test_nicomen_and_vedder_polygons_take_their_lines_answer_from_the_bundle(item_id, want):
    """The real stems — sections, shapes and line panels read from the bundle — chained by measure and run through
    the same pass the bundle build runs."""
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    rows, panels = _stem(db, item_id)
    nodes = [poly(str(sid)) if shape == 1 else N(str(sid), 100) for sid, shape in rows]
    graph = g(nodes, [E(str(a), str(b)) for (a, _), (b, _) in zip(rows, rows[1:])])
    water_of = {str(sid): item_id for sid, _ in rows}
    # Only the LINES' panels go in — a rebuilt bundle also has the polygons', which is what is being derived here.
    panelled = {str(sid): panels[sid] for sid, shape in rows if shape == 0 and sid in panels}
    got = stream_polygons(graph, water_of, {item_id: "stream"}, panelled)
    assert got == {str(p): str(line) for p, line in want.items()}
    assert all(sid in panels for sid in want.values())        # each line it takes does have an answer


# ------------------------------------------------------------------ the bundle invariants
#
# WHAT A REBUILT BUNDLE MUST SHOW, stated over the bundle's own tables so it can be checked without the graph. The first
# draft asked "does the water have ANY panelled line?" — too broad: run on the REAL graph (2026-10-22, the pass over the
# live section_panel), 38 of those 45 polygons take a line's answer and 7 cannot, because every line of their water
# near them is unanswered too (Nation River 4 and West Road (Blackwater) 2 sit above lakes of other waters the walk may
# not cross; Honna 1 above an unanswered line). Those are honestly unmeasured, so a correct rebuild failed that check.
# The rule is local: a stem polygon TOUCHING an answered stem line of its own water has an answer.

_STREAM_POLY = ("FROM item i JOIN item_section s ON s.ord = i.ord JOIN section_span sp ON sp.sid = s.sid "
                "WHERE i.kind = 'stream' AND sp.shape = 1")


def polygons_beside_an_answer_without_one(db) -> list[tuple[str, int]]:
    """`(water, polygon sid)` for each stem polygon of a stream item with no panel while a stem LINE of the same water
    that touches it (end to end by measure) has one."""
    return db.execute(
        f"SELECT i.item_id, s.sid {_STREAM_POLY} AND sp.off_stem = 0 "
        "AND s.sid NOT IN (SELECT sid FROM section_panel) "
        "AND EXISTS (SELECT 1 FROM item_section s2 JOIN section_span l ON l.sid = s2.sid "
        "            JOIN section_panel p ON p.sid = l.sid "
        "            WHERE s2.ord = i.ord AND l.shape = 0 AND l.off_stem = 0 "
        "              AND (l.hi_m = sp.lo_m OR l.lo_m = sp.hi_m)) "
        "ORDER BY i.item_id, s.sid").fetchall()


def polygons_with_another_water_s_answer(db) -> list[tuple[str, int]]:
    """`(water, polygon sid)` for each stream-item polygon whose (panel, target area) is no line of its OWN water's —
    the pass must never hand a polygon a neighbour's answer."""
    return db.execute(
        "SELECT i.item_id, s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_span sp ON sp.sid = s.sid JOIN section_panel pp ON pp.sid = s.sid "
        "WHERE i.kind = 'stream' AND sp.shape = 1 "
        "AND NOT EXISTS (SELECT 1 FROM item_section s2 JOIN section_span l ON l.sid = s2.sid "
        "                JOIN section_panel p ON p.sid = l.sid "
        "                WHERE s2.ord = i.ord AND l.shape = 0 "
        "                  AND p.panel_id = pp.panel_id AND p.area_km2 IS pp.area_km2) "
        "ORDER BY i.item_id, s.sid").fetchall()


def standing_water_with_a_flow_answer(db) -> list[tuple[str, int]]:
    """`(water, sid)` for each section of a lake or wetland item that carries a donor panel. A lake's answer is
    `lake_gauge`; none has had a panel (0 of 7,675 sections on the live bundle), and this pass must not give one."""
    return db.execute(
        "SELECT i.item_id, s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_panel p ON p.sid = s.sid WHERE i.kind IS NOT 'stream' ORDER BY i.item_id, s.sid").fetchall()


def _toy_bundle(rows, panels):
    """An in-memory bundle with just the tables the invariants read. `rows`: (item, kind, sid, lo_m, hi_m, shape)."""
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT, kind TEXT);"
                     "CREATE TABLE item_section (ord INTEGER, sid INTEGER);"
                     "CREATE TABLE section_span (sid INTEGER PRIMARY KEY, lo_m INTEGER, hi_m INTEGER, "
                     "  off_stem INTEGER NOT NULL DEFAULT 0, shape INTEGER NOT NULL DEFAULT 0);"
                     "CREATE TABLE section_panel (sid INTEGER PRIMARY KEY, panel_id INTEGER, area_km2 REAL);")
    ords: dict[str, int] = {}
    for item, kind, sid, lo, hi, shape in rows:
        if item not in ords:
            ords[item] = len(ords)
            db.execute("INSERT INTO item VALUES (?,?,?)", (ords[item], item, kind))
        db.execute("INSERT INTO item_section VALUES (?,?)", (ords[item], sid))
        db.execute("INSERT INTO section_span VALUES (?,?,?,0,?)", (sid, lo, hi, shape))
    db.executemany("INSERT INTO section_panel VALUES (?,?,?)", [(s, p, a) for s, (p, a) in panels.items()])
    return db


# Nicomen-like: line 1 (answered) · polygon 2 · line 3 (answered).  Nation-like: answered line 10 far below, then
# unanswered line 11 · polygon 12 · unanswered line 13 (a lake of another water broke the walk in between).
_TOY = [(SLOUGH, "stream", 1, 0, 100, 0), (SLOUGH, "stream", 2, 100, 200, 1), (SLOUGH, "stream", 3, 200, 300, 0),
        (RIVER, "stream", 10, 0, 50, 0), (RIVER, "stream", 11, 900, 1000, 0), (RIVER, "stream", 12, 1000, 1010, 1),
        (RIVER, "stream", 13, 1010, 2000, 0), (LAKE, "lake", 20, 0, 0, 0)]
_TOY_LINES = {1: (7, 1.5), 3: (8, 2.0), 10: (9, 30.0)}


def test_the_invariants_read_a_toy_bundle_as_intended():
    """Whole values. Today's state (no polygon answered) names the Nicomen-like polygon and NOT the Nation-like one —
    its neighbours are unanswered too, and that is the honest answer, not a missed one."""
    before = _toy_bundle(_TOY, _TOY_LINES)
    assert polygons_beside_an_answer_without_one(before) == [(SLOUGH, 2)]
    assert polygons_with_another_water_s_answer(before) == []
    assert standing_water_with_a_flow_answer(before) == []

    after = _toy_bundle(_TOY, {**_TOY_LINES, 2: (7, 1.5)})
    assert polygons_beside_an_answer_without_one(after) == []
    assert polygons_with_another_water_s_answer(after) == []

    # A polygon wearing the river's answer, a slough line's panel at another target area, a lake given a panel: named.
    wrong = _toy_bundle(_TOY, {**_TOY_LINES, 2: (9, 30.0), 12: (7, 9.9), 20: (9, 30.0)})
    assert polygons_with_another_water_s_answer(wrong) == [(RIVER, 12), (SLOUGH, 2)]
    assert standing_water_with_a_flow_answer(wrong) == [(LAKE, 20)]


@pytest.mark.needs_bundle
def test_every_stream_polygon_beside_an_answered_line_of_its_water_has_an_answer():
    """On a bundle built before this ruling, 22 stem polygons fail it (Nicomen's four, the Vedder Canal …). Checked
    against the real graph's output before any rebuild: 0 after (38 polygons answered, every one with its own water's
    pair, no lake touched)."""
    from pipeline.tests.conftest import predates
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    missing = polygons_beside_an_answer_without_one(db)
    if missing and not db.execute(
            "SELECT 1 FROM section_panel p JOIN section_span sp ON sp.sid = p.sid WHERE sp.shape = 1").fetchone():
        predates(f"{_bundle()} predates 'stream polygons take their water's answer' ({len(missing)} polygons "
                 "beside an answered line without one) — rerun pipeline.deliver")
    assert missing == []


@pytest.mark.needs_bundle
def test_no_stream_polygon_wears_another_water_s_answer_and_no_lake_wears_a_panel():
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    assert polygons_with_another_water_s_answer(db) == []
    assert standing_water_with_a_flow_answer(db) == []
