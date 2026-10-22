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
    it is a line, and why the walk cannot reach it is the separate, postponed question (T54)."""
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


@pytest.mark.needs_bundle
def test_every_stream_polygon_beside_a_panelled_line_of_its_water_has_a_panel():
    """The bundle-level invariant: a stream item's polygon whose water has a panelled line has a panel itself. On a
    bundle built before this ruling, 45 polygons on 20 waters (Nicomen, Vedder, Gravel Slough …) fail it."""
    from pipeline.tests.conftest import predates
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    missing = db.execute(
        "SELECT i.item_id, s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
        "JOIN section_span sp ON sp.sid = s.sid "
        "WHERE i.kind = 'stream' AND sp.shape = 1 "
        "AND s.sid NOT IN (SELECT sid FROM section_panel) "
        "AND EXISTS (SELECT 1 FROM item_section s2 JOIN section_span sp2 ON sp2.sid = s2.sid "
        "            JOIN section_panel p2 ON p2.sid = s2.sid "
        "            WHERE s2.ord = i.ord AND sp2.shape = 0) "
        "ORDER BY i.item_id, s.sid").fetchall()
    if missing and not db.execute(
            "SELECT 1 FROM section_panel p JOIN section_span sp ON sp.sid = p.sid WHERE sp.shape = 1").fetchone():
        predates(f"{_bundle()} predates 'stream polygons take their water's answer' ({len(missing)} polygons "
                 "without a panel) — rerun pipeline.deliver")
    assert missing == []
