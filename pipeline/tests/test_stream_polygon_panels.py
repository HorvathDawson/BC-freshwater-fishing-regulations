"""A stream water's polygons take that water's flow answer — OWNER 2026-10-22 (late), "sloughs and canals look
like streams in Conditions" (handoff/PLAN/app-integration/decisions.md).

The colouring follows the WATER'S KIND, never the geometry type. Nicomen Slough is a stream item with four polygon
sections on its stem; Vedder River carries 4.7 km of Vedder Canal as one polygon section. The panel walk sizes a
reach by its stream magnitude, which a polygon does not have, and the old code read the graph's `NodeKind` (a polygon
node is `lake:<wbk>`) as if it were the water's kind — so every one of the 146 stream-item polygons had no panel and
drew grey between coloured lines of the same water.

SINCE "a polygon-only slough or canal gets gauges like a stream" (same file) the WALK reads the water's kind
(`build_panels.flowing`, pinned by test_polygon_walk.py), so a sized polygon answers on its own; `stream_polygons` is the
fallback for one the walk leaves unanswered beside an answered line of its water. The fixtures here keep the polygon
UNSIZED (`stream_magnitude=None`), which no walk can answer, so they exercise that fallback alone.
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
    """An UNSIZED polygon section: a `lake:<wbk>` node with no stream magnitude — the case the walk cannot answer.
    (The real graph sizes all 145 stream-item polygons from the lines beneath them; see test_polygon_walk.py.)"""
    return N(node_id, stream_magnitude=None, kind="lake")


def _nicomen():
    """Nicomen Slough in miniature, flowing left to right (upper → lower):

        top ─▶ p_up ─▶ mid ─▶ p_low ─▶ foot          (all the slough; gauges on `top` and `foot`)
        lake ─▶ narrows ─▶ outlet                    (a TRUE lake item owning a line of its own, then a river)

    `mid` sits between two UNSIZED polygons and is reached by neither gauge, as 59620 was on the real slough before
    the walk went through polygons (T101; since then the slough's own gauges reach it — `_far_slough`). `narrows` is a
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
    """The FALLBACK keeps to the polygon's own water: it never copies a neighbour's answer. (A sized canal is reached by
    the walk itself, as a line would be — test_polygon_walk.py; an unsized one has nothing to be weighed by.)"""
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


# ------------------------------------------------------------------ a panel with no lead still answers
#
# T101 after deliver (2026-10-23): Nicomen Slough's 57 m connector 59620 (FWA 1410, not a side channel, so never
# adopted by `mainstem_of`) is now reached by the slough's own gauges 08MH085/086 through the walked polygons, 17 km
# from the section they stand on — past the own-gauge "far" limit, so they are members with `lead` 0. Its polygon takes
# that answer. LEAD IS NOT "ANSWERS": `lead` only re-weights the votes (`panel.leader_of` / `lead_shares`, the app's
# `leaderOf` / `leadShares`); with no leader a panel answers from its members' usual weights. Whether a member is LIVE
# is read-time, from the feed (owner 2026-10-22, "chosen live") — the build never decides it. So the polygon pass takes
# the nearest line of its own water with ANY panel, and never walks past one to a line whose gauge leads.

def _far_slough():
    """`top ─▶ p_up ─▶ mid ─▶ p_low ─▶ foot`, all the slough. The gauge stands on `foot`, a 16 km section, so `p_low`
    and `mid` are its own water 16 km from it — too far to lead (`may_lead`, 10 km at this size). `p_low` is sized and
    walked; `p_up` is unsized, so the walk stops there and only the polygon pass can answer it."""
    nodes = [N("top", 100), poly("p_up"), N("mid", 100), N("p_low", 100, kind="lake"), N("foot", 100, length_m=16_000.0)]
    edges = [E("top", "p_up"), E("p_up", "mid"), E("mid", "p_low"), E("p_low", "foot")]
    water_of = {s: SLOUGH for s in ("top", "p_up", "mid", "p_low", "foot")}
    return g(nodes, edges), water_of, [("FOOT", "foot", 40, False)]


def _led(panels) -> dict[str, tuple[tuple[tuple[str, int], ...], float | None]]:
    """`section -> ((station, lead), …), target area)` — the whole colour input with each member's lead rank."""
    return {sec: (tuple((m.station, m.lead) for m in panels.members[pid]), area)
            for sec, (pid, area) in panels.by_section.items()}


def test_a_polygon_takes_its_own_line_whose_own_gauge_does_not_lead():
    """Whole value. `mid` answers from its own slough's gauge without a lead, and `p_up` takes `mid` — the line
    downstream of it — not `foot`, where the gauge leads 16 km further on. The owner's Q44 clause: "the side channel
    and its own polygons still take their own gauge"."""
    from pipeline.atlas.gauges.panel import lead_shares, leader_of

    graph, water_of, donors = _far_slough()
    panels = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    assert _led(panels) == {
        "foot": ((("FOOT", 1),), 100.0),
        "p_low": ((("FOOT", 0),), 100.0),
        "mid": ((("FOOT", 0),), 100.0),
        "p_up": ((("FOOT", 0),), 100.0),
    }
    assert panels.by_section["p_up"] == panels.by_section["mid"]
    # And a panel with no leader answers: the usual weights, normalised (the app's `leadShares` is the mirror).
    assert lead_shares([0.4], leader_of([0], [0.4])) == [1.0]


def test_mutation_pin_lead_never_decides_whether_a_line_answers_its_polygon():
    """MUTATION PIN: hand the polygon pass only the lines whose panel has a leading member ("no lead answers nothing")
    and `p_up` walks past its own line to `foot`'s led panel — the wrong answer the needs_bundle test caught on 59620."""
    src = textwrap.dedent(inspect.getsource(bp.build))
    call = "stream_polygons(graph, water_of, kind_of, by_section)"
    assert src.count(call) == 1, "the polygon pass's call moved; update this pin to match"
    led_only = ("stream_polygons(graph, water_of, kind_of, "
                "{s: v for s, v in by_section.items() if any(m.lead for m in members[v[0]])})")
    ns = dict(vars(bp))
    exec(compile(src.replace(call, led_only), bp.__file__, "exec"), ns)

    graph, water_of, donors = _far_slough()
    real = build(graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    mutant = ns["build"](graph, MODEL, donors, water_of=water_of, kind_of=KINDS)
    assert real.by_section["p_up"] == real.by_section["mid"]
    assert _led(mutant)["p_up"] == ((("FOOT", 1),), 100.0)
    assert mutant.by_section["p_up"] == mutant.by_section["foot"] != real.by_section["p_up"]


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
    # Nicomen Slough: four stem polygons, each the line just downstream. 1265716 takes the 57 m connector 59620 (FWA
    # 1410, not adopted by `mainstem_of`), which since T101 answers from the slough's own gauges 08MH085/086 — members
    # without a lead (17 km away), which still answer (`_far_slough`). Before T101 59620 was unpanelled and 1265716
    # walked on to 59619 (the Fraser, by adoption).
    ("gnis:21105", {1453542: 59618, 1453408: 59619, 1265716: 59620, 1265712: 59621}),
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


# RETIRED 2026-10-22 (late): "no stream polygon wears another water's answer". It held while the only way a polygon got
# a panel was copying its own line's. Since the walk reads the water's kind, a polygon answers like a line in its
# place — Seton Canal from Cayoosh Creek and the Seton system, Lewis Slough from its river's gauges — and its (panel,
# area) is its own, not any line's. What stays invariant is below: a polygon touching an answered line of its water has
# an answer, and no lake or wetland section has a panel.


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
    assert standing_water_with_a_flow_answer(before) == []

    # The polygon answered — by its line's pair or by a walk of its own (another panel, its own area): either holds.
    for own in ((7, 1.5), (9, 30.0)):
        after = _toy_bundle(_TOY, {**_TOY_LINES, 2: own})
        assert polygons_beside_an_answer_without_one(after) == []
        assert standing_water_with_a_flow_answer(after) == []

    # A lake given a panel: named.
    wrong = _toy_bundle(_TOY, {**_TOY_LINES, 2: (7, 1.5), 20: (9, 30.0)})
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
def test_no_lake_or_wetland_wears_a_panel():
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    assert standing_water_with_a_flow_answer(db) == []
