"""T169 — one gauge panel for one side-channel water. OWNER 2026-10-23 (ticket T169):

"from my point of view a predicted section flows into unpredicted then back into predicted without reason for middle
section being unpredicted?" — NO GAP WITHOUT A REASON, and NARROW: only side-channel systems move; ordinary streams,
tributaries and lakes are untouched.

Two decisions, one function each, both asking `side_channel_waters` (the one map of which water is a side channel of
which river):
  - `water_panel`: every section of a side-channel WATER (lines of every edge type, its polygons) takes ONE panel —
    its own gauges leading, then the river's members (Q84 (a)).
  - `braid_connectors`: an unnamed run of lines between one river's side-channel pieces takes their answer.
`branch_points` asks the same map which water at a branch point is the side channel.

Fixtures: area = magnitude (MODEL). A side channel carries its river's watershed code; a tributary's code extends it.
"""

from __future__ import annotations

import inspect
import sqlite3
import textwrap

import pytest

import pipeline.atlas.gauges.build_panels as bp
from pipeline.atlas.gauges.build_panels import branch_points, build, side_channel_waters
from pipeline.tests.test_build_panels import AREA, MODEL, E, N, g

RIVER, SLOUGH, CREEK, OTHER, LAKE = "gnis:river", "gnis:slough", "gnis:creek", "gnis:other", "wbk:lake"
KINDS = {RIVER: "stream", SLOUGH: "stream", CREEK: "stream", OTHER: "stream", LAKE: "lake"}
CODE = "100-000000"
TRIB = CODE + "-400000"
TRIB2 = CODE + "-500000"
LEVEL, FLOW, BOTH = {"level"}, {"discharge"}, {"discharge", "level"}


def poly(node_id: str, mag: int, wsc: str = CODE) -> N:
    """A polygon section: a `lake:<wbk>` node, no blue-line key, sized by the lines under it."""
    return N(node_id, stream_magnitude=mag, kind="lake", blk="", length_m=0.0, wsc=wsc)


def answers(panels) -> dict[str, tuple[tuple[tuple, ...], float | None]]:
    """`section -> ((station, role, same_river, lead) per member, target area)` — the whole answer by value."""
    return {sec: (tuple(sorted((m.station, m.role, m.same_river, m.lead) for m in panels.members[pid])), area)
            for sec, (pid, area) in panels.by_section.items()}


SLOUGH_SECTIONS = ("c_head", "p1", "l1", "p2", "c_mid", "p3", "l2", "p4", "foot")


def _nicomen():
    """Nicomen Slough as the real graph draws it (2026-10-23), nine sections, upstream first:

        r_up ─▶ c_head ─▶ p1 ─▶ l1 ─▶ p2 ─▶ c_mid ─▶ p3 ─▶ l2 ─▶ p4 ─▶ foot ─▶ r_down
          └────────────────────────▶ r_mid ─────────────────────────────────────┘
                                                        creek ─▶ foot

    c_head, c_mid: 1410 CONNECTORS (59622, 59620) — never adopted by `mainstem_of`. l1, l2, foot: 1100/1350 side-channel
    lines (59621, 59619, 59618). p1..p4: the slough's polygons (1265712, 1265716, 1453408, 1453542). The river's gauge H
    (level + discharge) on r_up; the slough's own level gauge N on its foot (08MH085/086, 1960-64); a creek's flow gauge C
    on the creek that joins the foot (Norrish Creek).
    """
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_mid", 500, blk="R", wsc=CODE), N("r_down", 520, blk="R", wsc=CODE),
             N("c_head", 1, blk="S", wsc=CODE, edge_types=("1410",)), poly("p1", 1),
             N("l1", 10, blk="S", wsc=CODE, edge_types=("1100", "1350")), poly("p2", 11),
             N("c_mid", 11, blk="S", wsc=CODE, edge_types=("1410",), length_m=57.0), poly("p3", 14),
             N("l2", 14, blk="S", wsc=CODE, edge_types=("1100",)), poly("p4", 14),
             N("foot", 300, blk="S", wsc=CODE, edge_types=("1100", "1350")),
             N("creek", 250, blk="C", wsc=TRIB)]
    chain = ["r_up", *SLOUGH_SECTIONS, "r_down"]
    edges = [E(a, b) for a, b in zip(chain, chain[1:])]
    edges += [E("r_up", "r_mid"), E("r_mid", "r_down"), E("creek", "foot")]
    water = {"r_up": RIVER, "r_mid": RIVER, "r_down": RIVER, "creek": CREEK, **{s: SLOUGH for s in SLOUGH_SECTIONS}}
    donors = [("H", "r_up", 40, False), ("N", "foot", 40, False), ("C", "creek", 40, False)]
    return g(nodes, edges), water, donors, {"H": BOTH, "N": LEVEL, "C": FLOW}


def _run(fixture, **patch):
    graph, water, donors, meas = fixture
    real = {k: getattr(bp, k) for k in patch}
    for k, v in patch.items():
        setattr(bp, k, v)
    try:
        return answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    finally:
        for k, v in real.items():
            setattr(bp, k, v)


#: Before T169: no side-channel water, no connector — each line adopted on its own, the rest walked.
BEFORE = dict(side_channel_waters=lambda *a, **k: {}, braid_connectors=lambda *a, **k: {})


def test_the_slough_is_a_side_channel_water_of_the_river_below():
    graph, water, _d, _m = _nicomen()
    adopted = bp.mainstem_of(graph, AREA)
    assert adopted == {"l1": "r_up", "l2": "r_down", "foot": "r_down"}
    assert side_channel_waters(graph, AREA, water, KINDS, adopted) == {SLOUGH: "r_down"}


def test_nicomen_all_nine_sections_take_one_panel():
    """WHOLE VALUE. Every section of the slough — connectors, polygons, side-channel lines — has the same panel and the
    same target area: its own gauge N leading (Level), the river's answer at its mouth after it (H leading Flow, the
    creek C that reached the river through the slough). N is the member its own section has (in it: "up", same
    river), wherever along the slough it led."""
    got = _run(_nicomen())
    one = ((("C", "up", False, 0), ("H", "up", True, 2), ("N", "up", True, 1)), 520.0)
    assert {s: got.get(s) for s in SLOUGH_SECTIONS} == {s: one for s in SLOUGH_SECTIONS}


def test_before_t169_the_slough_had_three_answers():
    """The bug, in miniature: the adopted lines took the river (and its own gauge), the connectors and polygons walked
    on their own and took the slough's level gauge alone, or a creek gauge matched on size."""
    got = _run(_nicomen(), **BEFORE)
    assert len({got.get(s) for s in SLOUGH_SECTIONS}) >= 3
    assert "H" not in {m[0] for m in got["c_mid"][0]}


def test_nothing_outside_the_side_channel_water_moves():
    """THE NARROW PIN. Every section whose answer differs from before T169 is a section of the side-channel water —
    the river and the creek that drains into the slough are untouched."""
    fx = _nicomen()
    before, after = _run(fx, **BEFORE), _run(fx)
    moved = {s for s in set(before) | set(after) if before.get(s) != after.get(s)}
    assert moved and moved <= set(SLOUGH_SECTIONS)
    assert {s: after[s] for s in ("r_up", "r_mid", "r_down", "creek")} == \
        {s: before[s] for s in ("r_up", "r_mid", "r_down", "creek")}


# ------------------------------------------------------------------ the unnamed braid of a big river

BRAID = ("b1", "bc", "b2")


def _braid():
    """An unnamed braid of the river (the Fraser at Agassiz): two unowned FWA side-channel lines b1, b2 (adopted) and a
    1410 connector bc between them, which `mainstem_of` never adopts. A creek gauge F of the braid's arithmetic size
    joins the river above (the Herrling Island artefact). An unowned tributary stub t (its own code) flows into bc —
    a creek, not the braid — and a true lake drains into the river.

        r_up ─▶ r_mid ─▶ r_down              F on far ─▶ r_up
          └─▶ b1 ─▶ bc ─▶ b2 ─┘               t ─▶ bc        lake ─▶ r_mid
    """
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_mid", 500, blk="R", wsc=CODE), N("r_down", 520, blk="R", wsc=CODE),
             N("b1", 2, blk="B1", wsc=CODE, edge_types=("1100",)), N("bc", 2, blk="B2", wsc=CODE, edge_types=("1410",)),
             N("b2", 2, blk="B3", wsc=CODE, edge_types=("1100",)), N("t", 2, blk="T", wsc=TRIB),
             N("far", 2, blk="F", wsc=TRIB2), poly("lake", 400, wsc=CODE + "-600000")]
    edges = [E("r_up", "r_mid"), E("r_mid", "r_down"), E("r_up", "b1"), E("b1", "bc"), E("bc", "b2"), E("b2", "r_down"),
             E("far", "r_up"), E("t", "bc"), E("lake", "r_mid")]
    water = {"r_up": RIVER, "r_mid": RIVER, "r_down": RIVER, "far": OTHER, "lake": LAKE}
    donors = [("H", "r_up", 40, False), ("F", "far", 40, False), ("L", "lake", 40, False)]
    return g(nodes, edges), water, donors, {"H": BOTH, "F": FLOW, "L": LEVEL}


def test_an_unnamed_connector_between_one_river_s_side_channels_takes_their_answer():
    """WHOLE VALUE. bc takes b2's answer (downstream first) — the river's, as b1 and b2 have by adoption — instead of
    the creek gauge F its own size matched. Nothing else moves: the tributary stub t flowing into it, the river, the
    lake."""
    fx = _braid()
    before, after = _run(fx, **BEFORE), _run(fx)
    assert after["bc"] == after["b2"] == before["b2"]
    assert "F" in {m[0] for m in before["bc"][0]}
    assert {s for s in set(before) | set(after) if before.get(s) != after.get(s)} == {"bc"}


def test_a_connector_from_the_river_into_its_side_channel_takes_the_side_channel_s_answer():
    """The run's ends are the river's own section (r_up) and an adopted line (b1): one river, a side-channel piece
    among them — it takes b1's answer."""
    graph, water, donors, meas = _braid()
    graph.nodes["b1"] = N("b1", 2, blk="B1", wsc=CODE, edge_types=("1410",))      # b1 a connector too: r_up ─▶ b1 ─▶ bc
    adopted = bp.mainstem_of(graph, AREA)
    got = bp.braid_connectors(graph, AREA, water, adopted, {}, {"b2": (0, 520.0), "r_up": (1, 500.0)})
    assert got == {"b1": "b2", "bc": "b2"}


def test_a_run_between_two_rivers_or_ending_in_a_lake_is_not_a_connector():
    graph, water, _d, _m = _braid()
    adopted = bp.mainstem_of(graph, AREA)
    # b1 adopted onto one river, b2 onto ANOTHER: two rivers at the ends, no single answer to take
    other = {**water, "r_down": OTHER}
    assert bp.braid_connectors(graph, AREA, other, {"b1": "r_up", "b2": "r_down"}, {}, {"b2": (0, 520.0)}) == {}
    # an unowned lake polygon on the braid's code below the connector: not one river's piece
    graph.nodes["b2"] = poly("b2", 2)
    assert bp.braid_connectors(graph, AREA, water, bp.mainstem_of(graph, AREA), {}, {}) == {}


def test_a_connector_as_big_as_the_river_is_the_river():
    """An unowned piece at least as big as the river it would adopt (a main-stem stub FWA left unnamed) is not a braid
    piece."""
    graph, water, _d, _m = _braid()
    graph.nodes["bc"] = N("bc", 600, blk="B2", wsc=CODE, edge_types=("1410",))
    assert bp.braid_connectors(graph, AREA, water, bp.mainstem_of(graph, AREA), {}, {"b2": (0, 520.0)}) == {}


# ------------------------------------------------------------------ a slough with its own LIVE gauge

def test_a_slough_s_own_gauge_leads_the_whole_slough_and_never_the_river():
    """N (level) and Q (level + discharge) both stand on the slough — Q on the head connector, N on the foot. Both lead
    every section of it, nearest-ranked first wherever they led in the walk; the river's lead H follows. Neither
    answers the river: Q44 is untouched (Level never crosses; Flow only at a large share)."""
    graph, water, donors, meas = _nicomen()
    donors = donors + [("Q", "c_head", 40, False)]
    meas = {**meas, "Q": BOTH}
    got = _run((graph, water, donors, meas))
    leads = {s: tuple(sorted((m[0], m[3]) for m in got[s][0] if m[3])) for s in SLOUGH_SECTIONS}
    assert set(leads.values()) == {(("H", 3), ("N", 1), ("Q", 2))}
    for river in ("r_up", "r_mid", "r_down"):
        assert not {"N", "Q"} & {m[0] for m in got[river][0]}


def test_a_silent_river_leaves_the_slough_its_own_gauges_alone():
    """No river gauge: the slough's one panel is its own gauge N, on every section — the connector included."""
    graph, water, donors, meas = _nicomen()
    got = _run((graph, water, [d for d in donors if d[0] == "N"], {"N": LEVEL}))
    assert {s: got.get(s) for s in SLOUGH_SECTIONS} == {s: ((("N", "up", True, 1),), 520.0) for s in SLOUGH_SECTIONS}


def test_a_side_channel_with_no_gauge_of_its_own_takes_the_river_s_answer_or_none():
    graph, water, donors, meas = _nicomen()
    got = _run((graph, water, [d for d in donors if d[0] == "C"], {"C": FLOW}))
    # C reaches the river below the slough's mouth, and the slough takes the river's answer
    assert set(got) == {"creek", "r_down", *SLOUGH_SECTIONS} and {got[s][0] for s in SLOUGH_SECTIONS} == {got["r_down"][0]}
    got = _run((graph, water, [], {}))
    assert not set(SLOUGH_SECTIONS) & set(got)


# ------------------------------------------------------------------ what is NOT a side-channel water

def test_the_river_s_own_side_pieces_and_a_continued_system_are_the_river():
    graph, water, _d, _m = _nicomen()
    adopted = bp.mainstem_of(graph, AREA)
    river_own = {**water, **{s: RIVER for s in SLOUGH_SECTIONS}}
    assert side_channel_waters(graph, AREA, river_own, KINDS, adopted) == {}
    assert side_channel_waters(graph, AREA, water, KINDS, adopted, continues=[(SLOUGH, RIVER)]) == {}
    assert side_channel_waters(graph, AREA, water, {**KINDS, SLOUGH: "lake"}, adopted) == {}


def test_a_backwater_coded_like_a_tributary_is_not_one():
    """Nelson Slough: its own code under the river's, no FWA side-channel line — no adoption, so its answer stays the
    walk's (Q44, T101: Flow none, Level from its mouth)."""
    graph, water, _d, _m = _nicomen()
    for s in SLOUGH_SECTIONS:
        n = graph.nodes[s]
        graph.nodes[s] = N(s, n.stream_magnitude, kind=n.kind, blk=n.blk, wsc=TRIB + "-100000", length_m=n.length_m)
    assert side_channel_waters(graph, AREA, water, KINDS, bp.mainstem_of(graph, AREA)) == {}


# ------------------------------------------------------------------ the walk's cut asks the same map

def test_branch_points_name_the_adoption_s_channel_even_when_it_is_the_larger_there():
    """Two stream waters on one code, the river's piece beside the split SMALLER than the channel's (FWA stubs): size
    alone would call the river the side channel. `side_channel_waters` says which is which, and the walk's cut asks it."""
    nodes = [N("r_up", 5, blk="R", wsc=CODE), N("r_on", 5, blk="R", wsc=CODE), N("s", 50, blk="S", wsc=CODE)]
    graph = g(nodes, [E("r_up", "r_on"), E("r_up", "s")])
    water = {"r_up": RIVER, "r_on": RIVER, "s": SLOUGH}
    assert branch_points(graph, AREA, water, KINDS) == {("r_up", "s"): (0.1, RIVER)}
    assert branch_points(graph, AREA, water, KINDS, channels={SLOUGH: RIVER}) == {("r_up", "s"): (0.1, SLOUGH)}


# ------------------------------------------------------------------ mutation pins

def _mutant(fn, old: str, new: str):
    src = textwrap.dedent(inspect.getsource(fn))
    assert src.count(old) == 1, f"{old!r} moved in {fn.__name__}; update this pin to match"
    ns = dict(vars(bp))
    exec(compile(src.replace(old, new), bp.__file__, "exec"), ns)
    return ns[fn.__name__]


def test_mutation_pin_one_panel_per_water():
    """Without the water pass (no side-channel water), Nicomen's nine sections fall back to three answers."""
    got = _run(_nicomen(), side_channel_waters=lambda *a, **k: {})
    assert len({got.get(s) for s in SLOUGH_SECTIONS}) > 1


def test_mutation_pin_the_water_s_own_gauges_lead():
    """`water_panel` dropping its own gauges: N is gone from the slough."""
    no_own = _mutant(bp.water_panel, "if m.lead and", "if False and")
    got = _run(_nicomen(), water_panel=no_own)
    assert all("N" not in {m[0] for m in got[s][0]} for s in SLOUGH_SECTIONS)


def test_mutation_pin_the_river_section_is_the_largest():
    """The smallest adopted river section instead (r_up, above the head): the panel changes — the creek that reached
    the river through the slough is no longer in it."""
    smallest = _mutant(side_channel_waters, "max(ps, key=", "min(ps, key=")
    got = _run(_nicomen(), side_channel_waters=smallest)
    assert "C" not in {m[0] for m in got["foot"][0]}


def test_mutation_pin_connectors():
    """Without `braid_connectors`, bc keeps its own walk's creek gauge; with every end allowed (no single-river test),
    the run between two rivers is adopted."""
    fx = _braid()
    got = _run(fx, braid_connectors=lambda *a, **k: {})
    assert "F" in {m[0] for m in got["bc"][0]}
    graph, water, _d, _m = fx
    any_end = _mutant(bp.braid_connectors, "len({v[0] for v in ends.values()}) != 1", "False")
    other = {**water, "r_down": OTHER}
    assert any_end(graph, AREA, other, {"b1": "r_up", "b2": "r_down"}, {}, {"b2": (0, 520.0)}) != {}


def test_mutation_pin_the_size_guard():
    graph, water, _d, _m = _braid()
    graph.nodes["bc"] = N("bc", 600, blk="B2", wsc=CODE, edge_types=("1410",))
    no_guard = _mutant(bp.braid_connectors, ">= river_area(pick) for x in run", "> 1e18 for x in run")
    assert no_guard(graph, AREA, water, bp.mainstem_of(graph, AREA), {}, {"b2": (0, 520.0)}) == {"bc": "b2"}


def test_mutation_pin_branch_points_ask_the_adoption():
    nodes = [N("r_up", 5, blk="R", wsc=CODE), N("r_on", 5, blk="R", wsc=CODE), N("s", 50, blk="S", wsc=CODE)]
    graph = g(nodes, [E("r_up", "r_on"), E("r_up", "s")])
    water = {"r_up": RIVER, "r_on": RIVER, "s": SLOUGH}
    deaf = _mutant(branch_points, "(channels or {}).get(wb) == wa", "False")
    assert deaf(graph, AREA, water, KINDS, channels={SLOUGH: RIVER}) == {("r_up", "s"): (0.1, RIVER)}


def test_one_map_decides_the_side_channel():
    """ONE DECISION. `side_channel_waters` is computed once in `build` and handed to the walk (as `channels`), the water
    pass and the connector pass; nothing else names a side-channel water."""
    src = inspect.getsource(bp.build)
    assert src.count("side_channel_waters(") == 1
    assert "channels=" in src and "water_panel(" in src and "braid_connectors(" in src


# ------------------------------------------------------------------ on the real bundle

@pytest.mark.needs_bundle
@pytest.mark.parametrize("item_id, sections", [
    ("gnis:21105", 9),          # Nicomen Slough: 3 side-channel lines, 2 connectors (59620, 59622), 4 polygons
    ("blk:355992187", 7),       # Herrling Island Side Channel (its own 08MF047 leading all seven)
    ("gnis:13499", 7),          # Maria Slough
])
def test_a_side_channel_water_reads_one_panel_in_the_bundle(item_id, sections):
    """Measured on the real graph (2026-10-23, T169): every section of these waters takes one panel. A bundle built
    before T169 has three on Nicomen (Fraser by adoption, its own level gauges alone on 59620/59622/1265716)."""
    from pipeline.tests.conftest import predates
    from pipeline.tests.test_stream_polygon_panels import _bundle
    db = sqlite3.connect(f"file:{_bundle()}?mode=ro", uri=True)
    got = db.execute("SELECT s.sid, p.panel_id, p.area_km2 FROM item i JOIN item_section s ON s.ord = i.ord "
                     "LEFT JOIN section_panel p ON p.sid = s.sid WHERE i.item_id = ?", (item_id,)).fetchall()
    assert len(got) == sections
    panels = {(pid, area) for _sid, pid, area in got}
    if len(panels) > 1:
        predates(f"{_bundle()} predates T169 (one panel per side-channel water): {item_id} has {len(panels)} "
                 "panels — rerun pipeline.deliver")
    assert len(panels) == 1 and None not in next(iter(panels))
