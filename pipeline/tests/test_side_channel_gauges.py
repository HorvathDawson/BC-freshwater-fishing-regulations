"""Q44 — a gauge on a side channel or slough, and the river it branched from. OWNER 2026-10-23
(handoff/PLAN/app-integration/decisions.md "OWNER 2026-10-23"; ticket T101):

"a gauge on a side channel or slough never answers Level for the river it branched from (level is local to the
backwater). It answers Flow for that river only when the side channel carries a large share of the river's flow;
otherwise no. The side channel and its own polygons still take their own gauge."

One decision, one function: `build_panels.across_branch`. The graph says where a branch point is
(`build_panels.branch_points`: the same watershed code on two waters, or a river flowing into a code nested under its
own), the walk carries what `across_branch` lets through, and an adopted channel keeps its own gauges
(`build_panels.water_panel`, T169: one panel for the whole side-channel water).

Fixtures: area = magnitude (MODEL). A side channel carries its river's watershed code; a tributary's code extends it.
"""

from __future__ import annotations

import inspect
import re
import textwrap
from pathlib import Path

import pipeline.gauges.consume.panels as bp
from pipeline.gauges.consume.panels import LARGE_SHARE, across_branch, branch_points, build
from pipeline.tests.test_build_panels import AREA, MODEL, E, N, g

RIVER, SLOUGH, CREEK, CANAL = "gnis:river", "gnis:slough", "gnis:creek", "gnis:canal"
KINDS = {RIVER: "stream", SLOUGH: "stream", CREEK: "stream", CANAL: "stream"}
CODE = "100-000000"                 # the river's watershed code — and its side channel's
TRIB = CODE + "-400000"             # a tributary's (or a backwater FWA codes like one)
LEVEL, FLOW, BOTH = {"level"}, {"discharge"}, {"discharge", "level"}


def answers(panels) -> dict[str, tuple[tuple[tuple, ...], float | None]]:
    """`section -> ((station, role, same_river, lead) per member, target area)` — the whole answer by value."""
    return {sec: (tuple(sorted((m.station, m.role, m.same_river, m.lead) for m in panels.members[pid])), area)
            for sec, (pid, area) in panels.by_section.items()}


# ------------------------------------------------------------------ Nicomen Slough in miniature

def _nicomen():
    """A slough off the river, rejoining it below; Nicomen's level gauge N on its foot; the river's gauge H above;
    a creek C draining into the slough. The slough's lines are FWA side channels on the river's code.

        r_up ──────────────▶ r_mid ──────────────▶ r_down          H (level + discharge) on r_up
          └─▶ s_head ─▶ s_foot ───────────────────────┘             N (level) on s_foot
        creek ─▶ s_head                                             C (discharge) on creek
    """
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_mid", 500, blk="R", wsc=CODE), N("r_down", 520, blk="R", wsc=CODE),
             N("s_head", 6, blk="S", wsc=CODE, edge_types=("1100",)),
             N("s_foot", 10, blk="S", wsc=CODE, edge_types=("1350",)),
             N("creek", 4, blk="C", wsc=TRIB)]
    edges = [E("r_up", "r_mid"), E("r_mid", "r_down"), E("r_up", "s_head"), E("s_head", "s_foot"),
             E("s_foot", "r_down"), E("creek", "s_head")]
    water = {"r_up": RIVER, "r_mid": RIVER, "r_down": RIVER, "s_head": SLOUGH, "s_foot": SLOUGH, "creek": CREEK}
    donors = [("H", "r_up", 40, False), ("N", "s_foot", 40, False), ("C", "creek", 40, False)]
    return g(nodes, edges), water, donors, {"H": BOTH, "N": LEVEL, "C": FLOW}


def test_the_branch_points_of_the_miniature_are_its_head_and_its_mouth():
    """Whole value: the two edges where the slough leaves and rejoins the river, the slough the smaller water, its share
    10/520 measured around each (the largest of each water within MAINSTEM_HOPS). The creek's mouth is a tributary's."""
    graph, water, _donors, _m = _nicomen()
    got = branch_points(graph, AREA, water, KINDS)
    assert got == {("r_up", "s_head"): (10 / 520, SLOUGH), ("s_foot", "r_down"): (10 / 520, SLOUGH)}


def test_a_slough_gauge_never_answers_the_river_and_keeps_answering_its_slough():
    """Whole value. N (level, on the slough) is nowhere on the river: not below the mouth, not above the head — and
    still speaks up its own slough's tributary, the creek, which is no branch point. The slough's lines are side
    channels adopted by the river below (`mainstem_of`), so the slough is a side-channel water and takes ONE panel —
    the river's answer with its own gauge N leading it (`water_panel`, T169), the river's own lead H ranked after. The creek drains through the slough into the
    river, as any tributary does, so C still speaks for the river below. H does not enter the slough."""
    graph, water, donors, meas = _nicomen()
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    river_up = ((("H", "up", True, 1),), 500.0)
    assert got == {
        "creek": ((("C", "up", True, 1), ("N", "down", False, 0)), 4.0),
        "r_up": river_up,
        "r_mid": river_up,
        "r_down": ((("C", "up", False, 0), ("H", "up", True, 1)), 520.0),
        # one side-channel water, one panel (T169): the river's at its mouth, its own gauge N leading (the river's lead
        # H follows as 2), N as the section it stands in has it
        "s_head": ((("C", "up", False, 0), ("H", "up", True, 2), ("N", "up", True, 1)), 520.0),
        "s_foot": ((("C", "up", False, 0), ("H", "up", True, 2), ("N", "up", True, 1)), 520.0),
    }


def test_before_q44_the_slough_gauge_answered_the_river():
    """The same graph with every branch point open (`across_branch` letting everything through): N is a member of the
    river above the head and below the mouth — the 71,668 Fraser sections of the held measurement, in miniature."""
    graph, water, donors, meas = _nicomen()
    real = bp.across_branch
    bp.across_branch = lambda share: bp.ALL_QUANTITIES
    try:
        got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    finally:
        bp.across_branch = real
    assert "N" in {m[0] for m in got["r_down"][0]} and "N" in {m[0] for m in got["r_up"][0]}


# ------------------------------------------------------------------ a large share: Flow, never Level

def _canal(gauge_measures: set[str]):
    """A canal that takes most of its river (Seton Canal off the Seton River: 0.97 on the real graph): river and canal
    on one code, the canal's gauge K on it. Share 400/500 = 0.8 >= LARGE_SHARE.

        r_up ─▶ r_down                 r_up 500, r_down 500
          └─▶ canal ─▶ r_down          canal 400, K on it
    """
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_down", 500, blk="R", wsc=CODE),
             N("canal", 400, blk="K", wsc=CODE)]
    edges = [E("r_up", "r_down"), E("r_up", "canal"), E("canal", "r_down")]
    water = {"r_up": RIVER, "r_down": RIVER, "canal": CANAL}
    return g(nodes, edges), water, [("K", "canal", 40, False)], {"K": set(gauge_measures)}


def test_a_large_share_channel_s_flow_gauge_answers_the_river():
    graph, water, donors, meas = _canal(FLOW)
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    assert got == {
        "canal": ((("K", "up", True, 1),), 400.0),
        "r_up": ((("K", "down", False, 0),), 500.0),
        "r_down": ((("K", "up", False, 0),), 500.0),
    }


def test_a_large_share_channel_s_level_gauge_does_not():
    graph, water, donors, meas = _canal(LEVEL)
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    assert got == {"canal": ((("K", "up", True, 1),), 400.0)}


def test_a_gauge_reading_both_stays_on_its_channel():
    """A member is a station and the app answers each quantity from every member reporting it — a row cannot say "Flow
    only" — so a level-and-discharge gauge on a large-share channel stays on it rather than bring its Level along."""
    graph, water, donors, meas = _canal(BOTH)
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    assert got == {"canal": ((("K", "up", True, 1),), 400.0)}


def test_the_threshold_is_the_rule_s_and_nothing_else():
    """Whole value of the rule over the shares that matter."""
    assert LARGE_SHARE == 0.5
    assert [sorted(across_branch(s)) for s in (0.0, 0.0032, 0.49, 0.5, 0.97, 1.0)] == [
        [], [], [], ["discharge"], ["discharge"], ["discharge"]]


# ------------------------------------------------------------------ Bell / Nelson: the river into a backwater

def _nelson():
    """Nelson Slough: FWA codes it like a tributary (its own code under the river's), but the river flows INTO it at
    its head. A creek gauge C far up the river matches the slough's arithmetic (both small).

        creek ─▶ r_up ─▶ r_down
                   └─▶ slough ─▶ r_down              C on creek (2), river 300, slough 2
    """
    nodes = [N("creek", 2, blk="C", wsc=CODE + "-100000"), N("r_up", 300, blk="R", wsc=CODE),
             N("r_down", 300, blk="R", wsc=CODE), N("slough", 2, blk="S", wsc=TRIB)]
    edges = [E("creek", "r_up"), E("r_up", "r_down"), E("r_up", "slough"), E("slough", "r_down")]
    water = {"creek": CREEK, "r_up": RIVER, "r_down": RIVER, "slough": SLOUGH}
    return g(nodes, edges), water, [("C", "creek", 40, False)], {"C": FLOW}


def test_a_creek_up_the_river_no_longer_speaks_for_a_backwater_through_its_head():
    graph, water, donors, meas = _nelson()
    assert branch_points(graph, AREA, water, KINDS) == {("r_up", "slough"): (2 / 300, SLOUGH)}
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    assert got == {
        "creek": ((("C", "up", True, 1),), 2.0),
        "r_up": ((("C", "up", False, 0),), 300.0),
        "r_down": ((("C", "up", False, 0),), 300.0),
    }


def test_a_split_cut_into_a_stub_is_still_a_branch_point():
    """FWA cuts a split into stubs: the Fraser spills into Nelson Slough through a 1.4 km² Fraser piece whose ONLY way on
    is the slough (real graph, 355996184:214). The river beside is one piece further up — still the slough's head, and
    the creek far up the river still does not speak for the backwater. (A head looked for on the edge's own end alone
    read this as a new name and let the nine creek gauges back into Bell and Nelson Slough.)

        creek ─▶ r_up ─▶ r_down
                   └─▶ r_stub ─▶ slough ─▶ r_down      r_stub: the river's water and code, area 2
    """
    nodes = [N("creek", 2, blk="C", wsc=CODE + "-100000"), N("r_up", 300, blk="R", wsc=CODE),
             N("r_down", 300, blk="R", wsc=CODE), N("r_stub", 2, blk="R", wsc=CODE), N("slough", 2, blk="S", wsc=TRIB)]
    edges = [E("creek", "r_up"), E("r_up", "r_down"), E("r_up", "r_stub"), E("r_stub", "slough"), E("slough", "r_down")]
    water = {"creek": CREEK, "r_up": RIVER, "r_down": RIVER, "r_stub": RIVER, "slough": SLOUGH}
    graph = g(nodes, edges)
    assert branch_points(graph, AREA, water, KINDS) == {("r_stub", "slough"): (2 / 300, SLOUGH)}
    got = answers(build(graph, MODEL, [("C", "creek", 40, False)], water_of=water, kind_of=KINDS,
                        measures={"C": FLOW}))
    assert "slough" not in got


def test_a_braid_above_a_new_name_does_not_make_it_a_branch():
    """The upper water braids just above the name change and both pieces rejoin before it: no other way on, still one
    water taking another name.

        u0 ─▶ u1 ─▶ upper ─▶ lower
         └─▶ braid ──┘
    """
    assert branch_points(*_braided(), KINDS) == {}


def _braided():
    nodes = [N("u0", 50, blk="U", wsc=CODE), N("u1", 50, blk="U", wsc=CODE), N("braid", 1, blk="X", wsc=CODE),
             N("upper", 50, blk="U", wsc=CODE), N("lower", 50, blk="W", wsc=CODE)]
    edges = [E("u0", "u1"), E("u0", "braid"), E("braid", "upper"), E("u1", "upper"), E("upper", "lower")]
    return g(nodes, edges), AREA, {"u0": RIVER, "u1": RIVER, "upper": RIVER, "lower": SLOUGH}


# ------------------------------------------------------------------ what is not a branch point

def test_a_continued_system_is_one_river_not_a_branch():
    """The Chilliwack goes on as the Vedder (`water_continue`, T76): two waters, one river — even where the graph
    shows a split (a braid piece of the river's code beside the named one)."""
    nodes = [N("a", 100, blk="A", wsc=CODE), N("b", 100, blk="B", wsc=CODE), N("braid", 1, blk="X", wsc=CODE)]
    graph = g(nodes, [E("a", "b"), E("a", "braid")])
    water = {"a": RIVER, "b": CANAL}
    assert branch_points(graph, AREA, water, KINDS) == {("a", "b"): (1.0, RIVER)}       # equal: the first is "smaller"
    assert branch_points(graph, AREA, water, KINDS, continues=[(RIVER, CANAL)]) == {}


def _renamed():
    """ONE WATER, ANOTHER NAME (adversarial review 2026-10-23). Hogg Slough runs into Mountain Slough's polygon and on
    as Mountain Slough; South Hawks Creek through Hansen Slough's ponds; Hanson Creek through Lewis Slough. Same code,
    two stream waters, and nothing else leaves the upper end or reaches the lower on that code: all the water goes on.
    A gauge L (level) on the upper water, a creek joining the lower on its own (nested) code.

        upper ─▶ lower ─▶ below           upper 50 (L), lower 50, below 55
                 creek ─┘                 creek 5 on a tributary code
    """
    nodes = [N("upper", 50, blk="U", wsc=CODE), N("lower", 50, blk="W", wsc=CODE), N("below", 55, blk="W", wsc=CODE),
             N("creek", 5, blk="C", wsc=TRIB)]
    edges = [E("upper", "lower"), E("lower", "below"), E("creek", "lower")]
    water = {"upper": RIVER, "lower": SLOUGH, "below": SLOUGH, "creek": CREEK}
    return g(nodes, edges), water, [("L", "upper", 40, False)], {"L": LEVEL}


def test_one_water_taking_another_name_is_not_a_branch_point():
    """Whole value: no branch point, so the upper water's level gauge answers the renamed water below, as any gauge
    answers the next reach of its own river. As a branch (share 1.0) it would carry Flow only and drop L."""
    graph, water, donors, meas = _renamed()
    assert branch_points(graph, AREA, water, KINDS) == {}
    got = answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))
    assert got == {
        "upper": ((("L", "up", True, 1),), 50.0),
        "lower": ((("L", "up", False, 0),), 50.0),
        "below": ((("L", "up", False, 0),), 55.0),
    }


def test_mutation_pin_a_branch_is_a_split_or_a_join():
    """Without the split-or-join test the renamed pair is a branch point at share 0.91 and L stops at the name change;
    without either half alone a long slough's head (a split) or mouth (a join) disappears."""
    graph, water, donors, meas = _renamed()
    old = "if not (goes_on(a, b, ca) or fed(b, a, cb)):"
    every = _mutant(branch_points, old, "if False:")
    assert every(graph, AREA, water, KINDS) == {("upper", "lower"): (50 / 55, RIVER)}
    got = _with("branch_points", every, lambda: answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS,
                                                              measures=meas)))
    assert set(got) == {"upper"}
    # A slough longer than MAINSTEM_HOPS: its head is seen only as a split, its mouth only as a join.
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_mid", 500, blk="R", wsc=CODE), N("r_down", 500, blk="R", wsc=CODE)]
    nodes += [N(f"c{i}", 5, blk="S", wsc=CODE) for i in range(1, 7)]
    edges = [E("r_up", "r_mid"), E("r_mid", "r_down"), E("r_up", "c1"), E("c6", "r_down")]
    edges += [E(f"c{i}", f"c{i + 1}") for i in range(1, 6)]
    water = {"r_up": RIVER, "r_mid": RIVER, "r_down": RIVER, **{f"c{i}": SLOUGH for i in range(1, 7)}}
    long = g(nodes, edges)
    assert set(branch_points(long, AREA, water, KINDS)) == {("r_up", "c1"), ("c6", "r_down")}
    assert set(_mutant(branch_points, old, "if not goes_on(a, b, ca):")(long, AREA, water, KINDS)) == {("r_up", "c1")}
    assert set(_mutant(branch_points, old, "if not fed(b, a, cb):")(long, AREA, water, KINDS)) == {("c6", "r_down")}


def test_mutation_pin_the_split_is_looked_for_past_fwa_s_stubs_and_not_in_a_braid():
    """Looked for on the edge's own end only, the stubbed head is a "new name" and the creek reaches the backwater;
    without the braid check the braided name change is a branch."""
    import pipeline.tests.test_side_channel_gauges as me
    local = _mutant(branch_points, "mine = run(a, back, c)", "mine = {a}")
    assert not _with("branch_points", local, lambda: _passes(me.test_a_split_cut_into_a_stub_is_still_a_branch_point))
    braids = _mutant(branch_points, "y not in mine and y != b", "y != b")
    assert braids(*_braided(), KINDS) == {("upper", "lower"): (1.0, RIVER)}


def _passes(test) -> bool:
    try:
        test()
    except AssertionError:
        return False
    return True


def test_a_lake_or_an_unowned_section_is_never_a_branch_point():
    nodes = [N("a", 100, blk="A", wsc=CODE), N("b", 100, blk="B", wsc=CODE), N("c", 100, blk="C", wsc=CODE)]
    graph = g(nodes, [E("a", "b"), E("b", "c")])
    assert branch_points(graph, AREA, {"a": RIVER, "b": "wbk:lake"}, {**KINDS, "wbk:lake": "lake"}) == {}
    assert branch_points(graph, AREA, {"a": RIVER, "c": CANAL}, KINDS) == {}


def _canal_round():
    """The canal beside a river with a reach in between, so a depth-first walk can meet r_down either way first."""
    nodes = [N("r_up", 500, blk="R", wsc=CODE), N("r_mid", 500, blk="R", wsc=CODE), N("r_down", 500, blk="R", wsc=CODE),
             N("canal", 400, blk="K", wsc=CODE)]
    edges = [E("r_up", "canal"), E("canal", "r_down"), E("r_up", "r_mid"), E("r_mid", "r_down")]
    return g(nodes, edges), {"r_up": RIVER, "r_mid": RIVER, "r_down": RIVER, "canal": CANAL}, edges


def test_the_way_round_a_branch_point_beats_the_way_through_it():
    """r_down is reached from the river gauge H (level + discharge) both down the river, carrying everything, and
    through the large-share canal, carrying Flow only (which a gauge reading both cannot bring). Whichever way the walk
    meets it first, the river below keeps H — a section is walked again when a later path carries more. With the old
    seen-set, the edge order decided."""
    graph, water, edges = _canal_round()
    for order in (edges, list(reversed(edges))):
        graph.edges = order
        got = answers(build(graph, MODEL, [("H", "r_up", 40, False)], water_of=water, kind_of=KINDS,
                            measures={"H": BOTH}))
        river = ((("H", "up", True, 1),), 500.0)
        assert got == {"r_up": river, "r_mid": river, "r_down": river}


def test_mutation_pin_a_section_is_walked_again_when_it_carries_more():
    """The old seen-set in place of `kept`: one of the two edge orders loses H below the canal."""
    seen_set = _mutant(bp.candidates, "len(keep) <= kept.get(nxt, -1)", "nxt in kept")
    graph, water, edges = _canal_round()
    lost = []
    for order in (edges, list(reversed(edges))):
        graph.edges = order
        got = _with("candidates", seen_set, lambda: answers(build(graph, MODEL, [("H", "r_up", 40, False)],
                                                                  water_of=water, kind_of=KINDS,
                                                                  measures={"H": BOTH})))
        lost.append("r_down" not in got)
    assert any(lost)


# ------------------------------------------------------------------ mutation pins

def _mutant(fn, old: str, new: str):
    src = textwrap.dedent(inspect.getsource(fn))
    assert src.count(old) == 1, f"{old!r} moved in {fn.__name__}; update this pin to match"
    ns = dict(vars(bp))
    exec(compile(src.replace(old, new), bp.__file__, "exec"), ns)
    return ns[fn.__name__]


def _with(name: str, fn, run):
    real = getattr(bp, name)
    setattr(bp, name, fn)
    try:
        return run()
    finally:
        setattr(bp, name, real)


def test_mutation_pin_the_share_gate_holds_the_canal_and_nicomen_tests():
    """`across_branch` letting Level through, or Flow at any share, must each break a test above."""
    level_too = _mutant(across_branch, 'frozenset({"discharge"})', 'ALL_QUANTITIES')
    any_share = _mutant(across_branch, "share >= LARGE_SHARE", "True")

    def canal_level():
        graph, water, donors, meas = _canal(LEVEL)
        return answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))

    def nicomen():
        graph, water, donors, meas = _nicomen()
        return answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS, measures=meas))

    assert set(_with("across_branch", level_too, canal_level)) == {"canal", "r_up", "r_down"}
    nic = _with("across_branch", any_share, nicomen)
    assert "H" in {m[0] for m in nic["s_foot"][0]} and nic["s_head"][0] != (("C", "up", False, 0),)
    nelson = _nelson()
    got = _with("across_branch", any_share, lambda: answers(build(nelson[0], MODEL, nelson[2], water_of=nelson[1],
                                                                  kind_of=KINDS, measures=nelson[3])))
    assert "slough" in got


def test_mutation_pin_the_two_directions_of_the_walk_rule():
    """INTO the channel every reading is ruled on; OUT of it only its own gauges'. Shut either side and a test above
    breaks; rule on every reading leaving and the creek draining through the slough loses the river."""
    old = "water_of.get(nxt) == crossing[1] or own_water == crossing[1]"

    def run(cand):
        out = {}
        for name, fx in (("nicomen", _nicomen()), ("nelson", _nelson())):
            graph, water, donors, meas = fx
            out[name] = _with("candidates", cand, lambda: answers(build(graph, MODEL, donors, water_of=water,
                                                                        kind_of=KINDS, measures=meas)))
        return out

    no_out = run(_mutant(bp.candidates, old, "water_of.get(nxt) == crossing[1]"))
    assert "N" in {m[0] for m in no_out["nicomen"]["r_down"][0]}                 # Nicomen answers the river again
    no_in = run(_mutant(bp.candidates, old, "own_water == crossing[1]"))
    assert "slough" in no_in["nelson"]                                           # the creek reaches the backwater
    every = run(_mutant(bp.candidates, old, "True"))
    assert "C" not in {m[0] for m in every["nicomen"]["r_down"][0]}              # the creek loses the river


def test_mutation_pin_the_channel_keeps_its_own_gauge():
    """`water_panel` (T169: a side-channel water's one panel) keeping none of its own gauges: the slough takes the
    river's answer alone, and Nicomen's own gauge is gone from Nicomen Slough."""
    graph, water, donors, meas = _nicomen()
    none = _mutant(bp.water_panel, "if m.lead and", "if False and")
    got = _with("water_panel", none, lambda: answers(build(graph, MODEL, donors, water_of=water, kind_of=KINDS,
                                                           measures=meas)))
    assert "N" not in {m[0] for m in got["s_foot"][0]}


def test_no_second_copy_of_the_q44_rule():
    """ONE DECISION, ONE FUNCTION. `LARGE_SHARE` is read in `across_branch` and nowhere else; the walk rules on a
    branch point only by calling it; nothing else in the pipeline says which quantity crosses a branch."""
    src = Path(bp.__file__).read_text()
    rule = inspect.getsource(bp.across_branch)
    assert len(re.findall(r"\bLARGE_SHARE\b", src.replace(rule, ""))) == 1          # its definition, and that is all
    assert src.count("across_branch(") - rule.count("across_branch(") == 1         # the walk's one call
    assert "across_branch(" in inspect.getsource(bp.candidates)
    root = Path(bp.__file__).resolve().parents[2]
    others = [p for p in (root / "pipeline").rglob("*.py")
              if p.resolve() != Path(bp.__file__).resolve() and "tests" not in p.parts
              and re.search(r"\bLARGE_SHARE\b|across_branch|branch_points", p.read_text())]
    assert others == []
