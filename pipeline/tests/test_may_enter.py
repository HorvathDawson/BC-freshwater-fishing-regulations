"""T102 part B — ONE entry check, `tributaries.may_enter`, asked at every step of the tributary walk, at
every mouth seeded at a cut, for every piece of a lake's code run, and (T117) by every other walk that
climbs a water. Each refusal carries a named reason (`tributaries.REFUSALS`).

Whole-value tests on small graphs. Mutations, each named where it fails:
  M1 `may_enter` always None              -> test_the_walk_takes_its_steps_only_from_may_enter
  M2 `may_enter` refuses every SEED       -> test_a_seed_is_a_step_through_may_enter
  M3 `may_enter` refuses every RUN piece  -> test_a_code_run_piece_is_a_step_through_may_enter
  M4 a rule put back in a walk's body     -> test_no_walk_body_keeps_a_rule_of_its_own
  M5 a reason swapped, or the order of the questions changed (a carve-out asked after the barrier,
     the order guard before the code)    -> test_may_enter_names_every_refusal
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap

from pipeline.atlas.graph import tributaries as T
from pipeline.common.models import NodeKind
from pipeline.tests.test_reach_tributaries import _g, _n, _w


def _entry_fixture():
    """One reach piece R:0 (order 3, code 100-1) and one water of every kind flowing into it.

        Rup:0    the reach's own line above it, by `continuation`     -> river_continuing
        blk:0    a carve-out                                          -> carved_out
        own:0    the entry's own water past the cut                    -> own_water
        bar:0    a 2300 connector                                      -> barrier
        par:0    code 100 (the river R drains into), order 1           -> receiving_river
        big:0    no code, order 5                                      -> bigger_river
        div:0    across a code run's divide                            -> code_divide
        ok:0     a creek, code 100-1-2                                 -> None (enter, collect)
        ghost    an edge from no node                                  -> not_in_graph
    """
    nodes = [_w("R:0", order=3, wsc="100-1"),
             dataclasses.replace(_w("Rup:0", order=3, wsc="100-1"), blk="R"),
             _w("blk:0", order=1, wsc="100-1-3"), _w("own:0", order=1, wsc="100-1-5"),
             dataclasses.replace(_n("bar:0", barrier=True), wsc="100-1-6"),
             _w("par:0", order=1, wsc="100"), _w("big:0", order=5, wsc=""),
             _w("div:0", order=1, wsc="100-1-8"), _w("ok:0", order=1, wsc="100-1-2")]
    edges = [("Rup:0", "R:0", "continuation")] + [(s, "R:0", "confluence") for s in (
        "blk:0", "own:0", "bar:0", "par:0", "big:0", "div:0", "ok:0", "ghost")]
    return _g(nodes, edges)


def _ctx(g):
    return dataclasses.replace(T.reach_context(g, {"R:0"}, blocked={"blk:0"}, own={"own:0"}),
                               cuts=frozenset({("div:0", "R:0")}))


def test_may_enter_names_every_refusal():
    """M5: the whole verdict table for one reach piece, one step from each water into it."""
    g = _entry_fixture()
    ctx = _ctx(g)
    got = {e.from_node: T.may_enter(ctx, e.from_node, "R:0", e) for e in g.edges}
    assert got == {"Rup:0": T.RIVER_CONTINUING, "blk:0": T.CARVED_OUT, "own:0": T.OWN_WATER,
                   "bar:0": T.BARRIER, "par:0": T.RECEIVING_RIVER, "big:0": T.BIGGER_RIVER,
                   "div:0": T.CODE_DIVIDE, "ok:0": None, "ghost": T.NOT_IN_GRAPH}
    assert set(got.values()) - {None} == set(T.REFUSALS)
    # the ORDER of the questions: a carve-out that is also a canal, a bigger river and the receiving
    # river is still a carve-out; a canal that is also the receiving river is a canal
    g.nodes["blk:0"] = dataclasses.replace(g.nodes["bar:0"], node_id="blk:0", wsc="100", stream_order=9)
    g.nodes["bar:0"] = dataclasses.replace(g.nodes["bar:0"], wsc="100", stream_order=9)
    g.nodes["big:0"] = dataclasses.replace(g.nodes["big:0"], wsc="100-1-4")   # the code wins over order
    assert [T.may_enter(ctx, s, "R:0") for s in ("blk:0", "bar:0", "big:0")] == [
        T.CARVED_OUT, T.BARRIER, None]


def test_how_a_step_is_taken_decides_which_questions_are_asked():
    """A SEED arrives on another blue line by a confluence: never "the river continuing". A code RUN's
    piece is FWA's membership: never judged by order. A step past the reach's boundary is never "own
    water" (met above a true tributary, it is walked through)."""
    g = _entry_fixture()
    ctx = _ctx(g)
    up = g.edges[0]
    big = next(e for e in g.edges if e.from_node == "big:0")
    assert [T.may_enter(ctx, "Rup:0", "R:0", up, via=T.SEED),
            T.may_enter(ctx, "big:0", "R:0", big, via=T.RUN),
            T.may_enter(ctx, "par:0", "R:0", via=T.RUN),
            T.may_enter(ctx, "own:0", "ok:0")] == [None, None, T.RECEIVING_RIVER, None]


def test_a_lake_mid_reach_is_entered_as_the_river_passing_through():
    """Not a refusal: the lake between two pieces of one reach line is walked, never collected."""
    low = _n("L:0", order=3)
    top = dataclasses.replace(_n("L:300", order=3, blk="L"), down_m=300.0, up_m=400.0)
    lake = dataclasses.replace(_n("lake:1", order=3, kind=NodeKind.lake), blk="")
    g = _g([low, top, lake, _n("side:0", order=1)],
           [("L:300", "lake:1", "lake_in"), ("lake:1", "L:0", "lake_out"),
            ("side:0", "lake:1", "lake_in")])
    ctx = T.reach_context(g, {"L:0", "L:300"})
    assert T.may_enter(ctx, "lake:1", "L:0", g.edges[1]) == T.PASSING_THROUGH
    assert T.tributaries_of_reach(g, {"L:0", "L:300"}) == {"side:0"}


def test_the_walk_takes_its_steps_only_from_may_enter(monkeypatch):
    """M1: with `may_enter` saying yes to everything the walk climbs the reach's own river above it, the
    receiving river, the bigger river, the canal — it keeps no rule of its own to stop them."""
    g = _entry_fixture()
    assert T.tributaries_of_reach(g, {"R:0"}, blocked={"blk:0"}, own={"own:0"}) == {"ok:0", "div:0"}
    monkeypatch.setattr(T, "may_enter", lambda *a, **k: None)
    assert T.tributaries_of_reach(g, {"R:0"}, blocked={"blk:0"}) == {
        "Rup:0", "blk:0", "own:0", "bar:0", "par:0", "big:0", "div:0", "ok:0", "ghost"}


def _cut_fixture():
    reach, cuts = frozenset({"a:500"}), [("a", 500.0)]
    g = _g([dataclasses.replace(_n("a:500", order=3, blk="a"), down_m=500.0, up_m=1000.0),
            dataclasses.replace(_n("a:0", order=3, blk="a"), up_m=500.0), _n("x:0", order=1)],
           [("a:500", "a:0", "continuation", 500.0), ("x:0", "a:0", "confluence", 500.0)])
    return g, reach, cuts


def test_a_seed_is_a_step_through_may_enter(monkeypatch):
    """M2: a seed passes `may_enter` (`via=SEED`) — refuse seeds there and the creek at the cut goes; a
    carve-out of it is refused by the same question."""
    g, reach, cuts = _cut_fixture()
    assert T._mouths_at_lower_bound(g, reach, cuts) == {"x:0"}
    assert T.tributaries_of_reach(g, reach, cuts=cuts) == {"x:0"}
    assert T.tributaries_of_reach(g, reach, cuts=cuts, blocked={"x:0"}) == frozenset()
    real = T.may_enter
    monkeypatch.setattr(T, "may_enter", lambda ctx, s, d, e=None, *, via=T.FLOW:
                        "no" if via == T.SEED else real(ctx, s, d, e, via=via))
    assert T._mouths_at_lower_bound(g, reach, cuts) == set()


def test_a_code_run_piece_is_a_step_through_may_enter(monkeypatch):
    """M3: the pieces of a lake's code run are taken through `may_enter` (`via=RUN`)."""
    g = _entry_fixture()
    g.nodes["run:0"] = _w("run:0", order=1, wsc="100-1-2")
    monkeypatch.setattr(T, "code_runs", lambda graph: ({"ok:0": ("run:0",)}, frozenset()))
    assert "run:0" in T.tributaries_of_reach(g, {"R:0"})
    real = T.may_enter
    monkeypatch.setattr(T, "may_enter", lambda ctx, s, d, e=None, *, via=T.FLOW:
                        "no" if via == T.RUN else real(ctx, s, d, e, via=via))
    assert "run:0" not in T.tributaries_of_reach(g, {"R:0"})


#: Every name a rule of the walk is read through. A walk body naming one is a second copy of a rule.
RULE_NAMES = frozenset({
    "is_barrier", "MAINSTEM_EDGE_KINDS", "_refusal", "_receives", "_order_breaks", "_code_says_tributary",
    "_breaks_strahler", "_is_main_outlet", "_lake_mid_reach", "_lake_on_line", "_through_blks",
    "_on_a_tributary_of_the_reach", "_continuing", "THE_WALK_NEVER_ENTERS_A_PARENT",
    "LAKE_MID_REACH_IS_THE_RIVER", "NAMED_LAKE_ON_A_TRIBUTARY_IS_CLIMBED", "ORDER_ZERO_IS_UNKNOWN",
    "stream_order", "wsc", "kind"})

#: The walks in tributaries.py: they keep the books (seen / out / stack) and ask `may_enter`.
WALKERS = ("tributaries_of_reach", "_mouths_at_lower_bound", "subtree", "joining")


def _names_in_body(fn) -> set[str]:
    tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    body = tree.body[0].body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]                                    # the docstring
    out: set[str] = set()
    for b in body:
        for n in ast.walk(b):
            if isinstance(n, ast.Name):
                out.add(n.id)
            elif isinstance(n, ast.Attribute):
                out.add(n.attr)
    return out


def test_no_walk_body_keeps_a_rule_of_its_own():
    """M4 (no second copy): no walk in tributaries.py names a rule; `_mouths_at` names only the seeding's
    own topology (`MOUTH_SEEDS_ONLY_AT_THE_CUT`: the piece below on the reach's line, a confluence at
    the cut's measure) and asks `may_enter` for the rest."""
    bad = {w: sorted(_names_in_body(getattr(T, w)) & RULE_NAMES) for w in WALKERS}
    bad["_mouths_at"] = sorted(_names_in_body(T._mouths_at) & (RULE_NAMES - {"kind"}))
    assert {k: v for k, v in bad.items() if v} == {}
    assert all("may_enter" in _names_in_body(getattr(T, w)) or w == "_mouths_at_lower_bound"
               for w in WALKERS)
    assert "may_enter" in _names_in_body(T._mouths_at)


# --- T117: one owner for "which outlet is the lake's own river" ------------------------------------------------
#   M6 `lake_main_outlet` returns nothing       -> test_both_readers_ask_lake_main_outlet
#   M7 ties not broken by main flow              -> test_lake_main_outlet_whole

def _two_outlets(main_types=("1000",), other_types=("1100",), main_order=2, other_order=2, joins="a:0"):
    """A lake (order 4, code 100-7) draining by two outlets of equal code; `up:0` feeds the lake. The
    river R:0 (order 5, code 100) is joined by the outlet `joins`."""
    lake = dataclasses.replace(_n("lake:7", order=4, kind=NodeKind.lake), blk="", wsc="100-7")
    a = dataclasses.replace(_w("a:0", order=main_order, wsc="100-7"), edge_types=main_types)
    b = dataclasses.replace(_w("b:0", order=other_order, wsc="100-7"), edge_types=other_types)
    return _g([lake, a, b, _w("up:0", order=1, wsc="100-7-3"), _w("R:0", order=5, wsc="100")],
              [("lake:7", "a:0", "lake_out"), ("lake:7", "b:0", "lake_out"), ("up:0", "lake:7", "lake_in"),
               (joins, "R:0", "confluence")])


def test_lake_main_outlet_whole():
    """M7: the largest order wins; among equals, main flow; no main flow among them, all of them."""
    assert [T.lake_main_outlet(_two_outlets(), "lake:7"),
            T.lake_main_outlet(_two_outlets(main_order=1, other_order=3), "lake:7"),
            T.lake_main_outlet(_two_outlets(main_types=("1150",)), "lake:7"),
            T.lake_main_outlet(_two_outlets(), "a:0")] == [
        frozenset({"a:0"}), frozenset({"b:0"}), frozenset({"a:0", "b:0"}), frozenset()]


def test_the_order_guard_s_exemption_is_the_lake_s_main_outlet():
    """Equal-order outlets: the main-flow one is the lake's river, so a walk up it enters the lake (the
    Dester Lake exemption) — before T117 `_is_main_outlet` demanded STRICTLY larger and refused both. The
    secondary outlet still never climbs into its lake."""
    assert (T.tributaries_of_reach(_two_outlets(), {"R:0"}),
            T.tributaries_of_reach(_two_outlets(joins="b:0"), {"R:0"})) == (
        {"a:0", "lake:7", "up:0"}, {"b:0"})


def test_both_readers_ask_lake_main_outlet(monkeypatch):
    """M6: with no main outlet, the exemption is gone (the walk up the main outlet is refused)."""
    g = _two_outlets()
    monkeypatch.setattr(T, "lake_main_outlet", lambda graph, lake: frozenset())
    assert T.tributaries_of_reach(g, {"R:0"}) == {"a:0"}
    assert T._is_main_outlet(g, "lake:7", "a:0") is False


def test_code_runs_reads_lake_main_outlet_too(monkeypatch):
    """M6, the other reader (adversarial review): `code_runs` skips exactly the outlets `lake_main_outlet`
    names. Name every Dewar outlet the lake's river and there is no run; name none and Five Mile Creek (the
    main outlet) becomes a run too. Mutation: `code_runs` keeping its own main-outlet copy -> this fails."""
    from pipeline.tests.test_tributary_lakes import dewar
    make = getattr(dewar, "__wrapped__", dewar)
    g = make()
    assert T.code_runs(g)[0] == {"lake:dewar": ("chan_up", "lake:pond", "chan_dn")}
    monkeypatch.setattr(T, "lake_main_outlet", lambda graph, lake: frozenset({"five_mile", "chan_up"}))
    assert T.code_runs(make())[0] == {}
