"""(T493 reopened, 2026-11-08) No hole inside one water: `export.fill_zooms`.

Inside one water every piece got its own first zoom, so Nicomen Slough (a stream's own polygon between two lines) was
hidden z7-z10 between two pieces drawn from z7, and Camp Slough's z12/z13 connectors broke its z11 polygons. A piece of
water I hidden at z with drawn water OF I on both sides (through hidden pieces of I) is drawn from z.
"""

from __future__ import annotations

import random
from types import SimpleNamespace

from pipeline.common.models.graph import FlowEdge
from pipeline.deliver.tiles.export import fill_zooms, lake_link_zooms


def _graph(*flows: tuple[str, str]):
    """A graph of only its edges: `a` flows into `b` for each (a, b)."""
    edges = [FlowEdge(a, b, 0.0) for a, b in flows]
    up, down = {}, {}
    for i, e in enumerate(edges):
        up.setdefault(e.to_node, []).append(i)
        down.setdefault(e.from_node, []).append(i)
    return SimpleNamespace(edges=edges, up_adj=up, down_adj=down)


def _chain(*names: str):
    return _graph(*zip(names, names[1:]))


def _one(*secs: str, item: str = "I") -> dict[str, frozenset[str]]:
    return {s: frozenset({item}) for s in secs}


def test_nicomen_a_water_hidden_between_two_drawn_pieces_of_itself_is_drawn_with_them():
    # 59594 (z10) / 1454487 (its polygon's route z11, polygon z11) / 59593 (z10) between two z7 pieces of the slough
    g = _chain("top", "59594", "1454487", "59593", "bottom")
    zoom = {"top": 7, "59594": 10, "1454487": 11, "59593": 10, "bottom": 7}
    out = fill_zooms(g, zoom, _one(*zoom), {"1454487": 11})
    assert out == {"top": 7, "59594": 7, "1454487": 7, "59593": 7, "bottom": 7}


def test_camp_the_connectors_between_a_slough_s_drawn_polygons_come_in_with_them():
    # two polygons drawn from z11 (their routes later), the connectors between them z12 / z13
    g = _chain("p1", "c1", "c2", "p2")
    zoom = {"p1": 12, "c1": 12, "c2": 13, "p2": 13}
    out = fill_zooms(g, zoom, _one(*zoom), {"p1": 11, "p2": 11})
    assert out["c1"] == out["c2"] == 11
    # the polygons are drawn from z11 already: their routes are not the hole, and are left as they were
    assert out["p1"] == 12 and out["p2"] == 13
    # without the polygons counted as drawn there is no water beyond c2 below z13: no hole
    assert fill_zooms(g, zoom, _one(*zoom)) == zoom


def test_drawn_water_on_one_side_only_is_no_hole():
    g = _chain("top", "mid", "end")
    zoom = {"top": 7, "mid": 10, "end": 11}
    assert fill_zooms(g, zoom, _one(*zoom)) == zoom
    # a hidden piece below a drawn one with nothing after it at all
    assert fill_zooms(_chain("top", "mid"), {"top": 7, "mid": 10}, _one("top", "mid")) == {"top": 7, "mid": 10}


def test_another_water_on_either_side_is_no_hole():
    # a lake-outlet creek between two pieces of the river it joins: not drawn at the river's zoom
    g = _chain("top", "mid", "bottom")
    zoom = {"top": 7, "mid": 10, "bottom": 7}
    items = {"top": frozenset({"R"}), "mid": frozenset({"C"}), "bottom": frozenset({"R"})}
    assert fill_zooms(g, zoom, items) == zoom
    # nor is it walked THROUGH: R hidden behind a piece of C is not reached
    g = _chain("top", "c", "mid", "bottom")
    zoom = {"top": 7, "c": 10, "mid": 10, "bottom": 7}
    items = {"top": frozenset({"R"}), "c": frozenset({"C"}), "mid": frozenset({"R"}), "bottom": frozenset({"R"})}
    assert fill_zooms(g, zoom, items) == zoom
    # a section in no water is never filled and never walked
    assert fill_zooms(_chain("a", "b", "c"), {"a": 7, "b": 10, "c": 7}, _one("a", "c")) == {"a": 7, "b": 10, "c": 7}
    # the same water on both sides through a piece of two waters: filled for the one they share
    items = {"top": frozenset({"R"}), "mid": frozenset({"R", "S"}), "bottom": frozenset({"R"})}
    assert fill_zooms(_chain("top", "mid", "bottom"), {"top": 7, "mid": 10, "bottom": 7}, items)["mid"] == 7


def test_a_braid_loop_ends():
    # top -> a <-> b (a loop of hidden pieces) -> bottom
    g = _graph(("top", "a"), ("a", "b"), ("b", "a"), ("b", "bottom"))
    zoom = {"top": 7, "a": 11, "b": 12, "bottom": 8}
    out = fill_zooms(g, zoom, _one(*zoom))
    assert out == {"top": 7, "a": 8, "b": 8, "bottom": 8}
    # a loop with no way out downstream ends, and fills nothing: a is drawn on from top, so no hole starts there
    g = _graph(("top", "a"), ("a", "b"), ("b", "a"))
    assert fill_zooms(g, {"top": 7, "a": 11, "b": 12}, _one("top", "a", "b")) == {"top": 7, "a": 11, "b": 12}


def test_a_braid_beside_a_drawn_river_is_no_hole():
    # the river a -> b drawn from z7; its braid x leaves a and rejoins b, drawn from z13: the river is unbroken
    g = _graph(("a", "b"), ("a", "x"), ("x", "b"))
    zoom = {"a": 7, "b": 7, "x": 13}
    assert fill_zooms(g, zoom, _one(*zoom)) == zoom
    # ...but where the river itself is hidden there too, both ways through are the hole
    g = _graph(("a", "m"), ("m", "b"), ("a", "x"), ("x", "b"))
    zoom = {"a": 7, "m": 10, "b": 7, "x": 13}
    assert fill_zooms(g, zoom, _one(*zoom)) == {"a": 7, "m": 7, "b": 7, "x": 7}


def _random_case(seed: int):
    rnd = random.Random(seed)
    names = [f"s{i}" for i in range(40)]
    flows = [(rnd.choice(names), rnd.choice(names)) for _ in range(70)]
    zoom = {s: rnd.randint(6, 13) for s in names}
    items = {s: frozenset(rnd.sample("ABC", rnd.randint(0, 2))) for s in names}
    poly = {s: rnd.randint(6, 13) for s in rnd.sample(names, 8)}
    return flows, zoom, items, poly


def test_zooms_are_only_ever_lowered_and_the_result_is_a_fixed_point():
    for seed in range(50):
        flows, zoom, items, poly = _random_case(seed)
        g = _graph(*flows)
        out = fill_zooms(g, zoom, items, poly)
        assert set(out) == set(zoom)
        assert all(out[s] <= zoom[s] for s in zoom), seed
        assert fill_zooms(g, out, items, poly) == out, seed


def test_deterministic_whatever_the_order():
    for seed in range(20):
        flows, zoom, items, poly = _random_case(seed)
        want = fill_zooms(_graph(*flows), zoom, items, poly)
        rnd = random.Random(seed)
        for _ in range(3):
            f = flows[:]
            rnd.shuffle(f)
            keys = list(zoom)
            rnd.shuffle(keys)
            assert fill_zooms(_graph(*f), {k: zoom[k] for k in keys}, items, poly) == want, seed


def test_the_lake_link_is_computed_on_the_filled_zooms():
    # the creek: top (z7) -> mid (z10) -> bottom (z7), and mid also flows into a small lake (z12) -> out (z7)
    g = _graph(("top", "mid"), ("mid", "bottom"), ("mid", "lake"), ("lake", "out"))
    zoom = {"top": 7, "mid": 10, "bottom": 7, "out": 7}
    items = {**_one("top", "mid", "bottom"), "out": frozenset({"O"})}
    filled = fill_zooms(g, zoom, items)
    assert filled["mid"] == 7
    assert lake_link_zooms(g, {"lake": 12}, zoom) == {"lake": 10}
    assert lake_link_zooms(g, {"lake": 12}, filled) == {"lake": 7}


def test_export_streams_fills_before_it_writes_and_links_on_the_filled_zooms():
    """Pin the wiring: the stream features are buffered, filled, then written; the links read the filled zooms."""
    import inspect

    from pipeline.deliver.tiles import export as E
    src = inspect.getsource(E.export_streams)
    fill, link, write = (src.index("fill_zooms(graph, drawn_z"), src.index("lake_link_zooms(graph, lake_z, drawn_z)"),
                         src.index("_write(_to4326(geoms[sec], tf), props, drawn_z[sec])"))
    assert fill < write < link
    assert "drawn_z = filled" in src[fill:write]
