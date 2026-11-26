"""A DEAD END OF A COLLECTED WATER IS COLLECTED (`tributaries.A_DEAD_END_GOES_WITH_ITS_WATER`, 2026-11-15).

The Stikine part-name stop: six tributaries (Buckley, Iskut, Chukachida, Mess; Horsefeed and Sutlahine on the Taku)
each kept one 71-730 m piece FWA draws with no way on — a lake's second outlet that ends, a channel into a lake with
no outlet, a lake and channel draining into each other — outside "<river> including tributaries", so the release's
part names could not join it to the rest of its water (T517 run gap).

Mutation: `A_DEAD_END_GOES_WITH_ITS_WATER = False` -> the three shape tests fail; the guards keep passing.
"""
from __future__ import annotations

import dataclasses

from pipeline.atlas.graph import tributaries as T
from pipeline.common.models import NodeKind
from pipeline.tests.test_reach_tributaries import _g, _n, _w


def _lake(nid, *, wsc, order=1):
    return dataclasses.replace(_n(nid, order=order, kind=NodeKind.lake, blk=""), wsc=wsc)


def _stikine():
    """S:0 the river (code 600, the reach). Buckley B:0 (600-1) joins it; its lake L (600-1) has a main outlet
    B:0 and a second outlet BS:0 (600-1) that ends. Iskut I:0 (600-2) joins S:0; a loop of its own code, lake
    LI and channel IC (600-2), drains into itself and nowhere, fed by an unnamed creek IU (600-2-5); and a lone
    Iskut piece IX (600-2) touches nothing. A dead-end river mouth elsewhere, R:0 (700), is no Stikine water."""
    nodes = [_w("S:0", order=7, wsc="600"), _w("B:0", order=3, wsc="600-1"), _lake("lake:L", wsc="600-1", order=3),
             _w("BS:0", order=1, wsc="600-1"), _w("BU:0", order=2, wsc="600-1-1"),
             _w("I:0", order=5, wsc="600-2"), _lake("lake:LI", wsc="600-2"), _w("IC:0", order=1, wsc="600-2"),
             _w("IU:0", order=1, wsc="600-2-5"), _w("IX:0", order=1, wsc="600-2"), _w("R:0", order=2, wsc="700")]
    edges = [("B:0", "S:0", "confluence"), ("lake:L", "B:0", "lake_out"), ("lake:L", "BS:0", "lake_out"),
             ("BU:0", "lake:L", "lake_in"), ("I:0", "S:0", "confluence"),
             ("lake:LI", "IC:0", "lake_out"), ("IC:0", "lake:LI", "lake_in"), ("IU:0", "lake:LI", "lake_in")]
    return _g(nodes, edges)


def test_a_lakes_second_outlet_that_ends_goes_with_its_water():
    """Buckley's shape: the lake's other outlet BS:0 flows out of walked water and stops."""
    assert "BS:0" in T.tributaries_of_reach(_stikine(), {"S:0"})


def test_a_closed_loop_and_a_lone_piece_go_with_their_water_and_bring_what_feeds_them():
    """The Iskut's shapes: the loop (LI, IC) with its feeder IU, and the unconnected piece IX."""
    got = T.tributaries_of_reach(_stikine(), {"S:0"})
    assert {"lake:LI", "IC:0", "IU:0", "IX:0"} <= got
    assert "R:0" not in got                     # another basin's end: its code was never collected


def test_expand_keeps_the_streams_of_a_dead_end():
    got = T.expand(_stikine(), {"S:0"})
    assert {"BS:0", "IC:0", "IU:0", "IX:0"} <= got and "lake:LI" not in got


def test_the_switch_off_is_the_old_walk(monkeypatch):
    monkeypatch.setattr(T, "A_DEAD_END_GOES_WITH_ITS_WATER", False)
    got = T.tributaries_of_reach(_stikine(), {"S:0"})
    assert not {"BS:0", "lake:LI", "IC:0", "IU:0", "IX:0"} & got
    assert {"B:0", "lake:L", "BU:0", "I:0"} <= got


def test_never_for_the_reachs_own_code():
    """A stretch of the Iskut: a lone Iskut piece may lie outside the stretch, so it is not taken."""
    got = T.tributaries_of_reach(_stikine(), {"I:0"})
    assert "IX:0" not in got and "IC:0" not in got


def test_never_when_a_piece_of_its_code_is_carved_out():
    """Buckley carved out above its lake (BU blocked is another code; block the lake itself): the code 600-1
    is not wholly collected, so where the dead end lies is unknown and it stays out."""
    got = T.tributaries_of_reach(_stikine(), {"S:0"}, blocked={"lake:L"})
    assert "BS:0" not in got


def test_dead_ends_finds_sinks_and_closed_loops():
    by_code, nodes_of = T.dead_ends(_stikine())
    assert ("BS:0",) in by_code["600-1"] and ("IX:0",) in by_code["600-2"]
    assert ("IC:0", "lake:LI") in by_code["600-2"]
    assert ("S:0",) in by_code["600"]          # a river's real mouth is a sink too; the walk reaches it from below
    assert set(nodes_of["600-2"]) == {"I:0", "lake:LI", "IC:0", "IX:0"}


# --------------------------------------------------------------------------------------------------------------- #
# T1146: A SIDE CHANNEL GOES WITH ITS WATER (`tributaries.A_SIDE_CHANNEL_GOES_WITH_ITS_WATER`, owner 2026-11-25).
# Mutation: the flag False -> the two shape tests fail; dropping any one guard of `_side_channel_seeds` (barrier,
# carved out, own water, every touching piece collected, the item's own pieces only, no main flow) fails its negative.
# --------------------------------------------------------------------------------------------------------------- #

from types import SimpleNamespace


def _line(nid, *, order, wsc, types=("1000",)):
    return dataclasses.replace(_w(nid, order=order, wsc=wsc), edge_types=types)


def _forsyth():
    """R:0 (100) the river of the reach; its tributary T is T1:0 <- T2:0 (100-1, main flow, order 3) and T3:0 above T2.
    T's channels (1100, code 100-1, all in T's item): C leaves T2 into the pond P (another code) — the upward walk
    never meets it (Forsyth, MacMillan); D leaves T2 and rejoins T1 at order 4 > 3 — refused `bigger_river`
    (Shelagyote), fed by an unnamed creek F; B a canal (2300) off T2 into P; E off T3 into P; U between T2 and M, a
    main-flow piece of T no walk reaches; X a piece of T touching nothing; Q a big (order 4) channel off T2
    into P; O a channel off T2 into P owned by another
    item."""
    side = ("1100",)
    nodes = [_line("R:0", order=6, wsc="100"), _line("T1:0", order=3, wsc="100-1"), _line("T2:0", order=3, wsc="100-1"),
             _line("T3:0", order=3, wsc="100-1"), _line("M:0", order=2, wsc="100-1"),
             _line("C:0", order=1, wsc="100-1", types=side), _line("D:0", order=4, wsc="100-1", types=side),
             _line("F:0", order=1, wsc="100-1-3"), _line("B:0", order=1, wsc="100-1", types=("2300",)),
             _line("E:0", order=1, wsc="100-1", types=side), _line("U:0", order=1, wsc="100-1", types=side),
             _line("O:0", order=1, wsc="100-1", types=side), _line("X:0", order=1, wsc="100-1", types=side),
             _line("Q:0", order=4, wsc="100-1", types=side),
             _lake("lake:P", wsc="900")]
    edges = [("T1:0", "R:0", "confluence"), ("T2:0", "T1:0", "continuation"), ("T3:0", "T2:0", "continuation"),
             ("T2:0", "C:0", "confluence"), ("C:0", "lake:P", "lake_in"),
             ("T2:0", "D:0", "confluence"), ("D:0", "T1:0", "confluence"), ("F:0", "D:0", "confluence"),
             ("T2:0", "B:0", "confluence"), ("B:0", "lake:P", "lake_in"),
             ("T3:0", "E:0", "confluence"), ("E:0", "lake:P", "lake_in"),
             ("T2:0", "U:0", "confluence"), ("M:0", "U:0", "confluence"), ("U:0", "lake:P", "lake_in"),
             ("T2:0", "O:0", "confluence"), ("O:0", "lake:P", "lake_in"),
             ("T2:0", "Q:0", "confluence"), ("Q:0", "lake:P", "lake_in")]
    reg = {"gnis:T": SimpleNamespace(kind="stream", section_ids=["T1:0", "T2:0", "T3:0", "M:0", "C:0", "D:0", "B:0",
                                                                  "E:0", "U:0", "X:0", "Q:0"]),
           "gnis:R": SimpleNamespace(kind="stream", section_ids=["R:0"]),
           "gnis:O": SimpleNamespace(kind="stream", section_ids=["O:0"])}
    return _g(nodes, edges), reg


def _walk(**kw):
    g, reg = _forsyth()
    return T.tributaries_of_reach(g, {"R:0"}, registry=reg, **kw)


def test_a_channel_leaving_a_walked_water_goes_with_it():
    """Forsyth p1's shape: C leaves T and ends in a pond of another code; nothing of it flows back into the walk."""
    assert "C:0" in _walk()


def test_a_bigger_order_channel_goes_with_its_water_and_brings_what_feeds_it():
    """Shelagyote p0's shape: D rejoins T at a bigger order (`bigger_river`); taken, and F above it with it."""
    got = _walk()
    assert {"D:0", "F:0"} <= got


def test_expand_takes_the_channels_through_the_registry():
    g, reg = _forsyth()
    got = T.expand(g, {"R:0"}, registry=reg)
    assert {"C:0", "D:0", "F:0", "E:0"} <= got and "lake:P" not in got


def test_the_switch_off_is_the_old_walk(monkeypatch):
    monkeypatch.setattr(T, "A_SIDE_CHANNEL_GOES_WITH_ITS_WATER", False)
    got = _walk()
    assert not {"C:0", "D:0", "F:0", "E:0"} & got
    assert {"T1:0", "T2:0", "T3:0"} <= got


def test_no_registry_no_change():
    g, _ = _forsyth()
    assert not {"C:0", "D:0"} & T.tributaries_of_reach(g, {"R:0"})


def test_never_a_canal():
    assert "B:0" not in _walk()


def test_never_a_carved_out_channel():
    assert "C:0" not in _walk(blocked={"C:0"})


def test_never_a_channel_touching_a_carved_out_piece():
    """T3 carved out (with all above it): E leaves T3, so E stays out; C, off the walked T2, is still taken."""
    got = _walk(blocked={"T3:0"})
    assert "E:0" not in got and "C:0" in got


def test_never_the_entrys_own_water():
    """D is the entry's own water: never taken, so the walk never climbs from it to F."""
    got = _walk(own={"D:0"})
    assert not {"D:0", "F:0"} & got and "C:0" in got


def test_never_a_piece_touching_nothing_of_its_water():
    """X lies apart from every piece of T: where it lies along T is unknown."""
    assert "X:0" not in _walk()


def test_never_a_channel_with_one_uncollected_neighbour():
    """U touches T2 (walked) and M, a main-flow piece of T the walk never reached."""
    assert "U:0" not in _walk()


def test_never_another_items_channel():
    """O touches T2 but is another water's: none of its own item's pieces were walked."""
    assert "O:0" not in _walk()


def test_never_the_reachs_own_channels():
    """A stretch of T as the reach: its channels may lie outside the stretch, so none is taken."""
    g, reg = _forsyth()
    got = T.tributaries_of_reach(g, {"T1:0", "T2:0"}, registry=reg)
    assert not {"C:0", "D:0", "U:0"} & got


def test_never_returns_the_reach_itself():
    """The reach is the channel Q: the walk climbs T2 and T3 above it, and Q is never handed back as its own
    tributary though every piece of T it touches was walked."""
    g, reg = _forsyth()
    got = T.tributaries_of_reach(g, {"Q:0"}, registry=reg)
    assert {"T2:0", "T3:0"} <= got and "Q:0" not in got
