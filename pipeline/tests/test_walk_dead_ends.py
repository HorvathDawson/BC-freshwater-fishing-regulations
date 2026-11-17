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
