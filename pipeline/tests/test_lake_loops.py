"""T753 (owner 2026-11-15): a nameless line that leaves a lake and comes back into the same lake is the lake's, not its
river's (`pipeline.atlas.registry.lake_loops`). Toy graphs only; no FWA, no build."""

import pytest

from pipeline.atlas.registry import lake_loops as LL
from dataclasses import replace

from pipeline.common.models import (BoundaryKind, FlowEdge, NameSource, NameTuple, NodeKind, RegistryBoundary,
                                    RegistryItem, SectionBoundary, StreamGraph, StreamNode)

SIDE = (NameTuple("River R", NameSource.side_channel, gnis_id="1"),)


def _lake(w):
    return SectionBoundary(f"lake:{w}", BoundaryKind.lake, None, f"lake {w}")


def _piece(g, nid, blk, down, up, lo=None, hi=None, tuples=SIDE, gnis=""):
    g.nodes[nid] = StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc="100", gnis_id=gnis,
                              display_name="River R" if tuples else "", name_tuples=tuples,
                              down_m=down, up_m=up, length_m=up - down,
                              lower_bound=_lake(lo) if lo else None, upper_bound=_lake(hi) if hi else None)


def _edge(g, frm, to, kind):
    g.edges.append(FlowEdge(frm, to, 0.0, kind=kind))
    g.down_adj.setdefault(frm, []).append(len(g.edges) - 1)
    g.up_adj.setdefault(to, []).append(len(g.edges) - 1)


def _flow_from_bounds(g):
    """The flow edges the bounds describe: a piece flows into its lower lake and out of its upper one."""
    for n in list(g.nodes.values()):
        if n.kind == NodeKind.stream:
            if n.lower_bound is not None:
                _edge(g, n.node_id, n.lower_bound.boundary_id, "lake_in")
            if n.upper_bound is not None:
                _edge(g, n.upper_bound.boundary_id, n.node_id, "lake_out")


def _pond(g, w):
    g.nodes[f"lake:{w}"] = StreamNode(node_id=f"lake:{w}", kind=NodeKind.lake, wbk=w)


def _item(iid, kind, sections, name="", boundaries=(), refs=()):
    return RegistryItem(id=iid, name=name or iid, kind=kind, section_ids=tuple(sections),
                        boundaries=tuple(boundaries), ref_ids=tuple(refs))


def _bnd(item, w):
    return RegistryBoundary(id=f"{item}__lake_{w}", label=f"lake {w}", kind="lake", ref=f"lake:{w}", wbk=w)


def _world():
    """River R (gnis:1) runs through lake L on blk M. The Sproat shape on blk P: L -> unnamed pond Q -> L. Near misses
    beside it, each in a stream item."""
    g = StreamGraph()
    for w in ("L", "Q", "Y", "U", "F", "W"):
        _pond(g, w)
    _piece(g, "M:0", "M", 0, 1000, hi="L", tuples=(), gnis="1")
    _piece(g, "M:3000", "M", 3000, 5000, lo="L", tuples=(), gnis="1")
    _piece(g, "P:898", "P", 898, 999, lo="L", hi="Q")            # the Sproat pieces: 101 m + 66 m
    _piece(g, "P:1295", "P", 1295, 1361, lo="Q", hi="L")
    _piece(g, "D:10", "D", 10, 200, lo="L", hi="Y")              # lake L to ANOTHER lake Y
    _piece(g, "B:0", "B", 0, 50, hi="L")                         # a braid: meets the river below the lake
    _piece(g, "B:80", "B", 80, 300, lo="L", hi="L")
    _piece(g, "O:5", "O", 5, 40, lo="L", hi="Y")                 # a loop through another named lake Y
    _piece(g, "O:60", "O", 60, 90, lo="Y", hi="L")
    _piece(g, "U:0", "U", 0, 70, lo="U", hi="U")                 # a loop off an UNNAMED lake U
    _piece(g, "F:0", "F", 0, 70, lo="F", hi="F")                 # a loop off the river's own polygon F
    _piece(g, "N:0", "N", 0, 70, lo="L", hi="W")                 # a loop with a piece a curator named
    _piece(g, "N:100", "N", 100, 170, lo="W", hi="L", tuples=(NameTuple("Narrows Channel", NameSource.override),))
    _piece(g, "V:0", "V", 0, 70, lo="L", hi="L", tuples=(NameTuple("Creek V", NameSource.side_channel),))
    _piece(g, "T:0", "T", 0, 70, lo="L", hi="L")                 # a loop a nameless creek flows into (Stalk Creek)
    _piece(g, "K:0", "K", 0, 30, tuples=())                      # ...that creek
    _piece(g, "C:0", "C", 0, 70, lo="L", hi="L", tuples=(NameTuple("Rancheria River", NameSource.regulation),))
    _flow_from_bounds(g)
    _edge(g, "B:0", "M:0", "confluence")                        # the braid meets the river
    _edge(g, "K:0", "T:0", "confluence")
    # FWA starts the Sproat line INSIDE the lake: the piece's lower bound is a natural end; only the edge says L
    g.nodes["P:898"] = replace(g.nodes["P:898"], lower_bound=None)
    river = ["M:0", "M:3000", "P:898", "P:1295", "D:10", "B:0", "B:80", "O:5", "O:60", "U:0", "F:0", "N:0", "T:0", "C:0",
             "lake:F"]
    reg = {
        "gnis:1": _item("gnis:1", "stream", river, "River R",
                        boundaries=[_bnd("r", "L"), _bnd("r", "Q"), _bnd("r", "Y"), _bnd("r", "U"), _bnd("r", "F")],
                        refs=["blk:B", "blk:D", "blk:F", "blk:M", "blk:N", "blk:O", "blk:P", "blk:U", "gnis:1",
                              "wbk:F", "wsc:100"]),
        "wbk:L": _item("wbk:L", "lake", ["lake:L"], "Lake L", refs=["wbk:L"]),
        "wbk:Y": _item("wbk:Y", "lake", ["lake:Y"], "Lake Y", refs=["wbk:Y"]),
        "gnis:3": _item("gnis:3", "stream", ["V:0"], "Creek V"),          # made of nothing but a loop piece
    }
    return g, reg


def _run():
    g, reg = _world()
    rep: dict = {}
    return g, reg, LL.move_lake_loops(reg, g, rep), rep


def test_the_sproat_loop_leaves_the_river_and_joins_its_lake():
    _, _, out, rep = _run()
    assert {"P:898", "P:1295"}.isdisjoint(out["gnis:1"].section_ids)
    assert out["wbk:L"].section_ids == ("lake:L", "P:1295", "P:898")
    row = next(r for r in rep["moved"] if r["blk"] == "P")
    assert (row["left_items"], row["joined"], row["length_m"]) == (["gnis:1"], "wbk:L", 167.0)
    assert row["lakes_on_loop"] == ["L", "Q", "L"]


def test_the_pond_stays_its_own_water():
    _, _, out, _ = _run()
    assert not any("lake:Q" in it.section_ids for it in out.values())


def test_the_river_drops_only_the_boundaries_and_refs_the_moved_pieces_alone_carried():
    _, _, out, rep = _run()
    r = out["gnis:1"]
    refs = {b.ref for b in r.boundaries}
    assert "lake:Q" not in refs and "lake:U" not in refs               # the pond's edge, the unnamed lake's edge
    assert {"lake:L", "lake:Y", "lake:F"} <= refs                       # still stood on by kept pieces
    assert "blk:P" not in r.ref_ids and "blk:U" not in r.ref_ids and "blk:M" in r.ref_ids
    assert next(x for x in rep["moved"] if x["blk"] == "P")["boundaries_dropped"] == {"gnis:1": ["r__lake_Q"]}


def test_the_lake_takes_no_river_ref_name_or_boundary():
    g, reg, out, _ = _run()
    assert out["wbk:L"].ref_ids == ("wbk:L",) and out["wbk:L"].name == "Lake L" and out["wbk:L"].boundaries == ()


def test_a_loop_off_an_unnamed_lake_leaves_the_river_for_no_item():
    _, _, out, rep = _run()
    assert not any("U:0" in it.section_ids for it in out.values())
    assert next(r for r in rep["moved"] if r["blk"] == "U")["joined"] == ""


@pytest.mark.parametrize("blk,why", [
    ("D", LL.TWO_LAKES),
    ("B", LL.NOT_ALL_BETWEEN_LAKES),
    ("O", LL.OTHER_WATER_ON_LOOP),
    ("F", LL.LAKE_IS_A_STREAM),
    ("N", LL.OWN_NAME),
    ("C", LL.OWN_NAME),                                          # a curated (regulation) name on the piece itself
    ("T", LL.A_STREAM_JOINS),
    ("V", LL.ITEM_WOULD_VANISH),
])
def test_near_misses_stay_and_are_listed_with_their_reason(blk, why):
    _, reg, out, rep = _run()
    row = next(r for r in rep["not_moved"] if r["blk"] == blk)
    assert row["why"] == why
    for p in row["moved_sections"]:
        assert any(p in it.section_ids for it in out.values() if it.kind == "stream")


def test_only_the_two_loops_move():
    _, _, _, rep = _run()
    assert sorted(r["blk"] for r in rep["moved"]) == ["P", "U"]
    assert rep["counts"]["pieces_moved"] == 3 and rep["counts"]["joined_a_lake"] == 1


def test_the_pass_is_idempotent():
    g, _, out, _ = _run()
    rep2: dict = {}
    assert LL.move_lake_loops(out, g, rep2) == out
    assert rep2["moved"] == []


def test_the_input_registry_is_not_mutated():
    g, reg, out, _ = _run()
    assert "P:898" in reg["gnis:1"].section_ids and out is not reg


def test_mutation_loops_kept_leaves_them_in_the_river(monkeypatch):
    monkeypatch.setattr(LL, "LAKE_LOOPS_LEAVE_RIVER_ITEMS", False)
    _, reg, out, rep = _run()
    assert out == reg and rep["moved"] == []
    assert any(r["blk"] == "P" for r in rep["not_moved"])


def test_mutation_no_join_takes_them_out_of_the_river_only(monkeypatch):
    monkeypatch.setattr(LL, "LAKE_LOOPS_JOIN_THEIR_LAKE", False)
    _, _, out, _ = _run()
    assert "P:898" not in out["gnis:1"].section_ids
    assert out["wbk:L"].section_ids == ("lake:L",)


# ---- criterion 5: a loop never divides its lake (Kotcho Lake, 2026-11-15) ----------------------------------------

AREA_DEFS = [{"id": "ecological_reserves", "cut": "clean", "rejoin_m": 1000},
             {"id": "national_parks", "cut": "clean", "rejoin_m": 1000, "outside_items": {"np": ["wbk:L"]}},
             {"id": "provincial_parks", "kind": "park", "cut": False},
             {"id": "regions", "kind": "region", "cut": "first_last", "membership": "both_sides"}]
ER, NP, PARK, R7 = ("area:ecological_reserves:er", "area:national_parks:np", "area:park:shore_park", "area:region:7b")


def _areas(g, lake, *pieces_areas):
    """Stamp `in_areas`: the lake L, then the Sproat pieces P:898 / P:1295 in that order."""
    g.nodes["lake:L"] = replace(g.nodes["lake:L"], in_areas=tuple(lake))
    for nid, a in zip(("P:898", "P:1295"), pieces_areas):
        g.nodes[nid] = replace(g.nodes[nid], in_areas=tuple(a))


def _run_areas(lake, *pieces, defs=AREA_DEFS):
    g, reg = _world()
    _areas(g, lake, *pieces)
    rep: dict = {}
    return LL.move_lake_loops(reg, g, rep, area_defs=defs), rep


def test_a_loop_outside_its_lakes_reserve_stays_with_its_river():
    out, rep = _run_areas([ER, R7], [R7], [R7])                     # the Kotcho shape
    assert {"P:898", "P:1295"} <= set(out["gnis:1"].section_ids)
    assert out["wbk:L"].section_ids == ("lake:L",)
    row = next(r for r in rep["not_moved"] if r["blk"] == "P")
    assert row["why"] == LL.AREAS_DIFFER
    assert row["lake_areas"] == [ER, R7] and row["loop_areas"] == {"P:1295": [R7], "P:898": [R7]}
    assert rep["areas_checked"] is True and "P" not in {r["blk"] for r in rep["moved"]}


def test_one_piece_outside_holds_the_whole_loop():
    out, rep = _run_areas([ER, R7], [ER, R7], [R7])
    assert next(r for r in rep["not_moved"] if r["blk"] == "P")["loop_areas"] == {"P:1295": [R7]}
    assert "P:898" in out["gnis:1"].section_ids


def test_a_loop_in_its_lakes_areas_joins_it():
    out, rep = _run_areas([ER, R7], [ER, R7], [ER, R7])
    assert out["wbk:L"].section_ids == ("lake:L", "P:1295", "P:898")
    assert "membership_only_areas_differ" not in next(r for r in rep["moved"] if r["blk"] == "P")


def test_a_region_line_through_the_lake_holds_the_loop():
    _, rep = _run_areas([R7, "area:region:7a"], [R7], [R7])          # a lake in two regions, the loop in one
    assert next(r for r in rep["not_moved"] if r["blk"] == "P")["why"] == LL.AREAS_DIFFER


def test_a_membership_only_area_does_not_hold_the_loop_and_is_listed():
    out, rep = _run_areas([PARK, R7], [R7], [R7])                   # the Sproat shape: a shore park
    assert out["wbk:L"].section_ids == ("lake:L", "P:1295", "P:898")
    assert next(r for r in rep["moved"] if r["blk"] == "P")["membership_only_areas_differ"] == [PARK]


def test_an_area_the_lake_is_outside_of_by_ruling_is_not_compared():
    out, _ = _run_areas([NP, R7], [R7], [R7])                       # areas.json outside_items: wbk:L is outside np
    assert out["wbk:L"].section_ids == ("lake:L", "P:1295", "P:898")


def test_without_area_flags_the_pass_says_it_did_not_check():
    out, rep = _run_areas([ER, R7], [R7], [R7], defs=None)
    assert rep["areas_checked"] is False and "P:898" in out["wbk:L"].section_ids


def test_mutation_loops_may_divide_a_lake(monkeypatch):
    monkeypatch.setattr(LL, "LOOPS_NEVER_DIVIDE_A_LAKE", False)
    out, _ = _run_areas([ER, R7], [R7], [R7])
    assert "P:898" in out["wbk:L"].section_ids


def test_the_membership_only_prefixes_come_from_the_cut_policy():
    assert LL.membership_only_prefixes(AREA_DEFS) == ("area:park:",)
