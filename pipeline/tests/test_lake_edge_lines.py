"""T697 follow-up (owner 2026-11-12, verbatim: "Yes, read the real boundary"): a part end ALWAYS reads the real boundary
that stands there ("Region 8 boundary", "B.C. border", the cut's own name), never the lake; no part end reads
"Unnamed lake". Made where the end tokens are made, the bundle's spans (`pipeline.deliver.bundle.spans`):

  * a cut aliased onto ANOTHER water's lake edge keeps its own name (`end_token`; Gagnon Creek's fishing boundary
    signs, aliased onto an unnamed lake) — it was dropped for the bare lake token; and the `split` row names the cut,
    not the lake's label (`split_rows`);
  * where a region line or the B.C. border stands on an unnamed lake's edge (the atlas cut it once, as the lake), the
    pieces of the water either side differ in region or in-or-out of B.C., and both edges read that line
    (`lines_at_lake_edges`; Slack Creek, Regions 3 / 8 on a pond's outlet; Jones Creek re-entering B.C. through a lake
    on 60°N).

No data: synthetic graph nodes and an in-memory bundle. Each test fails on the code before this change (no alias read
at a foreign lake; no `lines_at_lake_edges`; the lake's label first in `split_rows`)."""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace as NS

import pytest

from pipeline.deliver.bundle import spans as SP


def _b(bid, kind="lake", m=0.0, aliases=()):
    return NS(boundary_id=bid, kind=kind, route_measure=m, label="", aliases=tuple(aliases))


NAMED = {"329083341": ("wbk:329083341", "lake")}.get


@pytest.mark.parametrize("bound,side,want", [
    # Gagnon Creek: the signs cut aliased onto an unnamed lake's outlet edge
    (_b("lake:329465442", aliases=("split:gagnon_creek__signs_400_m_above_williston_lake",)), "up",
     "gagnon_creek__signs_400_m_above_williston_lake"),
    # a named lake's edge carrying a cut: the cut's name, not "<lake> outlet"
    (_b("lake:329083341", aliases=("split:x_river__the_dam",)), "up", "x_river__the_dam"),
    # an auto cut aliased there names nothing: the lake stays
    (_b("lake:1", aliases=("split:gauge__08AB001", "split:area:3")), "down", "lake_inlet"),
    (_b("lake:1"), "up", "lake_outlet"),
])
def test_a_cut_aliased_onto_a_foreign_lake_edge_keeps_its_name(bound, side, want):
    assert SP.end_token(bound, side, lake_item=NAMED, own="gnis:7733") == want


def test_the_split_row_of_a_cut_aliased_onto_a_lake_edge_names_the_cut():
    resolved = [{"split_id": "gagnon__signs", "blk": "359565112", "route_measure": 2161.34, "anchor_type": "lake",
                 "label": "fishing boundary signs near the mouth (400 m above Williston Lake)"}]
    edges = [{"split_id": "gagnon__signs", "blk": "359565112", "route_measure": m, "label": "lake 329465442",
              "anchor_type": "lake_edge", "lake": "lake:329465442"} for m in (1835.0, 3659.0)]
    rows = {r[0]: r for r in SP.split_rows(resolved + edges, {"359565112": [("gnis:7733", 1)]})}
    assert rows["gagnon__signs"][1] == "fishing boundary signs near the mouth (400 m above Williston Lake)"


# ---- a line standing on an unnamed lake's edge ------------------------------------------------------------------------

def test_the_line_at_a_lake_edge_is_read_from_the_two_sides():
    L = SP.line_at_lake_edge
    assert L([(False, ("3",))], [(False, ("8",))]) == "region_line:8"           # Slack Creek: entered going upstream
    assert L([(False, ("8",))], [(False, ("3",))]) == "region_line:3"
    assert L([(False, ("3", "8"))], [(False, ("8",))]) == "region_line:3"      # a straddler below: the one it leaves
    assert L([(False, ("5",))], [(False, ("7a",))]) == "region_line:7A"
    assert L([(True, ())], [(False, ("6",))]) == "bc_border"                    # Jones Creek, back into B.C.
    assert L([(False, ("6",))], [(True, ())]) == "bc_border"
    assert L([(False, ("3",))], [(False, ("3",))]) is None                      # one region: only the lake stands there
    assert L([(True, ())], [(True, ())]) is None
    assert L([(False, ("3",))], []) is None                                     # the water's own end at the lake
    assert L([], [(False, ("3",))]) is None


def _piece(nid, blk, lo, hi, lower=None, upper=None):
    return NS(node_id=nid, kind="stream", blk=blk, down_m=lo, up_m=hi, length_m=hi - lo, wsc="",
              stream_order=1, stream_magnitude=1, lower_bound=lower, upper_bound=upper)


def _bundle(nodes, edges, regions, outside=()):
    """compute -> the span tables -> `lines_at_lake_edges`, as `build` runs them; -> {sid: (lo, hi)}."""
    ids = sorted(nodes)
    h = {n: i + 1 for i, n in enumerate(ids)}
    items = [(1, "gnis:27909", "stream", [h[n] for n in ids])]
    lake_edges: dict = {}
    rows = SP.compute(nodes, edges, h, items, {}, lake_edges=lake_edges)
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE span_end (eid INTEGER PRIMARY KEY, token TEXT NOT NULL);"
                     "CREATE TABLE section_span (sid INTEGER PRIMARY KEY, lo_m INTEGER, hi_m INTEGER, lo INTEGER, "
                     "hi INTEGER, off_stem INTEGER, shape INTEGER);"
                     "CREATE TABLE outside_bc (sid INTEGER PRIMARY KEY);")
    eid = {t: i + 1 for i, t in enumerate(sorted({r[3] for r in rows} | {r[4] for r in rows}))}
    db.executemany("INSERT INTO span_end VALUES (?,?)", [(i, t) for t, i in eid.items()])
    db.executemany("INSERT INTO section_span VALUES (?,?,?,?,?,?,?)",
                   [(s, a, b, eid[lo], eid[hi], o, sh) for s, a, b, lo, hi, o, sh in rows])
    db.executemany("INSERT INTO outside_bc VALUES (?)", [(h[n],) for n in outside])
    SP.lines_at_lake_edges(db, lake_edges, {h[n]: r for n, r in regions.items()})
    got = {s: (lo, hi) for s, lo, hi in db.execute(
        "SELECT s.sid, a.token, b.token FROM section_span s JOIN span_end a ON a.eid = s.lo "
        "JOIN span_end b ON b.eid = s.hi")}
    return {n: got[h[n]] for n in ids}


POND = _b("lake:329536042", m=2310.0)


def test_a_region_line_on_a_ponds_outlet_reads_the_region_line_at_both_edges():
    # Slack Creek: Region 3 below a 0.1 ha pond, Region 8 above it; no region cut on the line
    nodes = {"S:0": _piece("S:0", "S", 0, 2310, upper=POND), "S:2359": _piece("S:2359", "S", 2359, 8468, lower=POND)}
    got = _bundle(nodes, [], {"S:0": ("3",), "S:2359": ("8",)})
    assert got == {"S:0": ("mouth", "region_line:8"), "S:2359": ("region_line:8", "source")}


def test_the_border_on_a_lake_reads_the_border_at_both_edges():
    # Jones Creek: outside B.C. below a lake straddling 60°N, back inside above it; no second border cut
    lake = _b("lake:328961695", m=11051.0)
    nodes = {"J:9563": _piece("J:9563", "J", 9563, 11051, upper=lake),
             "J:11339": _piece("J:11339", "J", 11339, 11663, lower=lake)}
    got = _bundle(nodes, [], {"J:11339": ("6",)}, outside=("J:9563",))
    assert got == {"J:9563": ("mouth", "bc_border"), "J:11339": ("bc_border", "source")}


def test_an_unnamed_lake_with_no_line_on_it_stays_the_lake():
    # one region either side: the lake is the only thing there (and the display's guarded last resort says it)
    nodes = {"S:0": _piece("S:0", "S", 0, 2310, upper=POND), "S:2359": _piece("S:2359", "S", 2359, 8468, lower=POND)}
    got = _bundle(nodes, [], {"S:0": ("3",), "S:2359": ("3",)})
    assert got == {"S:0": ("mouth", "lake_outlet"), "S:2359": ("lake_inlet", "source")}


def test_a_water_beginning_at_an_unnamed_lake_keeps_its_lake_end():
    # Kanaka Creek: nothing of the water above the lake — its own head, said "Source" by the display
    nodes = {"K:0": _piece("K:0", "K", 0, 24767, upper=_b("lake:1", m=24767.0))}
    assert _bundle(nodes, [], {"K:0": ("2",)}) == {"K:0": ("mouth", "lake_outlet")}


def test_a_section_in_bc_with_no_region_stops_it():
    nodes = {"S:0": _piece("S:0", "S", 0, 2310, upper=POND), "S:2359": _piece("S:2359", "S", 2359, 8468, lower=POND)}
    with pytest.raises(SystemExit, match="in no region"):
        _bundle(nodes, [], {"S:0": ("3",)})
