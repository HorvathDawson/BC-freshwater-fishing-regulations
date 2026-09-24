"""Reach resolution (`pipeline.atlas.reach.extent`), driven through the review app's `entry_reaches`.

The helpers were lifted out of `curation-review/backend/reuse.py` so the app and the artifact builder
run one implementation; they now take the graph explicitly instead of reaching for a module cache.

This is what the curator sees highlighted on the map when they ask "what does this rule actually
cover", so it has to be exact AND stable: the same rule must resolve to the same reach on every page
load, a side channel must land on the correct side of the cut, and anything genuinely undecidable
must be surfaced rather than quietly included or dropped.

The fixtures mirror the real Chilliwack topology that exposed both defects — a mainstem blue line cut
once, with braid clusters hanging off each side.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pipeline.common.models import BoundaryKind, FlowEdge, NodeKind, SectionBoundary, StreamGraph, StreamNode

_BACKEND = Path(__file__).resolve().parents[2] / "curation-review" / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from pipeline.atlas.reach import extent as resolve

reuse = pytest.importorskip("reuse", reason="curation-review backend not importable")

MAIN = "100"          # the mainstem blue line
CUT = 1000.0          # the one curated cut on it


def _piece(nid: str, blk: str, down: float, up: float, *, lower=None, upper=None) -> StreamNode:
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, down_m=down, up_m=up,
                      length_m=up - down, lower_bound=lower, upper_bound=upper)


def _split_bound(split_id: str, m: float) -> SectionBoundary:
    return SectionBoundary(boundary_id=f"split:{split_id}", kind=BoundaryKind.split,
                           route_measure=m, label=split_id)


def _graph(nodes: list[StreamNode], edges: list[tuple[str, str]]) -> StreamGraph:
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, at_measure=0.0) for a, b in edges]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


@pytest.fixture
def braided(monkeypatch):
    """Mainstem cut at 1000 m, with two braid CLUSTERS that only touch one side each.

        lower piece 100:0  (0-1000, below the cut)      upper piece 100:1000 (1000-2000, above)
           |                                                |
           +-- b1 <-> b2   (a mutually-attached pair)       +-- u1 <-> u2
                                                            |
        straddler s1 attaches to BOTH pieces.

    b1/b2 and u1/u2 are the case a per-piece fixpoint cannot settle: each waits on the other, so both
    used to be reported as straddling even though every neighbour outside the pair agrees.
    """
    low = _piece(f"{MAIN}:0", MAIN, 0.0, CUT, upper=_split_bound("thecut", CUT))
    high = _piece(f"{MAIN}:1000", MAIN, CUT, 2000.0, lower=_split_bound("thecut", CUT))
    nodes = [low, high,
             _piece("200:0", "200", 0.0, 50.0), _piece("201:0", "201", 0.0, 50.0),   # b1, b2 (below)
             _piece("300:0", "300", 0.0, 50.0), _piece("301:0", "301", 0.0, 50.0),   # u1, u2 (above)
             _piece("400:0", "400", 0.0, 50.0)]                                      # s1 (straddles)
    edges = [("200:0", "201:0"), ("201:0", f"{MAIN}:0"),          # pair below -> lower piece only
             ("300:0", "301:0"), ("301:0", f"{MAIN}:1000"),       # pair above -> upper piece only
             ("400:0", f"{MAIN}:0"), ("400:0", f"{MAIN}:1000")]   # s1 touches both sides
    g = _graph(nodes, edges)
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    return {n.node_id for n in nodes}


def test_braid_cluster_below_the_cut_is_placed_below(braided):
    """b1/b2 hang off the lower piece only, so `upstream_of` must exclude BOTH."""
    inside, straddling = resolve._by_measure(reuse._graph(), braided, MAIN, CUT, float("inf"))
    assert "200:0" not in inside and "201:0" not in inside
    assert "200:0" not in straddling and "201:0" not in straddling


def test_braid_cluster_above_the_cut_is_placed_above(braided):
    """u1/u2 hang off the upper piece only, so `upstream_of` must include BOTH."""
    inside, straddling = resolve._by_measure(reuse._graph(), braided, MAIN, CUT, float("inf"))
    assert {"300:0", "301:0"} <= inside
    assert not ({"300:0", "301:0"} & straddling)


def test_a_real_straddler_is_still_reported(braided):
    """The whole point of the `unclassified` bucket: a channel attached on both sides stays there."""
    inside, straddling = resolve._by_measure(reuse._graph(), braided, MAIN, CUT, float("inf"))
    assert straddling == {"400:0"}
    assert "400:0" not in inside


def test_upstream_and_downstream_partition_the_water_exactly(braided):
    """The two halves of one cut must not overlap, and together with the straddlers must cover
    everything — the invariant a curator relies on when confirming a closure."""
    up, up_s = resolve._by_measure(reuse._graph(), braided, MAIN, CUT, float("inf"))
    dn, dn_s = resolve._by_measure(reuse._graph(), braided, MAIN, 0.0, CUT)
    assert not (up & dn), "a section cannot be both above and below one cut"
    assert up | dn | up_s == braided
    assert up_s == dn_s == {"400:0"}


def test_cut_is_found_on_the_principal_channel_not_whichever_came_first(monkeypatch):
    """One split id cut across a braid names several blue lines. The reach must be measured on the
    line carrying the most length in the item, and must not depend on set-iteration order."""
    main_lo = _piece(f"{MAIN}:0", MAIN, 0.0, CUT, upper=_split_bound("thecut", CUT))
    main_hi = _piece(f"{MAIN}:1000", MAIN, CUT, 5000.0, lower=_split_bound("thecut", CUT))
    side_lo = _piece("900:0", "900", 0.0, 20.0, upper=_split_bound("thecut", 20.0))
    side_hi = _piece("900:20", "900", 20.0, 40.0, lower=_split_bound("thecut", 20.0))
    g = _graph([main_lo, main_hi, side_lo, side_hi], [("900:20", f"{MAIN}:1000")])
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    universe = {n for n in g.nodes}
    for _ in range(20):                        # order-independent: same answer every time
        assert resolve._cut_at(reuse._graph(), {"split:thecut"}, universe) == (MAIN, CUT, [])


def test_a_cut_crossing_one_line_twice_is_reported_as_ambiguous(monkeypatch):
    """An area boundary can cross the same stream twice (Pinnacles Park on Baker Creek), so ONE split
    id lands at two measures and "upstream of it" has two honest readings. Pick the lower one, and
    hand back the other so the curator is told rather than silently given one of them."""
    a = _piece(f"{MAIN}:0", MAIN, 0.0, 700.0, upper=_split_bound("park", 700.0))
    b = _piece(f"{MAIN}:700", MAIN, 700.0, 800.0, lower=_split_bound("park", 700.0),
               upper=_split_bound("park", 800.0))
    c = _piece(f"{MAIN}:800", MAIN, 800.0, 900.0, lower=_split_bound("park", 800.0))
    g = _graph([a, b, c], [])
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    blk, used, also = resolve._cut_at(reuse._graph(), {"split:park"}, set(g.nodes))
    assert (blk, used) == (MAIN, 700.0)
    assert also == [800.0]


def test_cut_not_on_the_scoped_water_is_unresolvable(monkeypatch):
    g = _graph([_piece(f"{MAIN}:0", MAIN, 0.0, 100.0)], [])
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    assert resolve._cut_at(reuse._graph(), {"split:elsewhere"}, set(g.nodes)) is None


# --- a reach that spans a name change ----------------------------------------------------------

@pytest.fixture
def two_rivers(monkeypatch):
    """An upper river (blk 100) flowing into a lower one (blk 200) — a name change at the junction,
    the way the Chilliwack becomes the Vedder.

        100:0 --[cut A @1000]-- 100:1000  ->  200:0 --[cut B @500]-- 200:500
        (upper river, source end)              (lower river, mouth end)

    Water runs 100:1000 -> 100:0 -> 200:500 -> 200:0, so "between A and B" is the lower half of the
    upper river plus the upper half of the lower one — and no single route measure spans it."""
    up_hi = _piece("100:1000", "100", 1000.0, 2000.0, lower=_split_bound("cutA", 1000.0))
    up_lo = _piece("100:0", "100", 0.0, 1000.0, upper=_split_bound("cutA", 1000.0))
    lo_hi = _piece("200:500", "200", 500.0, 1500.0, lower=_split_bound("cutB", 500.0))
    lo_lo = _piece("200:0", "200", 0.0, 500.0, upper=_split_bound("cutB", 500.0))
    g = _graph([up_hi, up_lo, lo_hi, lo_lo],
               [("100:1000", "100:0"), ("100:0", "200:500"), ("200:500", "200:0")])
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    return set(g.nodes)


def test_between_two_cuts_on_different_blue_lines(two_rivers):
    sec, straddling = resolve._between_across_lines(reuse._graph(), 
        two_rivers, ("100", 1000.0, []), ("200", 500.0, []))
    assert sec == {"100:0", "200:500"}
    assert straddling == set()


def test_between_across_lines_is_order_independent(two_rivers):
    """The curator writes the two cuts in whichever order the synopsis does."""
    a = resolve._between_across_lines(reuse._graph(), two_rivers, ("100", 1000.0, []), ("200", 500.0, []))
    b = resolve._between_across_lines(reuse._graph(), two_rivers, ("200", 500.0, []), ("100", 1000.0, []))
    assert a[0] == b[0]


def test_between_across_lines_refuses_parallel_branches(monkeypatch):
    """Two tributaries joining the same river are neither above nor below each other, so there is no
    reach "between" them — better no answer than a plausible wrong one."""
    main = _piece("100:0", "100", 0.0, 5000.0)
    t1 = _piece("200:0", "200", 0.0, 100.0, upper=_split_bound("cutA", 100.0))
    t1b = _piece("200:100", "200", 100.0, 200.0, lower=_split_bound("cutA", 100.0))
    t2 = _piece("300:0", "300", 0.0, 100.0, upper=_split_bound("cutB", 100.0))
    t2b = _piece("300:100", "300", 100.0, 200.0, lower=_split_bound("cutB", 100.0))
    g = _graph([main, t1, t1b, t2, t2b],
               [("200:100", "200:0"), ("200:0", "100:0"), ("300:100", "300:0"), ("300:0", "100:0")])
    monkeypatch.setattr(reuse, "_graph", lambda: g)
    assert resolve._between_across_lines(reuse._graph(), set(g.nodes), ("200", 100.0, []), ("300", 100.0, [])) is None


# --- binding a split that exists only as an alias ------------------------------------------------

def test_a_reach_can_be_bound_by_an_aliased_split(monkeypatch):
    """A split that landed inside a lake run (Duncan Dam) is recorded as another NAME for that lake's
    boundary — there is no boundary whose own id is `duncan_river__duncan_dam`. Matching the bound id
    against `b.id` alone therefore found nothing and the reach stayed unresolvable even after the
    alias reached the registry."""
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem

    lake = SectionBoundary(boundary_id="lake:99", kind=BoundaryKind.lake, route_measure=1000.0,
                           label="Duncan Lake")
    below = _piece(f"{MAIN}:0", MAIN, 0.0, 1000.0, upper=lake)
    above = _piece(f"{MAIN}:5000", MAIN, 5000.0, 6000.0, lower=lake)
    g = _graph([below, above], [])
    monkeypatch.setattr(reuse, "_graph", lambda: g)

    item = RegistryItem(
        id="gnis:1", name="Duncan River", kind="stream",
        section_ids=(below.node_id, above.node_id),
        boundaries=(RegistryBoundary(id="duncan_river__duncan_lake", label="Duncan Lake",
                                     kind="lake", ref="lake:99", wbk="99",
                                     aliases=("split:duncan_river__duncan_dam",)),),
    )
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:1": item})

    # bound by the ALIAS, which is not any boundary's own id
    res = reuse.resolve_extent(["gnis:1"], {"op": "downstream_of",
                                            "splits": ["duncan_river__duncan_dam"]})
    assert res is not None, "the alias must resolve like a real cut-point"
    assert res["sections"] == [f"{MAIN}:0"]

    # and by the boundary's own id, to the same place
    same = reuse.resolve_extent(["gnis:1"], {"op": "downstream_of",
                                             "splits": ["duncan_river__duncan_lake"]})
    assert same == res, "the two names must select the same water"


# --- the entry's own stretch ---------------------------------------------------------------------

def test_a_rows_scope_clips_every_rule_in_it(braided, monkeypatch):
    """The synopsis qualifies a row in its NAME — "ADAMS RIVER (downstream of Adams Lake)" — and the
    rules inside it are written relative to that stretch, so a rule reading "whole" means the whole of
    THIS row. Unapplied, the two Adams rows resolved to the same river, and so did the Fraser's four
    regional rows: `whole` in the region 5 row and `whole` in the region 7 row came back identical."""
    from pipeline.common.models.registry import RegistryItem

    item = RegistryItem(id="gnis:1", name="Adams River", kind="stream",
                        section_ids=tuple(sorted(braided)))
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:1": item})
    entry = {"entry_id": "gnis:1#below", "name": "ADAMS RIVER", "matched": ["gnis:1"],
             "extents": [{"op": "downstream_of", "splits": ["thecut"]}],
             "rules": [{"rule_id": "r1", "extents": [{"op": "whole"}]}]}
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("1", entry)])
    monkeypatch.setattr(reuse, "_match_and_item", lambda e: (None, item))
    monkeypatch.setattr(reuse, "_covered_ids", lambda e, mr: ["gnis:1"])

    got = reuse.entry_reaches("gnis:1#below")
    whole = got["rules"]["r1"][0]["sections"]
    assert set(whole) == set(got["scope_sections"]), "a 'whole' rule means the whole of this row"
    assert f"{MAIN}:1000" not in whole, "the stretch above the cut is not part of this row"
    assert f"{MAIN}:0" in whole


def test_an_unresolvable_scope_clips_nothing_rather_than_everything(braided, monkeypatch):
    """Silently returning empty reaches for every rule would read as "this row regulates nothing"."""
    from pipeline.common.models.registry import RegistryItem

    item = RegistryItem(id="gnis:1", name="Adams River", kind="stream",
                        section_ids=tuple(sorted(braided)))
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:1": item})
    entry = {"entry_id": "gnis:1#x", "name": "ADAMS RIVER", "matched": ["gnis:1"],
             "extents": [{"op": "downstream_of", "splits": ["nosuchcut"]}],
             "rules": [{"rule_id": "r1", "extents": [{"op": "whole"}]}]}
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("1", entry)])
    monkeypatch.setattr(reuse, "_match_and_item", lambda e: (None, item))
    monkeypatch.setattr(reuse, "_covered_ids", lambda e, mr: ["gnis:1"])

    got = reuse.entry_reaches("gnis:1#x")
    assert set(got["rules"]["r1"][0]["sections"]) == braided


def test_a_broken_scope_is_reported_not_swallowed(braided, monkeypatch):
    """`_scope_sections` returned None both for "no scope" and for "scope is broken", and the caller
    read None as "do not clip". A regional row whose boundary stopped resolving would silently widen
    from its region to the whole river — fail-open, in the direction that tells someone a rule applies
    where it does not."""
    from pipeline.common.models.registry import RegistryItem

    item = RegistryItem(id="gnis:1", name="Fraser River", kind="stream",
                        section_ids=tuple(sorted(braided)))
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:1": item})
    entry = {"entry_id": "gnis:1#r3", "name": "FRASER RIVER", "matched": ["gnis:1"],
             "extents": [{"op": "between", "splits": ["thecut", "nosuchcut"]}],
             "rules": [{"rule_id": "r1", "extents": [{"op": "whole"}]}]}
    monkeypatch.setattr(reuse, "_all_entries", lambda: [("3", entry)])
    monkeypatch.setattr(reuse, "_match_and_item", lambda e: (None, item))
    monkeypatch.setattr(reuse, "_covered_ids", lambda e, mr: ["gnis:1"])

    got = reuse.entry_reaches("gnis:1#r3")
    assert got["scope_unresolved"], "a scope that cannot resolve must be surfaced"


# --------------------------------------------------------------------------- #
# The payload has to survive JSON. `upstream_of` has no upper bound.
# --------------------------------------------------------------------------- #

def test_an_unbounded_window_survives_json():
    """`resolve_extent` reports the measure window an extent resolved to, and `upstream_of`'s
    upper bound is `INF`. `json.dumps` refuses non-finite floats, so the whole /reaches payload
    raised `ValueError: Out of range float values are not JSON compliant` — a bare 500 that took
    out the map for **82 entries**, every rule with an `upstream_of` extent.

    `null` carries the same meaning ("unbounded on that side") and serialises."""
    import json
    import math

    payload = {"rules": {"r1": [{"sections": ["a:0"], "window": ["blk", 100.0, math.inf]}]}}
    with pytest.raises(ValueError):
        json.dumps(payload, allow_nan=False)          # the bug, verbatim

    safe = reuse._json_safe(payload)
    json.dumps(safe, allow_nan=False)                 # must not raise
    assert safe["rules"]["r1"][0]["window"] == ["blk", 100.0, None]


def test_json_safe_leaves_ordinary_values_alone():
    """It must not become a general-purpose mangler: only non-finite floats change."""
    src = {"a": 1, "b": 2.5, "c": "x", "d": None, "e": [1, {"f": 0.0}], "g": True}
    assert reuse._json_safe(src) == {"a": 1, "b": 2.5, "c": "x", "d": None,
                                     "e": [1, {"f": 0.0}], "g": True}


def test_nan_is_also_stripped():
    import math
    assert reuse._json_safe({"x": math.nan}) == {"x": None}


# --- what a lake cut-point does, and does not, bound ---------------------------------------------

def test_upstream_of_a_lake_runs_to_the_headwaters_and_the_polygon_is_a_separate_extent(monkeypatch):
    """`upstream_of` a LAKE cut does NOT stop at the lake — it runs on to the headwaters.

    Written down because the opposite was assumed once and a redundant `include_boundary_lakes` flag
    was added to `Extent` on the strength of it. What a lake cut actually leaves out is the lake
    POLYGON, which is a different registry item; and since a rule's `extents` are UNIONed, including
    it needs no new machinery — a second extent scoped to the polygon already does it.
    """
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem

    lake = SectionBoundary(boundary_id="lake:77", kind=BoundaryKind.lake, route_measure=1000.0,
                           label="Sumas River")
    below = _piece(f"{MAIN}:0", MAIN, 0.0, 1000.0, upper=lake)
    above = _piece(f"{MAIN}:5000", MAIN, 5000.0, 6000.0, lower=lake)
    head = _piece(f"{MAIN}:6000", MAIN, 6000.0, 9000.0)
    g = _graph([below, above, head], [])
    monkeypatch.setattr(reuse, "_graph", lambda: g)

    river = RegistryItem(
        id="gnis:1", name="Sumas River", kind="stream",
        section_ids=(below.node_id, above.node_id, head.node_id),
        boundaries=(RegistryBoundary(id="sumas_river__sumas_river", label="Sumas River",
                                     kind="lake", ref="lake:77", wbk="77"),))
    lake_item = RegistryItem(id="wbk:77", name="Sumas River", kind="lake", section_ids=("lake:77:0",))
    monkeypatch.setattr(reuse, "_registry", lambda: {"gnis:1": river, "wbk:77": lake_item})

    up = reuse.resolve_extent(["gnis:1"], {"op": "upstream_of",
                                           "splits": ["sumas_river__sumas_river"]})
    assert up["sections"] == [f"{MAIN}:5000", f"{MAIN}:6000"], "must not stop at the lake"

    # the polygon, when the rule wants it, is just another extent — extents are a UNION
    poly = reuse.resolve_extent(["gnis:1", "wbk:77"], {"op": "whole", "item_id": "wbk:77"})
    assert poly["sections"] == ["lake:77:0"]
