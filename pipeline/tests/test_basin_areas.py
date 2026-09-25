"""WATERSHEDS BY FWA CODE, and the pieces of a river no row could place (round W, 2026-09-24).

1. A zone rule printed as a WHOLE WATERSHED covers the watershed ∩ its region — `within
   area:basin:<code>` — including water the tributary walk cannot reach (user ruling). A basin the
   registry did not mint (the Chilcotin, the Peace) resolves from the graph by the same definition.
2. A DETACHED braid piece — one touching only unnamed lakes on its own blue line — is placed by that
   line's nearest placed pieces; before, every row on the Peace dropped three of them.
3. What a row's own scope cannot place is reported (`scope_unclassified`), never silently dropped.
"""

from __future__ import annotations

import json

import pytest

from pipeline.atlas.registry.basins import (BASIN_NAMES, basin_code, basin_members, basin_name,
                                           in_basin)
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.graph import FlowEdge


class _Reg:
    def __init__(self, ids):
        self.section_ids = tuple(ids)


# ------------------------------------------------------------------ the code prefix

def test_a_basin_id_is_a_code_prefix_or_nothing():
    assert basin_code("area:basin:100-") == "100-"
    assert basin_code("area:basin:100-342455-") == "100-342455-"
    for bad in ("area:basin:100", "area:basin:10-", "area:basin:100-34245-", "area:region:5",
                "area:basin:"):
        assert basin_code(bad) is None, bad


def test_membership_reads_trimmed_codes():
    """The graph stores codes trimmed of their zero groups: the Fraser mainstem is `100`, the
    Chilcotin `100-342455`. A member IS the prefix's code or starts with the prefix — and a code that
    merely shares digits (`100-3424551`) is not one."""
    assert in_basin("100", "100-") and in_basin("100-342455-001234", "100-")
    assert in_basin("100-342455", "100-342455-") and in_basin("100-342455-1", "100-342455-")
    assert not in_basin("100-3424551", "100-342455-")
    assert not in_basin("1000", "100-") and not in_basin("", "100-") and not in_basin(None, "100-")


def _nodes(**codes):
    g = StreamGraph()
    g.nodes = {nid: StreamNode(node_id=nid, kind=NodeKind.stream, blk="1", down_m=0.0, up_m=1.0,
                               length_m=1.0, wsc=w) for nid, w in codes.items()}
    return g


def test_an_unminted_basin_resolves_from_the_graph():
    """`area:basin:100-342455-` is not a registry item; the resolver answers it from the graph,
    with `within_area` and `feature_types` applied as for any area. A malformed code FAILS."""
    from pipeline.atlas.reach.extent import resolve_extent
    g = _nodes(chil="100-342455", trib="100-342455-000123", fraser="100", other="200-948755")
    reg = {"area:region:5": _Reg(["chil", "trib", "fraser"])}
    got = resolve_extent(reg, g, [], {"op": "within", "area_id": "area:basin:100-342455-",
                                      "within_area": "area:region:5"})
    assert got["sections"] == ["chil", "trib"]
    reasons: list = []
    assert resolve_extent(reg, g, [], {"op": "within", "area_id": "area:basin:100-34245-"},
                          reasons) is None
    assert reasons[0][0] == "area_id_not_in_registry"


def test_a_minted_basin_wins_over_the_graph():
    """The registry's item, when there is one, is the answer — the graph is only asked for a basin
    the registry did not mint."""
    from pipeline.atlas.reach.extent import area_sections
    g = _nodes(a="100", b="100-000001")
    assert area_sections({"area:basin:100-": _Reg(["x"])}, g, "area:basin:100-") == {"x"}
    assert area_sections({}, g, "area:basin:100-") == {"a", "b"}
    assert area_sections({}, g, "area:region:9") is None


def test_every_basin_the_corpus_names_has_a_name():
    """A reader is told "Fraser River watershed", never "100-" (`read._area_words`)."""
    from pipeline.deliver.bundle.read import _area_words
    from pipeline.regs.parsing import io
    named = set()
    for e in io.read_entries_dir().values():
        for r in e.get("rules") or []:
            for x in r.get("extents") or []:
                for a in [x.get("area_id"), x.get("within_area"), *(x.get("outside_areas") or [])]:
                    if a and basin_code(a):
                        named.add(a)
    assert named, "the corpus names no basin — the ruling-2 rules are gone"
    for a in sorted(named):
        assert basin_name(a), f"{a} has no name in registry.basins.BASIN_NAMES"
        assert _area_words(a) == BASIN_NAMES[basin_code(a)]


# ------------------------------------------------------------------ the ruled rules

#: (entry, rule) -> the basins it binds, all limited to the rule's region; the ruling's list.
WHOLE_WATERSHEDS = {
    ("z5:spring_stream_closure", "spring_stream_closure.r1"): ["area:basin:100-"],
    ("z6:iskut_fraser_closure", "iskut_fraser_closure.r2"): ["area:basin:100-"],
    ("z6:skeena_nass_bait_ban", "skeena_nass_bait_ban.r1"): ["area:basin:400-"],
    ("z6:skeena_nass_bait_ban", "skeena_nass_bait_ban.r2"): ["area:basin:500-"],
    ("z6:trout_char_quota", "trout_char_quota.r8"): ["area:basin:100-", "area:basin:400-"],
    ("z7b:trout_char_quota", "trout_char_quota.r9"): ["area:basin:200-948755-"],
    ("z5:steelhead_management", "steelhead_management.r1"): ["area:basin:100-342455-"],
}


@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.parsing import io
    return {e["entry_id"]: e for e in io.read_entries_dir().values()}


def test_whole_watershed_rules_bind_the_basin_in_their_region_and_do_not_walk(corpus):
    """The walk from a named river missed water with no outflow and water behind connectors; the
    basin holds it by definition. Each rule is area-ranked now (`read.source_of` -> Scope.area),
    below the water rows, so a water's own row still speaks over it; and it does not walk, or its
    sections would speak at the `inherited` rung and outrank the water rows' tributaries."""
    from pipeline.deliver.bundle.read import Scope, source_of
    for (eid, rid), basins in WHOLE_WATERSHEDS.items():
        e = corpus[eid]
        r = next(x for x in e["rules"] if x["rule_id"] == rid)
        region = eid.split(":", 1)[0][1:]
        assert [x["area_id"] for x in r["extents"]] == basins, (eid, rid)
        assert all(x["op"] == "within" and x["within_area"] == f"area:region:{region}"
                   for x in r["extents"]), (eid, rid)
        assert not r.get("includes_tributaries") and not r.get("tributaries_only"), (eid, rid)
        src = source_of({"entry": eid, "rule": rid, "extents": r["extents"],
                         "entry_name": e.get("name")})
        assert src.scope is Scope.area, (eid, rid, src)


def test_the_spring_closure_still_spares_the_fraser_mainstem(corpus):
    """"EXCEPT the mainstem of the Fraser River" was the walk's `tributaries_only`; without a walk
    it is the Fraser taken back out of the basin."""
    r = next(x for x in corpus["z5:spring_stream_closure"]["rules"]
             if x["rule_id"] == "spring_stream_closure.r1")
    assert r["extents"][0]["outside_items"] == ["gnis:39325"]
    assert r["extents"][0]["feature_types"] == ["stream"]


# ------------------------------------------------------------------ detached braid pieces

def _braid_graph():
    """A mainstem M (0..3000, cut at 1500) and a side line S threading two unnamed lakes:

        S:0 (0..100) - lake L1 - S:200 (200..300) - lake L2 - S:400 (400..500)
        S:0 and S:400 join M below the cut; S:200 touches only the lakes.
    """
    g = StreamGraph()

    def node(nid, blk, lo, hi, kind=NodeKind.stream):
        return StreamNode(node_id=nid, kind=kind, blk=blk, down_m=lo, up_m=hi, length_m=hi - lo)
    g.nodes = {"M:0": node("M:0", "M", 0.0, 1500.0), "M:1500": node("M:1500", "M", 1500.0, 3000.0),
               "S:0": node("S:0", "S", 0.0, 100.0), "S:200": node("S:200", "S", 200.0, 300.0),
               "S:400": node("S:400", "S", 400.0, 500.0),
               "lake:1": node("lake:1", "", 0.0, 0.0, NodeKind.lake),
               "lake:2": node("lake:2", "", 0.0, 0.0, NodeKind.lake)}
    flow = [("M:1500", "M:0"), ("S:400", "lake:2"), ("lake:2", "S:200"), ("S:200", "lake:1"),
            ("lake:1", "S:0"), ("S:0", "M:0"), ("M:0", "S:400")]
    g.edges = [FlowEdge(from_node=a, to_node=b, at_measure=0.0, x=0.0, y=0.0, kind="continuation")
               for a, b in flow]
    for i, e in enumerate(g.edges):
        g.down_adj.setdefault(e.from_node, []).append(i)
        g.up_adj.setdefault(e.to_node, []).append(i)
    return g, {"M:0", "M:1500", "S:0", "S:200", "S:400"}      # the lakes are no part of the river


def test_a_detached_piece_goes_where_its_own_line_puts_it():
    from pipeline.atlas.reach.extent import _by_measure
    g, universe = _braid_graph()
    below, strad = _by_measure(g, universe, "M", 0.0, 1500.0)
    assert below == {"M:0", "S:0", "S:200", "S:400"} and not strad
    above, strad = _by_measure(g, universe, "M", 1500.0, float("inf"))
    assert above == {"M:1500"} and not strad, "a piece bracketed outside is outside, not reported"


def test_a_detached_piece_with_one_side_unplaced_stays_unplaceable():
    """Only when BOTH its line's neighbours are placed, on the same side, is it placed. Mutation
    guard: placing on one bracket alone would put S:200 inside here."""
    from pipeline.atlas.reach.extent import _by_measure
    g, universe = _braid_graph()
    universe = universe - {"S:400"}
    below, strad = _by_measure(g, universe, "M", 0.0, 1500.0)
    assert "S:200" in strad and "S:200" not in below


# ------------------------------------------------------------------ the row's own scope

def test_what_a_rows_scope_cannot_place_is_reported(monkeypatch):
    from pipeline.atlas.reach import build as B
    calls = []

    def fake(reg, g, covered, ex, reasons=None):
        calls.append(ex)
        return {"sections": ["a"], "unclassified": ["x", "a"]}
    monkeypatch.setattr(B._resolve, "resolve_extent", fake)
    e = {"entry_id": "r7:x@7-1", "extents": [{"op": "between", "splits": ["p", "q"]},
                                             {"op": "whole"}]}
    assert B.scope_unclassified(e, [], None, None, {"a"}) == ["x"]
    assert len(calls) == 1, "a whole/within scope places everything it names"


# ------------------------------------------------------------------ against the atlas (slow)

@pytest.mark.slow
def test_every_named_basin_is_its_rivers_code_and_a_minted_basin_matches_the_graph():
    from pipeline.atlas.registry import load_registry
    from pipeline.atlas.registry.basins import BASIN_RIVERS
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    build = GENERATED.require_build()
    reg = load_registry(str(build / "registry.json"))
    g = read_artifact(str(build / "graph.pkl"))
    for code, river in BASIN_RIVERS.items():
        codes = {g.nodes[s].wsc for s in reg[river].section_ids if s in g.nodes}
        assert code[:-1] in codes, (code, river, sorted(codes)[:3])
        assert code in BASIN_NAMES
    for code in ("100-", "400-"):
        assert set(basin_members(g.nodes.values(), code)) == set(
            reg[f"area:basin:{code}"].section_ids)
