"""T690: Whiteswan Lake's inlet & outlet streams, as the owner reads the row (2026-11-12).

Book, printed p.40 (PDF p.42), row "WHITESWAN LAKE'S INLET & OUTLET STREAMS" (4-24): "No Fishing EXCEPT the outlet
stream downstream of the falls 2.4 km downstream of Whiteswan Lake, No Fishing Aug 1-Mar 31, rainbow trout daily quota
= 5 (catch and release all other species) when open".

Owner, verbatim: "It is this (no Fishing EXCEPT the outlet stream downstream of the falls 2.4 km downstream of Whiteswan
Lake, No Fishing Aug 1-Mar 31, rainbow ) so not the inlet. The outlet is below the falls. Inlet is all no fishing (every
inlet) trout daily quota = 5 (catch and release all other species) when open)".

So: every inlet stream and the outlet from the lake down to the falls are No Fishing all year; the outlet BELOW the
falls is No Fishing Aug 1-Mar 31 and, when open, rainbow trout 5 a day, every other game fish released.

The world here is Whiteswan's shape in the atlas: the outlet blue line W cut at the falls (W:0 below, W:599 lake to
falls), the lake L, the same line above the lake (W:8623, an inlet fed by Moose Lake M), one inlet per kind the row
names (Inlet Creek, gnis; an unnamed inlet named "Whiteswan Lake inlet stream" by name_variants, wsc-keyed) and a
creek T feeding Inlet Creek (a tributary of an inlet, not an inlet). The REAL corpus row is bound on it.
"""
from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import BoundaryKind, FlowEdge, NodeKind, SectionBoundary, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.regs.parsing.io import read_all_entries
from pipeline.tests import under_test

EID = "r4:whiteswan_lake_s_inlet_outlet_streams@4-24"
FALLS = "whiteswan_lake_s_inlet_outlet_streams__whiteswan_outlet_falls"
OUTLET = "wsc:300-625474-814939-312002"
INLET_CREEK, CAVE_CREEK = "gnis:10239", "gnis:11699"
#: the 12 unnamed inlets name_variants names "Whiteswan Lake inlet stream" (each its own FWA code)
UNNAMED_INLET_BLKS = {
    "356410751": "454095", "356421702": "746382", "356444314": "694181", "356475930": "311267",
    "356485246": "516126", "356498612": "564682", "356510138": "521868", "356517928": "523969",
    "356526594": "626721", "356527622": "460285", "356544529": "431565", "356559006": "323030",
}
S, L = NodeKind.stream, NodeKind.lake


def _entry():
    return read_all_entries()[EID]


def _rule(rid):
    return next(r for r in _entry()["rules"] if r["rule_id"] == f"whiteswan_lakes_inlet_outlet_streams.{rid}")


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, wsc="", lower=None, upper=None):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, down_m=lo, up_m=hi, length_m=hi - lo,
                      stream_order=3, lower_bound=lower, upper_bound=upper)


@pytest.fixture
def whiteswan():
    falls = SectionBoundary(f"split:{FALLS}", BoundaryKind.split, 599.2, "the falls")
    lake_edge = SectionBoundary("lake:329247790", BoundaryKind.lake, 3023.5, "Whiteswan Lake")
    unnamed = sorted(UNNAMED_INLET_BLKS)[0]
    nodes = [
        _n("W:0", blk="W", lo=0, hi=599.2, wsc="300-625474-814939-312002", upper=falls),
        _n("W:599", blk="W", lo=599.2, hi=3023.5, wsc="300-625474-814939-312002", lower=falls, upper=lake_edge),
        _n("lake:329247790", L, wsc="300-625474-814939-312002"),
        _n("W:8623", blk="W", lo=8623.3, hi=9727.4, wsc="300-625474-814939-312002"),
        _n("lake:M", L, wsc="300-625474-814939-312002"),
        _n("IC", blk="IC", lo=816, hi=10791, wsc="300-625474-814939-312002-380497"),
        _n("T", blk="T", hi=1975, wsc="300-625474-814939-312002-380497-577604"),
        _n("U", blk=unnamed, lo=522, hi=1683, wsc=f"300-625474-814939-312002-{UNNAMED_INLET_BLKS[unnamed]}"),
        _n("WR", blk="WR", lo=22000, hi=24000, wsc="300-625474-814939"),
    ]
    edges = [("W:0", "WR", "confluence", 22960), ("W:599", "W:0", "continuation", 599.2),
             ("lake:329247790", "W:599", "lake_out", 3023.5), ("W:8623", "lake:329247790", "lake_in", 8623.3),
             ("lake:M", "W:8623", "lake_out", 9727.4), ("IC", "lake:329247790", "lake_in", 816),
             ("T", "IC", "confluence", 5000), ("U", "lake:329247790", "lake_in", 522)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {
        OUTLET: RegistryItem(id=OUTLET, name="Whiteswan Lake outlet stream", kind="stream",
                             section_ids=("W:0", "W:599", "W:8623")),
        "wbk:329247790": RegistryItem(id="wbk:329247790", name="Whiteswan Lake", kind="lake",
                                      section_ids=("lake:329247790",)),
        INLET_CREEK: RegistryItem(id=INLET_CREEK, name="Inlet Creek", kind="stream", section_ids=("IC",)),
        f"wsc:300-625474-814939-312002-{UNNAMED_INLET_BLKS[unnamed]}": RegistryItem(
            id=f"wsc:300-625474-814939-312002-{UNNAMED_INLET_BLKS[unnamed]}", name="Whiteswan Lake inlet stream",
            kind="stream", section_ids=("U",)),
        "gnis:white": RegistryItem(id="gnis:white", name="White River", kind="stream", section_ids=("WR",)),
    }
    return g, reg


def _bind(world, rid):
    g, reg = world
    b, _ = build_reach(_entry(), _rule(rid), reg, g, owned={}, regional=False, outside=frozenset())
    return set(b.sections)


def test_every_inlet_and_the_outlet_above_the_falls_are_closed(whiteswan):
    """r1, "No Fishing EXCEPT the outlet stream downstream of the falls": the outlet from the lake to the falls, the
    line above the lake, Inlet Creek and the unnamed inlet. Never the outlet below the falls, the lake, an inlet's own
    tributary or Moose Lake upstream."""
    assert _bind(whiteswan, "r1") == {"W:599", "W:8623", "IC", "U"}


@pytest.mark.parametrize("rid", ["r2", "r3", "r4"])
def test_the_exception_binds_only_the_outlet_below_the_falls(whiteswan, rid):
    assert _bind(whiteswan, rid) == {"W:0"}


def test_the_closure_is_all_year_and_the_exception_is_seasonal_with_its_quota():
    r1, r2, r3, r4 = (_rule(r) for r in ("r1", "r2", "r3", "r4"))
    assert r1["species"] == ["ALL_GAME_FISH"] and r1["take"] == 0 and r1["may_target"] is False and not r1.get("when")
    assert r2["may_target"] is False and r2["when"] == {"dates": [
        {"from_month": 8, "from_day": 1, "to_month": 3, "to_day": 31}]}
    assert r3["species"] == ["RB"] and r3["take"] == 5
    assert r4["take"] == 0 and r4["may_target"] is True and set(r4["species_except"]) == {"RB", "CRA"}


def test_the_closure_names_every_inlet_of_the_lake():
    """The 15 streams that flow straight into Whiteswan Lake (lake_in edges of lake:329247790, atlas read 2026-11-12):
    the outlet line above the lake (in the outlet's item, cut by the falls extent), Cave Creek, Inlet Creek and the 12
    unnamed lines. Changing this list is an owner call, not a refactor."""
    whole = [x for x in _rule("r1")["extents"] if x["op"] == "whole"]
    assert len(whole) == 1
    want = {INLET_CREEK, CAVE_CREEK} | {f"wsc:300-625474-814939-312002-{c}" for c in UNNAMED_INLET_BLKS.values()}
    assert set(whole[0]["item_ids"]) == want
    assert {x["op"]: x.get("item_id") for x in _rule("r1")["extents"]}["upstream_of"] == OUTLET


def test_the_unnamed_inlets_are_named_whole_so_they_become_items():
    nv = json.loads(under_test.curated().waters.name_variants.read_text())
    named = {b for e in nv if not (e.get("target") or {}).get("reach") and not e.get("reach")
             for b in (e.get("target") or {}).get("blks") or ()
             if any(n.get("name") == "Whiteswan Lake inlet stream" for n in e.get("names") or ())}
    assert named == set(UNNAMED_INLET_BLKS)
