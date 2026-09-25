"""A PART OF A WATERSHED, cut by FWA code (`Extent.watershed`), and the walk fixes of the same round.

The book prints several rules as a part of a watershed: "CLOSED TO ALL FISHING in the Fraser River
Watershed upstream of Williams Lake River", "CATCH AND RELEASE … downstream of and including
Williams Lake River", the White Sturgeon Conservation Licence "in the Fraser River Watershed
(including tributaries) from the CPR Bridge at Mission to and including Williams Lake River", the
Iskut above Forrest Kerr Canyon, the Skeena above Cedarvale, the Nass above Kitsault Bridge. They
bound by the tributary walk, and the walk (streams only, since the 2026-09-24 ruling) dropped every
lake of them — z5 white sturgeon r1 lost 14,840 lake sections, r2 20,532, the licence 47,719.

A watershed is every lake and stream whose FWA code lies under the river's; a PART of it is the
members whose code joins the river on one side of the cut. The group after the river's code is the
confluence's distance along the river in millionths of its length, so the side is arithmetic on the
code — not a walk, which follows real bifurcations across a divide (Dewar Lake drains to both the
WLR and Hawks Creek) and stops where it cannot climb.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.atlas.graph.tributaries import expand
from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S, L = NodeKind.stream, NodeKind.lake


def _split(sid, m):
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind("split"), route_measure=m)


def _river(nid, lo, hi, lower=None, upper=None):
    """A piece of the river (blk R, code 100). The line is 1,000,000 m, so a code group IS its
    confluence's measure."""
    return StreamNode(node_id=nid, kind=S, blk="R", wsc="100", gnis_id="1", down_m=lo, up_m=hi,
                      length_m=hi - lo, stream_order=6,
                      lower_bound=_split(*lower) if lower else None,
                      upper_bound=_split(*upper) if upper else None)


def _trib(nid, code, order=2, blk=None):
    return StreamNode(node_id=nid, kind=S, blk=blk or nid, wsc=code, down_m=0, up_m=100,
                      length_m=100, stream_order=order)


def _lake(nid, *, wsc="", basin_wsc=""):
    return StreamNode(node_id=nid, kind=L, wbk=nid.split(":")[1], wsc=wsc, basin_wsc=basin_wsc)


@pytest.fixture
def basin():
    """The river R (code 100) in three pieces, cut at 200,000 ("low") and 400,000 ("cut").

        group  100,000  A  (+ lake:a on it)            below both cuts
        group  300,000  B                              between the cuts
        group  400,000  W  (its mouth IS the cut)      at the cut: neither side
        group  600,000  C  (+ C2 above it, + a codeless pond placed by its named watershed)
        group  800,000  D
        code   100      lake:f  a floodplain lake draining into R above the cut
                        lake:i  a pond FWA places only in R's own watershed, touching nothing
                        S       an unnamed side channel off and back onto the lowest piece
        code   200-…    X       another basin
    """
    nodes = [
        _river("R:0", 0, 200_000, upper=("low", 200_000)),
        _river("R:200000", 200_000, 400_000, lower=("low", 200_000), upper=("cut", 400_000)),
        _river("R:400000", 400_000, 1_000_000, lower=("cut", 400_000)),
        _trib("A", "100-100000"), _lake("lake:a", wsc="100-100000-500000"),
        _trib("B", "100-300000"), _trib("W", "100-400000"),
        _trib("C", "100-600000", order=3), _trib("C2", "100-600000-200000"),
        _lake("lake:p", basin_wsc="100-600000"),
        _trib("D", "100-800000"), _trib("E", "100-900000"),
        _lake("lake:f", wsc="100"), _lake("lake:i", basin_wsc="100"),
        StreamNode(node_id="S", kind=S, blk="S", wsc="100", down_m=0, up_m=50, length_m=50),
        _trib("X", "200-123456"),
    ]
    edges = [("A", "R:0", "confluence", 100_000.5), ("lake:a", "A", "lake_out", 50),
             ("B", "R:200000", "confluence", 300_000.5), ("W", "R:200000", "confluence", 400_000.5),
             ("C", "R:400000", "confluence", 600_000.5), ("C2", "C", "confluence", 20),
             ("D", "R:400000", "confluence", 800_000.5), ("E", "R:400000", "confluence", 900_000.5),
             ("lake:f", "R:400000", "lake_out", 700_000), ("S", "R:0", "confluence", 50_000),
             ("R:400000", "R:200000", "continuation", 400_000),
             ("R:200000", "R:0", "continuation", 200_000)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    bds = tuple(RegistryBoundary(id=x, label=x, kind="split", ref=f"split:{x}") for x in ("low", "cut"))
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="R", kind="stream",
                                  section_ids=("R:0", "R:200000", "R:400000"), boundaries=bds),
           "area:region:9": RegistryItem(id="area:region:9", name="9", kind="area",
                                         section_ids=tuple(sorted(g.nodes)))}
    return g, reg


def _bind(fx, *extents, **rule):
    g, reg = fx
    b, d = build_reach({"entry_id": "z9:t", "matched": []},
                       {"rule_id": "t.r1", "extents": list(extents), **rule}, reg, g)
    return set(b.sections), {x.kind: x.payload for x in d}


def ws(op, *splits, **kw):
    return {"op": op, "splits": list(splits), "item_id": "gnis:1", "watershed": True, **kw}


BASIN = {"R:0", "R:200000", "R:400000", "A", "lake:a", "B", "W", "C", "C2", "lake:p", "D", "E",
         "lake:f", "lake:i", "S"}


def test_the_two_sides_of_a_cut_are_complementary_by_code(basin):
    up, dup = _bind(basin, ws("upstream_of", "cut"))
    dn, ddn = _bind(basin, ws("downstream_of", "cut"))
    assert up == {"R:400000", "C", "C2", "lake:p", "D", "E", "lake:f"}
    assert dn == {"R:0", "R:200000", "A", "lake:a", "B", "S"}
    assert not up & dn
    # what neither side takes is exactly the tributary AT the cut and the pond nothing places
    assert BASIN - up - dn == {"W", "lake:i"}
    assert dup["watershed"]["at_cut"] == ["100-400000"]
    assert dup["watershed"]["unplaced"] == 1 and dup["watershed"]["unplaced_sample"] == ["lake:i"]
    assert "X" not in up | dn, "another basin's water"


def test_lakes_come_with_the_watershed_even_codeless_ones(basin):
    """A walk collects streams; a watershed is lakes and streams. `lake:p` has no FWA code of its
    own (999) and is placed by the named watershed it sits in (`basin_wsc`)."""
    up, _ = _bind(basin, ws("upstream_of", "cut"))
    assert {"lake:p", "lake:f"} <= up


def test_and_including_names_the_tributary_at_the_cut(basin):
    """"downstream of AND INCLUDING Williams Lake River": the tributary whose mouth is the cut is
    on neither side until the book names it, as its own sub-basin."""
    dn, _ = _bind(basin, ws("downstream_of", "cut"), {"op": "within", "area_id": "area:basin:100-400000-"})
    assert "W" in dn


def test_between_two_cuts(basin):
    got, _ = _bind(basin, ws("between", "low", "cut"))
    assert got == {"R:200000", "B"}


def test_a_watershed_part_is_never_walked(basin):
    """A walk from the part below the cut climbs into the part above it — the side the book
    excluded. The flag on the rule changes nothing."""
    dn, _ = _bind(basin, ws("downstream_of", "cut"))
    walked, _ = _bind(basin, ws("downstream_of", "cut"), includes_tributaries=True)
    assert walked == dn


def test_tributaries_only_is_the_part_without_the_river(basin):
    """"any stream in the watersheds of the Skeena River upstream of Cedarvale … the Skeena River
    mainstem … only closed Jan 1-May 31": the part, without the river's own pieces."""
    got, _ = _bind(basin, ws("upstream_of", "cut"), tributaries_only=True)
    assert got == {"C", "C2", "lake:p", "D", "E", "lake:f"}


def test_feature_types_still_limit_it(basin):
    got, _ = _bind(basin, ws("upstream_of", "cut", feature_types=["stream"]))
    assert got == {"R:400000", "C", "C2", "D", "E"}


def test_within_area_limits_it(basin):
    g, reg = basin
    reg = dict(reg)
    reg["area:region:8"] = RegistryItem(id="area:region:8", name="8", kind="area",
                                        section_ids=("R:400000", "C", "lake:p"))
    got, _ = _bind((g, reg), ws("upstream_of", "cut", within_area="area:region:8"))
    assert got == {"R:400000", "C", "lake:p"}


def test_the_model_refuses_a_watershed_without_a_cut_or_with_an_area():
    from pipeline.regs.parsing.entry_models import Extent
    Extent(op="upstream_of", splits=["x"], item_id="gnis:1", watershed=True)
    for bad in ({"op": "whole", "item_id": "gnis:1"},
                {"op": "within", "area_id": "area:basin:100-"},
                {"op": "upstream_of", "splits": ["x"], "item_ids": ["gnis:1", "gnis:2"]},
                {"op": "upstream_of", "splits": ["x"], "area_id": "area:basin:100-"}):
        with pytest.raises(ValueError):
            Extent(**bad, watershed=True)
    with pytest.raises(ValueError, match="extra"):
        Extent(op="whole", watershd=True)             # a misspelt key is refused, not dropped


def test_the_catalogue_refuses_a_watershed_part_that_walks():
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    x = {"op": "upstream_of", "splits": ["s"], "item_id": "gnis:1", "watershed": True}
    rule = {"rule_id": "x.r1", "type": "bait_restriction", "verbatim": "Bait ban.",
            "gear": [{"slot": "bait", "ban": ["any_bait"]}], "extents": [x]}
    base = {"entry_id": "z5:x", "name": "X", "regs_verbatim": "Bait ban."}
    CatalogueEntry.model_validate({**base, "rules": [rule]})
    with pytest.raises(ValueError, match="already holds the tributaries"):
        CatalogueEntry.model_validate({**base, "rules": [{**rule, "includes_tributaries": True}]})
    with pytest.raises(ValueError, match="inherited from the entry"):
        CatalogueEntry.model_validate({**base, "includes_tributaries": True, "rules": [rule]})
    with pytest.raises(ValueError, match="extent 0"):
        CatalogueEntry.model_validate({**base, "rules": [{**rule, "extents": [{"op": "whole",
                                                                                "splits": ["s"]}]}]})


# ============================================================ a lake a row names beside its river

@pytest.fixture
def sumallo():
    """SUMALLO RIVER (includes "Cedar" Lake): the row matches the river AND Cedar Lake, which sits
    on Ferguson Creek, a tributary of the river.

        feeder --lake_in--> lake:c <--lake_in-- F_up          (Ferguson above the lake)
                              |
                            F_low (Ferguson below) --confluence--> river
    """
    def n(nid, blk, kind=S, order=1):
        return StreamNode(node_id=nid, kind=kind, blk="" if kind == L else blk, stream_order=order)
    g = StreamGraph()
    g.nodes = {x.node_id: x for x in (n("river", "R", order=4), n("F_low", "F", order=2),
                                       n("F_up", "F", order=2), n("lake:c", "", L, order=2), n("feeder", "Q"))}
    g.edges = [FlowEdge(a, b, 0.0, kind=k) for a, b, k in (
        ("F_low", "river", "confluence"), ("lake:c", "F_low", "lake_out"),
        ("F_up", "lake:c", "lake_in"), ("feeder", "lake:c", "lake_in"))]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def test_a_named_lake_on_a_tributary_does_not_stop_the_walk_up_it(sumallo):
    """Adding Cedar Lake to the row's water lost 69 sections: the walk took Ferguson above the lake
    for "the reach's own flow continuing", which is true of a lake row and not of a lake on a
    tributary of the row's river."""
    assert expand(sumallo, {"river", "lake:c"}, only=True) == {"F_low", "F_up", "feeder"}


def test_a_lake_row_still_does_not_take_the_river_through_it(sumallo):
    """The through-line rule it narrows: a row that IS the lake (Koocanusa) does not take the
    river threading it — the lake drains into no stream of the reach."""
    assert "F_up" not in expand(sumallo, {"lake:c"}, only=True)
    assert "feeder" in expand(sumallo, {"lake:c"}, only=True)


def test_the_named_lake_rule_can_be_switched_off(sumallo, monkeypatch):
    """The policy flag is what the fix hangs on: off, the old loss comes back."""
    from pipeline.atlas.graph import tributaries as T
    monkeypatch.setattr(T, "NAMED_LAKE_ON_A_TRIBUTARY_IS_CLIMBED", False)
    assert "F_up" not in expand(sumallo, {"river", "lake:c"}, only=True)


# ============================================================================ real data (slow)
# `ATLAS_BUILD` points these at a side build; the default is the promoted one.

@pytest.fixture(scope="module")
def real():
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    if not (b / "graph.pkl").exists():
        pytest.skip("no built graph")
    return read_artifact(str(b / "graph.pkl")), load_registry(str(b / "registry.json"))


@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.parsing.io import read_entries_dir
    return read_entries_dir()


def _rule(real, corpus, eid, rid, *, licensing=False):
    from pipeline.atlas.reach.licensing import as_rule
    g, reg = real
    e = corpus[eid]
    if licensing:
        rec = next(x for x in e["licensing"] if x["id"] == rid)
        r = as_rule(rec, rec.get("extents") or e.get("extents"))
    else:
        r = next(x for x in e["rules"] if x["rule_id"] == rid)
    b, _ = build_reach(e, r, reg, g, regional=not licensing)
    return set(b.sections)


def _group(g, sid, river="100"):
    from pipeline.atlas.registry.basins import node_basin_code
    c = node_basin_code(g.nodes[sid])
    return None if c == river else int(c[len(river) + 1:].split("-")[0])


@pytest.mark.slow
def test_williams_lake_river_cuts_the_region_5_sturgeon_rules(real, corpus):
    """z5 White Sturgeon: r1 CLOSED upstream of the WLR, r2 CATCH AND RELEASE downstream of and
    including it. Disjoint; every tributary group on its printed side of 382626; the WLR, Williams
    Lake and the San Jose River in r2; lakes in both."""
    g, reg = real
    r1 = _rule(real, corpus, "z5:white_sturgeon", "white_sturgeon.r1")
    r2 = _rule(real, corpus, "z5:white_sturgeon", "white_sturgeon.r2")
    assert not r1 & r2
    for s in r1:
        p = _group(g, s)
        assert p is None or p > 382626, s
    for s in r2:
        p = _group(g, s)
        assert p is None or p <= 382626, s
    assert set(reg["gnis:27764"].section_ids) <= r2
    assert "lake:329494714" in r2, "Williams Lake"
    names2 = {g.nodes[s].display_name for s in r2}
    assert "San Jose River" in names2
    lakes = lambda xs: sum(1 for s in xs if g.nodes[s].kind == L)
    assert lakes(r1) > 10_000 and lakes(r2) > 10_000, "the watershed keeps its lakes"
    # the part covers the basin in Region 5, bar the ponds nothing places and the at-cut creek
    from pipeline.atlas.registry.basins import node_basin_code
    r5 = set(reg["area:region:5"].section_ids)
    basin = {s for s in r5 if s in g.nodes and (node_basin_code(g.nodes[s]) + "-").startswith("100-")}
    assert len(basin - r1 - r2) < 0.005 * len(basin)


@pytest.mark.slow
def test_the_sturgeon_licence_is_mission_to_and_including_the_wlr(real, corpus):
    g, reg = real
    rule = _rule(real, corpus, "zp:white_sturgeon_licence", "white_sturgeon_licence.r2")
    rec = _rule(real, corpus, "zp:white_sturgeon_licence", "white_sturgeon_licence", licensing=True)
    assert rule == rec, "the rule and the licensing record say the same place"
    for s in rule:
        p = _group(g, s)
        assert p is None or 55_000 < p <= 382_626, s
    assert set(reg["gnis:27764"].section_ids) <= rule
    names = {g.nodes[s].display_name for s in rule}
    assert {"Harrison Lake", "Thompson River", "Williams Lake"} <= names
    assert "Quesnel Lake" not in names


@pytest.mark.slow
def test_the_skeena_above_cedarvale_is_streams_without_the_mainstem(real, corpus):
    g, reg = real
    got = _rule(real, corpus, "z6:skeena_nass_winter_closure", "skeena_nass_winter_closure.r1")
    assert not got & set(reg["gnis:2936"].section_ids), "the mainstem is exempt"
    assert all(g.nodes[s].kind == S for s in got)
    assert all((_group(g, s, "400") or 10**7) > 326_256 for s in got), "Insect Creek and below out"


@pytest.mark.slow
def test_lake_mid_reach_gains_the_book_does_not_support_are_gone(real, corpus):
    """Rows whose water ends at a lake the FWA name runs through (checked against the book, p59
    Stellako; p38 Duncan Lake's tributaries; p20 Puntledge and p17 Comox Lake / Cruickshank River;
    p19 Nitinat Lake is tidal; p60 Tatsatua Creek = Tatsamenie Lake's outlet streams)."""
    g, reg = real
    name = lambda xs: {g.nodes[s].display_name for s in xs}
    # the glyph is on "Class II water", not on the river: the rules are the river's
    stel = _rule(real, corpus, "r6:stellako_river@6-4+7-12", "stellako_river.r1")
    assert stel == set(reg["gnis:7836"].section_ids)
    des = _rule(real, corpus, "r6:stellako_river@6-4+7-12", "stellako_river", licensing=True)
    assert len(des) > len(stel), "the designation includes tributaries"
    assert "Cruickshank River" not in name(_rule(real, corpus, "r1:puntledge_river@1-6",
                                                 "puntledge_river.r5"))
    duncan = _rule(real, corpus, "r4:duncan_river@4-19", "duncan_river.r1")
    feeders = {g.edges[i].from_node for i in g.up_adj.get("lake:329120714", [])}
    assert not duncan & feeders, "Duncan Lake's tributaries are their own row"
    nit = _rule(real, corpus, "r1:nitinat_river@1-4", "nitinat_river.r1")
    tidal = {g.edges[i].from_node for i in g.up_adj.get("lake:329504244", [])}
    assert not nit & (tidal - set(reg["gnis:23318"].section_ids)), "the tidal lake's other inflows"
    assert not nit & {s for s in reg["gnis:23318"].section_ids if g.nodes[s].up_m <= 1_500
                      and g.nodes[s].blk == "354155007"}, "the tidal narrows below the lake"
    tat = _rule(real, corpus,
                "r6:tatsatua_creek_formerly_known_as_tatsamenie_lake_s_outlet_st@6-26",
                "tatsatua_creek.r1")
    assert not tat & {g.edges[i].from_node for i in g.up_adj.get("lake:329514788", [])}


@pytest.mark.slow
def test_sumallo_binds_cedar_lake_and_ferguson_above_it(real, corpus):
    g, reg = real
    got = _rule(real, corpus, "r2:sumallo_river_includes_cedar_lake_at_sunshine_valley@2-2",
                "sumallo_river.r1")
    assert "lake:329522925" in got, "Cedar Lake, named by the row"
    above = {g.edges[i].from_node for i in g.up_adj.get("lake:329522925", [])}
    assert above <= got, "Ferguson Creek above Cedar Lake and the streams feeding the lake"
