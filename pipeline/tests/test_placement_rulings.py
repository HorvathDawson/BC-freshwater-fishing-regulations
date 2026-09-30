"""The placement rulings of 2026-09-29, each pinned and each with its switch shown to matter.

  K  a water row's tributary walk is not held to a region (`outside.WATER_ROW_WALKS_CROSS_REGIONS`)
  -  tidal water (Nitinat Lake) leaves every other row's binding (`CatalogueEntry.tidal`)
  B  the includes-tributaries glyph on a row means the row walks (`CatalogueEntry._glyph_walks`)
  J  a classified-water designation stops at a national park (`licensing.without_national_parks`)
  M  a code-less floodplain lake takes its side of a watershed cut from the river piece nearest it
     (`extent.CODE_LAKES_PLACED_BY_POSITION`, `reach.position`)
Plus the corpus rows those fixes needed, and (slow) the real atlas.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.atlas.reach import extent as X
from pipeline.atlas.reach import licensing as LIC
from pipeline.atlas.reach import outside as O
from pipeline.atlas.reach.build import build_reach
from pipeline.atlas.reach.models import Outcome, Reason
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S, L = NodeKind.stream, NodeKind.lake


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _s(nid, blk=None, lo=0, hi=100, wsc="", order=2, **kw):
    return StreamNode(node_id=nid, kind=S, blk=blk or nid, wsc=wsc, down_m=lo, up_m=hi,
                      length_m=hi - lo, stream_order=order, **kw)


def _item(iid, secs, kind="stream", bds=()):
    return RegistryItem(id=iid, name=iid, kind=kind, section_ids=tuple(secs), boundaries=bds)


# =============================================================================== K
@pytest.fixture
def two_regions():
    """River R (region 1) with a creek C that climbs into region 2 (C2)."""
    g = _graph([_s("R", order=5), _s("C"), _s("C2")],
               [("C", "R", "confluence", 50), ("C2", "C", "continuation", 100)])
    reg = {"gnis:1": _item("gnis:1", ["R"]),
           "area:region:1": _item("area:region:1", ["R", "C"], "area"),
           "area:region:2": _item("area:region:2", ["C2"], "area")}
    return g, reg


def _walk(fx, entry_id, shared, **rule):
    g, reg = fx
    b, d = build_reach({"entry_id": entry_id, "matched": ["gnis:1"], "includes_tributaries": True},
                       {"rule_id": "r", "extents": [{"op": "whole"}], **rule}, reg, g,
                       shared=shared)
    return set(b.sections), {x.kind for x in d}


def test_a_water_row_walks_across_the_region_line(two_regions):
    got, kinds = _walk(two_regions, "r1:r@1-1", frozenset())
    assert got == {"R", "C", "C2"} and "region_clip" not in kinds


def test_a_per_region_row_still_stops_at_its_line(two_regions):
    got, kinds = _walk(two_regions, "r1:r@1-1", frozenset({"gnis:1"}))
    assert got == {"R", "C"} and "region_clip" in kinds


def test_a_row_named_for_its_zone_says_so_in_its_extents(two_regions):
    """Williston Lake "in Zone B": the row's own words hold its walk (`within_area`)."""
    got, _ = _walk(two_regions, "r1:r@1-1", frozenset(),
                   extents=[{"op": "whole", "within_area": "area:region:1"}])
    assert got == {"R", "C"}


def test_mutation_water_rows_held_to_their_water_s_regions(two_regions, monkeypatch):
    monkeypatch.setattr(O, "WATER_ROW_WALKS_CROSS_REGIONS", False)
    got, _ = _walk(two_regions, "r1:r@1-1", frozenset())
    assert got == {"R", "C"}


# =========================================================================== tidal
@pytest.fixture
def lagoon():
    g = _graph([_s("R", order=5), StreamNode(node_id="lake:9", kind=L, wbk="9")],
               [("R", "lake:9", "lake_in", 0)])
    reg = {"gnis:1": _item("gnis:1", ["R"]), "wbk:9": _item("wbk:9", ["lake:9"], "lake"),
           "area:region:1": _item("area:region:1", ["R", "lake:9"], "area")}
    return g, reg


def test_tidal_water_leaves_every_other_rows_binding(lagoon):
    g, reg = lagoon
    tidal_row = {"entry_id": "r1:lagoon@1-3", "matched": ["wbk:9"], "tidal": True}
    tidal = O.tidal_sections([tidal_row], reg)
    assert tidal == {"lake:9"}
    zone = {"entry_id": "z1:quota", "matched": []}
    b, d = build_reach(zone, {"rule_id": "z", "extents": [{"op": "within", "area_id": "area:region:1"}]},
                       reg, g, tidal=tidal)
    assert set(b.sections) == {"R"} and any(x.kind == "tidal" for x in d)
    only, _ = build_reach(zone, {"rule_id": "z", "extents": [{"op": "whole", "item_id": "wbk:9"}]},
                          reg, g, tidal=tidal)
    assert only.outcome is Outcome.unresolved and only.reason is Reason.tidal
    # the tidal row's own note keeps its water
    note, _ = build_reach(tidal_row, {"rule_id": "n", "extents": [{"op": "whole"}]}, reg, g,
                          tidal=tidal)
    assert set(note.sections) == {"lake:9"}


def test_mutation_without_the_tidal_set_the_zone_binds_the_lagoon(lagoon):
    g, reg = lagoon
    b, _ = build_reach({"entry_id": "z1:quota", "matched": []},
                       {"rule_id": "z", "extents": [{"op": "within", "area_id": "area:region:1"}]},
                       reg, g, tidal=None)
    assert "lake:9" in b.sections


def _entry(**kw):
    base = {"entry_id": "r1:x@1-1", "name": "X", "regs_verbatim": "Note: X is tidal water",
            "matched": ["wbk:9"],
            "rules": [{"rule_id": "x.r1", "type": "advisory", "verbatim": "Note: X is tidal water",
                       "extents": [{"op": "whole"}]}]}
    base.update(kw)
    return base


def test_a_tidal_row_carries_only_notes():
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    CatalogueEntry.model_validate(_entry(tidal=True))
    bad = _entry(tidal=True)
    bad["rules"][0] = {**bad["rules"][0], "type": "hazard"}
    with pytest.raises(ValueError, match="tidal row carries only advisory"):
        CatalogueEntry.model_validate(bad)


# =========================================================================== glyph
def test_the_glyph_means_the_row_walks():
    from pipeline.regs.parsing.catalogue import CatalogueEntry
    row = _entry(symbols=["Incl. Tribs"])
    with pytest.raises(ValueError, match="includes-tributaries glyph"):
        CatalogueEntry.model_validate(row)
    CatalogueEntry.model_validate({**row, "includes_tributaries": True})
    said = _entry(symbols=["Incl. Tribs"])
    said["rules"][0] = {**said["rules"][0], "review_reason": "the note is about the lake alone"}
    CatalogueEntry.model_validate(said)
    with pytest.raises(ValueError, match="the row has no rules"):
        CatalogueEntry.model_validate({**row, "rules": [], "regs_verbatim": "See Y"})


# ================================================================== national parks
def test_a_designation_stops_at_a_national_park():
    p = LIC.LicensingPlacement("e", "d", "designation", "sections", sections=("a", "b", "c"),
                               via_tributary=("c",))
    got, diags = LIC.without_national_parks(p, frozenset({"b", "c"}))
    assert got.sections == ("a",) and got.via_tributary == () and diags[0].kind == "national_park"
    gone, _ = LIC.without_national_parks(p, frozenset({"a", "b", "c"}))
    assert gone.placement == "unresolved" and gone.reason == "national_park"
    req = LIC.LicensingPlacement("e", "q", "requirement", "sections", sections=("a", "b"))
    assert LIC.without_national_parks(req, frozenset({"b"}))[0] is req, "designations only"


def test_mutation_the_switch_keeps_the_park(monkeypatch):
    monkeypatch.setattr(LIC, "DESIGNATIONS_STOP_AT_NATIONAL_PARKS", False)
    p = LIC.LicensingPlacement("e", "d", "designation", "sections", sections=("a", "b"))
    assert LIC.without_national_parks(p, frozenset({"b"}))[0] is p


# ========================================================================= position
@pytest.fixture
def floodplain():
    """River R (code 100) cut at 500 into R:0 and R:500; two floodplain lakes coded 100 touching
    nothing, one beside each piece; and a creek above the cut so the watershed has a member."""
    from shapely.geometry import LineString, Polygon
    cut = SectionBoundary(boundary_id="split:cut", kind=BoundaryKind("split"), route_measure=500)
    nodes = [_s("R:0", "R", 0, 500, wsc="100", order=6, upper_bound=cut),
             _s("R:500", "R", 500, 1000, wsc="100", order=6, lower_bound=cut),
             _s("A", wsc="100-800000"),
             StreamNode(node_id="lake:lo", kind=L, wbk="lo", basin_wsc="100"),
             StreamNode(node_id="lake:hi", kind=L, wbk="hi", basin_wsc="100")]
    g = _graph(nodes, [("R:500", "R:0", "continuation", 500), ("A", "R:500", "confluence", 800)])
    g._position_geoms = (
        {"lake:lo": Polygon([(100, 10), (120, 10), (120, 30), (100, 30)]),
         "lake:hi": Polygon([(700, 10), (720, 10), (720, 30), (700, 30)])},
        {"R:0": LineString([(0, 0), (500, 0)]), "R:500": LineString([(500, 0), (1000, 0)])})
    reg = {"gnis:1": _item("gnis:1", ["R:0", "R:500"],
                           bds=(RegistryBoundary(id="cut", label="cut", kind="split",
                                                 ref="split:cut"),))}
    return g, reg


def _part(fx, op):
    g, reg = fx
    b, d = build_reach({"entry_id": "z9:t", "matched": []},
                       {"rule_id": "t", "extents": [{"op": op, "splits": ["cut"],
                                                     "item_id": "gnis:1", "watershed": True}]},
                       reg, g)
    return set(b.sections), next(x.payload for x in d if x.kind == "watershed")


def test_a_floodplain_lake_goes_with_the_river_beside_it(floodplain):
    up, dup = _part(floodplain, "upstream_of")
    down, _ = _part(floodplain, "downstream_of")
    assert "lake:hi" in up and "lake:lo" in down and dup["unplaced"] == 0


def test_mutation_without_position_the_lakes_are_unplaced(floodplain, monkeypatch):
    monkeypatch.setattr(X, "CODE_LAKES_PLACED_BY_POSITION", False)
    up, dup = _part(floodplain, "upstream_of")
    assert "lake:hi" not in up and dup["unplaced"] == 2


def test_no_geometry_places_nothing(floodplain):
    g, reg = floodplain
    del g._position_geoms
    up, dup = _part((g, reg), "upstream_of")
    assert dup["unplaced"] == 2


# ====================================================================== the corpus
@pytest.fixture(scope="module")
def corpus():
    from pipeline.regs.parsing.io import read_entries_dir
    return read_entries_dir()


def test_nitinat_lake_is_tidal_and_the_only_tidal_row(corpus):
    tidal = sorted(k for k, e in corpus.items() if e.get("tidal"))
    assert tidal == ["r1:nitinat_lake@1-3"]
    e = corpus["r1:nitinat_lake@1-3"]
    assert [r["type"] for r in e["rules"]] == ["advisory"]
    assert "Tidal Waters Sport Fishing Licence" in e["rules"][0]["verbatim"]


def test_the_elk_river_s_tributaries_except_the_eleven_listings(corpus):
    e = corpus["r4:elk_river_s_tributaries_see_exceptions@4-2+4-23"]
    items = {"gnis:8798", "gnis:9343", "gnis:12449", "gnis:16777", "gnis:19955", "gnis:11815",
             "gnis:10373", "gnis:19452", "gnis:28953", "gnis:14951", "gnis:27703"}
    for r in e["rules"]:
        assert {x["item_id"] for x in r["tributary_excludes"]} == items, r["rule_id"]


def test_the_atnarko_excepts_its_three_waters_on_every_walking_rule(corpus):
    e = corpus["r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8"]
    want = {"gnis:2100", "gnis:22290", "gnis:1914"}
    for r in e["rules"]:
        if r.get("includes_tributaries") is False:
            continue
        assert want <= {x.get("item_id") for x in r.get("tributary_excludes") or []}, r["rule_id"]


def test_the_shuswap_row_and_stamp_reach_little_river(corpus):
    assert "gnis:33426" in corpus[
        "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"]["matched"]
    stamp = corpus["zp:shuswap_rainbow_stamp"]
    assert {"op": "whole", "item_id": "gnis:33426"} in stamp["licensing"][0]["extents"]


def test_every_glyph_row_walks(corpus):
    from pipeline.regs.parsing.catalogue import GLYPH_INCLUDES_TRIBUTARIES
    bad = [k for k, e in corpus.items() if GLYPH_INCLUDES_TRIBUTARIES in (e.get("symbols") or [])
           and e.get("includes_tributaries") is not True
           and not all(r.get("includes_tributaries") or r.get("tributaries_only")
                       or r.get("review_reason") for r in e.get("rules") or [])]
    assert bad == []


# ================================================================ real data (slow)
@pytest.fixture(scope="module")
def real():
    from pipeline.atlas.reach import position
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    if not (b / "graph.pkl").exists():
        pytest.skip("no built graph")
    g = read_artifact(str(b / "graph.pkl"))
    position.attach(g, b)
    return g, load_registry(str(b / "registry.json"))


def _bind_real(real, corpus, eid, rid, licensing=False):
    from pipeline.atlas.reach.licensing import as_rule
    g, reg = real
    e = corpus[eid]
    if licensing:
        rec = next(x for x in e["licensing"] if x["id"] == rid)
        r = as_rule(rec, rec.get("extents") if rec.get("extents") is not None else e.get("extents"))
    else:
        r = next(x for x in e["rules"] if x["rule_id"] == rid)
    b, d = build_reach(e, r, reg, g, regional=not licensing,
                       shared=O.shared_waters(list(corpus.values())),
                       tidal=O.tidal_sections(list(corpus.values()), reg))
    return set(b.sections), d


@pytest.mark.slow
def test_the_bulkley_closure_stays_out_of_the_morice(real, corpus):
    g, reg = real
    got, _ = _bind_real(real, corpus, "r6:bulkley_river@6-9", "bulkley_river.r2")
    for iid in ("gnis:14918",):                                   # the Morice
        assert not got & set(reg[iid].section_ids)
    assert {g.nodes[s].display_name for s in got} >= {"Bulkley River"}


@pytest.mark.slow
def test_the_duncan_closure_stays_out_of_the_lardeau(real, corpus):
    g, reg = real
    got, _ = _bind_real(real, corpus, "r4:duncan_river@4-19", "duncan_river.r6")
    assert not got & set(reg["gnis:16359"].section_ids)


@pytest.mark.slow
def test_harris_creek_takes_hemmingsen_creek_in(real, corpus):
    g, reg = real
    got, _ = _bind_real(real, corpus, "r1:harris_creek@1-3", "harris_creek.r2")
    assert set(reg["gnis:20701"].section_ids) & got


@pytest.mark.slow
def test_skeena_river_2_is_the_mainstem_stretch(real, corpus):
    g, _ = real
    got, _ = _bind_real(real, corpus, "r6:skeena_river_mainstem_only@6-10", "skeena_river_2",
                        licensing=True)
    main = sum(g.nodes[s].length_m for s in got if g.nodes[s].blk == "360887278")
    assert 50_000 < main < 65_000


@pytest.mark.slow
def test_region_5_sturgeon_lakes_all_placed(real, corpus):
    from pipeline.atlas.registry.basins import node_basin_code
    g, reg = real
    r1, _ = _bind_real(real, corpus, "z5:white_sturgeon", "white_sturgeon.r1")
    r2, _ = _bind_real(real, corpus, "z5:white_sturgeon", "white_sturgeon.r2")
    r5 = set(reg["area:region:5"].section_ids)
    lakes = {s for s in r5 if s in g.nodes and g.nodes[s].kind == L
             and node_basin_code(g.nodes[s]) == "100"}
    assert lakes and not lakes - r1 - r2 and not r1 & r2


@pytest.mark.slow
def test_the_kootenay_class_ii_stops_at_kootenay_national_park(real, corpus):
    g, reg = real
    parks = LIC.national_park_sections(reg)
    got, _ = _bind_real(real, corpus,
                        "r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35",
                        "kootenay_river", licensing=True)
    placed, _ = LIC.without_national_parks(
        LIC.LicensingPlacement("e", "d", "designation", "sections", sections=tuple(sorted(got))),
        parks)
    assert got & parks and not set(placed.sections) & parks


def test_each_carve_out_hands_only_its_own_water_to_its_owner():
    """The Atnarko EXCEPTs three creeks; only Burnt Bridge has a designation. Pooled, Hunlen's and
    Young's upper creeks were handed to Burnt Bridge's Class II. Keyed per carve-out, each owner
    takes only what its own carve-out removed."""
    p = [LIC.LicensingPlacement("r5:atnarko", "d", "designation", "sections", sections=("a",)),
         LIC.LicensingPlacement("r5:burnt", "b", "designation", "sections", sections=("b1",))]
    carved = {("r5:atnarko", "d", 0): ({"b1", "b2"}, {"gnis:burnt"}),
              ("r5:atnarko", "d", 1): ({"h1", "h2"}, {"gnis:hunlen"})}
    claims = {"gnis:burnt": ["r5:burnt"], "gnis:hunlen": ["r5:hunlen"]}
    out, diags = LIC.carve_outs_to_owner(p, carved, claims)
    assert set(out[1].sections) == {"b1", "b2"}
    orphans = LIC.carve_out_orphans(out, carved, claims)
    assert [d.payload["orphans"] for d in orphans] == [["h1", "h2"]]
    # MUTATION: pooled under one key, Burnt Bridge takes Hunlen's water too
    pooled = {("r5:atnarko", "d"): ({"b1", "b2", "h1", "h2"}, {"gnis:burnt", "gnis:hunlen"})}
    bad, _ = LIC.carve_outs_to_owner(p, pooled, {"gnis:burnt": ["r5:burnt"]})
    assert {"h1", "h2"} <= set(bad[1].sections)
