"""T115 — ONE REGION MEMBERSHIP: "which sections are in region R?" is answered once, by
`registry.regions` (owner 2026-10-22, unification audit finding reach 1). T183 (owner 2026-10-24)
REVERSES T115's answer, keeping the one function: a section is in EVERY region whose polygon holds
more than `SLIVER_M` of it — a straddling stream piece carries both regions' rules, as a lake does.

Every reader calls `regions.region_sections` / `regions.region_members` /
`regions.area_family_sections` / `regions.section_regions`.

Mutation pins:
  * `regions.STRADDLERS_IN_EVERY_REGION = False` (T115's home members) fails
    `test_a_per_region_row_binds_a_piece_straddling_into_its_region` and the steelhead-twin test.
  * a module outside `regions.py` reading a region item's sections (an `"area:region:"` literal, a
    bare `REGION_PREFIX`, an `in_region` call) fails `test_no_second_copy_of_region_membership`.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach import outside as O
from pipeline.atlas.registry import regions
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

ROOT = Path(__file__).resolve().parents[1]


def _node(nid):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid.split(":")[0], wsc="100",
                      down_m=0.0, up_m=100.0, length_m=100.0, stream_order=6)


def _world(attach=True):
    """The Fraser in three pieces: f3:0 wholly in Region 3, f35:0 inside BOTH polygons with its home
    in Region 3 (the live 356364114:418405), f5:0 wholly in Region 5. A lake L straddles too (home 3)."""
    g = StreamGraph()
    g.nodes = {n: _node(n) for n in ("f3:0", "f35:0", "f5:0")}
    reg = {"area:region:3": RegistryItem(id="area:region:3", name="Region 3", kind="area",
                                         section_ids=("f3:0", "f35:0", "lake:L")),
           "area:region:5": RegistryItem(id="area:region:5", name="Region 5", kind="area",
                                         section_ids=("f35:0", "f5:0", "lake:L")),
           "gnis:F": RegistryItem(id="gnis:F", name="Fraser River", kind="stream",
                                  section_ids=("f3:0", "f35:0", "f5:0"))}
    if attach:
        regions.attach(g, {"f35:0": "3", "lake:L": "3"}, both={"lake:L"})
    return g, reg


FRASER_5 = {"entry_id": "r5:fraser_river@5-2", "matched": ["gnis:F"]}
SHARED = frozenset({"gnis:F"})


def _bind(g, reg):
    b, _ = B.build_reach(FRASER_5, {"rule_id": "fraser_river.r2", "extents": [{"op": "whole"}]},
                         reg, g, shared=SHARED)
    return set(b.sections)


def test_a_per_region_row_binds_a_piece_straddling_into_its_region(monkeypatch):
    """T183: the Fraser's Region 5 row binds every piece in Region 5's polygon, the straddling piece
    homed in Region 3 included (the Region 3 row binds it too). MUTATION (`STRADDLERS_IN_EVERY_REGION`
    off, T115's home members): the straddling piece is left out again."""
    g, reg = _world()
    assert O.region_limit(FRASER_5, reg, SHARED, graph=g) == {"f35:0", "f5:0", "lake:L"}
    assert _bind(g, reg) == {"f35:0", "f5:0"}
    monkeypatch.setattr(regions, "STRADDLERS_IN_EVERY_REGION", False)
    assert _bind(g, reg) == {"f5:0"}


def test_the_row_limit_and_the_zone_rules_are_one_answer():
    """`area:region:5` for a zone rule and Region 5 for a per-region row: the same set."""
    from pipeline.atlas.reach.extent import area_sections
    g, reg = _world()
    assert area_sections(reg, g, "area:region:5") == set(regions.region_sections(("5",), reg, g)) \
        == {"f35:0", "f5:0", "lake:L"}
    assert area_sections(reg, g, "area:region:3") == {"f3:0", "f35:0", "lake:L"}


def test_the_families_and_the_province_read_the_same_membership():
    g, reg = _world()
    assert regions.area_family_sections(reg, "region", g) == {"f3:0", "f35:0", "f5:0", "lake:L"}
    assert regions.area_family_sections(reg, "park", g) is None
    assert regions.section_regions(reg, g) == {
        "f3:0": {"3"}, "f35:0": {"3", "5"}, "f5:0": {"5"}, "lake:L": {"3", "5"}}
    assert O.water_regions(FRASER_5, reg, g) == ("3", "5")
    assert O.outside_bc(reg, g) == frozenset()


def test_the_home_map_filters_nothing():
    """The home map is a measured fact (display); attached or not, the membership is the same."""
    g, reg = _world(attach=False)
    g2, _ = _world()
    for graph in (g, g2, None):
        assert regions.region_sections(("5",), reg, graph) == {"f35:0", "f5:0", "lake:L"}


def test_re_attaching_a_home_map_is_not_served_from_a_cache(monkeypatch):
    """With the pin off (T115's home members), a re-attached map is read afresh, never cached."""
    monkeypatch.setattr(regions, "STRADDLERS_IN_EVERY_REGION", False)
    g, reg = _world(attach=False)
    assert "f35:0" in regions.region_sections(("5",), reg, g)
    regions.attach(g, {"f35:0": "3"}, both=())
    assert "f35:0" not in regions.region_sections(("5",), reg, g)


# --------------------------------------------------------------------------- no second copy

#: Where "which sections are in region R" may be read. Everything else asks these modules.
OWNER = {ROOT / "atlas" / "registry" / "regions.py"}
#: The readers T115 unified: the reach builder, the entry-id parser, and the bundle's section places
#: (`deliver.bundle.build._water_places`, which looped `in_region` over the region items itself until
#: the adversarial review).
SCANNED = sorted([*(ROOT / "atlas" / "reach").glob("*.py"), ROOT / "common" / "entry_id.py",
                  ROOT / "deliver" / "bundle" / "build.py"])


def _docstrings(tree) -> set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
                and node.body and isinstance(node.body[0], ast.Expr) \
                and isinstance(node.body[0].value, ast.Constant):
            out.add(id(node.body[0].value))
    return out


@pytest.mark.parametrize("path", SCANNED, ids=lambda p: p.name)
def test_no_second_copy_of_region_membership(path):
    """No module but `registry/regions.py` names a region item (an `"area:region:"` string), uses
    `REGION_PREFIX` except through `regions.`, or calls `in_region` itself."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docs = _docstrings(tree)
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                and id(node) not in docs and "area:region" in node.value:
            bad.append(f"{node.lineno}: string {node.value!r}")
        if isinstance(node, ast.Name) and node.id in ("REGION_PREFIX", "in_region"):
            bad.append(f"{node.lineno}: bare {node.id}")
        if isinstance(node, ast.Attribute) and node.attr == "in_region":
            bad.append(f"{node.lineno}: calls in_region")
        if isinstance(node, ast.FunctionDef) and node.name in (
                "region_sections", "_membership", "_region_items", "_zones_of",
                "_area_kind_sections"):
            bad.append(f"{node.lineno}: defines {node.name}")
    assert not bad, f"{path.name} reads region membership itself: {bad}"


def test_the_scan_covers_the_readers_it_unified():
    names = {p.name for p in SCANNED}
    assert {"outside.py", "extent.py", "build.py", "entry_id.py"} <= names
    assert ROOT / "deliver" / "bundle" / "build.py" in SCANNED


# ------------------------------------------------------------- the steelhead twin (review)

def _twin(extent):
    entry = {"entry_id": "z5:zone@5", "matched": []}
    rule = {"rule_id": "r6b", "extents": [{"op": "steelhead_waters", "siblings": [], **extent}]}
    return entry, rule


def test_the_steelhead_twin_s_region_is_the_one_membership(monkeypatch):
    """A zone twin's `area_id: area:region:5` binds the book-known waters in Region 5's polygon — the
    Fraser piece straddling into it included (T183) — and `outside_area` subtracts the same
    membership. MUTATION (`STRADDLERS_IN_EVERY_REGION` off): T115's home members, f35:0 goes."""
    g, reg = _world()
    known = {"f3:0", "f35:0", "f5:0", "lake:L"}

    def twin(extent):
        got, _, _ = B._bind_steelhead_waters([_twin(extent)], [], known, [], [], reg, g)
        return [(b.rule_id, set(b.sections)) for b in got]

    assert twin({"area_id": "area:region:5"}) == [("r6b", {"f35:0", "f5:0", "lake:L"})]
    assert twin({"outside_area": "area:region:5"}) == [("r6b", {"f3:0"})]
    monkeypatch.setattr(regions, "STRADDLERS_IN_EVERY_REGION", False)
    assert twin({"area_id": "area:region:5"}) == [("r6b", {"f5:0", "lake:L"})]


# ------------------------------------------------- Z1 on a straddler's two tables (T183 side run)

def test_z1_names_the_row_that_revived_the_other_regions_identical_quota():
    """Willow River (a Zone 7A row) lifts both regions' spring closures on a piece in Regions 5 and 7A.
    In the base alone, Zone 7A's closure beats Region 5's bass quota (stricter region) and Zone 7A's
    identical quota speaks; on the page the closures are lifted, Region 5's quota speaks and Zone 7A's
    is shown once, under it (same as peer). That swap is the row's doing: `attribute` names it.
    MUTATION (the revived-displacer branch removed): Zone 7A's quota is unattributable."""
    from pipeline.tools.audit import base_deltas as BD
    q5, q7, c5, c7 = ("z5:species_quotas", "r2"), ("z7a:species_quotas", "r2"), \
        ("z5:spring", "r1"), ("z7a:spring", "r1")
    willow = ("r7:willow_river@7-9", "willow_river.r1")
    rules = {k: {"_rank": 3} for k in (q5, q7, c5, c7)}
    rules[willow] = {"_rank": 1}

    def x(k, state, by=None):
        return {"entry": k[0], "rule": k[1], "state": state, "by": by and f"{by[0]}::{by[1]}",
                "reason": None}

    base = [x(q5, "displaced", c7), x(q7, "speaks"), x(c5, "speaks"), x(c7, "speaks")]
    page = [x(q5, "speaks"), x(q7, "displaced", q5), x(c5, "lifted", willow), x(c7, "lifted", willow),
            x(willow, "speaks")]
    got = {d.rule: d.row for d in BD.attribute(base, page, rules)}
    assert got["z7a:species_quotas::r2"] == "r7:willow_river@7-9"
    assert got["z5:species_quotas::r2"] == "r7:willow_river@7-9"
    assert None not in got.values()


# ------------------------------------------------------------- a touch is not membership (T183)

def test_a_region_holds_a_piece_only_past_a_sliver():
    """A straddler carries both regions' rules, so a piece reaching under `SLIVER_M` over the line is
    positional error, not membership: `regions.holds` (the atlas's membership pass asks it) wants
    more than SLIVER_M inside a REGION; any other area keeps its own threshold. MUTATION (`holds`
    using `otherwise_m` for regions): the 3 m reach into Region 5 is flagged."""
    from shapely.geometry import box
    from pipeline.atlas.graph.blk_chains import build_blk_chains
    from pipeline.atlas.graph.graph import build_section_geometries, build_stream_graph
    from pipeline.atlas.splits.border import mark_inside_areas
    from pipeline.common.tolerances import SLIVER_M
    from pipeline.tests.test_border import _fid

    assert not regions.holds("area:region:5", SLIVER_M, 1.0)
    assert regions.holds("area:region:5", SLIVER_M + 0.01, 1.0)
    assert regions.holds("area:park:x", 2.0, 1.0)
    # Y lies in x 50..103, reaching 3 m past x=100 into the east polygon; Z reaches 40 m
    fids = [_fid("Y1", "Y", "300", [(50, 10), (103, 10)], 0, 53),
            _fid("Z1", "Z", "400", [(50, 30), (140, 30)], 0, 90)]
    chains = build_blk_chains(fids, {})
    graph = build_stream_graph(chains, fids, {}, {})
    geoms = build_section_geometries(chains, fids, {})
    east = box(100, 0, 200, 100)
    mark_inside_areas(graph, geoms, {"area:region:5": east, "area:park:east": east})
    by_blk = {nid.split(":")[0]: node.in_areas for nid, node in graph.nodes.items()}
    assert "area:region:5" not in by_blk["Y"] and "area:park:east" in by_blk["Y"]
    assert "area:region:5" in by_blk["Z"] and "area:park:east" in by_blk["Z"]
