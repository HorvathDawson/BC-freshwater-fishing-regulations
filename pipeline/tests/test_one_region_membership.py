"""T115 — ONE REGION MEMBERSHIP: "which sections are in region R?" is answered once, by
`registry.regions` (owner 2026-10-22, unification audit finding reach 1).

The zone rules took a region's HOME members (`extent.area_sections` -> `regions.in_region`: a stream
piece straddling a region line is its home region's, a lake is in both), but a per-region row's limit
(`outside.region_sections` -> `region_limit`), `outside._membership`, the steelhead twin's `area_id`
and the `area_kind`/`outside_area_kind` families read the raw polygon membership. So
`r5:fraser_river@5-2`'s rules r2/r3/r4 bound Fraser piece 356364114:418405, home Region 3 (inside
polygons 3 and 5): 12 rule–section pairs of another region's rules on 4 Fraser pieces.

Now every reader calls `regions.region_sections` / `regions.region_members` /
`regions.area_family_sections` / `regions.section_regions`.

Mutation pins:
  * `regions.ONE_REGION_MEMBERSHIP = False` (raw polygon membership) fails
    `test_a_per_region_row_does_not_bind_a_piece_homed_in_another_region`.
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


def test_a_per_region_row_does_not_bind_a_piece_homed_in_another_region(monkeypatch):
    """The Fraser's Region 5 row binds Region 5's pieces only; the piece homed in Region 3 is the
    Region 3 row's. MUTATION (`ONE_REGION_MEMBERSHIP` off): the raw polygon puts it back."""
    g, reg = _world()
    assert O.region_limit(FRASER_5, reg, SHARED, graph=g) == {"f5:0", "lake:L"}
    assert _bind(g, reg) == {"f5:0"}
    monkeypatch.setattr(regions, "ONE_REGION_MEMBERSHIP", False)
    assert _bind(g, reg) == {"f35:0", "f5:0"}


def test_the_row_limit_and_the_zone_rules_are_one_answer():
    """`area:region:5` for a zone rule and Region 5 for a per-region row: the same set."""
    from pipeline.atlas.reach.extent import area_sections
    g, reg = _world()
    assert area_sections(reg, g, "area:region:5") == set(regions.region_sections(("5",), reg, g)) \
        == {"f5:0", "lake:L"}
    assert area_sections(reg, g, "area:region:3") == {"f3:0", "f35:0", "lake:L"}


def test_the_families_and_the_province_read_the_same_membership():
    g, reg = _world()
    assert regions.area_family_sections(reg, "region", g) == {"f3:0", "f35:0", "f5:0", "lake:L"}
    assert regions.area_family_sections(reg, "park", g) is None
    assert regions.section_regions(reg, g) == {
        "f3:0": {"3"}, "f35:0": {"3"}, "f5:0": {"5"}, "lake:L": {"3", "5"}}
    assert O.water_regions(FRASER_5, reg, g) == ("3", "5")
    assert O.outside_bc(reg, g) == frozenset()


def test_without_a_home_map_it_is_plain_membership():
    """A fixture graph (no `region_home.json` attached) cannot say a home: plain membership."""
    g, reg = _world(attach=False)
    assert regions.region_sections(("5",), reg, g) == {"f35:0", "f5:0", "lake:L"}
    assert regions.region_sections(("5",), reg, None) == {"f35:0", "f5:0", "lake:L"}


def test_re_attaching_a_home_map_is_not_served_from_a_cache():
    g, reg = _world(attach=False)
    assert "f35:0" in regions.region_sections(("5",), reg, g)
    regions.attach(g, {"f35:0": "3"}, both=())
    assert "f35:0" not in regions.region_sections(("5",), reg, g)


# --------------------------------------------------------------------------- no second copy

#: Where "which sections are in region R" may be read. Everything else asks these modules.
OWNER = {ROOT / "atlas" / "registry" / "regions.py"}
#: The readers T115 unified: the reach builder and the entry-id parser.
SCANNED = sorted([*(ROOT / "atlas" / "reach").glob("*.py"), ROOT / "common" / "entry_id.py"])


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
