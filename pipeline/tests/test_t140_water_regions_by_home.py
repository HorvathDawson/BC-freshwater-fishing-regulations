"""T140 — the bundle's REGIONS OF A WATER (G3) and the AREA FAMILIES ask the one region membership.

T115 made `registry.regions` the one answer to "which sections are in region R?" for the reach run.
Two bundle-side readers were left: `rules.regions_of_waters` called `outside.water_regions` with no
graph (plain polygon membership: a stream piece straddling a region line was in BOTH regions), and
`reach.licensing.national_park_sections` / `bundle.licensing`'s `province_except` read an
`area:<kind>:` family themselves.

Now:
  * `regions_of_waters(registry, docs, home)` reads the atlas's home map (`regions.home_view`, taken
    off the graph the bundle build holds and handed to `rules.write(home=)`), so a printed lift
    (G3, `equivalent_regions`) carries into a region only where the row's water really lies.
  * both area-family readers call `regions.area_family_sections`.

T183 (owner 2026-10-24) made the one membership RAW — a straddling stream piece lies in both regions,
as a lake does — so the regions of a water now include a region it only straddles into, and the lift
carries there (the piece carries that region's closure too). The T140 behaviour is what
`regions.STRADDLERS_IN_EVERY_REGION = False` gives, and the tests below hold it under that pin.

Mutation pins:
  * `regions.STRADDLERS_IN_EVERY_REGION = False` fails `test_a_straddling_piece_is_a_region_of_the_water`
    and `test_the_lift_carries_into_a_region_the_water_straddles_into`.
  * with that pin off, `rules.regions.WATER_REGIONS_BY_HOME = False` fails `test_a_straddling_piece_homed_elsewhere_is_not_a_region_of_the_water`
    and `test_the_lift_does_not_carry_into_a_region_the_water_only_straddles_into`.
  * `bundle.build` dropping `home=` on `_rules.write` fails `test_the_bundle_hands_its_home_map_to_the_rules`.
  * an `f"area:{kind}:"` family prefix built outside `regions.py` fails `test_no_second_copy_of_an_area_family`.
"""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.atlas.reach import licensing as LIC
from pipeline.atlas.registry import regions
from pipeline.common.models import NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem
from pipeline.deliver.bundle import rules as R

ROOT = Path(__file__).resolve().parents[1]


def _node(nid):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=nid.split(":")[0], wsc="100",
                      down_m=0.0, up_m=100.0, length_m=100.0, stream_order=6)


def _world(attach=True):
    """Region 3 holds c3:0 and the straddling c35:0 (inside polygons 3 AND 5, home 3); Region 5 holds
    c35:0 and a lake L straddling the line too (in both). Creek C is c3:0 + c35:0 — it lies in Region
    3 only. Lake water W is the straddling lake — it lies in both."""
    g = StreamGraph()
    g.nodes = {n: _node(n) for n in ("c3:0", "c35:0")}
    reg = {"area:region:3": RegistryItem(id="area:region:3", name="Region 3", kind="area",
                                         section_ids=("c3:0", "c35:0", "lake:L")),
           "area:region:5": RegistryItem(id="area:region:5", name="Region 5", kind="area",
                                         section_ids=("c35:0", "lake:L")),
           "area:national_parks:yoho": RegistryItem(id="area:national_parks:yoho", name="Yoho",
                                                    kind="area", section_ids=("c3:0",)),
           "gnis:C": RegistryItem(id="gnis:C", name="C Creek", kind="stream",
                                  section_ids=("c3:0", "c35:0")),
           "wbk:W": RegistryItem(id="wbk:W", name="W Lake", kind="lake", section_ids=("lake:L",))}
    if attach:
        regions.attach(g, {"c35:0": "3", "lake:L": "3"}, both={"lake:L"})
    return g, reg


def _ce(eid, matched):
    return SimpleNamespace(entry_id=eid, matched=tuple(matched), rules=())


DOCS = [_ce("r3:c_creek@3-1", ["gnis:C"]), _ce("r3:w_lake@3-1", ["wbk:W"])]


def test_a_straddling_piece_is_a_region_of_the_water(monkeypatch):
    """T183: the creek's piece straddling into Region 5 makes Region 5 one of its regions. Under the
    T115 pin, the home map holds it to Region 3 (T140), and WATER_REGIONS_BY_HOME off undoes that."""
    g, reg = _world()
    both = {"r3:c_creek@3-1": frozenset({"3", "5"}), "r3:w_lake@3-1": frozenset({"3", "5"})}
    assert R.regions_of_waters(reg, DOCS, regions.home_view(g)) == both
    monkeypatch.setattr(regions, "STRADDLERS_IN_EVERY_REGION", False)
    assert R.regions_of_waters(reg, DOCS, regions.home_view(g)) == {
        "r3:c_creek@3-1": frozenset({"3"}), "r3:w_lake@3-1": frozenset({"3", "5"})}
    from pipeline.deliver.bundle.rules import regions as RR
    monkeypatch.setattr(RR, "WATER_REGIONS_BY_HOME", False)
    assert R.regions_of_waters(reg, DOCS, regions.home_view(g)) == both


def test_without_a_home_map_it_is_plain_membership():
    """A fixture (no home) reads plain polygon membership — what the bundle read before T140."""
    _, reg = _world()
    assert R.regions_of_waters(reg, DOCS) == {
        "r3:c_creek@3-1": frozenset({"3", "5"}), "r3:w_lake@3-1": frozenset({"3", "5"})}


def test_the_lift_carries_into_a_region_the_water_straddles_into(monkeypatch):
    """G3 end to end (`equivalent_regions`): since T183 the creek's straddling piece carries Region
    5's closure, so its Region 3 row's printed lift carries into Region 5 too. Under the T115 pin it
    carries into Region 3 only (T140)."""
    g, reg = _world()
    water = R.regions_of_waters(reg, DOCS, regions.home_view(g))
    assert R.equivalent_regions(DOCS[0], "c_creek.r1", water, {}, {}) == frozenset({"3", "5"})
    assert R.equivalent_regions(DOCS[1], "w_lake.r1", water, {}, {}) == frozenset({"3", "5"})
    monkeypatch.setattr(regions, "STRADDLERS_IN_EVERY_REGION", False)
    water = R.regions_of_waters(reg, DOCS, regions.home_view(g))
    assert R.equivalent_regions(DOCS[0], "c_creek.r1", water, {}, {}) == frozenset({"3"})


def test_the_view_answers_as_the_graph_does():
    g, reg = _world()
    view = regions.home_view(g)
    for regs in (("3",), ("5",), ("3", "5")):
        assert regions.region_sections(regs, reg, view) == regions.region_sections(regs, reg, g)
    assert regions.section_regions(reg, view) == regions.section_regions(reg, g) == {
        "c3:0": {"3"}, "c35:0": {"3", "5"}, "lake:L": {"3", "5"}}


def test_a_view_of_a_graph_with_no_home_map_is_refused():
    g, _ = _world(attach=False)
    with pytest.raises(ValueError, match="no home map"):
        regions.home_view(g)


def test_the_area_families_are_the_one_reader_s():
    _, reg = _world()
    assert LIC.national_park_sections(reg) == regions.area_family_sections(reg, "national_parks") \
        == frozenset({"c3:0"})
    del reg["area:national_parks:yoho"]
    assert LIC.national_park_sections(reg) == frozenset()


# --------------------------------------------------------------------------- the wiring

def _calls(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            if (isinstance(f, ast.Attribute) and f.attr == name) or \
                    (isinstance(f, ast.Name) and f.id == name):
                yield node


def test_the_bundle_hands_its_home_map_to_the_rules():
    calls = list(_calls(ROOT / "deliver" / "bundle" / "build.py", "write"))
    rules_write = [c for c in calls if isinstance(c.func, ast.Attribute)
                   and isinstance(c.func.value, ast.Name) and c.func.value.id == "_rules"]
    assert len(rules_write) == 1
    assert "home" in {k.arg for k in rules_write[0].keywords}
    got = list(_calls(ROOT / "deliver" / "bundle" / "rules" / "tables.py", "regions_of_waters"))
    assert len(got) == 1 and len(got[0].args) == 3


#: Where an `area:<kind>:` family may be read. Everything else asks `regions.area_family_sections`.
SCANNED = sorted([*(ROOT / "atlas" / "reach").glob("*.py"), *(ROOT / "deliver" / "bundle").rglob("*.py")])


@pytest.mark.parametrize("path", SCANNED, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_no_second_copy_of_an_area_family(path):
    """No f-string `f"area:{kind}:"` (a family PREFIX: starts "area:", ends ":") outside `regions.py`."""
    bad = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.JoinedStr) and node.values:
            first, last = node.values[0], node.values[-1]
            if isinstance(first, ast.Constant) and str(first.value).startswith("area:") \
                    and isinstance(last, ast.Constant) and str(last.value).endswith(":"):
                bad.append(node.lineno)
    assert not bad, f"{path.name} builds an area-family prefix itself at lines {bad}"


def test_the_scan_covers_the_readers_it_unified():
    names = {f"{p.parent.name}/{p.name}" for p in SCANNED}
    assert {"reach/licensing.py", "bundle/licensing.py", "rules/row.py", "rules/regions.py"} <= names
