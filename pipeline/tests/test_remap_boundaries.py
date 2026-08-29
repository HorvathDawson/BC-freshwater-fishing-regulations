"""remap_boundaries re-points entries at renamed auto boundaries, keyed on the stable `ref`."""

import json

from pipeline.models import RegistryBoundary, RegistryItem
from pipeline.parsing.remap_boundaries import apply_remap, build_remap


def _item(iid, name, boundaries):
    return {iid: RegistryItem(id=iid, name=name, kind="stream", section_ids=(f"{iid}:0",),
                              boundaries=tuple(RegistryBoundary(id=i, label=l, kind="lake", ref=r)
                                               for i, l, r in boundaries))}


def test_remap_follows_the_ref_not_the_name():
    """Renaming the item renames its auto boundaries; `ref` is what stays stable across the rebuild."""
    old = _item("gnis:39492", "McArthur Island Slough",
                [("mcarthur_island_slough__kamloops_lake", "Kamloops Lake", "lake:329")])
    new = _item("gnis:39492", "Thompson River",
                [("thompson_river__kamloops_lake", "Kamloops Lake", "lake:329")])
    assert build_remap(old, new) == {
        "mcarthur_island_slough__kamloops_lake": "thompson_river__kamloops_lake"}


def test_unchanged_boundary_is_not_in_the_remap():
    same = _item("gnis:1", "River X", [("river_x__foo_lake", "Foo Lake", "lake:1")])
    assert build_remap(same, same) == {}


def test_a_ref_missing_from_the_new_registry_is_not_guessed():
    old = _item("gnis:1", "River X", [("river_x__gone_lake", "Gone Lake", "lake:99")])
    new = _item("gnis:1", "River X", [("river_x__foo_lake", "Foo Lake", "lake:1")])
    assert "river_x__gone_lake" not in build_remap(old, new)


def _entry(eid, splits, *, locked=False):
    return {
        "entry_id": eid, "identity": {"name": "X", "region": "3", "mus": []},
        "regs_verbatim": "**No Fishing**", "locked": locked, "registry_status": "matched",
        "rules": [{"rule_id": "x.r1", "restriction_type": "closure", "details": "No fishing",
                   "rule_text": "**No Fishing**",
                   "extents": [{"op": "upstream_of", "splits": splits}]}],
    }


def test_rewrites_the_bound_split_id(tmp_path):
    p = tmp_path / "region-3.json"
    p.write_text(json.dumps({"region": "3", "entries": [
        _entry("gnis:39492#a", ["mcarthur_island_slough__kamloops_lake"], locked=True)]}), encoding="utf-8")
    report = apply_remap(tmp_path, {"mcarthur_island_slough__kamloops_lake": "thompson_river__kamloops_lake"})

    assert report["locked_changed"] == ["gnis:39492#a"]
    e = json.loads(p.read_text())["entries"][0]
    assert e["rules"][0]["extents"][0]["splits"] == ["thompson_river__kamloops_lake"]
    assert e["locked"] is True


def test_unaffected_entry_and_dry_run_write_nothing(tmp_path):
    p = tmp_path / "region-3.json"
    p.write_text(json.dumps({"region": "3", "entries": [_entry("gnis:1", ["river_x__foo_lake"])]}),
                 encoding="utf-8")
    before = p.read_text()
    assert apply_remap(tmp_path, {"other__id": "new__id"})["changed"] == []
    assert p.read_text() == before
    assert apply_remap(tmp_path, {"river_x__foo_lake": "y__foo_lake"}, dry_run=True)["changed"]
    assert p.read_text() == before
