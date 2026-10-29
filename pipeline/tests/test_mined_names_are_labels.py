"""T224/T222 (owner 2026-10-28): a mined name is a label, never a grouping key (`registry.mined`)."""
from types import SimpleNamespace as NS

from pipeline.atlas.registry.mined import add_mined_names
from pipeline.common.models import NodeKind, RegistryItem


def _node(nid, blk, kind=NodeKind.stream, wsc="100-1"):
    return NS(node_id=nid, blk=blk, kind=kind, gnis_id="", name_tuples=(), wbk="", wsc=wsc)


def _graph(*nodes):
    return NS(nodes={n.node_id: n for n in nodes})


def _entry(blk, name, source="osm"):
    return {"target": {"blks": [blk]}, "names": [{"name": name, "source": source, "source_label": "OpenStreetMap"}]}


def test_owned_feature_gets_a_variant_and_keeps_its_item():
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="Quinsam River", kind="stream", variants=("Quinsam River",),
                                  section_ids=("10:0", "11:0"))}
    out = add_mined_names(reg, _graph(_node("10:0", "10"), _node("11:0", "11")), [_entry("11", "Headpond")])
    assert set(out) == {"gnis:1"}
    assert out["gnis:1"].section_ids == ("10:0", "11:0") and out["gnis:1"].name == "Quinsam River"
    assert "Headpond" in out["gnis:1"].variants


def test_unowned_feature_is_its_own_item_and_never_joins_a_river():
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="Quinsam River", kind="stream", section_ids=("10:0",))}
    rep: dict = {}
    out = add_mined_names(reg, _graph(_node("10:0", "10"), _node("12:0", "12")), [_entry("12", "Diversion")], rep)
    assert out["gnis:1"].section_ids == ("10:0",)
    assert out["blk:12"].section_ids == ("12:0",) and out["blk:12"].name == "Diversion"
    assert rep["own_items"] == 1


def test_two_features_with_one_name_stay_two_items_and_a_lake_is_never_taken():
    g = _graph(_node("12:0", "12"), _node("13:0", "13"), _node("lake:9", "12", kind=NodeKind.lake))
    out = add_mined_names({}, g, [_entry("12", "Silverstream Slough"), _entry("13", "Silverstream Slough")])
    assert out["blk:12"].section_ids == ("12:0",) and out["blk:13"].section_ids == ("13:0",)


def test_a_feature_not_in_the_atlas_is_reported_not_applied():
    rep: dict = {}
    out = add_mined_names({}, _graph(), [_entry("99", "Gone Creek")], rep)
    assert out == {} and rep["absent"] == 1
