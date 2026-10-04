"""`item.part_of` — a lake's curated PARTS carried into the bundle, so no reader opens the
curated polygons to learn that Kootenay Lake's Main Body is part of Kootenay Lake.

The relation is written ONCE, by the atlas build, onto the registry (`registry.add_lake_parts`
from what the added-lakes ingest read); the bundle reads the registry's answer and nothing else
(`build._lake_parts`). Neither step may skip a disagreement: a part the atlas lacks, or a parent
that is not a water, is refused."""

import importlib

import pytest

from pipeline.atlas.registry import add_lake_parts
from pipeline.atlas.waters.added_lakes.ingest import lake_parts
from pipeline.common.models import RegistryItem

B = importlib.import_module("pipeline.deliver.bundle.build")


def _lakes(*parts):
    """Added-lake records in the shape `added_lakes.ingest.load` returns."""
    out = []
    for lake_id, name, parent in parts:
        props = {"id": lake_id, "name": name}
        if parent:
            props["part_of"] = {"wbk": parent}
        out.append({"id": lake_id, "wbk": f"-{lake_id}", "name": name, "props": props})
    return out


def _registry(*ids):
    return {i: RegistryItem(id=i, name=i, kind="lake") for i in ids}


def test_the_ingest_reports_each_parts_lake_and_a_standalone_lake_none():
    got = lake_parts(_lakes((15, "Shannon Lake — netted-off portion", "329459193"),
                            (23, "Shannon Lake — proper", "329459193"),
                            (1, "Redsand Lake", None)))
    assert got == {"-15": "329459193", "-23": "329459193"}


def test_the_atlas_writes_part_of_onto_the_registry_once():
    reg = add_lake_parts(_registry("wbk:329459193", "wbk:-15", "wbk:-23", "wbk:-1"),
                         {"-15": "329459193", "-23": "329459193"})
    assert reg["wbk:-15"].part_of == "wbk:329459193"
    assert reg["wbk:-23"].part_of == "wbk:329459193"
    assert reg["wbk:-1"].part_of == "" and reg["wbk:329459193"].part_of == ""


def test_the_registry_round_trips_part_of(tmp_path):
    from pipeline.atlas.registry import load_registry, write_registry
    reg = add_lake_parts(_registry("wbk:1", "wbk:-2"), {"-2": "1"})
    write_registry(reg, tmp_path / "registry.json")
    back = load_registry(tmp_path / "registry.json")
    assert back["wbk:-2"].part_of == "wbk:1" and back["wbk:1"].part_of == ""
    assert '"part_of"' in (tmp_path / "registry.json").read_text().split('"id": "wbk:-2"')[1] \
        .split("}")[0]


def test_a_part_this_build_lacks_is_refused():
    """The polygon was drawn after the atlas was built: a reader grouping by `part_of` would
    show the lake with a rung missing, and nothing would say so."""
    with pytest.raises(SystemExit, match=r"wbk:-20 is not in this build"):
        add_lake_parts(_registry("wbk:328974235"), {"-20": "328974235"})


def test_a_part_of_a_lake_this_build_lacks_is_refused():
    with pytest.raises(SystemExit, match=r"part_of wbk:328974235, which is not a water here"):
        add_lake_parts(_registry("wbk:-20"), {"-20": "328974235"})


def test_the_bundle_reads_the_registrys_answer_and_refuses_a_dangling_parent():
    items = [{"id": "wbk:329459193", "name": "Shannon Lake"},
             {"id": "wbk:-15", "name": "netted-off portion", "part_of": "wbk:329459193"},
             {"id": "wbk:-1", "name": "Redsand Lake"}]
    assert B._lake_parts(items) == {"wbk:-15": "wbk:329459193"}
    with pytest.raises(SystemExit, match=r"part_of wbk:9, which is not a water here"):
        B._lake_parts([{"id": "wbk:-15", "name": "x", "part_of": "wbk:9"}])
