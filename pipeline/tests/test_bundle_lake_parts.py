"""`item.part_of` — a lake's curated PARTS carried into the bundle, so no reader opens the
curated polygons to learn that Kootenay Lake's Main Body is part of Kootenay Lake."""

import pytest

import importlib

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


@pytest.fixture
def added(monkeypatch):
    def use(*parts):
        monkeypatch.setattr("pipeline.atlas.waters.added_lakes.ingest.load",
                            lambda path=None: _lakes(*parts))
    return use


def test_parts_name_their_lake_and_a_standalone_lake_names_none(added):
    added((15, "Shannon Lake — netted-off portion", "329459193"),
          (23, "Shannon Lake — proper", "329459193"),
          (1, "Redsand Lake", None))
    ords = {"wbk:329459193": 0, "wbk:-15": 1, "wbk:-23": 2, "wbk:-1": 3}
    assert B._lake_parts(ords) == {"wbk:-15": "wbk:329459193", "wbk:-23": "wbk:329459193"}


def test_a_part_this_build_lacks_is_refused(added):
    """The polygon was drawn after the atlas was built: a reader grouping by `part_of` would
    show the lake with a rung missing, and nothing would say so."""
    added((20, "Kootenay Lake — Main Body", "328974235"))
    with pytest.raises(SystemExit, match=r"wbk:-20 .* not in this build"):
        B._lake_parts({"wbk:328974235": 0})


def test_a_part_of_a_lake_this_build_lacks_is_refused(added):
    added((20, "Kootenay Lake — Main Body", "328974235"))
    with pytest.raises(SystemExit, match=r"part_of wbk:328974235, which is not a water here"):
        B._lake_parts({"wbk:-20": 0})

