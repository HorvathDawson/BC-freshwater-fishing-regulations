"""Every layer must actually emit the feature id its contract declares.

THE SEAM NOBODY WAS CHECKING. `app/tools/tile-contract.test.ts` compares the style to the
contract. `test_tiles.py` compares layers.py to areas.json. Nothing compared the EXPORTER
to either, so `export_admin` wrote `id` where the contract says `area_id`, `_writer`
silently dropped it as an unknown attribute, and six admin layers shipped with no feature
id at all — `indigenous_land.geojsonl` has `"properties": {}` on every feature.

A feature with no id cannot be addressed by `setFeatureState`, so a park closure could
never be coloured. Nothing errors. The map is simply, quietly, wrong.
"""

from __future__ import annotations

import inspect
import re

from pipeline.tiles import export as E
from pipeline.tiles.layers import ALL, BY_NAME


def _emitted_prop_keys(fn) -> set[str]:
    """The literal dict keys each `write(...)` call in `fn` passes as properties."""
    src = inspect.getsource(fn)
    keys: set[str] = set()
    # every `"key":` inside the source — deliberately generous, since a MISSING key is the
    # failure and a spurious extra one is caught by the allowlist test below.
    for m in re.finditer(r'"([a-z_0-9]+)"\s*:', src):
        keys.add(m.group(1))
    return keys


EXPORTERS = {
    "stream": E.export_streams, "under_lake": E.export_streams,
    "lake": E.export_waterbodies, "wetland": E.export_waterbodies,
    "park": E.export_admin, "eco_reserve": E.export_admin, "wma": E.export_admin,
    "indigenous_land": E.export_admin, "no_access": E.export_admin,
    "watershed": E.export_admin, "mu": E.export_admin,
    "place": E.export_places, "contour": E.export_contours,
    "outside": E.export_outside,
}


def test_every_layer_emits_its_declared_feature_id():
    missing = []
    for spec in ALL:
        fn = EXPORTERS.get(spec.name)
        if fn is None:
            missing.append(f"{spec.name}: no exporter is mapped in this test")
            continue
        feature_id = spec.attrs[0]
        if feature_id not in _emitted_prop_keys(fn):
            missing.append(
                f"{spec.name}: contract feature id {feature_id!r} is never written by "
                f"{fn.__name__} — _writer will drop it and the layer ships with no id")
    assert not missing, "\n".join(missing)


def test_writer_drops_anything_not_declared():
    """The allowlist is what made the bug silent, so pin that it IS an allowlist."""
    src = inspect.getsource(E._writer)
    assert "k in allowed" in src, (
        "_writer no longer filters to spec.attrs; if it started passing unknown keys "
        "through, a typo'd feature id would reach the tile and this whole class of bug "
        "would change shape rather than disappear")


def test_the_contract_names_a_feature_id_for_every_layer():
    for spec in ALL:
        assert spec.attrs, f"{spec.name} declares no attributes at all"
        assert BY_NAME[spec.name] is spec
