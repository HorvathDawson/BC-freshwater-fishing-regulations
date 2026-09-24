"""Split loading/validation tests (04). Anchor resolution is a later (sections-step) test."""

import json
from pathlib import Path

import pytest

from pipeline.common.models import SplitDef
from pipeline.atlas.splits.splits import load_split_defs
from pipeline.common.curated import CURATED, SOURCE

# The anchor/target forms the loader must accept, inline (was splits.example.json — removed).
# The by-waterbody shape — the only one the loader reads. Each split names its own target in
# `applies_to`, so one fixture still covers the blk-target and wsc-target (braided) forms.
_SAMPLE_SPLITS = {
    "waterbodies": [
        {"name": "SOUTH THOMPSON RIVER", "applies_to": {"blk": "356362743"},
         "splits": [
             {"id": "adams_lake", "anchor": {"type": "lake", "wbk": "329480864"}},
             {"id": "example_point_on_blk",
              "anchor": {"type": "point", "coord": [-119.615, 51.010], "is_lonlat": True},
              "label": "Squilax bridge"},
             {"id": "example_confluence",
              "anchor": {"type": "confluence", "tributary_blk": "356358584"}},
         ]},
        {"name": "FRASER RIVER", "applies_to": {"wsc": "100-190442"},
         "splits": [
             {"id": "fraser_section_boundary_example", "proximity_m": 1200,
              "anchor": {"type": "point", "coord": [-121.5, 49.1], "is_lonlat": True}},
             {"id": "fraser_mu_2_3", "applies_to": {"blk": "356362743"},
              "anchor": {"type": "mu_boundary", "mu_a": "2-3", "mu_b": "3-12"}},
         ]},
    ]
}


def test_sample_splits_load(tmp_path):
    p = tmp_path / "splits.json"
    p.write_text(json.dumps(_SAMPLE_SPLITS))
    defs = load_split_defs(str(p))
    assert len(defs) == 5
    by_id = {d.id: d for d in defs}
    assert by_id["adams_lake"].anchor.type.value == "lake"
    # wsc-target (braided, proximity-limited) form present
    assert by_id["fraser_section_boundary_example"].wsc == "100-190442"
    assert by_id["fraser_section_boundary_example"].proximity_m == 1200
    # coord parsed as lon/lat tuple
    assert by_id["example_point_on_blk"].anchor.coord == (-119.615, 51.010)
    assert by_id["example_point_on_blk"].anchor.is_lonlat is True
    # mu_boundary carries two MUs; confluence carries a tributary blk
    assert (by_id["fraser_mu_2_3"].anchor.mu_a, by_id["fraser_mu_2_3"].anchor.mu_b) == ("2-3", "3-12")
    assert by_id["example_confluence"].anchor.tributary_blk == "356358584"


def test_at_most_one_target_and_point_needs_one():
    with pytest.raises(ValueError):   # two targets
        SplitDef.from_dict({"id": "bad", "anchor": {"type": "lake", "wbk": "1"},
                            "blk": "1", "wsc": "2"})
    with pytest.raises(ValueError):   # point anchor with no target mainstem
        SplitDef.from_dict({"id": "bad2", "anchor": {"type": "point", "coord": [0, 0]}})
    # lake anchor with no target is allowed (geometry defines scope)
    ok = SplitDef.from_dict({"id": "ok", "anchor": {"type": "lake", "wbk": "1"}})
    assert ok.id == "ok"


def test_anchor_field_requirements():
    with pytest.raises(ValueError):   # mu_boundary needs both MUs
        SplitDef.from_dict({"id": "m", "gnis_id": "1",
                            "anchor": {"type": "mu_boundary", "mu_a": "2-3"}})
    with pytest.raises(ValueError):   # line needs >=2 coords
        SplitDef.from_dict({"id": "l", "wsc": "1", "anchor": {"type": "line", "coords": [[0, 0]]}})
    with pytest.raises(ValueError):   # confluence needs tributary_blk
        SplitDef.from_dict({"id": "c", "blk": "1", "anchor": {"type": "confluence"}})


def test_duplicate_ids_rejected(tmp_path):
    import json
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"waterbodies": [
        {"name": "A", "applies_to": {"blk": "1"},
         "splits": [{"id": "dup", "anchor": {"type": "point", "coord": [0, 0]}}]},
        {"name": "B", "applies_to": {"blk": "2"},
         "splits": [{"id": "dup", "anchor": {"type": "point", "coord": [1, 1]}}]},
    ]}))
    with pytest.raises(ValueError, match="duplicate split ids"):
        load_split_defs(str(p))


@pytest.mark.parametrize("body", [
    {"splits": [{"id": "flat", "blk": "1", "anchor": {"type": "point", "coord": [0, 0]}}]},
    [{"id": "bare", "blk": "1", "anchor": {"type": "point", "coord": [0, 0]}}],
])
def test_the_flat_shape_is_refused(tmp_path, body):
    """The retired flat shape is refused outright, not read: a second accepted shape is a second
    place a target can hide."""
    p = tmp_path / "s.json"
    p.write_text(json.dumps(body))
    with pytest.raises(ValueError, match="by-waterbody shape"):
        load_split_defs(str(p))


def test_inherited_multi_gnis_target_uses_the_primary_water():
    """A waterbody spanning several rivers resolves an INHERITED target to the first (primary) one;
    a split that belongs to another member names it in its own applies_to."""
    from pipeline.atlas.splits.splits import _flatten_waterbodies

    flat, untargeted = _flatten_waterbodies({"waterbodies": [{
        "name": "CHILLIWACK / VEDDER RIVERS",
        "applies_to": {"gnis_ids": ["8634", "3062", "29662"]},
        "splits": [
            {"id": "tamihi", "kind": "point",
             "anchor": {"type": "point", "coord": [-121.83788, 49.07184], "is_lonlat": True}},
            {"id": "vedder_crossing", "kind": "confluence", "applies_to": {"gnis_id": "3062"},
             "anchor": {"type": "confluence", "tributary_blk": "380887781"}},
        ],
    }]})
    assert untargeted == []
    by = {d["id"]: d for d in flat}
    assert by["tamihi"]["gnis_id"] == "8634", "inherited -> primary water"
    assert by["vedder_crossing"]["gnis_id"] == "3062", "explicit target wins"


def test_lake_anchor_accepts_an_offset():
    """A lake anchor CAN carry an along-channel offset — "100 m downstream of the lake outlet" is a
    cut that must travel with the lake. Model validation used to refuse it, and `load_split_defs`
    downgraded that refusal to a warning about targeting, so six curated cuts silently vanished from a
    full build."""
    from pipeline.common.models import AnchorType, SplitAnchor

    a = SplitAnchor.from_dict({"type": "lake", "wbk": "329216614",
                               "offset_m": 1500, "offset_dir": "downstream"})
    assert a.type == AnchorType.lake and a.wbk == "329216614"
    assert a.offset_m == 1500 and a.offset_dir == "downstream"


def test_lake_anchor_offset_still_needs_a_direction():
    import pytest as _pytest
    from pipeline.common.models import SplitAnchor

    with _pytest.raises(ValueError, match="offset_dir"):
        SplitAnchor.from_dict({"type": "lake", "wbk": "1", "offset_m": 100})


def test_every_waterbody_block_targets_a_real_registry_item():
    """A `applies_to` naming an item that does not exist strands every split under it, silently.

    `load_split_defs` validates the SHAPE of a target, not that anything answers to it — so a made-up
    id parses fine, resolves to nothing at build time, and the cut simply never appears. That is how
    `telkwa_river__howson_creek_into_telkwa_river` was written against `gnis:3773` (a number I did not
    check; the Telkwa is `gnis:24733`) and went missing from a full build without a single warning.

    Skipped when there is no built registry to check against.
    """
    import json
    from pathlib import Path

    from pipeline.atlas.registry import load_registry

    from pipeline.common.curated import GENERATED
    reg_path = GENERATED.build() / "registry.json"
    if not reg_path.exists():
        pytest.skip("no built registry")
    from pipeline.regs.matching.matcher import build_id_index

    reg = load_registry(reg_path)
    # Resolve through the ID INDEX, not the item ids: a target may legitimately name an FWA id that
    # is not itself an item key — `gnis:29662` is the Vedder Canal's gnis and resolves to
    # `wbk:329707189`. Checking membership in `reg` alone flags those as broken when they are fine.
    idx = build_id_index(reg)
    body = json.loads(CURATED.waters.splits.read_text())

    bad = []
    for wb in body.get("waterbodies", []):
        ap = wb.get("applies_to") or {}
        targets = []
        for key, prefix in (("gnis_id", "gnis"), ("waterbody_key", "wbk"), ("wbk", "wbk")):
            if ap.get(key):
                targets.append(f"{prefix}:{ap[key]}")
        for g in ap.get("gnis_ids", []):
            targets.append(f"gnis:{g}")
        for t in targets:
            if t not in reg and t not in idx:
                bad.append(f"{wb.get('name')!r} -> {t} ({len(wb.get('splits') or [])} split(s) stranded)")
    assert not bad, ("waterbody block(s) whose applies_to resolves to NOTHING — their splits are "
                     "silently stranded:\n  " + "\n  ".join(bad))
