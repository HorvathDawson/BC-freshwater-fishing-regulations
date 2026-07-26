"""Split loading/validation tests (04). Anchor resolution is a later (sections-step) test."""

from pathlib import Path

import pytest

from stream_sections.models import SplitDef
from stream_sections.splits import load_split_defs

_EXAMPLE = Path(__file__).resolve().parents[1] / "splits.example.json"


def test_example_file_loads():
    defs = load_split_defs(str(_EXAMPLE))
    assert len(defs) == 6
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
    p.write_text(json.dumps({"splits": [
        {"id": "dup", "blk": "1", "anchor": {"type": "point", "coord": [0, 0]}},
        {"id": "dup", "blk": "2", "anchor": {"type": "point", "coord": [1, 1]}},
    ]}))
    with pytest.raises(ValueError):
        load_split_defs(str(p))
