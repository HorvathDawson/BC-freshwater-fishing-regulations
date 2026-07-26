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
    assert by_id["example_dam"].barrier is True
    # wsc-target (multi-cut) form present
    assert by_id["fraser_zone_boundary_example"].wsc == "100-190442"
    # coord parsed as lon/lat tuple
    assert by_id["example_point_on_blk"].anchor.coord == (-119.615, 51.010)
    assert by_id["example_point_on_blk"].anchor.is_lonlat is True


def test_target_must_be_exactly_one():
    with pytest.raises(ValueError):
        SplitDef.from_dict({"id": "bad", "anchor": {"type": "lake", "wbk": "1"},
                            "blk": "1", "wsc": "2"})   # two targets
    with pytest.raises(ValueError):
        SplitDef.from_dict({"id": "bad2", "anchor": {"type": "lake", "wbk": "1"}})  # zero targets


def test_duplicate_ids_rejected(tmp_path):
    import json
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"splits": [
        {"id": "dup", "blk": "1", "anchor": {"type": "point", "coord": [0, 0]}},
        {"id": "dup", "blk": "2", "anchor": {"type": "point", "coord": [1, 1]}},
    ]}))
    with pytest.raises(ValueError):
        load_split_defs(str(p))
