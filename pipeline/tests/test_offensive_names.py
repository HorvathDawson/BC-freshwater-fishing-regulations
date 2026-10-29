"""(owner 2026-10-28) no slur is ever a name we ship: the deny-list, and the mined-names writer refusing it."""
import json
from pathlib import Path

from pipeline.common.offensive_names import offensive
from pipeline.tools.mined_names import refused

REPO = Path(__file__).resolve().parents[2]


def test_deny_list_matches_slurs_on_word_boundaries_and_known_compounds():
    for name in ("Negro Creek", "Jap Lake", "Squaw Creek", "Squawfish Lake", "Niggerhead Mountain", "Half-Breed Creek",
                 "Half Breed Creek", "Chinaman Lake", "Klootchman Creek", "Siwash Rock", "Coon Creek"):
        assert offensive(name), name


def test_deny_list_leaves_ordinary_names_alone():
    for name in ("Japan Creek", "Raccoon Lake", "Squawk Lake", "Squawkbird Creek", "Baker Creek", "Breed Lake",
                 "Negroni Creek", "Cooney Lake", "Chinook River"):
        assert offensive(name) is None, name


def test_mined_names_writer_refuses_a_slur_whoever_accepted_it():
    assert refused([{"name": "Negro Creek"}, {"name": "Baker Creek"}]) == ["Negro Creek"]
    assert refused([{"name": "Baker Creek"}]) == []


def test_no_shipped_mined_name_carries_a_slur():
    doc = json.loads((REPO / "data/curated/waters/mined_names.json").read_text())
    entries = doc["entries"] if isinstance(doc, dict) else doc
    assert not [n["name"] for e in entries for n in e["names"] if offensive(n["name"])]
