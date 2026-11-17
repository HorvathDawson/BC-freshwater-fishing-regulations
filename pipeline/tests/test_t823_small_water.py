"""T823 — "small next to its gauge", on the SPEAKER: per paint section, one bit per member of the panel the map colours
it from, set where that member drains at least 50x the section (owner 2026-11-16 22:18-22:19;
pipeline/gauges/consume/small_water.py, the `flow-small` file in paint.py).

MUTATION PINS — each mutation and the test it fails:
  * `>=` -> `>` at the threshold                          -> test_the_threshold_is_inclusive
  * flag a section BIGGER than its gauge (symmetric)      -> test_a_section_bigger_than_its_gauge_is_never_small
  * bit i for some other member than ord i (reversed)     -> test_the_bits_follow_the_member_ord
  * a missing area read as small / not small              -> test_a_missing_area_is_refused
  * the switch ignored (one hard-coded ratio)             -> test_the_switch_moves_the_slesse_neighbour
  * one mask too few or too many for paint's sections     -> test_flow_small_pairs_with_flow_area_row_for_row
  * a gap in a panel's ords packed as if dense             -> test_a_gap_in_the_member_ords_is_refused
  * the file dropped from the contract                    -> test_the_contract_ships_flow_small
"""

from __future__ import annotations

import sqlite3

import pytest

from pipeline.deliver.release import contract as C
from pipeline.deliver.release import paint as P
from pipeline.gauges.consume import small_water as SW


def test_the_sentence_is_the_one_the_app_shows_word_for_word():
    assert SW.SMALL_WATER_TEXT == ("This stream is much smaller than the river its gauge measures: "
                                   "the reading may not match it.")
    assert (SW.META_TEXT_KEY, SW.META_RATIO_KEY) == ("small_water_text", "small_water_ratio")


def test_the_owners_threshold_is_the_default():
    assert SW.SMALL_WATER_RATIOS == {"x50": 50.0, "x100": 100.0, "x200": 200.0}
    assert SW.SMALL_WATER_POLICY == "x50"
    assert SW.ratio_for() == 50.0


def test_an_unknown_policy_is_refused_never_defaulted():
    with pytest.raises(ValueError, match="no policy 'x75'"):
        SW.ratio_for("x75")


@pytest.mark.parametrize("ratio", [50.0, 100.0, 200.0])
def test_the_threshold_is_inclusive(ratio):
    assert SW.is_small(1.0, ratio, ratio)                  # exactly at the line: small
    assert not SW.is_small(1.0, ratio - 0.001, ratio)      # just under: not
    assert SW.is_small(2.0, 2 * ratio + 0.001, ratio)


def test_a_section_bigger_than_its_gauge_is_never_small():
    assert not SW.is_small(64_706.0, 1_121.0, 50.0)
    assert not SW.is_small(100.0, 100.0, 50.0)


def test_a_missing_area_is_refused():
    for s, g in ((None, 100.0), (0.0, 100.0), (1.0, None), (1.0, 0.0), (-1.0, 100.0)):
        with pytest.raises(ValueError, match="no drainage to compare"):
            SW.is_small(s, g, 50.0)
    with pytest.raises(ValueError, match="as big as its gauge"):
        SW.is_small(1.0, 100.0, 1.0)


def test_the_owners_creek_is_warned_for_every_gauge_that_paints_it():
    # sid 63623 on the 2026-11-16 bundle: 1.421 km², panel 08MH016 / 08MH103 (643.989) and 08MH001 (960.016, 676x)
    assert SW.member_mask(1.421, [643.989, 643.989, 960.016], 50.0) == 0b111


def test_the_switch_moves_the_slesse_neighbour():
    # sid 63923: 1.421 km², panel 08MH056 Slesse (186.075, 131x) and 08MH001 (960.016, 676x)
    members = [186.075, 960.016]
    assert SW.member_mask(1.421, members, SW.ratio_for("x50")) == 0b11
    assert SW.member_mask(1.421, members, SW.ratio_for("x100")) == 0b11
    assert SW.member_mask(1.421, members, SW.ratio_for("x200")) == 0b10    # only Vedder Crossing past 200x


def test_the_bits_follow_the_member_ord():
    # the North Thompson at Birch Island's section (111.4 km²): ord 0 Birch Island 2,335 (21x), ord 1 McLure 8,829
    # (79x), ord 2 Kamloops 13,387 (120x) — the two far ones, not the near one
    assert SW.member_mask(111.4, [2334.795, 8829.022, 13387.317], 50.0) == 0b110
    assert SW.member_mask(111.4, [8829.022, 2334.795, 13387.317], 50.0) == 0b101


def test_a_panel_wider_than_the_mask_is_refused():
    with pytest.raises(ValueError, match="the mask holds"):
        SW.member_mask(1.0, [100.0] * (SW.MAX_MEMBERS + 1), 50.0)


def _bundle(sections, members) -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE section_panel (sid INTEGER PRIMARY KEY, panel_id INTEGER NOT NULL, area_km2 REAL)")
    db.execute("CREATE TABLE panel_member (panel_id INTEGER, ord INTEGER, area_km2 REAL, PRIMARY KEY (panel_id, ord))")
    db.executemany("INSERT INTO section_panel VALUES (?,?,?)", sections)
    db.executemany("INSERT INTO panel_member VALUES (?,?,?)", members)
    return db


def test_flow_small_pairs_with_flow_area_row_for_row():
    # inserted out of sid order: both files are in section_panel's sid order, the order paint's runs are cut in
    db = _bundle([(9, 1, 1.421), (2, 0, 100.0), (5, 1, 19.0)],
                 [(0, 0, 150.0), (1, 0, 186.075), (1, 1, 960.016)])
    small = P.decode_small(P.encode_small(db, 50.0), 3)
    assert small == [0b00, 0b10, 0b11]                       # sid 2: 1.5x; sid 5: 10x / 51x; sid 9: 131x / 676x
    assert P.decode_area(P.encode_area(db), 3) == [100.0, 19.0, 1.421]
    with pytest.raises(ValueError, match="trailing bytes"):
        P.decode_small(P.encode_small(db, 50.0), 2)          # paired with a paint of other sections: refused
    with pytest.raises(ValueError, match="not a flow-small/1"):
        P.decode_small(P.encode_area(db), 3)


def test_a_gap_in_the_member_ords_is_refused():
    db = _bundle([(1, 0, 1.0)], [(0, 0, 100.0), (0, 2, 100.0)])
    with pytest.raises(ValueError, match="ords are not 0..n-1"):
        P.encode_small(db, 50.0)


def test_a_section_naming_a_panel_with_no_members_is_refused():
    db = _bundle([(1, 7, 1.0)], [(0, 0, 100.0)])
    with pytest.raises(ValueError, match="has no members"):
        P.encode_small(db, 50.0)


def test_the_contract_ships_flow_small():
    assert C.FILES["flow-small"] == {"format": "flow-small/1", "kind": "bin", "tables": {}}
    assert "flow-small" not in C.OPTIONAL
    assert C.FILES["conditions"]["tables"]["section_gauge"] == ["sid", "station", "trust", "mag", "rel", "dist_b"]
