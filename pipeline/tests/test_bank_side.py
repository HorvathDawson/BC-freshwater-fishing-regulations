"""T1029 — which side of the DRAWN line a named bank is, from the line as written, never assumed; every section asked
for ends with a side or a typed reason (never a guess, never a silent skip)."""

from __future__ import annotations

import json
import random

import pytest

from pipeline.common.bank_side import drawn_banks, side_from_code
from pipeline.deliver.bank_side import Side, Why, bank_side, bank_side_of_parts, polygon_sections, report, sides_from_layer

#: the Kitimat's section at the hatchery signs as the 0.9.0 tiles write it (section 1234098): d/s sign first, u/s last —
#: the line runs UPSTREAM, north-east
KITIMAT = [[-128.682089, 54.03179], [-128.6805, 54.0335], [-128.6788, 54.0352], [-128.676787, 54.037269]]


def _row(sid, coords, gtype="LineString", **props):
    return json.dumps({"type": "Feature", "geometry": {"type": gtype, "coordinates": coords},
                       "properties": {"section_id": sid, **props}}, separators=(",", ":")) + "\n"


def test_the_kitimat_west_bank_is_left_of_its_drawn_line():
    # drawn upstream (NE), the west bank is on the LEFT; looking downstream it is the right bank (T754 §4)
    assert bank_side(KITIMAT, "west").sign == -1
    assert bank_side(KITIMAT, "east").sign == 1
    # the same water drawn the other way round: the answer turns with the line, which is why it is measured
    assert bank_side(KITIMAT[::-1], "west").sign == 1


def test_a_line_along_the_words_axis_has_no_answer():
    # a river flowing due west: "west bank" names neither bank
    s = bank_side([[-120.0, 50.0], [-120.01, 50.0001]], "west")
    assert (s.sign, s.share, s.why) == (0, 0.0, Why.along_axis)


def test_a_meander_whose_pieces_disagree_has_no_answer():
    # north for 100 m, then south for 100 m: west is left, then right
    s = bank_side([[-120.0, 50.0], [-120.0, 50.0009], [-119.999, 50.0009], [-119.999, 50.0]], "west")
    assert s.sign == 0 and s.why is Why.pieces_disagree


def test_a_side_and_a_reason_never_come_together():
    with pytest.raises(ValueError):
        Side(1, 1.0, Why.along_axis)
    with pytest.raises(ValueError):
        Side(0, 0.0)


def test_every_section_asked_for_ends_with_a_side_or_a_typed_reason(tmp_path):
    stream, lake = tmp_path / "stream.geojsonl", tmp_path / "lake.geojsonl"
    stream.write_text(_row(7, KITIMAT, ord=7) + _row(9, KITIMAT) + _row(9, KITIMAT)
                      + _row(11, [[-128.0, 54.0], [-128.001, 54.0001]]) + _row(12, KITIMAT))
    # 12: a river wide enough to be a polygon reach (its route still in the stream layer); 13: a lake, not a river
    lake.write_text(_row(12, [[[0, 0], [1, 0], [1, 1], [0, 0]]], "Polygon", water="stream")
                    + _row(13, [[[0, 0], [1, 0], [1, 1], [0, 0]]], "Polygon", water="lake"))
    got = sides_from_layer(stream, {7: "west", 8: "west", 9: "west", 11: "west", 12: "west"}, lake)
    assert sorted(got) == [7, 8, 9, 11, 12]
    assert got[7].sign == -1
    assert got[8].why is Why.no_line
    assert got[9].why is Why.written_twice
    assert got[11].why is Why.along_axis
    assert got[12].why is Why.polygon_reach
    ok, bad = report(got)
    assert ok == {7: -1}
    assert bad == {"no_line": [8], "written_twice": [9], "along_axis": [11], "polygon_reach": [12]}
    assert polygon_sections(lake, [12, 13]) == frozenset({12})
    with pytest.raises(ValueError, match="bank word"):
        sides_from_layer(stream, {7: "upstream"})


def test_the_answer_does_not_depend_on_the_order_asked(tmp_path):
    stream = tmp_path / "stream.geojsonl"
    stream.write_text(_row(7, KITIMAT) + _row(8, KITIMAT[::-1]))
    a = sides_from_layer(stream, {7: "west", 8: "west"})
    b = sides_from_layer(stream, {8: "west", 7: "west"})
    assert list(a) == list(b) == [7, 8] and a == b and (a[7].sign, a[8].sign) == (-1, 1)


def test_the_atlas_code_is_the_same_rule_as_the_bundle_side():
    # `drawn_banks` (numpy, every stream piece at the atlas build) and `bank_side_of_parts` (the bundle) must agree on
    # every word for any line: one rule, two speeds
    rnd = random.Random(1029)
    for _ in range(300):
        parts = []
        for _p in range(rnd.choice([1, 1, 2])):
            x, y = rnd.uniform(-130, -115), rnd.uniform(49, 59)
            pts = [[x, y]]
            for _i in range(rnd.randint(1, 12)):
                x += rnd.uniform(-0.002, 0.002)
                y += rnd.uniform(-0.002, 0.002)
                pts.append([x, y])
            parts.append(pts)
        code = drawn_banks(parts)
        for word in ("west", "east", "north", "south"):
            assert side_from_code(code, word) == bank_side_of_parts(parts, word).sign, (parts, word, code)
    # the Kitimat at the signs, drawn mouth -> source as FWA writes it: west is LEFT, north is ... the line runs NE
    assert side_from_code(drawn_banks([KITIMAT]), "west") == -1
    assert side_from_code("", "west") == 0
    with pytest.raises(ValueError):
        side_from_code("RL", "up")
