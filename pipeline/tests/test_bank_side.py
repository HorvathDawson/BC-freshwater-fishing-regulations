"""T1029 (prototype) — which side of the DRAWN line a named bank is, from the line as written, never assumed."""

from __future__ import annotations

import json

import pytest

from pipeline.deliver.bank_side import bank_side, sides_from_layer

#: the Kitimat's section at the hatchery signs as the 0.9.0 tiles write it (section 1234098): d/s sign first, u/s last —
#: the line runs UPSTREAM, north-east
KITIMAT = [[-128.682089, 54.03179], [-128.6805, 54.0335], [-128.6788, 54.0352], [-128.676787, 54.037269]]


def test_the_kitimat_west_bank_is_left_of_its_drawn_line():
    # drawn upstream (NE), the west bank is on the LEFT; looking downstream it is the right bank (T754 §4)
    assert bank_side(KITIMAT, "west").sign == -1
    assert bank_side(KITIMAT, "east").sign == 1
    # the same water drawn the other way round: the answer turns with the line, which is why it is measured
    assert bank_side(KITIMAT[::-1], "west").sign == 1


def test_a_line_along_the_words_axis_has_no_answer():
    # a river flowing due west: "west bank" names neither bank
    s = bank_side([[-120.0, 50.0], [-120.01, 50.0001]], "west")
    assert (s.sign, s.share) == (0, 0.0) and "axis" in s.why


def test_a_meander_whose_pieces_disagree_has_no_answer():
    # north for 100 m, then south for 100 m: west is left, then right
    s = bank_side([[-120.0, 50.0], [-120.0, 50.0009], [-119.999, 50.0009], [-119.999, 50.0]], "west")
    assert s.sign == 0 and "disagree" in s.why


def test_a_section_read_from_the_layer_file(tmp_path):
    f = tmp_path / "stream.geojsonl"
    row = {"type": "Feature", "geometry": {"type": "LineString", "coordinates": KITIMAT},
           "properties": {"section_id": 7, "ord": 7}, "tippecanoe": {"layer": "stream", "minzoom": 5, "maxzoom": 13}}
    f.write_text(json.dumps(row, separators=(",", ":")) + "\n")
    assert sides_from_layer(f, {7: "west"})[7].sign == -1
    with pytest.raises(SystemExit, match=r"no line for section\(s\) \[8\]"):
        sides_from_layer(f, {7: "west", 8: "west"})
    with pytest.raises(ValueError, match="bank word"):
        bank_side(KITIMAT, "upstream")
