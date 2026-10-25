"""T113 — ONE NAME CASER. A water's or place's name is cased for display by
`pipeline.deliver.bundle.place_names.display_case` and by nothing else, so the map label (tiles) and
the card (the export's `waters[].name`) are the same words for the same item."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pipeline.deliver.bundle.place_names import display_case, is_shouting, words_display
from pipeline.deliver.tiles.names import display

DELIVER = Path(__file__).resolve().parents[1] / "deliver"
#: The one module allowed to call `str.title()` / `str.capitalize()`: the caser itself.
CASER = DELIVER / "bundle" / "place_names.py"


@pytest.mark.parametrize("raw, shown", [
    ("O'ROURKE LAKE", "O'Rourke Lake"),
    ("MCDONALD CREEK PARK", "McDonald Creek Park"),
    ("#14A", "#14A"),
    ('"MOSS POTHOLE" LAKES', '"Moss Pothole" Lakes'),
    ("HART LAKE (Fort St. James)", "Hart Lake (Fort St. James)"),
    ("McArthur Island Slough", "McArthur Island Slough"),
    ("Sts'a'í:les / Sts'ailes", "Sts'a'í:les / Sts'ailes"),
    ("  EAST   WHITE RIVER ", "East White River"),
])
def test_the_map_label_is_the_card_name(raw, shown):
    """The tile's `display` is `display_case` (whitespace aside) — the function the export cases the
    card's name with — so label == card for every registry name, by construction."""
    assert display(raw) == shown
    assert display(raw) == display_case(" ".join(raw.split()))
    assert not is_shouting(display(raw))


def test_words_display_is_the_caser_not_str_title():
    assert words_display("lake of the woods") == "Lake of the Woods"     # .title(): "Of The"
    assert words_display("elk") == "Elk"                                 # a lone word, not an acronym
    assert words_display("wma") == "WMA"


def test_no_other_deliver_module_title_cases_a_name():
    """The pin: `.title()` / `.capitalize()` appear in `pipeline/deliver` only inside the caser."""
    pat = re.compile(r"\.(title|capitalize)\(\)")
    offenders = [f"{p.relative_to(DELIVER)}:{n}" for p in sorted(DELIVER.rglob("*.py")) if p != CASER
                 for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
                 if pat.search(line.split("#", 1)[0])]
    assert not offenders, f"a second name caser: {offenders} — use place_names.display_case"


def test_a_shouting_tile_name_fails_the_export(tmp_path, monkeypatch):
    """Mutation: a name that reaches a tile without `display` (here, written raw) stops the export."""
    from pipeline.deliver.tiles import export
    from pipeline.deliver.tiles.layers import BY_NAME

    write, close = export._writer(tmp_path, BY_NAME["lake"])
    write({"type": "Point", "coordinates": [0, 0]}, {"section_id": 1, "name": "O'ROURKE LAKE"}, 4)
    with pytest.raises(SystemExit, match="capitals"):
        close()
    write, close = export._writer(tmp_path, BY_NAME["lake"])
    write({"type": "Point", "coordinates": [0, 0]}, {"section_id": 1, "name": display("O'ROURKE LAKE")}, 4)
    assert close() == 1
