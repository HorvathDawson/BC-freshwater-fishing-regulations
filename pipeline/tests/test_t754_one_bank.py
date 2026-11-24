"""T754 (owner 2026-11-22 23:45, "Other bank: the water's other rules"): a DFO place worded for ONE BANK of its river is
read BESIDE the water's other places (AGENTS rule 49, as the book's `side`) — no data needed but the curated places.

  * `locations.bank_of` reads the bank from DFO's words, and only a bank of the place's own river (the Stave's "Ruskin
    Spawning Channel on the east bank of the BC Hydro park" is a channel of its own);
  * placed, the place rides exactly its own sections in `u` (never `l`), so those sections answer and paint as if it
    were not there, `p[lid]["side"]` names its bank and its own chain (`unmapped`, `n`) carries its rules for the card;
  * not placed, it is an ordinary undrawn place of its water (never widened, no `side`).
"""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.deliver import salmon as S
from pipeline.regs.dfo_salmon.locations import bank_of
from pipeline.tests import under_test
from pipeline.tests.test_salmon_file import BAND, _fixture, _loc

def test_bank_of_reads_one_bank_of_the_river_only():
    assert bank_of("on west bank between signs at Kitimat hatchery outfall.") == "west"
    assert bank_of("That portion of the Nakina River from the centre line to the northern bank, starting from a "
                   "boundary sign") == "north"
    assert bank_of("from the center line to the south bank") == "south"
    # a bank OF something else names a channel of its own; a side or a shore is not a bank; two banks name no one bank
    assert bank_of("the Ruskin Spawning Channel on the east bank of the BC Hydro park from the inlet") is None
    assert bank_of("on the east side of the Stave River, known as the Ruskin Spawning Channel") is None
    assert bank_of("near eastern shore") is None
    assert bank_of("from boundary signs on either side of the Stamp River") is None
    assert bank_of("on west bank and on east bank between signs") is None
    assert bank_of(None) is None and bank_of("") is None
    # (review of #61) a bank inside an excepted part is where the place is NOT
    assert bank_of("downstream of B.C. Hydro Dam to the CPR Railway Bridge; except you shall not fish in that portion "
                   "on the east bank between the signs") is None
    assert bank_of("from the bridge to the falls, excluding the waters on the west bank below the weir") is None
    assert bank_of("on west bank between signs; except within 50 m of the outfall") == "west"


def test_the_curated_places_that_name_one_bank_are_the_kitimat_and_the_nakina():
    """The pin on the real files: exactly these two places read beside (the ticket's scan, 2026-11-15). A new DFO wording
    naming a bank lands here first, for a look."""
    found = {}
    for f in sorted(Path(under_test.curated().dfo_salmon.places).glob("region-*.json")):
        for loc in json.loads(f.read_text(encoding="utf-8"))["locations"]:
            b = bank_of((loc.get("source_text") or {}).get("specific_area"))
            if b:
                found[loc["location_id"]] = b
    assert found == {"6:kitimat-river:on-west-bank-between-signs-at-kiti": "west",
                     "6:nakina-river:that-portion-of-the-nakina-river-f": "north"}


def _with_bank(mapped: bool, area: str = "on west bank between signs at the hatchery outfall."):
    files, feeds, placed, sid_of = _fixture()
    files["6"].locations.append(_loc("6:bank", 3, ["fk"], "Kispiox River", "6:kispiox-river", area))
    feeds["6"]["locators"].append({"fingerprint": "fk", "rules": BAND})          # no fishing for coho, all year
    # s3 (12) is on the Kispiox itself; s4 (13) is a tributary its walk took ("including tributaries")
    placed = placed + [{"location_id": "6:bank", "mapped": mapped, "sections": ["s3", "s4"] if mapped else []}]
    return files, feeds, placed, sid_of


def test_a_placed_one_bank_place_stands_beside_the_waters_other_places():
    """MUTATION: drop `lid in banks` from the build and 6:bank is first in s3's `l` — coho there reads closed and paints
    so, `p` has no `side`: this test fails."""
    p0, s0 = S.build(*_fixture(), {"gnis:4674": [11, 12]}, section_regions=None)
    payload, sections = S.build(*_with_bank(True), {"gnis:4674": [11, 12]}, section_regions=None)
    ch, ch0 = payload["chains"][dict(sections)[12]], p0["chains"][dict(s0)[12]]
    # the other bank: the water's other places answer, as if the bank place were not there — the map too
    assert ch["l"] == ch0["l"] == ["6:kispiox", "6:b(i)", "6:a"]
    assert ch["u"] == ["6:bank", *ch0["u"]] == ["6:bank", "6:resort"]                # beside the undrawn resort
    assert (ch["sp"], ch["m"], ch["g"]) == (ch0["sp"], ch0["m"], ch0["g"])
    # exactly its own sections: s2 (11) is on the water but not the bank place's
    assert "6:bank" not in (payload["chains"][dict(sections)[11]].get("u") or [])
    # (review of #61) only its own river's sections stand beside: the tributary its walk took (s4, 13) carries no bank
    # caveat. (T1029, owner 2026-11-24 "i think closed") it is the place's WHOLE: in its chain's `l`, closed to coho
    trib = payload["chains"][dict(sections)[13]]
    assert "6:bank" not in (trib.get("u") or []) and "6:bank" in trib["l"]
    assert payload["verdicts"][trib["sp"][S.SPECIES.index("CO")][1]]["s"] == "closed"
    # the card: its bank, and its own chain with its rules and the days DFO speaks for it
    assert payload["p"]["6:bank"]["side"] == "west"
    (own,) = [u for u in payload["unmapped"] if payload["chains"][u["chain"]]["l"] == ["6:bank"]]
    oc = payload["chains"][own["chain"]]
    assert payload["verdicts"][oc["sp"][S.SPECIES.index("CO")][1]]["s"] == "closed" and oc["n"]
    assert own["w"] == "Kispiox River — on west bank between signs at the hatchery outfall."


def test_a_one_bank_place_not_placed_is_an_undrawn_place_never_widened():
    """Not placed: on its water as any undrawn place (the card's "not drawn yet", DFO's words naming the bank), no `side`,
    never in `l` of any section."""
    payload, sections = S.build(*_with_bank(False), {"gnis:4674": [11, 12]}, section_regions=None)
    assert "side" not in payload["p"]["6:bank"]
    for sid in (11, 12):
        ch = payload["chains"][dict(sections)[sid]]
        assert "6:bank" in ch["u"] and "6:bank" not in ch["l"]


def test_a_bank_of_something_else_still_governs_its_place():
    """The Stave's wording (a channel "on the east bank of the BC Hydro park") is a place of its own: it answers in `l`."""
    payload, sections = S.build(*_with_bank(True, "the Ruskin Spawning Channel on the east bank of the BC Hydro park"),
                                {"gnis:4674": [11, 12]}, section_regions=None)
    ch = payload["chains"][dict(sections)[12]]
    assert "6:bank" in ch["l"] and "6:bank" not in (ch.get("u") or []) and "side" not in payload["p"]["6:bank"]


def test_a_rest_that_lists_a_bank_place_still_answers_for_the_other_bank():
    """(review of #61) a "rest of" place listing the bank place stands behind it on its sections (`behind`). The bank place
    is beside there, not in `covering`, so it still counts for `rests_behind`: the rest answers in `l` on the stretch.
    The rest sits a tier below the river's own place (6:kispiox): at the same tier and size the two would be ordered
    only by id, and T771's equal-place check (rightly) stops the build.
    MUTATION: drop `beside_on` from `rests_behind` and the rest is gone from s3's chain: this test fails."""
    files, feeds, placed, sid_of = _with_bank(True)
    files["6"].locations.append(_loc("6:rest", 2, ["fr2"], "Kispiox River", "6:kispiox-river", "all other parts"))
    feeds["6"]["locators"].append({"fingerprint": "fr2", "rules": BAND})
    placed = placed + [{"location_id": "6:rest", "mapped": True, "sections": ["s2"], "behind": {"6:bank": ["s3"]}}]
    payload, sections = S.build(files, feeds, placed, sid_of, {"gnis:4674": [11, 12]}, section_regions=None)
    ch = payload["chains"][dict(sections)[12]]
    assert "6:rest" in ch["l"] and ch["u"][0] == "6:bank" and "6:bank" not in ch["l"]


def test_a_bank_place_needs_its_rivers_sections():
    """No `item_sections`: the build cannot tell the river's own sections from its tributaries' — it stops, never guesses."""
    import pytest
    with pytest.raises(S.SalmonError, match="names one bank"):
        S.build(*_with_bank(True), section_regions=None)
