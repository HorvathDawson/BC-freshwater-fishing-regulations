"""T805 on the REAL waters (the live twins of `test_answers_t805_*.py`): the trout and char cards of the waters the
adversarial review of the trout redesign named (2026-11-16), as the answers build writes them, and the card gate on
them. Printed pages are the book's (synopsis 2025-2027).

Part C — one wild limit across rows; the char of the seven wild-2 lakes one way (RULINGS C5):
  Lois Lake (p.24) and Khartoum Lake (p.23): "Wild trout/char daily quota = 2 (no wild trout 40 cm or more), hatchery
  rainbow trout = 6 / Rainbow trout/hatchery steelhead quota = 6 in the aggregate": 2 wild fish in all, said on both
  cards. Dodd, Horseshoe, Ireland (p.23), Nanton (p.24), Windsor (p.26): "(no wild trout over 40 cm)": with Lois and
  Khartoum, every wild char goes back (hatchery char only, Region 2's 60 cm floor).

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.tree import walk
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

LOIS, KHARTOUM, DODD = "wbk:329197058", "wbk:329197063", "wbk:329197061"
HORSESHOE, NANTON, WINDSOR, IRELAND = "wbk:329197066", "wbk:329197073", "wbk:329197074", "wbk:329197113"
WILD_TWO = (LOIS, KHARTOUM, DODD, HORSESHOE, NANTON, WINDSOR, IRELAND)
WATERS = WILD_TWO
TROUT = {"RB", "ST", "CT", "GB", "EB", "DV", "LT"}
CHAR = "Up to 1 hatchery Dolly Varden/bull trout or lake trout combined, 60 cm (24 in) or longer"


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    model = A.build(str(BUNDLE), EXPORT_DIR, workers=1, items=WATERS, sections=["rows"],
                    export=(data, guide), log=lambda *_: None)
    return data, model


def _frame(built, item, part=0, md=(7, 15)):
    _data, model = built
    k = model.parts[item][part]
    return model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]


def _kinds(frame) -> dict:
    return {tuple(n["species"]): n for r in frame["rows"] if r.get("group")
            for n, _p in walk(r["group"]["tree"]) if n["node"] == "kind" and set(n["species"]) & TROUT}


@pytest.mark.needs_bundle
@pytest.mark.parametrize("lake", [LOIS, KHARTOUM])
def test_lois_and_khartoum_say_two_wild_fish_in_all_on_both_cards(built, lake):
    k = _kinds(_frame(built, lake))
    assert k[("EB", "GB", "CT")]["sentence"] == (
        "Up to 4 brook, brown or cutthroat trout combined, any size — up to 2 wild brook, brown or cutthroat trout "
        "combined under 40 cm (16 in); no wild brook, brown or cutthroat trout 40 cm (16 in) or longer; no more than 2 "
        "wild trout and char in all, counting any wild rainbow trout you keep")
    assert k[("RB",)]["sentence"] == (
        "Up to 6 rainbow trout, any size — up to 2 wild rainbow trout under 40 cm (16 in); no wild rainbow trout 40 cm "
        "(16 in) or longer; no more than 2 wild trout and char in all, counting any wild brook, brown or cutthroat trout "
        "you keep")
    assert k[("ST",)]["sentence"] == "Up to 6 hatchery steelhead, any size — wild steelhead go back"


@pytest.mark.needs_bundle
@pytest.mark.parametrize("lake", WILD_TWO)
def test_the_seven_wild_two_lakes_keep_hatchery_char_only(built, lake):
    f = _frame(built, lake)
    assert _kinds(f)[("DV", "LT")]["sentence"] == CHAR
    assert {f["fish"][S]["wild"]["status"] for S in ("DV", "LT")} == {"release"}


@pytest.mark.needs_bundle
def test_the_card_gate_proves_the_t805_waters():
    got = CC.check_built(str(BUNDLE), EXPORT_DIR, items=list(WATERS), log=lambda *_: None)
    assert got["groups"] > 20 and got["problems"] == [], got["problems"][:10]
