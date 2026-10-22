"""LAKE FACTS (owner 2026-10-21: stocked lakes and the depth colouring go through the one colouring pathway, and their
colours update from DATA ALONE — no tile change). The release ships, per lake SECTION, whether it was surveyed (a sheet,
or a digitised chart) and the day it was last stocked: `lake_facts.bin` (`pipeline.deliver.lake_facts`), copied into
the release as the file `lakes` (format `lakes/1`).

No data: the binding rule, the folding of sheets and releases, the byte layout and the refusals, on synthetic inputs.
On the live inputs (`needs_bundle`): built from the atlas's handle table, the FWA lakes layer, both survey-sheet sources
and the FFSBC release roster, then decoded back and held to hand-checked lakes.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

import pytest

from pipeline.deliver import lake_facts as LF


def _day(s: str) -> int:
    return (dt.date.fromisoformat(s) - dt.date(1900, 1, 1)).days


# ---- the binding rule ------------------------------------------------------------------------------------------

LAKES = {
    "00330VICT": [LF.FwaLake("329676318", ("Langford Lake", "", ""))],
    "00001BRID": [LF.FwaLake("329060781", ("Bobbs Lake", "", ""))],
    "00999UNNM": [LF.FwaLake("329000001", ("", "", ""))],
    # one code, two waters the FWA names separately (a sheet titled for both)
    "00002BURN": [LF.FwaLake("329000010", ("North Burnie Lake", "", "")),
                  LF.FwaLake("329000011", ("South Burnie Lake", "", ""))],
    "00003RENM": [LF.FwaLake("329000020", ("Nendatoo Lake", "Cripple Lake(formerly)", ""))],
    # one code: the named water, a differently-named neighbour, and an unnamed piece
    "00004MEAD": [LF.FwaLake("329000030", ("Meadow Lake", "", "")),
                  LF.FwaLake("329000031", ("Pigeon Lake", "", "")),
                  LF.FwaLake("329000032", ("", "", ""))],
}


def test_an_identifier_binds_only_when_a_name_agrees_or_the_fwa_names_nothing():
    assert LF.bind("00330VICT", {"LANGFORD"}, LAKES) == (("329676318",), "verified")
    assert LF.bind("00001BRID", {"BOBBS LAKE", "BOBB'S LAKE"}, LAKES) == (("329060781",), "verified")
    assert LF.bind("00999UNNM", {"MYSTERY LAKE"}, LAKES) == (("329000001",), "fwa_unnamed")
    # only the waters whose names agree: a name that corroborates ONE lake under a shared code is no licence to
    # colour its differently-named neighbour (review: BUNTZEN bound McCombe Lake, MARA LAKE bound Rosemond Lake)
    assert LF.bind("00002BURN", {"SOUTH BURNIE L."}, LAKES) == (("329000011",), "verified")
    # every agreeing water, not the first: freezing one would lose the other silently
    assert LF.bind("00002BURN", {"SOUTH BURNIE L.", "NORTH BURNIE LAKE"}, LAKES) == \
        (("329000010", "329000011"), "verified")
    # an unnamed part under a verified code is the same water's piece: it binds too
    assert LF.bind("00004MEAD", {"MEADOW LAKE"}, LAKES) == (("329000030", "329000032"), "verified")
    # a renaming the FWA records as "(formerly)" is the same water
    assert LF.bind("00003RENM", {"CRIPPLE LAKE"}, LAKES) == (("329000020",), "verified")
    # a key that no name corroborates is more likely a stale identifier than a surprise: refused, never a coin toss
    assert LF.bind("00330VICT", {"ELK LAKE"}, LAKES) == ((), "name_disagrees")
    assert LF.bind("FFSBC3893", {"SANCTUARY POND"}, LAKES) == ((), "no_polygon")


def test_a_name_core_drops_only_the_generic_word():
    assert [LF.core(n) for n in ("LANGFORD", "Langford Lake", "Twin Lakes", "Elk Pond", "Alexis Lake(formerly)",
                                 "SOUTH BURNIE L.", "Lake Errock", "")] == \
        ["langford", "langford", "twin", "elk", "alexis", "south burnie", "lake errock", ""]


# ---- folding the sources -------------------------------------------------------------------------------------

def test_survey_is_digitised_where_either_source_says_vectorised_else_a_sheet():
    csv_rows = [{"WATERBODY_IDENTIFIER_WSA_50K": "00001BRID", "GAZETTED_NAME": "BOBBS LAKE", "MAP_TITLE": "BOBB'S LAKE"},
                {"WATERBODY_IDENTIFIER_WSA_50K": "00002BURN", "GAZETTED_NAME": "BURNIE LAKES", "MAP_TITLE": ""}]
    wfs_rows = [{"waterbody_identifier": "00002BURN", "gazetted_name": "BURNIE LAKES", "map_title": "SOUTH BURNIE L.",
                 "vectorized_flag": "Y"},
                {"waterbody_identifier": "00330VICT", "gazetted_name": "LANGFORD LAKE", "map_title": None,
                 "vectorized_flag": "N"}]
    assert LF.surveys(csv_rows, wfs_rows) == {
        "00001BRID": ({"BOBBS LAKE", "BOBB'S LAKE"}, LF.SHEET),
        "00002BURN": ({"BURNIE LAKES", "SOUTH BURNIE L."}, LF.DIGITISED),
        "00330VICT": ({"LANGFORD LAKE"}, LF.SHEET),
    }


def test_last_stocked_is_the_latest_release_on_or_before_the_roster_day():
    rows = [{"wbid": "00330VICT", "water": "LANGFORD", "date": "2025-11-24"},
            {"wbid": "00330VICT", "water": "LANGFORD LAKE", "date": "2026-10-20"},
            {"wbid": "00330VICT", "water": "LANGFORD", "date": "2027-01-01"},   # after the roster's day: not yet
            {"wbid": "00001BRID", "water": "BOBBS", "date": "1990-05-01"},
            {"wbid": "", "water": "NOWHERE", "date": "2026-01-01"}]
    assert LF.last_stocked(rows, asof=_day("2026-10-21")) == {
        "00330VICT": ({"LANGFORD", "LANGFORD LAKE"}, _day("2026-10-20")),
        "00001BRID": ({"BOBBS"}, _day("1990-05-01")),
    }


def test_facts_by_section_join_the_bound_waters_to_their_handles():
    handles = {"lake:329676318": 7, "lake:329060781": 3, "lake:329000010": 12, "lake:329000011": 13}
    survey = {"00001BRID": ({"BOBBS LAKE"}, LF.SHEET), "00002BURN": ({"SOUTH BURNIE L."}, LF.DIGITISED),
              "00330VICT": ({"ELK LAKE"}, LF.SHEET)}                    # a name that disagrees: no survey for Langford
    stocked = {"00330VICT": ({"LANGFORD"}, 20381), "00001BRID": ({"BOBBS"}, 7425), "00999UNNM": ({"X"}, 100)}
    facts, report = LF.facts(survey, stocked, LAKES, handles)
    # North Burnie (12) is not South Burnie: the sheet titled for the south lake leaves it unsurveyed
    assert facts == {3: (LF.SHEET, 7425), 7: (LF.NONE, 20381), 13: (LF.DIGITISED, None)}
    # every identifier is accounted for: bound, or refused with the reason (00999UNNM's lake has no section)
    assert report == {"survey": {"verified": 2, "name_disagrees": 1},
                      "stocked": {"verified": 2, "fwa_unnamed:no_section": 1}}


# ---- the bytes ----------------------------------------------------------------------------------------------------

def test_the_file_is_header_then_delta_coded_sections_and_round_trips():
    raw = LF.encode({3: (LF.SHEET, 7425), 7: (LF.NONE, 20381), 300: (LF.DIGITISED, None)},
                    handles="d768d6e135e62b99", asof=20382)
    assert raw == (b"BCLK" + bytes([1]) + bytes.fromhex("d768d6e135e62b99")
                   + bytes([0x9f, 0x9f, 0x01])          # asof + 1 = 20383, LEB128
                   + bytes([3])                          # three sections
                   + bytes([3, 1, 0x82, 0x3a])           # sid 3: sheet, stocked day 7425 + 1
                   + bytes([4, 0, 0x9e, 0x9f, 0x01])     # sid 7 (+4): no survey, stocked 20381 + 1
                   + bytes([0xa5, 0x02, 2, 0]))          # sid 300 (+293): digitised, never stocked
    assert LF.decode(raw) == {"handles": "d768d6e135e62b99", "asof": 20382,
                              "facts": {3: (LF.SHEET, 7425), 7: (LF.NONE, 20381), 300: (LF.DIGITISED, None)}}


def test_a_file_from_another_atlas_is_refused_by_the_release():
    from pipeline.deliver.release import build as B
    raw = LF.encode({1: (LF.SHEET, None)}, handles="0" * 16, asof=None)
    with pytest.raises(SystemExit, match="lake facts are from another atlas"):
        B.lakes_bytes(raw, "d768d6e135e62b99")
    assert B.lakes_bytes(raw, "0" * 16) == raw


def test_the_contract_names_the_file_and_its_format():
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release import contract as C
    assert (B.FORMATS["lakes"], C.FILES["lakes"]) == ("lakes/1", {"format": "lakes/1", "kind": "bin", "tables": {}})
    assert C.UNITS["lakes"].startswith("lakes/1: 'BCLK'")


# ---- on the live inputs ---------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_built_from_the_live_inputs_the_hand_checked_lakes_hold(request, tmp_path):
    from pipeline.common.curated import GENERATED, SOURCE
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, BUNDLE_HINT
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    roster = need(request, "bundle", SOURCE / LF.STOCKING, "python -m pipeline.stocking.fetch")
    out = tmp_path / "lake_facts.bin"
    assert LF.main(["--bundle", str(bundle), "--out", str(out), "--build", str(GENERATED.require_build())]) == 0
    got = LF.decode(out.read_bytes())
    from pipeline.common import section_handles as SHD
    _, index = SHD.read(GENERATED.require_build())
    at = lambda wbk: got["facts"].get(index[f"lake:{wbk}"])          # noqa: E731
    assert got["asof"] == _day(json.loads(roster.read_text())["fetched"])
    # Langford Lake (00330VICT): a sheet, and FFSBC's roster stocks it every few weeks
    assert at(329676318)[0] == LF.SHEET and at(329676318)[1] >= _day("2026-10-01")
    # Wahleach Lake (00338HARR): one of the digitised charts
    assert at(329177886)[0] == LF.DIGITISED
    # Bobbs Lake (00001BRID): surveyed
    assert at(329060781)[0] in (LF.SHEET, LF.DIGITISED)
    n_survey = sum(1 for s, _ in got["facts"].values() if s != LF.NONE)
    n_stocked = sum(1 for _, d in got["facts"].values() if d is not None)
    assert 2_000 < n_survey < 3_500 and 1_500 < n_stocked < 3_000
