"""LAKE FACTS — PREPARED, NOT FED (owner 2026-10-21, handoff/PLAN/app-integration/decisions.md: "we dont have the
stocking and bathy map data in a feed yet. but we just want it prepared for it when we do have the feed"). The release's
optional `lakes` slot (format `lakes/1`) carries, per lake SECTION, a survey state and a last stocked day; nothing
produces one yet, and `deliver all` ships none.

Synthetic inputs only: the binding rule a future feed's identifiers go through, the folding of bound values per
section, the byte layout, the release's refusals, and that the release ships the slot only when it is handed a file.
"""
from __future__ import annotations

import sqlite3

import pytest

from pipeline.deliver import lake_facts as LF


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
    assert LF.bind("POND00001", {"SANCTUARY POND"}, LAKES) == ((), "no_polygon")


def test_a_name_core_drops_only_the_generic_word():
    assert [LF.core(n) for n in ("LANGFORD", "Langford Lake", "Twin Lakes", "Elk Pond", "Alexis Lake(formerly)",
                                 "SOUTH BURNIE L.", "Lake Errock", "")] == \
        ["langford", "langford", "twin", "elk", "alexis", "south burnie", "lake errock", ""]


# ---- folding bound values per section ----------------------------------------------------------------------

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
    assert (B.FORMATS["lakes"], C.FILES["lakes"], C.OPTIONAL) == \
        ("lakes/1", {"format": "lakes/1", "kind": "bin", "tables": {}}, frozenset({"lakes"}))
    assert C.UNITS["lakes"].startswith("lakes/1: 'BCLK'")


def test_the_release_ships_lakes_only_when_handed_a_file(monkeypatch, tmp_path):
    # `deliver all` hands it none (test_deliver_atomic): the slot is prepared, and a release without it is the norm
    from pipeline.deliver.release import __main__ as RM
    from pipeline.deliver.release import build as B
    from pipeline.deliver.release import manifest as M
    bundle = tmp_path / "bundle.sqlite"
    con = sqlite3.connect(bundle)
    con.execute("CREATE TABLE meta (k TEXT, v TEXT)")
    con.execute("INSERT INTO meta VALUES ('section_handles', 'd768d6e135e62b99')")
    con.commit()
    con.close()
    seen = []
    monkeypatch.setattr(B, "build", lambda **kw: seen.append(kw["lakes"]) or tmp_path)
    monkeypatch.setattr(B, "sizes", lambda d: {})
    monkeypatch.setattr(B, "gate_problems", lambda d: [])
    monkeypatch.setattr(M, "tiles_block", lambda *a: None)
    st = ["--store", str(tmp_path / "regs.sqlite")]          # required: no regs.sqlite is kept (P20)
    assert RM.main(["--bundle", str(bundle), *st]) == 0
    assert RM.main(["--bundle", str(bundle), *st, "--lake-facts", str(tmp_path / "lakes.bin")]) == 0
    assert seen == [None, tmp_path / "lakes.bin"]
