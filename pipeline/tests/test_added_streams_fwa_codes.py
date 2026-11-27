"""(T1246 / T1150) A city line never takes an FWA line's watershed code.

A city creek that drains to the sea was given the code of the nearest coastal FWA river, and the uniqueness bump looked
only at other city codes, so Turner Creek (Port Moody) became a part of Noons Creek, Tunnel Creek (Burnaby) of the
Seymour River and Capitol Creek of "Squatters Creek". `validate.fwa_code_collisions` refuses such a line by name; these
tests pin what it refuses and passes, its named exemptions, and that the three creeks keep codes of their own.
"""

from __future__ import annotations

import json

import pytest

from pipeline.atlas.waters.added_streams import validate as V
from pipeline.tests import under_test


def _row(blk, wsc, name="X Creek", klass="novel", receiver_kind="tidal", source="burnaby"):
    return {"blk": blk, "wsc": wsc, "name": name, "klass": klass, "receiver_kind": receiver_kind, "source": source}


def test_a_city_line_on_an_fwa_code_is_refused_by_name():
    out = V.fwa_code_collisions([_row("-9", "900-078372", "Tunnel Creek")], [("900-078372", "360886339")],
                                whole_province=False)
    assert len(out) == 1
    assert "blk -9 (Tunnel Creek, burnaby)" in out[0] and "360886339" in out[0] and "900-078372" in out[0]


def test_a_tidal_root_with_an_fwa_line_under_its_code_is_refused():
    out = V.fwa_code_collisions([_row("-9", "900-057238", "Turner Creek")], [("900-057238-123456", "360000001")],
                                whole_province=False)
    assert len(out) == 1 and "coded under its 900-057238" in out[0]


def test_a_tributary_minted_under_an_fwa_river_is_not_refused():
    # a city tributary OF an FWA river descends from the river's code: that is the river's own system
    rows = [_row("-9", "100-019698-123456", receiver_kind="fwa")]
    assert V.fwa_code_collisions(rows, [("100-019698", "356000001")], whole_province=False) == []


def test_a_piece_carrying_the_code_of_the_line_it_joins_is_that_water():
    # Village Creek's upper piece joins its own lower piece; a same-name piece joins the FWA line it extends
    rows = [_row("-7", "900-058384", "Village Creek", receiver_kind="added") | {"receiver_wsc": "900-058384"},
            _row("-6", "900-103611", "Logger's Lane Creek", receiver_kind="fwa") | {"receiver_wsc": "900-103611"}]
    assert V.fwa_code_collisions(rows, [("900-058384", "360827018"), ("900-103611", "360767895")],
                                 whole_province=False) == []


def test_an_extension_shares_its_fwa_lines_code_by_design():
    rows = [_row("360827018", "900-058384", klass="extension", receiver_kind="fwa")]
    assert V.fwa_code_collisions(rows, [("900-058384", "360827018")], whole_province=False) == []


def test_own_codes_pass():
    rows = [_row("-9", "900-057238", "Turner Creek"),
            _row("-8", "900-057238-436900", "Turner Creek Trib.1", receiver_kind="added")]
    assert V.fwa_code_collisions(rows, [("900-057239", "360880849")], whole_province=False) == []


def test_the_named_exemptions_pass_and_go_stale_by_name():
    blk = "-2400000001"                                               # Sheldens Creek, a T806 continuation
    rows = [_row(blk, "950-016367", "Sheldens Creek", source="manual")]
    assert V.fwa_code_collisions(rows, [("950-016367", "360846083")], whole_province=False) == []
    # the whole province: an exemption that shares nothing, or names no added stream, is refused
    stale = V.fwa_code_collisions(rows, [], whole_province=True)
    assert any(f"blk {blk}" in m and "shares no FWA code now" in m for m in stale)
    assert any("is no added stream" in m for m in stale)            # the pending entries absent from `rows`
    # a bbox build sees only its own FWA lines: no staleness is judged there
    assert V.fwa_code_collisions(rows, [], whole_province=False) == []


def test_exemption_lists_pinned():
    """Mutation pin: the exemptions are named policy; a new collision is refiled, never added here."""
    assert V.FWA_CODE_CONTINUATIONS.keys() == {"-2400000001"}
    assert V.FWA_CODE_SHARED_PENDING.keys() == {"-2000000091", "-2000000243", "-2000000277", "-2100000079",
                                                "-2200000296", "-2200000395", "-2200000420"}
    fixed = {"-2200000392", "-2200000393", "-2000000390", "-2000000141", "-2000000142"}
    assert not fixed & (V.FWA_CODE_CONTINUATIONS.keys() | V.FWA_CODE_SHARED_PENDING.keys())


#: (T1246, owner 2026-11-27 "All three now, in the re-cut") the three creeks' own codes, and their tributaries under them
OWN_CODES = {
    "-2200000392": ("Turner Creek", "900-057238", ""),
    "-2200000393": ("Turner Creek Trib.1", "900-057238-436900", "900-057238"),
    "-2000000390": ("Tunnel Creek", "900-078371", ""),
    "-2000000141": ("Capitol Creek", "900-053686", ""),
    "-2000000142": ("Capitol Trib.1", "900-053686-249200", "900-053686"),
}


@pytest.mark.parametrize("blk", sorted(OWN_CODES))
def test_the_three_creeks_keep_their_own_codes(blk):
    streams = {s["blk"]: s for s in json.loads(under_test.curated().waters.added_streams.read_text("utf-8"))["streams"]}
    name, wsc, receiver_wsc = OWN_CODES[blk]
    s = streams[blk]
    assert (s["name"], s["wsc"], s["receiver_wsc"]) == (name, wsc, receiver_wsc)
    # no other added stream holds the code (an own water, not a part of another)
    assert [o["blk"] for o in streams.values() if o["wsc"] == wsc] == [blk]


@pytest.mark.needs_source
def test_the_stored_file_puts_no_city_line_on_an_fwa_code_in_bc(request):
    """The whole province, as the full atlas build checks it: every FWA line whose code shares a primary code with an
    added stream, against the committed file; no collision, and no exemption stale."""
    import pipeline.atlas.build as b
    from pipeline.atlas.fwa import FWADataAccessor
    from pipeline.common.utils.wsc import trim_wsc
    from pipeline.tests.conftest import GPKG_HINT, need
    gpkg = need(request, "source", b._DEFAULT_GPKG, GPKG_HINT)
    streams = json.loads(under_test.curated().waters.added_streams.read_text("utf-8"))["streams"]
    roots = sorted({s["wsc"].split("-")[0] for s in streams})
    where = " OR ".join(f"FWA_WATERSHED_CODE LIKE '{r}-%'" for r in roots)
    df = FWADataAccessor(str(gpkg)).get_layer("streams", columns=["FWA_WATERSHED_CODE", "BLUE_LINE_KEY"],
                                              geometry=False, where=where)
    assert len(df) > 100_000                                             # the province's lines, not an empty read
    lines = ((trim_wsc(c), str(k)) for c, k in zip(df["FWA_WATERSHED_CODE"], df["BLUE_LINE_KEY"]))
    assert V.fwa_code_collisions(streams, lines, whole_province=True) == []
