"""T807 (owner 2026-11-16): A DFO PLACE'S SIDE STREAMS ARE DERIVED FROM DFO'S WORDS FOR IT — its own line, else the
EXACT heading its row sits under, else river only — and a saved value is only a stated exception (RULINGS T16).

"this depends if the name has tributraries included in it. if it doesnt then no tribs otrherwise yes tribs"; "The
heading counts"; "the locations should be matched only to their exact naming then"; the ten disagreeing places "Yes,
river only for all ten (Recommended)"; "A named creek keeps its side streams"; the Kitimat outfall strip "Heading
counts here too". The words below are DFO's, as printed on the pages of 2026-11-06.
"""
from __future__ import annotations

import pytest

from pipeline.regs.dfo_salmon import entries as E
from pipeline.regs.dfo_salmon import side_streams as SS
from pipeline.tests import under_test

TAT_UP = "Tatshenshini River (upstream of the BC/ Yukon border – along the Haines Hwy.)"
KITSUMKALUM = ("Kitsumkalum River (including tpinributaries) Note: The mouth of the Kitsumkalum River is designated by "
               "fishing boundary signs located approx. 1.25 km downstream of the CNR bridge.")


@pytest.mark.parametrize("words, heading, want, source", [
    # the ten that go river-only: blank or stretch words under a heading that says nothing of side streams
    ("downstream of the BC/Yukon border", "Tatshenshini River (downstream of the BC/Yukon border)", (False, False),
     "nothing said"),
    ("", "Kitseguecla River", (False, False), "nothing said"),
    ("from the Cedar Road Bridge crossing upstream to the Highway 19 Bridge crossing", "Nanaimo River", (False, False),
     "nothing said"),
    ("Bulkley River and Morice River waters within the four white triangular fishing boundary signs located at the "
     "confluence of the Bulkley River and Morice River", "Morice River", (False, False), "nothing said"),
    # the heading counts
    ("on west bank between signs at Kitimat hatchery outfall.", "Kitimat River (including tributaries)", (True, False),
     "heading"),
    ("downstream of railway bridge.", KITSUMKALUM, (True, False), "heading"),
    ("downstream of the 216th Street bridge", "Alouette River and tributaries", (True, False), "heading"),
    # a named creek keeps its side streams
    ("upstream of the BC/ Yukon border – along the Haines Hwy.; Blanchard River.", TAT_UP, (True, False),
     "named water"),
    ("upstream of the BC/ Yukon border – along the Haines Hwy.; Kwatini Creek, Stanley Creek and Goat Creek.",
     TAT_UP, (True, False), "named water"),
    # the place's own words speak first
    ("upstream of the BC/ Yukon border – along the Haines Hwy.; Tatshenshini mainstem and all other tributaries.",
     TAT_UP, (True, False), "own words"),
    ("all tributaries of the Bulkley River other than the Suskwa River.", "Bulkley River", (True, True), "own words"),
    ("downstream of the logging bridge 2.4 km downstream of Chehalis Lake, including tributaries to that part",
     "Chehalis River", (True, False), "own words"),
    ("Babine Lake excluding tributaries", "Babine Lake (including tributaries)", (False, False), "own words"),
    ("mainstem waters downstream of the Mission CPR bridge", "Fraser River", (False, False), "own words"),
    ("including tributaries, except in the area described below", "Nanaimo River", (True, False), "own words"),
    # "including its tributary" belongs to the next sentence (the Northrop channel), not to this place
    ("in that portion of the Stave River, known as the Ruskin Spawning Channel, downstream to the boat ramp crossing; "
     "and you shall not fish for Salmon in that portion known as the Northrop Spawning Channel, including its "
     "tributary", "Stave River", (False, False), "nothing said"),
    # one tributary named as a place is not "side streams"
    ("That portion of the San Juan River from the hatchery counting fence downstream, and that portion of the Fairy "
     "Lake tributary", "San Juan River", (False, False), "nothing said"),
])
def test_the_words_decide(words, heading, want, source):
    got = SS.derive(words, heading)
    assert (got.tributaries, got.only, got.source) == (*want, source)


def _loc(**kw):
    d = {"location_id": "6:x-river:whole", "region": "6", "region_number": 6, "kind": "water", "precedence": 3,
         "water": "X River", "water_id": "6:x-river", "source_text": {"specific_area": ""}, "heading": "X River",
         "binding": {"extents": [{"op": "whole", "item_id": "gnis:1"}], "tributaries": None}}
    d.update(kw)
    return E.EntryLocation.from_dict(d)


def _water(flag):
    return E.WaterBinding(water_id="6:x-river", region="6", region_number=6, name="X River", item_ids=["gnis:1"],
                          tributaries=flag)


def test_the_merged_water_flag_never_decides_a_named_water():
    """Kitseguecla, Sustut, Cranberry, Kiteen, Meziadin: a sibling row "including tributaries" set the merged water's
    flag, and the blank whole-river row inherited it — its bait ban rode 733 to 4,243 side pieces."""
    entry, rules = E.to_reach_input(_loc(), [{}], _water(True))
    assert entry["includes_tributaries"] is False and rules[0]["includes_tributaries"] is False


def test_the_heading_takes_the_side_streams_in():
    entry, rules = E.to_reach_input(_loc(heading="X River (including tributaries)"), [{}], _water(None))
    assert entry["includes_tributaries"] is True and rules[0]["includes_tributaries"] is True


def test_a_stored_value_is_the_exception_it_states():
    loc = _loc(heading="X River (including tributaries)",
               tributaries_exception={"reason": "the owner's save", "save": "Dawson 2026-11-15"})
    loc.binding.tributaries = False
    assert SS.effective(loc).source == "exception"
    assert E.to_reach_input(loc, [{}], _water(True))[0]["includes_tributaries"] is False


def test_a_default_place_keeps_its_own_reading():
    """A region / section default is a basin, not a named water: its stored flag, else its water's, as before."""
    loc = _loc(kind="section_default", heading=None)
    assert SS.effective(loc, _water(True)).tributaries is True
    loc.binding.tributaries = False
    assert SS.effective(loc, _water(True)).tributaries is False


def test_the_new_fields_leave_the_file_when_empty():
    d = _loc(heading=None).to_dict()
    assert not {"heading", "tributaries_exception", "side_streams_overruled"} & set(d)
    assert _loc().to_dict()["heading"] == "X River"


class _M:
    def __init__(self, date, reviewer="Dawson"):
        self.date, self.reviewer = date, reviewer


def test_a_save_that_matches_the_words_stores_nothing():
    from pipeline.regs.review.proposals import _side_stream_fields
    loc = {"kind": "water", "heading": "X River (including tributaries)", "source_text": {"specific_area": ""},
           "binding": {"tributaries": None}}
    fields = {"tributaries": True}
    assert _side_stream_fields(_M("2026-11-17"), loc, fields) == {} and fields == {"tributaries": None}


def test_a_save_that_differs_is_stored_with_its_reason():
    from pipeline.regs.review.proposals import _side_stream_fields
    loc = {"kind": "water", "heading": "X River", "source_text": {"specific_area": ""}, "binding": {"tributaries": None}}
    fields = {"tributaries": True}
    got = _side_stream_fields(_M("2026-11-17"), loc, fields)
    assert fields == {"tributaries": True}
    assert got["tributaries_exception"]["save"] == "Dawson 2026-11-17"
    assert "river only" in got["tributaries_exception"]["reason"]


def test_an_overruled_save_sets_nothing():
    """The Morice Forks: saved "side streams in" on 2026-11-10; the owner's 2026-11-16 rule overrules it. A save after
    the overrule lands again."""
    from pipeline.regs.review.proposals import _side_stream_fields
    loc = {"kind": "water", "heading": "Morice River", "source_text": {"specific_area": ""},
           "binding": {"tributaries": None}, "side_streams_overruled": {"through": "2026-11-16", "owner": "…"}}
    fields = {"tributaries": True, "extents": []}
    assert _side_stream_fields(_M("2026-11-10"), loc, fields) == {} and "tributaries" not in fields
    fields = {"tributaries": True}
    assert _side_stream_fields(_M("2026-11-18"), loc, fields)["tributaries_exception"]


def test_every_named_water_place_states_its_stored_setting():
    """THE CURATED FILES: every named-water place carries its exact heading, and a stored side-stream setting only with
    its reason (an exception); one that only repeats the words is not stored. Read in place (curated is in git)."""
    import json
    from pathlib import Path
    bad = []
    for p in sorted(Path(under_test.curated().dfo_salmon.places).glob("region-*.json")):
        for loc in json.loads(p.read_text(encoding="utf-8"))["locations"]:
            if not SS.applies(loc):
                continue
            if not loc.get("heading"):
                bad.append(f"{loc['location_id']}: no heading stamped")
            stored = (loc.get("binding") or {}).get("tributaries")
            if stored is not None:
                if not (loc.get("tributaries_exception") or {}).get("reason"):
                    bad.append(f"{loc['location_id']}: stored {stored} with no reason")
                elif stored == SS.derived_of(loc).tributaries:
                    bad.append(f"{loc['location_id']}: an exception that repeats the words")
    assert not bad, "\n".join(bad)


@pytest.mark.needs_cache
def test_the_stamped_headings_are_the_pages():
    """A heading DFO changed, or one place's wordings under two headings, is flagged (`side_streams check`)."""
    assert SS.problems(SS.survey()) == []
