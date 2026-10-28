"""T211: THE DFO SALMON FILE (`pipeline/deliver/salmon.py`) — no data needed.

  * the cascade: per species and day the NARROWEST location of the chain with a rule that day answers (Region 6's
    Kispiox: the water reopens coho for five weeks inside B(i)'s closure, under A's region-wide opening);
  * CLOSED UNLESS DFO OPENS IT (owner 2026-10-27): a species nothing opens that day is closed, and chain 0 is the
    UNLISTED water — closed every day — stated in the file, never an absence;
  * build(): sections → chains (narrowest first), a location with no extent shipped under its water's items and never
    placed, a placed section the handle table does not know refused;
  * salmon/1 round-trips byte for byte, runs and all.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.deliver import salmon as S

ALL_YEAR = {"dates": [{"from_month": 4, "from_day": 1, "to_month": 3, "to_day": 31}]}
COHO_WINDOW = {"dates": [{"from_month": 7, "from_day": 15, "to_month": 8, "to_day": 23}]}

REGION = [{"rule_id": "a.r0", "type": "retention_limit", "verbatim": "4 per day", "species": ["CO"], "when": ALL_YEAR,
           "take": 4},
          {"rule_id": "a.r1", "type": "retention_limit", "verbatim": "4 per day", "species": ["CH"], "when": ALL_YEAR,
           "take": 4}]
BAND = [{"rule_id": "b.r0", "type": "retention_limit", "verbatim": "No fishing for coho", "species": ["CO"],
         "when": ALL_YEAR, "take": 0, "may_target": False}]
WATER = [{"rule_id": "w.r0", "type": "retention_limit", "verbatim": "4 per day", "species": ["CO"], "when": COHO_WINDOW,
          "take": 4},
         {"rule_id": "w.r0b", "type": "retention_limit", "verbatim": "only 2 over 50 cm", "species": ["CO"],
          "when": COHO_WINDOW, "take": 2, "within": "w.r0", "lengths": [{"min_cm": 50}]},
         {"rule_id": "w.bait", "type": "bait_restriction", "verbatim": "No natural bait", "gear": [{"slot": "bait"}],
          "when": {}, "when_targeting": ["SA"]}]
CHAIN = [WATER, BAND, REGION]                                      # narrowest first


def test_the_narrowest_location_answers_per_species_and_day():
    on = lambda sp, md: (S.verdict_on(CHAIN, sp, md) or {}).get("s")    # noqa: E731
    assert [on("CO", 720), on("CO", 915), on("CO", 101)] == ["open", "closed", "closed"]
    # the water says nothing of chinook: the band neither — the region default answers
    assert on("CH", 720) == "open"
    # a size sub-limit is said with its parent, and the season in words
    assert S.verdict_on(CHAIN, "CO", 720)["t"] == "4 per day, only 2 over 50 cm (Jul 15–Aug 23)"
    assert S.gear_on(CHAIN, 101) == ("No natural bait",)


def test_a_species_nothing_opens_is_closed_and_an_unread_season_is_check():
    assert S.verdict_on(CHAIN, "SK", 720) is None                   # no row: the CALENDAR writes NOT_LISTED for it
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cal = S.chain_calendar(CHAIN, v, g)
    assert [v.items[i]["s"] for i in cal["sp"][S.SPECIES.index("SK")][1::2]] == ["closed"]
    assert v.items[cal["sp"][S.SPECIES.index("SK")][1]] == S.NOT_LISTED
    tbd = [{"rule_id": "t", "type": "advisory", "verbatim": "To be determined", "when": {"unparsed": ["To be determined"]}}]
    assert S.verdict_on([tbd], "CO", 720)["s"] == "check"


def test_the_map_status_of_a_day():
    o, c, r = {"s": "open"}, {"s": "closed"}, {"s": "release"}
    assert [S.map_status([c, o, c, c, c]), S.map_status([c, r, c, c, c]), S.map_status([c] * 5),
            S.map_status([None] * 5), S.map_status([c, {"s": "check"}, c, c, c])] == \
        ["open", "release", "closed", "closed", "check"]


def _loc(lid, prec, fps, water="", water_id=None, area=""):
    return SimpleNamespace(location_id=lid, precedence=prec, fingerprints=fps, water=water, water_id=water_id,
                           source_text={"specific_area": area})


def _ef(locs, waters=()):
    ws = {w.water_id: w for w in waters}
    return SimpleNamespace(region_name="Skeena", locations=locs, water=lambda wid: ws.get(wid))


def _fixture():
    locs = [_loc("6:a", 0, ["fa"]), _loc("6:b(i)", 1, ["fb"]), _loc("6:kispiox", 3, ["fw"], "Kispiox River"),
            _loc("6:resort", 3, ["fr"], "Kispiox River", "6:kispiox-river", "above the resort signs"),
            _loc("6:dormant", 3, ["gone"])]
    water = SimpleNamespace(water_id="6:kispiox-river", item_ids=["gnis:4674"])
    feed = {"date_modified": "2026-08-28", "locators": [
        {"fingerprint": "fa", "rules": REGION}, {"fingerprint": "fb", "rules": BAND},
        {"fingerprint": "fw", "rules": WATER}, {"fingerprint": "fr", "rules": BAND}]}
    placed = [{"location_id": "6:a", "mapped": True, "sections": ["s1", "s2", "s3"]},
              {"location_id": "6:b(i)", "mapped": True, "sections": ["s2", "s3"]},
              {"location_id": "6:kispiox", "mapped": True, "sections": ["s3"]},
              {"location_id": "6:resort", "mapped": False, "sections": []},
              {"location_id": "6:dormant", "mapped": True, "sections": ["s1"]}]
    return {"6": _ef(locs, [water])}, {"6": feed}, placed, {"s1": 10, "s2": 11, "s3": 12, "s4": 13}


def test_build_chains_narrowest_first_with_the_unlisted_chain_first():
    payload, sections = S.build(*_fixture())
    chains = payload["chains"]
    assert chains[0]["l"] == [] and chains[0]["m"] == [0, S.MAP_CODES.index("closed")]
    assert payload["default"] == S.UNLISTED_CHAIN == 0
    assert payload["verdicts"][0] == S.NOT_LISTED
    # the dormant location (no rules on the page today) sits in its chain and says nothing: the live feed fills it in
    by_sid = {sid: chains[c]["l"] for sid, c in sections}
    assert by_sid == {10: ["6:dormant", "6:a"], 11: ["6:b(i)", "6:a"], 12: ["6:kispiox", "6:b(i)", "6:a"]}
    c10 = dict(sections)[10]
    assert payload["verdicts"][chains[c10]["sp"][S.SPECIES.index("CO")][1]]["s"] == "open"    # A's coho, as before
    # the location with no extent: on the card under its water's item, on no section
    (u,) = payload["unmapped"]
    assert (u["items"], u["w"], u["r"]) == (["gnis:4674"], "Kispiox River — above the resort signs", "6")
    assert u["chain"] not in {c for _, c in sections}
    assert payload["regions"]["6"]["asof"] == "2026-08-28"


def test_a_placed_section_the_handle_table_does_not_know_stops_the_build():
    files, feeds, placed, sid_of = _fixture()
    del sid_of["s3"]
    with pytest.raises(S.SalmonError, match="not in the handle table"):
        S.build(files, feeds, placed, sid_of)


def test_salmon_1_round_trips_and_runs_are_compact():
    payload, sections = S.build(*_fixture())
    b = S.encode(payload, sections, "147b20dce7d8576c")
    handles, back, secs = S.decode(b)
    assert (handles, back, secs) == ("147b20dce7d8576c", payload, sections)
    assert b[:5] == b"BCSA\x01"
    # three consecutive sids on three chains are three runs; on one chain, one
    one = S.encode(payload, [(10, 1), (11, 1), (12, 1)], "147b20dce7d8576c")
    assert len(one) < len(b)
