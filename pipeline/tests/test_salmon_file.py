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


def _loc(lid, prec, fps, water="", water_id=None, area="", tidal_water=False):
    return SimpleNamespace(location_id=lid, precedence=prec, fingerprints=fps, water=water, water_id=water_id,
                           source_text={"specific_area": area}, tidal_water=tidal_water)


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
    payload, sections = S.build(*_fixture(), section_regions=None)
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


def test_a_place_accepted_as_tidal_water_is_not_an_undrawn_part_of_its_water():
    """T449: a place the owner accepted as wholly in tidal water (`tidal_water`) lies off the freshwater map: it is not
    "not yet mapped" on the card and no section of its water carries it as an undrawn part."""
    files, feeds, placed, sid_of = _fixture()
    files["6"].locations[3].tidal_water = True                  # 6:resort
    payload, sections = S.build(files, feeds, placed, sid_of, {"gnis:4674": [11, 14]}, section_regions=None)
    assert payload["unmapped"] == []
    assert not any(c.get("u") for c in payload["chains"]) and 14 not in dict(sections)


def test_a_water_with_an_undrawn_place_carries_it_but_paints_its_drawn_answer():
    """PARTIAL COVERAGE: the resort (no extent, rules on the page) is on the Kispiox, so every section of the Kispiox —
    placed (11) or in no location (14) — carries it (`u`) for the card's caveat. (T372, owner 2026-11-04: the trout way)
    the map paints the DRAWN answer, never `check` for the undrawn place."""
    payload, sections = S.build(*_fixture(), {"gnis:4674": [11, 14]}, section_regions=None)
    chains, by_sid = payload["chains"], dict(sections)
    check = [0, S.MAP_CODES.index("check")]
    p0, s0 = S.build(*_fixture(), section_regions=None)
    assert (chains[by_sid[11]]["l"], chains[by_sid[11]]["u"]) == (["6:b(i)", "6:a"], ["6:resort"])
    assert chains[by_sid[11]]["m"] == p0["chains"][dict(s0)[11]]["m"] != check      # as if the resort were not there
    assert (chains[by_sid[14]]["l"], chains[by_sid[14]]["u"]) == ([], ["6:resort"]) and chains[by_sid[14]]["m"] != check
    assert chains[by_sid[14]]["w"] == "Kispiox River"
    # the species answers stay DFO's (the card keeps them, with the caveat); a section off the water is untouched
    assert chains[by_sid[11]]["sp"] == p0["chains"][dict(s0)[11]]["sp"]
    assert "u" not in chains[by_sid[10]] and chains[by_sid[10]]["m"] != check
    # (T372) the undrawn place's own chain says which days DFO speaks for it (`n`, the caveat's "in force today"): a
    # day some species has DFO's own verdict; the drawn chains carry none
    (resort,) = [u for u in payload["unmapped"] if u["w"].startswith("Kispiox River — above")]
    rc = chains[resort["chain"]]
    from pipeline.regs.dfo_salmon.live import _at
    days = [int(any(not payload["verdicts"][_at(r, d)].get("d") for r in rc["sp"])) for d in range(len(S.DAYS))]
    assert rc["n"] == S.runs_of(days) and 1 in days
    assert all("n" not in chains[c] for c in by_sid.values())
    # a DORMANT undrawn place (no rules today) marks nothing: the feed paints it the day DFO lists it again
    files, feeds, placed, sid_of = _fixture()
    feeds["6"]["locators"] = [l for l in feeds["6"]["locators"] if l["fingerprint"] != "fr"]
    p2, s2 = S.build(files, feeds, placed, sid_of, {"gnis:4674": [11]}, section_regions=None)
    assert p2["chains"][dict(s2)[11]]["u"] == ["6:resort"] and p2["chains"][dict(s2)[11]]["m"] != check


CLOSED_6 = {"id": "6:closed:abc", "k": "closed", "t": "There is no fishing for salmon in Region 6 until further notice."}


def test_a_header_closure_is_in_the_file_on_its_verdicts_and_chains():
    """(T356) the region header's closure is stated, not inferred by the app: every place of the page with rules answers
    with it (`quota.header_closure`), its verdict carries the note id `c`, and a chain it closes every day carries `c`
    (the note ids) — then its map says closed even with an undrawn place on its water, when that place is closed too.
    `live.compose` writes the same chain `c` from a feed."""
    from pipeline.regs.dfo_salmon import live as L
    payload, sections = S.build(*_fixture(), {"gnis:4674": [11]}, {"6": [CLOSED_6]}, section_regions=None)
    chains, by_sid = payload["chains"], dict(sections)
    (closed,) = [v for v in payload["verdicts"] if v.get("c")]
    assert (closed["s"], closed["c"], closed["t"]) == ("closed", "6:closed:abc", CLOSED_6["t"])
    for sid in (11, 12):
        assert chains[by_sid[sid]]["c"] == ["6:closed:abc"]
        assert chains[by_sid[sid]]["m"] == [0, S.MAP_CODES.index("closed")]       # not `check`: the resort is shut too
    # chain 10's first location (dormant) says nothing, 6:a answers every day with the closure: closed by the header
    assert chains[by_sid[10]]["c"] == ["6:closed:abc"]
    assert "c" not in chains[0]                                                  # the unlisted water: not opened
    assert {r["sizes"] == [] and r["shared"] == [] for r in payload["regions"].values()} == {True}
    # an open page: no `c` anywhere
    p0, _ = S.build(*_fixture(), section_regions=None)
    assert not any(v.get("c") for v in p0["verdicts"]) and not any(c.get("c") for c in p0["chains"])
    # the feed's composition says the same
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    closure = [{"rule_id": "6:header-closed", "type": "retention_limit", "verbatim": CLOSED_6["t"], "species": ["SA"],
                "take": 0, "may_target": False, "header_note": CLOSED_6["id"]}]
    locs = {"6:a": {"r": "6", **L.location_calendar(closure, v, g)}, "6:resort": {"r": "6", **L.location_calendar(closure, v, g)}}
    got = L.compose(["6:dormant", "6:a"], locs, v.items, ["6:resort"])
    assert got["c"] == ["6:closed:abc"] and got["m"] == [0, S.MAP_CODES.index("closed")]
    locs["6:resort"] = {"r": "6", **L.location_calendar(REGION, v, g)}            # an undrawn place still open
    got = L.compose(["6:dormant", "6:a"], locs, v.items, ["6:resort"])
    # not closed by the header (the resort is open) — and (T372) the map paints the drawn answer, never `check`
    assert "c" not in got and got["m"] == [0, S.MAP_CODES.index("closed")]


def test_a_placed_section_the_handle_table_does_not_know_stops_the_build():
    files, feeds, placed, sid_of = _fixture()
    del sid_of["s3"]
    with pytest.raises(S.SalmonError, match="not in the handle table"):
        S.build(files, feeds, placed, sid_of, section_regions=None)


def test_salmon_2_splits_per_bundle_region_and_round_trips():
    """(owner 2026-10-28) the head (payload) + one slice of runs per bundle region: a section in two regions is in both,
    one in none is in `x`; every region has its slice (empty ones too), and the slices put back are the sections."""
    payload, sections = S.build(*_fixture(), section_regions=None)
    regions = {10: ["6"], 11: ["5", "6"], 12: []}
    files = S.encode_files(payload, sections, "147b20dce7d8576c", lambda sid: regions.get(sid, ()))
    from pipeline.deliver.release.shards import REGIONS
    assert sorted(files) == sorted(["salmon", *(S.shard_name(r) for r in (*REGIONS, "x"))])
    assert files["salmon"][:5] == b"BCSA\x04" and files["salmon-6"][:5] == b"BCSR\x01"
    handles, back, secs = S.decode_files(files)
    assert (handles, {k: v for k, v in back.items() if k != "shards"}, secs) == ("147b20dce7d8576c", payload, sections)
    assert [s for s, _ in S.decode_shard(files["salmon-6"])[2]] == [10, 11]
    assert [s for s, _ in S.decode_shard(files["salmon-5"])[2]] == [11]
    assert [s for s, _ in S.decode_shard(files["salmon-x"])[2]] == [12]
    assert S.decode_shard(files["salmon-1"])[2] == []


def test_t228_an_area_bound_place_and_a_partial_place_carry_nothing():
    """T228: an area-bound place's edge is a real line (Klinaklini leaves MUs 5-6 to 5-11) and a place covering only
    part of a water's stem (1, 2 of 1, 2, 6) is bound there: neither takes a side channel."""
    from pipeline.deliver.salmon import carry_side_channels
    sid_of = {f"h{i}": i for i in range(1, 7)}
    ord_of = {1: 10, 2: 10, 3: 10, 4: 10, 5: 20, 6: 10}
    touch = {1: {2, 5, 6}, 2: {1, 3}, 3: {2, 4}, 4: {3}, 5: {1}, 6: {1}}
    off = [3, 4, 5]                                         # 3 off 2, 4 off 3 (a channel off a channel), 5 another water's
    placed = [{"location_id": "tat", "mapped": True, "sections": ["h1", "h2"]},
              {"location_id": "mu", "mapped": True, "sections": ["h1", "h2"]}]
    got = carry_side_channels(placed, {"mu"}, sid_of, ord_of, off, touch)
    assert got[0] is placed[0]                              # 6 on-stem uncovered: not the whole water, no carry
    assert got[1] is placed[1]


def test_t228_side_channels_carry_only_on_a_water_the_place_covers_whole():
    """T228: water 10's main stem is 1, 2, 6; side channels 3 (off 2) and 4 (off 3); 5 another water's channel. A place
    covering the whole stem carries 3 and 4; one covering only 1, 2 (Morice: part of the water) carries nothing."""
    from pipeline.deliver.salmon import carry_side_channels
    sid_of = {f"h{i}": i for i in range(1, 7)}
    ord_of = {1: 10, 2: 10, 3: 10, 4: 10, 5: 20, 6: 10}
    touch = {1: {2, 5, 6}, 2: {1, 3}, 3: {2, 4}, 4: {3}, 5: {1}, 6: {1}}
    whole = [{"location_id": "w", "mapped": True, "sections": ["h1", "h2", "h6"]}]
    part = [{"location_id": "p", "mapped": True, "sections": ["h1", "h2"]}]
    assert carry_side_channels(whole, set(), sid_of, ord_of, [3, 4, 5], touch)[0]["sections"] == ["h1", "h2", "h6", "h3", "h4"]
    assert carry_side_channels(part, set(), sid_of, ord_of, [3, 4, 5], touch)[0] is part[0]


def test_a_place_the_release_does_not_show_is_said_and_the_feed_is_checked_against_it():
    """T549: every curated place is in a chain or in `off` with why (tidal, not placed by the reach run) — never dropped
    silently; `feed_vs_release` names a feed place the release neither shows nor states. MUTATION: build without `off`
    and the tidal and unplaced places are `feed_only`."""
    files, feeds, placed, sid_of = _fixture()
    files["6"].locations += [_loc("6:tidal", 3, ["ft"], "Kispiox River", "6:kispiox-river", tidal_water=True),
                             _loc("6:late", 3, ["fl"])]
    placed = placed + [{"location_id": "6:tidal", "mapped": False, "sections": []}]
    payload, _ = S.build(files, feeds, placed, sid_of, {"gnis:4674": [11]}, section_regions=None)
    assert payload["off"] == {
        "6:late": {"why": S.OFF_UNPLACED, "k": "unplaced", "r": "6", "w": "6:late", "items": []},
        # (T549 app half) its water's items, so the card shows it as a text row on that water's page
        "6:tidal": {"why": S.OFF_TIDAL, "k": "tidal", "r": "6", "w": "Kispiox River", "items": ["gnis:4674"]}}
    feed = {"locs": {"6:a": {}, "6:kispiox": {}, "6:tidal": {}, "6:late": {}, "6:new": {}}}
    got = S.feed_vs_release(payload, feed)
    assert got["feed_only"] == ["6:new"] and got["said"] == ["6:late", "6:tidal"]
    assert "6:b(i)" in got["release_only"]
    assert S.feed_vs_release({**payload, "off": {}}, feed)["feed_only"] == ["6:late", "6:new", "6:tidal"]
