"""T668: A DFO PLACE ANSWERS FOR ITS OWN REGION (`pipeline/deliver/salmon.py`; D1 the default, D2/D3 the owner's
answer 2026-11-11: "some streams genuinely have 2 regions like the parts of the Fraser that straddle boundaries").

  D1  an undrawn place rides only its water's sections that are members of its own region (a straddler: both);
  D2  on a section in ONE region, a tie of tier goes to that region's place (size broke it across regions); on a
      GENUINE STRADDLER both regions' places hold, each region cascades apart, the most strict answer per fish speaks;
  D3  a region-wide place binds exactly its region's members (the one membership) — a straddler takes both regions'
      region-wide places, strictest per fish; a named water keeps binding across the line.

The regions come from the ONE membership (`registry.regions.section_regions`, AGENTS 59) through
`salmon.section_regions_of`. Each test also builds with `section_regions=None` — the behaviour before T668 — and shows
the assertion would fail there, so none is a tautology.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.common.models.registry import RegistryItem
from pipeline.deliver import salmon as S
from pipeline.regs.parsing.entry_models import Extent

ALL_YEAR = {"dates": [{"from_month": 1, "from_day": 1, "to_month": 12, "to_day": 31}]}
OPEN = [{"rule_id": "o", "type": "retention_limit", "verbatim": "4 per day", "species": ["CH"], "when": ALL_YEAR,
         "take": 4}]
SHUT = [{"rule_id": "c", "type": "retention_limit", "verbatim": "No fishing for chinook", "species": ["CH"],
         "when": ALL_YEAR, "take": 0, "may_target": False}]


def _loc(lid, region, prec, extents=(), water_id=None):
    return SimpleNamespace(location_id=lid, region_number=region, precedence=prec, fingerprints=[lid], water=lid,
                           water_id=water_id, source_text={}, tidal_water=False,
                           binding=SimpleNamespace(extents=[Extent(**e) for e in extents]))


def _files(locs, waters=()):
    ws = {w.water_id: w for w in waters}
    out = {}
    for l in locs:
        slug = l.location_id.split(":")[0]
        out.setdefault(slug, SimpleNamespace(region_name=slug, locations=[], water=lambda wid: ws.get(wid)))
        out[slug].locations.append(l)
    return out


def _feeds(files, rules):
    return {slug: {"locators": [{"fingerprint": l.location_id, "rules": rules[l.location_id]} for l in ef.locations]}
            for slug, ef in files.items()}


def _registry(regions, areas=None):
    reg = {f"area:region:{r}": RegistryItem(id=f"area:region:{r}", name=f"Region {r}", kind="area",
                                            section_ids=tuple(hs)) for r, hs in regions.items()}
    for a, hs in (areas or {}).items():
        reg[a] = RegistryItem(id=a, name=a, kind="area", section_ids=tuple(hs))
    return reg


SID = {f"h{n}": n for n in (10, 11, 12, 16, 20, 27, 60, 67, 70, 76)}


def _build(locs, rules, placed, regions, item_sections=None, waters=()):
    files = _files(locs, waters)
    return S.build(files, _feeds(files, rules), placed, SID, item_sections, section_regions=regions)


def _chain_of(payload, sections, sid):
    return payload["chains"][dict(sections).get(sid, S.UNLISTED_CHAIN)]


def _chinook(payload, chain):
    return payload["verdicts"][chain["sp"][S.SPECIES.index("CH")][1]]["s"]


# ── D1 ───────────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_an_undrawn_place_rides_only_its_own_regions_sections():
    """Region 2's Fraser place, not drawn: on the Fraser's Region 2 section (h20) and the 2/7A straddler (h27, in both
    by the one membership), never on the Region 7A section (h70). Before T668 (`section_regions=None`) it rode h70 too."""
    fraser = SimpleNamespace(water_id="2:fraser", item_ids=["gnis:F"])
    locs = [_loc("2:mission", 2, 3, water_id="2:fraser"),
            _loc("7:default", 7, -1, [{"op": "within", "area_id": "area:region:7a"}])]
    rules = {"2:mission": OPEN, "7:default": SHUT}
    placed = [{"location_id": "2:mission", "mapped": False, "sections": []},
              {"location_id": "7:default", "mapped": True, "sections": ["h27", "h70"]}]
    reg = S.section_regions_of(_registry({"2": ["h20", "h27"], "7a": ["h27", "h70"]}), SID)
    items = {"gnis:F": [20, 27, 70]}
    payload, secs = _build(locs, rules, placed, reg, items, [fraser])
    assert _chain_of(payload, secs, 20).get("u") == ["2:mission"]
    assert _chain_of(payload, secs, 27).get("u") == ["2:mission"]
    on70 = _chain_of(payload, secs, 70)
    assert "u" not in on70 and on70["r"] == ["7"] and on70["l"] == ["7:default"]
    old, osecs = _build(locs, rules, placed, None, items, [fraser])
    assert _chain_of(old, osecs, 70).get("u") == ["2:mission"]             # the defect T668 found


# ── D2 ───────────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_on_a_tie_of_tier_the_sections_own_region_answers_first():
    """A Region 5B MU-group place (1 section) and a Region 1 basin place (3 sections), both tier 1, on h10 — wholly in
    Region 1. The Region 1 place answers there; size alone (before T668) put 5B's first and opened chinook."""
    locs = [_loc("5b:mu", 5, 1, [{"op": "within", "area_id": "area:mu_group:m"}]),
            _loc("1:basin", 1, 1, [{"op": "within", "area_id": "area:basin:b"}])]
    rules = {"5b:mu": OPEN, "1:basin": SHUT}
    placed = [{"location_id": "5b:mu", "mapped": True, "sections": ["h10"]},
              {"location_id": "1:basin", "mapped": True, "sections": ["h10", "h11", "h12"]}]
    reg = S.section_regions_of(_registry({"1": ["h10", "h11", "h12"], "5": ["h20"]}), SID)
    payload, secs = _build(locs, rules, placed, reg)
    c = _chain_of(payload, secs, 10)
    assert c["l"] == ["1:basin", "5b:mu"] and _chinook(payload, c) == "closed"
    old, osecs = _build(locs, rules, placed, None)
    c0 = _chain_of(old, osecs, 10)
    assert c0["l"] == ["5b:mu", "1:basin"] and _chinook(old, c0) == "open"


def test_within_one_region_size_still_breaks_the_tie():
    """Two Region 1 places of one tier: the narrower first, as T229 — the own-region key changes nothing there."""
    locs = [_loc("1:wide", 1, 1, [{"op": "within", "area_id": "area:basin:b"}]),
            _loc("1:narrow", 1, 1, [{"op": "within", "area_id": "area:basin:c"}])]
    placed = [{"location_id": "1:wide", "mapped": True, "sections": ["h10", "h11"]},
              {"location_id": "1:narrow", "mapped": True, "sections": ["h10"]}]
    reg = S.section_regions_of(_registry({"1": ["h10", "h11"]}), SID)
    payload, secs = _build(locs, {"1:wide": OPEN, "1:narrow": SHUT}, placed, reg)
    assert _chain_of(payload, secs, 10)["l"] == ["1:narrow", "1:wide"]


# ── D3 + D2 on a straddler ───────────────────────────────────────────────────────────────────────────────────────────

R6E = {"op": "within", "area_id": "area:region:6", "outside_areas": ["area:basin:400-"]}
R7 = [{"op": "within", "area_id": "area:region:7a"}, {"op": "within", "area_id": "area:region:7b"}]


def _d3_world():
    locs = [_loc("6:e", 6, 1, [R6E]), _loc("7:default", 7, -1, R7),
            _loc("6:sustut", 6, 3, [{"op": "whole"}]),
            _loc("6:a", 6, 0, [{"op": "within", "area_id": "area:region:6"},
                               {"op": "within", "area_id": "area:mu_group:haida"}])]
    rules = {"6:e": OPEN, "7:default": SHUT, "6:sustut": OPEN, "6:a": OPEN}
    # the reach run's placements, as a stale or touching one could hand them: E on h70 (7A only), 6:a on h16 (Region 1
    # only, inside the Haida Gwaii MU group)
    placed = [{"location_id": "6:e", "mapped": True, "sections": ["h60", "h67", "h70"]},
              {"location_id": "7:default", "mapped": True, "sections": ["h60", "h67", "h70"]},
              {"location_id": "6:sustut", "mapped": True, "sections": ["h70"]},
              {"location_id": "6:a", "mapped": True, "sections": ["h16", "h60"]}]
    # h67: a genuine straddler, a member of Region 6 and 7A
    reg = S.section_regions_of(
        _registry({"1": ["h16"], "6": ["h60", "h67"], "7a": ["h67", "h70"]}, {"area:mu_group:haida": ["h16"]}),
        SID, ["area:mu_group:haida"])
    return locs, rules, placed, reg


def test_a_straddler_takes_both_regions_places_and_the_strictest_answer_per_fish():
    """h67 lies in Region 6 and 7A: Region 6 E (opens chinook) and the Region 7 default (closes it) both hold, each in
    its own group, and chinook is CLOSED — the stricter. Before T668 E (tier 1) answered alone and opened it."""
    locs, rules, placed, reg = _d3_world()
    payload, secs = _build(locs, rules, placed, reg)
    c = _chain_of(payload, secs, 67)
    assert c["l"] == ["6:e", "7:default"] and c["s"] == [[0], [1]] and _chinook(payload, c) == "closed"
    old, osecs = _build(locs, rules, placed, None)
    c0 = _chain_of(old, osecs, 67)
    assert "s" not in c0 and _chinook(old, c0) == "open"


def test_a_region_wide_place_binds_only_its_regions_members():
    """Region 6 E handed h70 (a 7A section only) is not bound there: the Region 7 default answers, under the Sustut —
    a named water, which keeps binding across the line. Region 6 A stays on h16 (Region 1 only) because its Haida Gwaii
    MU group, not clipped to a region, holds h16."""
    locs, rules, placed, reg = _d3_world()
    payload, secs = _build(locs, rules, placed, reg)
    assert _chain_of(payload, secs, 70)["l"] == ["6:sustut", "7:default"]
    assert _chain_of(payload, secs, 16)["l"] == ["6:a"]
    old, osecs = _build(locs, rules, placed, None)
    assert "6:e" in _chain_of(old, osecs, 70)["l"]


def test_strictest_ranks_closed_release_check_open():
    o, r, k, c = ({"s": x} for x in ("open", "release", "check", "closed"))
    assert S.strictest([o, r]) is r and S.strictest([k, o]) is k and S.strictest([o, c, r]) is c
    assert S.strictest([o, None]) == S.NOT_LISTED                         # a region saying nothing: closed to it



def _open(n, t, ceil=None, over=()):
    kids = [{"node": "tier", "tier": "size", "n": k, "over_cm": cm} for cm, k in over]
    if ceil is not None:
        kids.append({"node": "tier", "tier": "size", "n": 0, "over_cm": ceil})
    return {"s": "open", "t": t, "tree": {"node": "tier", "tier": "count", "n": n, "children": kids}}


def test_two_openings_on_a_straddler_the_stricter_limit_speaks():
    """Owner 2026-11-11: "straddles should always take the most strict rules from both regions". A lower daily count
    with the same sizes wins; a narrower size ceiling with the same count wins; where each is stricter on one limit
    (2 a day vs 4 a day none over 50 cm) BOTH regions' words stand, no number (`u`) — neither limit dropped; an
    unread number likewise. MUTATION: `strictest` without `stricter_opening` keeps the first region's opening."""
    four, two = _open(4, "4 a day"), _open(2, "2 a day")
    assert S.strictest([four, two]) is two and S.strictest([two, four]) is two
    small = _open(4, "4 a day, none over 50 cm", ceil=50)
    assert S.strictest([four, small]) is small
    # "only 1 over 50 cm" and "none over 50 cm" are two statements to the freshwater reader (`rules.statement`): both bind
    assert S.strictest([_open(4, "4, only 1 over 50 cm", over=[(50, 1)]), small]) == \
        {"s": "open", "t": "4, only 1 over 50 cm; 4 a day, none over 50 cm", "u": 1}
    assert S.strictest([_open(4, "4, only 2 over 50", over=[(50, 2)]), _open(4, "4, only 1 over 50", over=[(50, 1)])]
                       )["t"] == "4, only 1 over 50"
    both = S.strictest([two, small])
    assert both == {"s": "open", "t": "2 a day; 4 a day, none over 50 cm", "u": 1}
    unread = {"s": "open", "t": "see notice", "u": 1}
    assert S.strictest([two, unread]) == {"s": "open", "t": "2 a day; see notice", "u": 1}
    assert S.strictest([two, {"s": "closed", "t": "no fishing"}])["s"] == "closed"


def test_a_straddler_shows_both_regions_gear_lines():
    """Owner 2026-11-11: both regions' gear and method lines, each said once, none dropped. MUTATION: the first region's
    lines only."""
    assert S.both_gear([("Barbless hooks",), ("No natural bait", "Barbless hooks")]) == \
        ("Barbless hooks", "No natural bait")
    shut6 = [dict(SHUT[0]), {"rule_id": "g6", "type": "bait_restriction", "verbatim": "No natural bait",
                             "gear": [{"slot": "bait"}], "when": {}, "when_targeting": ["SA"]}]
    gear7 = [{"rule_id": "g7", "type": "tackle_restriction", "verbatim": "Single barbless hook",
              "gear": [{"slot": "hook"}], "when": {}, "when_targeting": ["SA"]}]
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cal = S.chain_calendar([shut6, gear7], v, g, groups=[[0], [1]])
    assert g.items[cal["g"][1]] == ["No natural bait", "Single barbless hook"]


def test_the_live_feed_composes_a_two_region_chain_as_the_release():
    """`live.compose` with the chain's `s` equals `chain_calendar` with its groups (the app's withSalmonFeed reference)."""
    from pipeline.regs.dfo_salmon import live as L
    locs, rules, placed, reg = _d3_world()
    payload, secs = _build(locs, rules, placed, reg)
    c = _chain_of(payload, secs, 67)
    v, g = S._Interner(), S._Interner()
    v(S.NOT_LISTED)
    cals = {l: L.location_calendar(rules[l], v, g) for l in c["l"]}
    got = L.compose(c["l"], cals, v.items, groups=c["s"], intern=v, gears=g.items, gear_intern=g)
    ref = S.chain_calendar([rules[l] for l in c["l"]], v, g, groups=c["s"])
    assert got == ref
    # two openings with different counts, and gear on both sides: a merged verdict and a merged gear list, both ways
    two = [dict(OPEN[0], take=2, verbatim="2 per day"), {"rule_id": "g", "type": "bait_restriction",
                                                        "verbatim": "No natural bait", "gear": [{"slot": "bait"}],
                                                        "when": {}, "when_targeting": ["SA"]}]
    four = [dict(OPEN[0], take=4, verbatim="4 per day, none over 50 cm"),
            {"rule_id": "g2", "type": "tackle_restriction", "verbatim": "Barbless", "gear": [{"slot": "hook"}],
             "when": {}, "when_targeting": ["SA"]}]
    cals = {"a": L.location_calendar(two, v, g), "b": L.location_calendar(four, v, g)}
    got = L.compose(["a", "b"], cals, v.items, groups=[[0], [1]], intern=v, gears=g.items, gear_intern=g)
    assert got == S.chain_calendar([two, four], v, g, groups=[[0], [1]])
    with pytest.raises(S.SalmonError):
        L.compose(["a", "b"], cals, v.items, groups=[[0], [1]])


def test_region_clip_reads_only_region_wide_places():
    assert S.region_clip(_loc("6:e", 6, 1, [R6E])) == (frozenset({"6"}), ())
    assert S.region_clip(_loc("6:f", 6, 1, [{"op": "within", "area_id": "area:basin:100-",
                                             "within_area": "area:region:6"}])) == (frozenset({"6"}), ())
    assert S.region_clip(_loc("6:b", 6, 1, [{"op": "within", "area_id": "area:basin:400-"}])) is None
    assert S.region_clip(_loc("6:w", 6, 3, [{"op": "whole"}])) is None
    assert S.region_clip(_loc("6:u", 6, 3)) is None


def test_no_region_items_stops_the_build():
    """W46: a registry with no region item would let every DFO place answer everywhere — raised, never assumed."""
    with pytest.raises(S.SalmonError, match="no region items"):
        S.section_regions_of({}, SID)


def test_a_straddler_shows_the_tighter_hours_line_or_both():
    """RULINGS Z4a: daylight hours — the region whose hours lie inside the other's; a region printing none sets no
    limit; two that are not comparable both show. MUTATION: every region's line, or the first region's only."""
    day = {"start": {"solar": "sunrise", "offset_min": -60}, "end": {"solar": "sunset", "offset_min": 60}}
    tight = {"start": {"solar": "sunrise", "offset_min": 0}, "end": {"solar": "sunset", "offset_min": 60}}
    odd = {"start": {"solar": "sunrise", "offset_min": 30}, "end": {"solar": "sunset", "offset_min": 90}}
    assert S.tighter_hours({"3": {"hours": day}, "8": {"hours": tight}}) == ["8"]
    assert S.tighter_hours({"3": {"hours": day}, "5a": None}) == ["3"]
    assert S.tighter_hours({"3": {"hours": day}, "8": {"hours": day}}) == ["3"]
    assert S.tighter_hours({"3": {"hours": tight}, "8": {"hours": odd}}) == ["3", "8"]
    assert S.tighter_hours({"4": None, "7": None}) == []
