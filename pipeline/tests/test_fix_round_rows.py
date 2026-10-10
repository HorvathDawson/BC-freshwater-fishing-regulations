"""FIX round (2026-10-06), item 8 — two rows of Region 4 that bound more than the book prints.

  * KOOTENAY RIVER (upstream of Koocanusa Reservoir): its entry and its "Bait ban all year" and
    "Trout/char catch and release, Nov 1-Mar 31" said `whole`, so they reached the Creston reach and
    everything below Corra Linn Dam, which the downstream row exempts. Now `upstream_of`
    `kootenay_river__kootenay_koocanusa` (route measure 431,808 m on blk 356570348, where the river
    comes back into B.C. from Montana in the reservoir).
  * KOOTENAY LAKE — LOWER WEST ARM: "NOTE: the combined daily quota for kokanee from the Upper West
    Arm (when open to kokanee harvest) and the Lower West Arm (when open to kokanee harvest) cannot
    exceed 5" was an all-week "kokanee 5" that spoke on catch-and-release weekdays. It holds when the
    arm is open to harvest: Saturday and Sunday, as the row's own quota (book p.37).

Corpus tests read the catalogue; placement tests read a bundle (`UI_EXPORT_BUNDLE`, else the
shipped one).
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
UPPER = "r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35"
LOWER_ARM = "r4:kootenay_lake_lower_west_arm_for_location_see_map_on_page_34@4-7"
KOOCANUSA = "kootenay_river__kootenay_koocanusa"
KOOTENAY_BLK = "356570348"


def _entries():
    from pathlib import Path
    path = Path(__file__).resolve().parents[2] / "data/curated/regulations/entries/catalogue/region-4.json"
    return {e["entry_id"]: e for e in json.loads(path.read_text())["entries"]}


def test_the_upper_kootenay_row_is_upstream_of_koocanusa_in_the_catalogue():
    e = _entries()[UPPER]
    want = [{"op": "upstream_of", "splits": [KOOCANUSA], "item_id": "gnis:14097"}]
    assert e["extents"] == want
    rules = {r["rule_id"]: r for r in e["rules"]}
    assert rules["kootenay_river_upper.r1"]["extents"] == want      # bait ban all year
    assert rules["kootenay_river_upper.r2"]["extents"] == want      # trout/char C&R Nov 1-Mar 31
    assert not [r for r in e["rules"] if r["extents"] == [{"op": "whole"}]]


def test_the_lower_west_arm_kokanee_cap_holds_on_its_harvest_days():
    rules = {r["rule_id"]: r for r in _entries()[LOWER_ARM]["rules"]}
    assert rules["kootenay_lake_lower_west_arm.r5"]["when"] == rules[
        "kootenay_lake_lower_west_arm.r3"]["when"] == {"weekdays": ["Saturday", "Sunday"]}


@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _kootenay_sections(db) -> dict[int, float]:
    """{sid: lower route measure} for the Kootenay River's blue line, from the section spans."""
    out = {}
    for sid, lo in db.execute(
            "SELECT s.sid, p.lo_m FROM item i JOIN item_section s ON s.ord = i.ord "
            "JOIN section_span p ON p.sid = s.sid WHERE i.item_id = 'gnis:14097'"):
        out[sid] = lo
    return out


@pytest.mark.needs_bundle
def test_the_upper_row_binds_nothing_below_koocanusa(db):
    """No section of the Kootenay River below the Koocanusa cut (the Creston reach, Kootenay Lake's
    outflow, below Corra Linn) carries the upper row's bait ban or winter catch and release; every
    section of it above the cut in B.C. does. MUTATION: put `whole` back and the Creston reach
    carries them again (the bundle before this round)."""
    at = db.execute("SELECT at FROM split WHERE split_id = ?", (KOOCANUSA,)).fetchone()
    assert at, "the Koocanusa cut is in the bundle"
    cut = 1000.0 * dict(json.loads(at[0]))["gnis:14097"]                  # km -> route metres
    secs = _kootenay_sections(db)
    assert secs
    on = {}
    for sid in secs:
        on[sid] = {r for (r,) in db.execute(
            "SELECT r.rule_id FROM section_ruleset sr JOIN ruleset r ON r.set_id = sr.set_id "
            "WHERE sr.sid = ? AND r.entry_id = ?", (sid, UPPER))}
    below = [s for s, lo in secs.items() if lo is not None and lo < cut - 1]
    above = [s for s, lo in secs.items() if lo is not None and lo >= cut - 1]
    assert below and above
    assert not [s for s in below if on[s] & {"kootenay_river_upper.r1", "kootenay_river_upper.r2"}]
    assert all({"kootenay_river_upper.r1", "kootenay_river_upper.r2"} <= on[s]
               for s in above if not db.execute("SELECT 1 FROM outside_bc WHERE sid = ?",
                                                (s,)).fetchone())


@pytest.mark.needs_bundle
def test_lower_west_arm_kokanee_5_never_speaks_as_an_all_week_quota(db):
    sid = db.execute("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
                     "WHERE i.item_id = 'wbk:-22'").fetchone()[0]
    if sid is None:
        pytest.fail("pinned water wbk:-22 (Lower West Arm) is not in this bundle")
    for day in ((4, 2), (7, 15), (11, 20)):
        got = {R.rid(x): x["state"] for x in R.effective_rules(sid, day, "KO", BUNDLE)}
        assert got.get(f"{LOWER_ARM}::kootenay_lake_lower_west_arm.r5") == "beside", got


# ---- user decision 2026-10-06: every row named upstream/downstream/between binds only that part ----
PAUL = "r3:paul_creek_downstream_of_paul_lake@3-27"
HARRISON = "r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18"
FRASER = "r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4"
MISSION = "fraser_river__the_cpr_bridge_at_mission"


def _region(n):
    from pathlib import Path
    path = Path(__file__).resolve().parents[2] / f"data/curated/regulations/entries/catalogue/region-{n}.json"
    return {e["entry_id"]: e for e in json.loads(path.read_text())["entries"]}


def test_the_named_parts_are_drawn_in_the_catalogue():
    """Book p.31/p.23/p.23: PAUL CREEK (downstream of Paul Lake); HARRISON RIVER (from the Fraser
    River upstream to Harrison Lake); FRASER RIVER (upstream of the CPR Bridge at Mission). Their
    `whole` rules bound the creek above Paul Lake (Pinantan, Pemberton, Hyas lakes), the Harrison's
    1.2 km between Harrison and Little Harrison lakes, and the Fraser (and the row's lower-Fraser
    channels, Annacis to Tilbury) below Mission."""
    paul = _region(3)[PAUL]
    want = [{"op": "downstream_of", "splits": ["paul_creek__paul_lake"], "item_id": "gnis:22702"}]
    assert paul["extents"] == want and all(r["extents"] == want for r in paul["rules"])
    har = _region(2)[HARRISON]
    want = [{"op": "downstream_of", "splits": ["harrison_river__harrison_lake"],
             "item_id": "gnis:15333"}]
    assert har["extents"] == want and all(r["extents"] == want for r in har["rules"])
    fr = {r["rule_id"]: r for r in _region(2)[FRASER]["rules"]}
    up = {"op": "upstream_of", "splits": [MISSION], "item_id": "gnis:39325"}
    for rid in ("fraser_river_upstream_of_mission.r1", "fraser_river_upstream_of_mission.r6"):
        assert fr[rid]["extents"] == [up, {"op": "whole", "item_id": "gnis:11481"},
                                      {"op": "whole", "item_id": "gnis:13499"}]
    assert fr["fraser_river_upstream_of_mission.r5"]["extents"] == [up]
    assert not [r for e in (paul, har) for r in e["rules"] if r["extents"] == [{"op": "whole"}]]


def _spans(db, item):
    return {sid: (lo_m, hi_m, lo, hi) for sid, lo_m, hi_m, lo, hi in db.execute(
        "SELECT s.sid, p.lo_m, p.hi_m, a.token, b.token FROM item i JOIN item_section s "
        "ON s.ord = i.ord JOIN section_span p ON p.sid = s.sid JOIN span_end a ON a.eid = p.lo "
        "JOIN span_end b ON b.eid = p.hi WHERE i.item_id = ?", (item,))}


def _entry_rules_on(db, sid, entry):
    return {r for (r,) in db.execute(
        "SELECT r.rule_id FROM section_ruleset sr JOIN ruleset r ON r.set_id = sr.set_id "
        "WHERE sr.sid = ? AND r.entry_id = ?", (sid, entry))}


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item, entry, lake", [
    ("gnis:22702", PAUL, "wbk:329320869"),          # Paul Lake
    ("gnis:15333", HARRISON, "wbk:329177884"),      # Harrison Lake
])
def test_a_row_downstream_of_a_lake_binds_nothing_above_it(db, item, entry, lake):
    """Every section of the water above the lake's inlet carries none of the row's rules; every
    section below its outlet carries them all. MUTATION: the live bundle before this round (the
    rules `whole`) fails both."""
    sp = _spans(db, item)
    inlet = [v[0] for v in sp.values() if v[2] == f"lake_inlet:{lake}"]
    outlet = [v[1] for v in sp.values() if v[3] == f"lake_outlet:{lake}"]
    assert inlet and outlet
    above = [s for s, v in sp.items() if v[0] is not None and v[0] >= min(inlet)]
    below = [s for s, v in sp.items() if v[1] is not None and v[1] <= max(outlet)
             and v[0] is not None]
    assert above and below
    assert not [s for s in above if _entry_rules_on(db, s, entry)]
    assert all(len(_entry_rules_on(db, s, entry)) == 3 for s in below)


@pytest.mark.needs_bundle
def test_the_fraser_row_binds_nothing_below_mission(db):
    """The Fraser's mainstem below the CPR Bridge at Mission (walked down from the split by
    measure) carries none of the row's r1 (dead fin fish for sturgeon), r5 (no fishing for
    steelhead, Aug 15-Dec 31) or r6 (boats); the mainstem just above carries all three."""
    sp = _spans(db, "gnis:39325")
    start = [s for s, v in sp.items() if v[3] == MISSION]
    above = [s for s, v in sp.items() if v[2] == MISSION]
    assert len(start) == 1 and above
    chain, cur = [], start[0]
    by_hi = {}
    for s, v in sp.items():
        if v[1] is not None:
            by_hi.setdefault(v[1], []).append(s)
    while cur is not None and len(chain) < 500:
        chain.append(cur)
        nxt = [s for s in by_hi.get(sp[cur][0], []) if s not in chain]
        cur = nxt[0] if len(nxt) == 1 else None
    assert len(chain) >= 2, chain
    three = {f"fraser_river_upstream_of_mission.r{n}" for n in (1, 5, 6)}
    assert not [s for s in chain if _entry_rules_on(db, s, FRASER) & three]
    assert all(three <= _entry_rules_on(db, s, FRASER) for s in above)


def test_the_provincial_entries_on_those_waters_are_drawn_the_same_way():
    """Code review A-1/A-2: the same named parts bound `whole` by a provincial entry. `bait.r3`
    ("Lower Harrison River (Fraser River upstream to Harrison Lake)") and the Youth/Disabled
    Accompanied Waters advisory on Paul Creek (below Paul Lake, its row's part) and Coal Creek (below
    the Old MF&M Railway bridge, its row's part)."""
    prov = _region("provincial")
    bait = {r["rule_id"]: r for r in prov["zp:bait"]["rules"]}["bait.r3"]
    assert {"op": "downstream_of", "splits": ["harrison_river__harrison_lake"],
            "item_id": "gnis:15333"} in bait["extents"]
    assert not [x for x in bait["extents"] if x == {"op": "whole", "item_id": "gnis:15333"}]
    yd = prov["zp:youth_disabled_waters"]["rules"][0]
    whole_ids = {i for x in yd["extents"] if x["op"] == "whole" for i in x.get("item_ids") or ()}
    assert not whole_ids & {"gnis:22702", "gnis:17550"}
    assert {"op": "downstream_of", "splits": ["paul_creek__paul_lake"], "item_id": "gnis:22702"} \
        in yd["extents"]
    coal = _region(4)["r4:coal_creek_downstream_of_old_mf_m_railway_bridge_7_km_upstre@4-23"]
    assert {"op": "downstream_of", "splits": coal["extents"][0]["splits"], "item_id": "gnis:17550"} \
        in yd["extents"]
