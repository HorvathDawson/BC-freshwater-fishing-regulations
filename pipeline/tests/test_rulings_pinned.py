"""T99 — the rulings RULINGS §16 listed as held by nothing, or by curated data no test pinned.

Each test reads an OUTPUT — the bundle's binding and the reference reader's answer
(`read.effective_rules_bound`, traced), the status index, the export guide — and asserts the WHOLE value
the ruling states (memory `checks-seeded-from-input-launder`: never seeded from the curated row it pins).
Each carries a MUTATION: the curated row's change, applied to what the bundle holds (its rule, or the
binding its extents made), must turn the same check red. A mutated rule table is seeded under a path that is
no file (`read._RULES_BY_PATH`), the established pattern of `test_competition.py`.

  Z1   a section's answer is its region base plus the named rows bound to it, every difference named by
       one row (`pipeline.tools.base_deltas`; T57's vitest pins the unnamed strata against the release page)
  Z2   Region 2's bass rule binds Mill Lake; Mill Lake's row speaks over it
  Z10  Peaceful Lake stays in the Malcolm Knapp closure; Goose Lake (2.2 % inside) is out
  Q20  Adams Lake's bull trout / lake trout quota is permanent; the Adams River's dated bull trout quota
       holds on its dates only, the zone outside them
  L14  the Stellako is closed Nov 15-Jun 30 (its own closure and the Fraser-watershed spring closures)
  L16  Chimney Creek is open all year below Brunson Lake; above, the spring closure holds
  T8   a spot closure closes the tributaries joining within the spot, and only those
  G8   the Columbia's towing and engine rules follow Fairmont-Donald; the Salmon River's two clauses
       follow the Hwy 97 bridge at Falkland — none is a note on the whole river
  Li1  licensing never colours a water: sections of one rule key share one status, designated or not
  Li10 the West Road keeps its Class II designation (no CW glyph printed), Jun 15-Oct 31
  E13  the Goat River's catch and release holds on the whole mainstem, and only there; the Fraser night
       closure covers the river's side channels, not the sloughs joining it
  E14  Hays Creek: the culvert split sits at the mouth; the closure binds upstream of it
  C3   the shipped guide says, in words, that trout includes char
"""
from __future__ import annotations

import datetime
import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle.rules import closure_grade
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
from pipeline.tools import base_deltas as T

pytestmark = pytest.mark.needs_bundle

BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(BUNDLE).parent.parent / "regs")


@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


# --------------------------------------------------------------------------------------------- helpers
def _sections(db, item_id: str) -> list:
    return [s for (s,) in db.execute("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
                                     "WHERE i.item_id = ? ORDER BY s.sid", (item_id,))]


def _bound_to(db, entry: str, rule: str, via: str | None = None) -> set:
    q = ("SELECT sr.sid FROM ruleset r JOIN section_ruleset sr ON sr.set_id = r.set_id "
         "WHERE r.entry_id = ? AND r.rule_id = ?")
    return {s for (s,) in db.execute(q + (" AND r.via = ?" if via else ""),
                                     (entry, rule, via) if via else (entry, rule))}


def _span(db, sid: int):
    return db.execute("SELECT lo_m, hi_m FROM section_span WHERE sid = ?", (sid,)).fetchone()


def _ask(db, sid: int, day, fish: str, path: str = BUNDLE, bound=None) -> dict:
    """The reader's traced answer on one section: {rid: state}, the section's own steelhead facts."""
    bound = T.bind(db, sid) if bound is None else bound
    got = R.effective_rules_bound(bound, fish == "RB" and R.steelhead_water(db, sid), day, fish, path,
                                  steelhead_rules_here=fish != "ST" or R.steelhead_rules(db, sid),
                                  trace=True)
    return {R.rid(x): x for x in got}


_SEEDED = [0]


def _mutated(**changes) -> str:
    """The bundle's rule table with the named rules changed (`{"entry::rule": {field: value}}`),
    seeded under a path that is no file; the reader reads it as it reads the bundle."""
    real = R._rules_of(BUNDLE)
    _SEEDED[0] += 1
    path = f"{BUNDLE}#t99-mutation-{_SEEDED[0]}"
    got = {k: dict(v) for k, v in real.items()}
    for rid, fields in changes.items():
        k = tuple(rid.split("::", 1))
        assert k in got, rid
        got[k].update(fields)
        got[k]["_rank"] = R.source_of(got[k]).rank
    R._RULES_BY_PATH[path] = got
    R._NEARER_BY_PATH[path] = R._nearer_of(BUNDLE)
    return path


def _speakers(ans: dict, keep=lambda k: True) -> list:
    return sorted(k for k, x in ans.items() if x["state"] in R.SPEAKER_STATES and keep(k))


def _closed(ans: dict) -> bool:
    return any(x["state"] == "speaks" and closure_grade(x) == "full" and not x.get("partly_lifted")
               for x in ans.values())


DAYS = [datetime.date(2024, 1, 1) + datetime.timedelta(days=i) for i in range(366)]


def _closed_ranges(db, sid: int, fish: str, path: str = BUNDLE) -> list:
    """The days a fish is closed on a section, as (first, last) "MM-DD" runs."""
    bound = T.bind(db, sid)
    flags = [_closed(_ask(db, sid, (d.month, d.day), fish, path, bound)) for d in DAYS]
    out, start = [], None
    for i, c in enumerate(flags + [False]):
        if c and start is None:
            start = i
        if not c and start is not None:
            out.append((DAYS[start].strftime("%m-%d"), DAYS[i - 1].strftime("%m-%d")))
            start = None
    return out


# ============================================================================================ Z1
def test_z1_every_difference_from_the_region_base_is_named_by_one_row(db):
    """A stratified sample in EVERY region (a water row's own reach, the tributary walk, a lake, base
    only) and on every region line: per section, day and fish, the page's answer minus the base's answer
    is attributable, rule by rule, to one named row; and the base bound there is its own regions'."""
    got = T.sweep(BUNDLE, per=2)
    want_strata = {f"{r}/{s}": 2 for r in T.regions_of(db) for s in T.STRATA}
    assert {k: v for k, v in got["strata"].items() if not k.endswith("region_line")} == want_strata
    assert [k for k, v in got["strata"].items() if k.endswith("region_line") and not v] == []
    assert got["unattributable"] == []
    n = got["counts"]
    assert n["attributed"] == n["differences"] > 0 and len(got["rows"]) > 9


def _a_base_only_lake(db, region: str) -> int:
    return T.sample(db, per=1, regions=[region], strata=["base_only"])[(region, "base_only")][0]


def test_z1_a_region_4_lake_with_no_row_answers_exactly_region_4s_table(db):
    """RULINGS Z1's example: no row, no difference — the page IS the base, on every day and fish."""
    rules = R._rules_of(BUNDLE)
    sid = next(s for s in T.sample(db, per=20, regions=["4"], strata=["lake"])[("4", "lake")]
               if all(T.is_base(rules[(e, r)]) for e, r, _ in T.bind(db, s)))
    diffs = {(d, f): T.attribute(*T.answers(db, sid, d, f, BUNDLE), rules)
             for d in T.DAYS for f in T.leaf_fish()}
    assert {k: v for k, v in diffs.items() if v} == {}
    assert {R.base_region(e) for e, r, _ in T.bind(db, sid)} - {None} == {"4"}


def test_z1_kootenay_lakes_rainbow_10_is_traced_to_the_kootenay_lake_row(db):
    """RULINGS Z1's example: the rainbow 10 is a difference, and every difference on that answer is the
    Kootenay Lake row's. MUTATION: drop Region 4's trout/char quota from the page only — a base rule
    leaves the page with no row to name: unattributable."""
    sid = next(s for s in _sections(db, "wbk:-20")
               if any(e.startswith("r4:kootenay_lake_all_parts") for e, _, _ in T.bind(db, s)))
    rules = R._rules_of(BUNDLE)
    base, page = T.answers(db, sid, (7, 1), "RB", BUNDLE)
    main = "r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19"
    got = {d.rule: (d.base, d.page, d.row) for d in T.attribute(base, page, rules)}
    assert got == {
        f"{main}::kootenay_lake_main_body.r1": (None, "speaks", main),
        f"{main}::kootenay_lake_main_body.r4": (None, "speaks", main),
        f"{main}::kootenay_lake_main_body.r6": (None, "speaks", main),
        # the region's table writing about this lake, and the province's Kootenay rainbow stamp: rows
        "z4:kootenay_lake_boundaries::kootenay_lake_boundaries.r1": (None, "shown", "z4:kootenay_lake_boundaries"),
        "z4:kootenay_lake_boundaries::kootenay_lake_boundaries.r2": (None, "shown", "z4:kootenay_lake_boundaries"),
        "zp:kootenay_rainbow_stamp::kootenay_rainbow_stamp.r1": (None, "speaks", "zp:kootenay_rainbow_stamp"),
        # the rainbow 10 lifts Region 4's "Trout/char: 5" and its "1 over 50 cm" for rainbow
        "z4:trout_char_quota::trout_char_quota.r1": ("speaks", "lifted", main),
        "z4:trout_char_quota::trout_char_quota.r2": ("speaks", "lifted", main),
    }
    ten = {R.rid(x): (x["take"], x["species"]) for x in page if x["state"] == "speaks" and x.get("take") == 10}
    assert ten == {f"{main}::kootenay_lake_main_body.r4": (10, ["RB"])}
    # MUTATION
    bound = T.bind(db, sid)
    drop = next(x for x in bound if x[0] == "z4:trout_char_quota" and x[1].endswith(".r1"))
    _base, _ = T.answers(db, sid, (7, 1), "RB", BUNDLE, bound, rules)
    _, page2 = T.answers(db, sid, (7, 1), "RB", BUNDLE, [x for x in bound if x != drop], rules)
    lost = [d for d in T.attribute(_base, page2, rules) if d.row is None]
    assert [d.rule for d in lost] == [f"{drop[0]}::{drop[1]}"]


def test_z1_the_base_belongs_to_the_sections_regions(db):
    """MUTATION: bind another region's base rule on a Region 4 section — named as out of place."""
    sid = _a_base_only_lake(db, "4")
    rules = R._rules_of(BUNDLE)
    bound = T.bind(db, sid)
    assert T.base_problems(db, sid, bound, rules) == []
    stray = ("z2:species_quotas", "species_quotas.r1", "reach")
    assert T.base_problems(db, sid, bound + [stray], rules) == \
        [f"{sid}: base rules of Region 2 bound outside it (['4'])"]
    no4 = [x for x in bound if R.base_region(x[0]) != "4"]
    assert T.base_problems(db, sid, no4, rules) == [f"{sid}: no base rule of Region 4"]


# ============================================================================================ Z2
MILL = "wbk:329291901"
MILL_ROW = "r2:mill_lake_abbotsford@2-4"
R2_BASS = "z2:species_quotas::species_quotas.r1"


def _mill(db, path=BUNDLE) -> dict:
    sid, = _sections(db, MILL)
    ans = _ask(db, sid, (7, 1), "SMB", path)
    return {k: (x["state"], x.get("by")) for k, x in ans.items()
            if k.startswith(("z2:species", MILL_ROW)) and x["type"] == "retention_limit"}


def test_z2_region_2s_bass_rule_binds_mill_lake_and_the_lakes_row_speaks_over_it(db):
    """"Bass: 20" binds Mill Lake (the exclusion is not encoded); the lake's "bass daily quota = 4"
    displaces it. MUTATION: the row's quota no longer about bass — Region 2's 20 speaks again."""
    sid, = _sections(db, MILL)
    assert ("z2:species_quotas", "species_quotas.r1", "reach") in T.bind(db, sid)
    assert _mill(db) == {R2_BASS: ("displaced", f"{MILL_ROW}::mill_lake.r2"),
                         f"{MILL_ROW}::mill_lake.r2": ("speaks", None)}
    bad = _mutated(**{f"{MILL_ROW}::mill_lake.r2": {"species": ["RB"]}})
    assert _mill(db, bad) == {R2_BASS: ("speaks", None)}


# =========================================================================================== Z10
PEACEFUL, GOOSE = "wbk:329292654", "wbk:329291952"
KNAPP = ("z2:malcolm_knapp_research_forest", "malcolm_knapp_research_forest.r1", "reach")


def _z10(db, binding=None) -> dict:
    out = {}
    for name, item in (("Peaceful Lake", PEACEFUL), ("Goose Lake", GOOSE)):
        sid, = _sections(db, item)
        bound = (binding or {}).get(name) or T.bind(db, sid)
        out[name] = (KNAPP in bound, _closed(_ask(db, sid, (7, 1), "RB", bound=bound)))
    return out


def test_z10_peaceful_lake_stays_in_the_knapp_closure_goose_lake_is_out(db):
    """MUTATION: Goose Lake back in the area (its `outside_items` dropped) — closed."""
    assert _z10(db) == {"Peaceful Lake": (True, True), "Goose Lake": (False, False)}
    g, = _sections(db, GOOSE)
    assert _z10(db, {"Goose Lake": T.bind(db, g) + [KNAPP]})["Goose Lake"] == (True, True)


# =========================================================================================== Q20
ADAMS_LAKE, ADAMS_RIVER = "wbk:329014384", "gnis:39257"
AL, AR = "r3:adams_lake@3-37", "r3:adams_river_upstream_of_adams_lake@3-37"
Q20_DAYS = ((3, 1), (7, 15), (9, 15), (11, 15), (12, 1))


def _q20(db, path=BUNDLE) -> dict:
    lake = next(s for s in _sections(db, ADAMS_LAKE) if any(e == AL for e, _, _ in T.bind(db, s)))
    river = next(s for s in _sections(db, ADAMS_RIVER) if any(e == AR for e, _, _ in T.bind(db, s)))
    out = {}
    for day in Q20_DAYS:
        a, b = _ask(db, lake, day, "DV", path), _ask(db, river, day, "DV", path)
        out[day] = (_speakers(a, lambda k: k.startswith(AL)),
                    _speakers(b, lambda k: k.startswith(AR)),
                    _speakers(b, lambda k: k.startswith("z3:trout_char_quota")))
    return out


def test_q20_only_lines_that_print_dates_are_dated(db):
    """Adams Lake's "Daily quota = 1 bull trout or lake trout" speaks every day; the Adams River's "Bull
    trout daily quota = 1 (none under 80 cm), July 1-30 and Nov 1-Dec 31" on those days only, and on the
    others Region 3's table answers (its Aug 1-Oct 31 stream release on Sept 15). MUTATIONS: the lake's
    quota dated (Jul 1-30) — gone Sept 15; the river's undated — it speaks Sept 15."""
    lk = [f"{AL}::adams_lake.r3", f"{AL}::adams_lake.r4", f"{AL}::adams_lake.r6"]
    rv0, rv = [f"{AR}::adams_river_upstream.r3"], [f"{AR}::adams_river_upstream.r{i}" for i in (3, 4, 5)]
    zone = ["z3:trout_char_quota::trout_char_quota.r" + x for x in ("1", "2", "3", "4", "4b")]
    want = {(3, 1): (lk, rv0, []), (7, 15): (lk, rv, zone), (9, 15): (lk, rv0, ["z3:trout_char_quota::trout_char_quota.r6"]),
            (11, 15): (lk, rv, zone), (12, 1): (lk, rv, zone)}
    assert _q20(db) == want
    july = {"when": {"dates": [{"from_month": 7, "from_day": 1, "to_month": 7, "to_day": 30}]}}
    assert f"{AL}::adams_lake.r3" not in _q20(db, _mutated(**{f"{AL}::adams_lake.r3": july}))[(9, 15)][0]
    undated = _mutated(**{f"{AR}::adams_river_upstream.r4": {"when": None}})
    assert f"{AR}::adams_river_upstream.r4" in _q20(db, undated)[(9, 15)][1]


# =========================================================================================== L14
STELLAKO = "gnis:7836"
STELLAKO_CLOSED = [("01-01", "06-30"), ("11-15", "12-31")]


def test_l14_the_stellako_stays_under_the_fraser_watershed_spring_closure(db):
    """Every Stellako section, both regions: closed Nov 15-Jun 30 — its own "No Fishing Nov 15-May 14"
    and the spring closure (Zone 7A's streams; Region 6's Fraser watershed) to Jun 30. MUTATION: the row
    prints an exemption (its all-year rainbow release lifting both spring closures, as "exempt from
    spring closure" would) — closed to May 14 only."""
    sids = _sections(db, STELLAKO)
    assert len(sids) > 3
    assert {s: _closed_ranges(db, s, "RB") for s in sids} == {s: STELLAKO_CLOSED for s in sids}
    lift = [{"entry_id": "z7a:spring_stream_closure", "rule_id": "spring_stream_closure.r1"},
            {"entry_id": "z6:iskut_fraser_closure", "rule_id": "iskut_fraser_closure.r2"}]
    bad = _mutated(**{"r6:stellako_river@6-4+7-12::stellako_river.r3": {"exempts": lift},
                      "r7:stellako_river@7-12::stellako_river.r4": {"exempts": lift}})
    assert _closed_ranges(db, sids[0], "RB", bad) == [("01-01", "05-14"), ("11-15", "12-31")]


# =========================================================================================== L16
CHIMNEY, CHIMNEY_ROW = "gnis:39269", "r5:chimney_creek@5-2::chimney_creek.r1"
SPRING5 = "z5:spring_stream_closure::spring_stream_closure.r1"
BRUNSON = "chimney_creek__brunson_lake"


def _l16(db, path=BUNDLE) -> dict:
    (at,), = db.execute("SELECT at FROM split WHERE split_id = ?", (BRUNSON,)).fetchall()
    lo_edge = min(m for _, m in json.loads(at)) * 1000
    hi_edge = max(m for _, m in json.loads(at)) * 1000
    out = {"below": set(), "above": set()}
    for sid in _sections(db, CHIMNEY):
        lo, hi = _span(db, sid)
        side = "below" if hi <= lo_edge + 5 else "above" if lo >= hi_edge - 5 else "inside"
        x = _ask(db, sid, (5, 1), "RB", path).get(SPRING5)
        out.setdefault(side, set()).add((x["state"], x.get("by")))
    return out


def test_l16_chimney_creek_is_open_all_year_below_brunson_lake_only(db):
    """On May 1 (inside Region 5's spring closure): every section below Brunson Lake has the closure
    lifted by the row's "Open all year downstream of Brunson Lake"; every section above keeps it.
    MUTATION: the row's lift dropped — below is closed too."""
    want = {"below": {("lifted", CHIMNEY_ROW)}, "above": {("speaks", None)}}
    assert _l16(db) == want
    assert _l16(db, _mutated(**{CHIMNEY_ROW: {"exempts": None}}))["below"] == {("speaks", None)}


# ============================================================================================ T8
ANZAC_ROW, ANZAC = "r7:anzac_river@7-23", "gnis:7607"


def _window(db, item: str, *splits: str) -> tuple:
    """The measure window (m) between a water's printed cuts, from the bundle's `split`."""
    at = []
    for sp in splits:
        (a,), = db.execute("SELECT at FROM split WHERE split_id = ?", (sp,)).fetchall()
        at += [round(m * 1000) for i, m in json.loads(a) if i == item]
    return min(at), max(at)


def _off_stem(db, sid: int):
    """(join measure, off_stem) of a section — an off-stem piece sits at the measure where it joins."""
    lo_m, _hi, off = db.execute("SELECT lo_m, hi_m, off_stem FROM section_span WHERE sid = ?", (sid,)).fetchone()
    return lo_m, bool(off)


def _t8(db, reach=None, trib=None) -> dict:
    """The Anzac spot: which of the river's joining pieces (off-stem sections, placed at the measure where
    they join) the closure binds, inside and outside its 500 m window; and its tributaries against the
    river's own walk upstream of the North Anzac (r2, the same ✱ row)."""
    lo, hi = _window(db, ANZAC, "anzac_river__within_500_m_downstream", "anzac_river__within_500_m_upstream")
    reach = _bound_to(db, ANZAC_ROW, "anzac_river.r1", "reach") if reach is None else reach
    trib = _bound_to(db, ANZAC_ROW, "anzac_river.r1", "trib") if trib is None else trib
    river_walk = _bound_to(db, ANZAC_ROW, "anzac_river.r2", "trib")
    joins = {}
    for s in _sections(db, ANZAC):
        at, off = _off_stem(db, s)
        if off and at is not None:            # a piece with no join measure is not placed
            joins[s] = lo - 5 <= at <= hi + 5
    return {"joining_inside_bound": sorted({s in reach for s, inside in joins.items() if inside}),
            "joining_outside_bound": sorted({s in reach for s, inside in joins.items() if not inside}),
            "tributaries": bool(trib), "fewer_than_the_rivers_walk": trib < river_walk}


def test_t8_a_spot_closure_closes_the_tributaries_joining_within_the_spot(db):
    """"No Fishing within 500 m radius of the Upper Anzac bridge" (✱ row): the pieces joining inside the
    spot are bound with it and those joining elsewhere are not; its tributary walk holds streams, and fewer
    than the river's own walk. The Mitchell's 100 m around the weir has no joining water and no tributary.
    MUTATIONS: a piece joining outside the spot bound — named; the spot walking the whole river — refused."""
    assert _t8(db) == {"joining_inside_bound": [True], "joining_outside_bound": [False],
                       "tributaries": True, "fewer_than_the_rivers_walk": True}
    assert _bound_to(db, "r5:mitchell_river@5-15", "mitchell_river.r1", "trib") == set()
    lo, hi = _window(db, ANZAC, "anzac_river__within_500_m_downstream", "anzac_river__within_500_m_upstream")
    outside = next(s for s in _sections(db, ANZAC)
                   if _off_stem(db, s)[1] and _off_stem(db, s)[0] is not None
                   and not lo - 5 <= _off_stem(db, s)[0] <= hi + 5)
    reach = _bound_to(db, ANZAC_ROW, "anzac_river.r1", "reach")
    assert _t8(db, reach=reach | {outside})["joining_outside_bound"] == [False, True]
    whole = _bound_to(db, ANZAC_ROW, "anzac_river.r2", "trib")
    assert _t8(db, trib=whole)["fewer_than_the_rivers_walk"] is False


# ============================================================================================ G8
def _follows(db, item: str, entry: str, rules: tuple, cuts: tuple, bound=None) -> dict:
    """Per rule: does it bind its water's blue line exactly inside the printed cuts (every on-stem section
    it binds lies inside, every one it does not lies outside), and is it a note on the whole water?"""
    lo, hi = cuts
    out = {}
    for r in rules:
        got = (bound or {}).get(r)
        got = got if got is not None else _bound_to(db, entry, r, "reach")
        inside = outside = 0
        ok = True
        for s in _sections(db, item):
            a, b, off = db.execute("SELECT lo_m, hi_m, off_stem FROM section_span WHERE sid = ?", (s,)).fetchone()
            if off or a is None:
                continue
            within = lo - 5 <= a and b <= hi + 5
            ok &= within == (s in got)
            inside += within
            outside += not within
        (note,), = db.execute("SELECT undrawn_part FROM rule WHERE entry_id = ? AND rule_id = ?", (entry, r))
        out[r] = {"follows_its_reach": bool(ok and inside and outside), "note_on_the_whole": note is not None}
    return out


def test_g8_towing_and_engine_rules_follow_their_printed_reach(db):
    """The Columbia's "no towing" and "15 kW … in main channel from Fairmont to Donald" bind exactly the
    Fairmont-Donald stretch of the river; the Salmon River's catch and release and bait ban both bind
    exactly the mouth to the Hwy 97 bridge at Falkland. None is a note on the whole river (as the Chilko's
    "on part" speed limit is, Q28). MUTATION: the towing rule held on the whole river — refused."""
    col = ("gnis:37414", "r4:columbia_river@4-15+4-26+4-34+4-38+4-8", ("columbia_river.r13", "columbia_river.r14"),
           _window(db, "gnis:37414", "columbia_river__donald", "columbia_river__fairmont"))
    sal = ("gnis:39454", "r3:salmon_river@3-26", ("salmon_river.r1", "salmon_river.r2"),
           (0, _window(db, "gnis:39454", "salmon_river__hwy_97_bridge_at_falkland")[0]))
    held = {"follows_its_reach": True, "note_on_the_whole": False}
    assert {**_follows(db, *col), **_follows(db, *sal)} == {r: held for r in col[2] + sal[2]}
    whole = set(_sections(db, "gnis:37414"))
    assert _follows(db, *col, bound={"columbia_river.r13": whole})["columbia_river.r13"] == \
        {"follows_its_reach": False, "note_on_the_whole": False}


# =========================================================================================== Li1
def test_li1_licensing_never_colours_a_water(db, request):
    """Sections under one rule key read one status every day of the year, whatever licensing binds them:
    a Class II water is coloured by its rules. Proved on every key the index colours that has sections
    both with and without a Classified Waters designation. MUTATION: a status index that closes designated
    sections — the pairs disagree."""
    from pipeline.deliver.status_index import Index
    idx_path = need(request, "bundle", Path(BUNDLE).with_name("status_index.bin"), BUNDLE_HINT)
    idx = Index(idx_path.read_bytes())
    designated = {s for (s,) in db.execute("SELECT DISTINCT sid FROM designation_section")}
    by_key: dict = {}
    for sid, key in db.execute("SELECT sid, key_ix FROM section_ruleset WHERE sid IN "
                               "(SELECT sid FROM section_licensing)"):
        by_key.setdefault(key, [set(), set()])[sid in designated].add(sid)
    pairs = [(min(a), min(b)) for a, b in by_key.values() if a and b]
    assert len(pairs) > 20

    def disagree(code) -> list:
        return [(a, b) for a, b in pairs if any(code(a, (d.month, d.day)) != code(b, (d.month, d.day))
                                                for d in DAYS[::7])]

    assert disagree(idx.code) == []
    assert disagree(lambda s, on: 2 if s in designated else idx.code(s, on)) != []


# ========================================================================================== Li10
WEST_ROAD, WEST_ROAD_ROW = "gnis:26104", "r5:west_road_blackwater_river@5-12+5-13"


def _li10(con) -> dict:
    out = {}
    for day in ((7, 1), (12, 1)):
        out[day] = sorted({tuple((d["entry_id"], d["id"], d.get("classified")) for d in
                                 R.designations_in_force(con, s, day))
                           for s in _sections(con, WEST_ROAD)})
    (sym,), = con.execute("SELECT symbols FROM entry WHERE entry_id = ?", (WEST_ROAD_ROW,)).fetchall()
    return {"symbols": json.loads(sym), **out}


def test_li10_the_west_road_keeps_its_class_ii_designation(db):
    """The row prints "Mainstem only: Class II water June 15-Oct 31" with no (CW) glyph: every mainstem
    section is Class II water on Jul 1, none on Dec 1. MUTATION: the designation dropped (as a reading
    from the glyph would) — the mainstem is not classified."""
    want = {"symbols": [], (7, 1): [((WEST_ROAD_ROW, "west_road_river", "II"),)], (12, 1): [()]}
    assert _li10(db) == want
    mem = sqlite3.connect(":memory:")
    db.backup(mem)
    mem.execute("DELETE FROM licensing_set WHERE kind = 'designation' AND entry_id = ?", (WEST_ROAD_ROW,))
    assert _li10(mem)[(7, 1)] == [()]
    mem.close()


# =========================================================================================== E13
GOAT, GOAT_ROW = "gnis:11220", ("r4:goat_river@4-6", "goat_river.r1")
FRASER_NIGHT = ("r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4", "fraser_river_upstream_of_mission.r3")


def test_e13_goat_river_catch_and_release_on_the_whole_mainstem_only(db):
    """"Trout/char catch and release (mainstem only)": every Goat River section, no tributary.
    MUTATION: the clause read as a stretch (one section left out) — not the whole mainstem."""
    def goat(reach, trib):
        return {"whole_mainstem": reach == set(_sections(db, GOAT)), "tributaries": sorted(trib)}
    reach, trib = _bound_to(db, *GOAT_ROW, "reach"), _bound_to(db, *GOAT_ROW, "trib")
    assert goat(reach, trib) == {"whole_mainstem": True, "tributaries": []}
    assert goat(reach - {min(reach)}, trib)["whole_mainstem"] is False


def _night(db, bound=None) -> dict:
    sids = bound if bound is not None else _bound_to(db, *FRASER_NIGHT)
    names = {}
    for (name, kind, sid) in db.execute("SELECT i.name, i.kind, s.sid FROM item i JOIN item_section s "
                                        "ON s.ord = i.ord"):
        if sid in sids:
            names.setdefault(name, kind)
    # the named sloughs whose flow joins a section the night closure binds
    down = dict(db.execute("SELECT sid, down_sid FROM section_down"))
    joining = sorted({n for (n, sid) in db.execute(
        "SELECT i.name, s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.name LIKE '% Slough'")
        if down.get(sid) in sids})
    return {"bound": sorted(names), "sloughs_joining": joining,
            "sloughs_bound": sorted(n for n in names if n.endswith(" Slough"))}


def test_e13_the_fraser_night_closure_covers_its_side_channels_not_its_sloughs(db):
    """Q12: the Fraser's own side channels carry the night closure; a separately named slough that joins
    the river does not. MUTATION: a joining slough bound — named."""
    got = _night(db)
    assert got["bound"] == ["Fraser River", "Herrling Island Side Channel", "Jeperson Side Channel",
                            "Seabird Island North Side Channel"]
    assert got["sloughs_bound"] == [] and got["sloughs_joining"] != []
    slough = next(sid for (sid,) in db.execute(
        "SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.name = ?",
        (got["sloughs_joining"][0],)))
    assert _night(db, _bound_to(db, *FRASER_NIGHT) | {slough})["sloughs_bound"] == [got["sloughs_joining"][0]]


# =========================================================================================== E14
HAYS, HAYS_ROW, HAYS_SPLIT = "gnis:4152", ("r6:hays_creek_in_prince_rupert@6-14", "hays_creek.r1"), \
    "hays_creek__lower_culvert_near_cannery"


def _e14(db, reach=None) -> dict:
    (at,), = db.execute("SELECT at FROM split WHERE split_id = ?", (HAYS_SPLIT,)).fetchall()
    cut, = [round(m * 1000) for i, m in json.loads(at) if i == HAYS]
    reach = _bound_to(db, *HAYS_ROW, "reach") if reach is None else reach
    return {"cut_m_from_mouth": cut,
            "bound": sorted(_span(db, s) for s in _sections(db, HAYS) if s in reach),
            "free": sorted(_span(db, s) for s in _sections(db, HAYS) if s not in reach)}


def test_e14_hays_creek_culvert_is_essentially_the_mouth_and_the_split_exists(db):
    """The lower culvert split sits 130 m from the mouth; the closure binds the creek above it and not the
    piece below. MUTATION: the closure on the whole creek — the piece below is bound too."""
    got = _e14(db)
    assert got["cut_m_from_mouth"] == 130 and got["free"] == [(0, got["bound"][0][0])]
    assert got["bound"][0][0] == pytest.approx(130, abs=10)
    assert _e14(db, set(_sections(db, HAYS)))["free"] == []


# ============================================================================================ C3
TROUT_NOTE = ("Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) unless the line "
              "excludes them or char have their own line about the same thing.")


def _notes(guide: dict) -> dict:
    return {"species.trout_note": guide["species"].get("trout_note"),
            "gotchas.trout_includes_char.note": guide["guide"]["gotchas"]["trout_includes_char"].get("note")}


def test_c3_the_shipped_guide_says_trout_includes_char(request):
    """Both places the guide gives the app the note carry the whole sentence. MUTATION: a note that no
    longer says char are included — not the sentence."""
    from pipeline.tools.export_ui_rules import GUIDE_NAME
    path = need(request, "bundle", EXPORT_DIR / GUIDE_NAME, EXPORT_HINT)
    guide = json.loads(path.read_text(encoding="utf-8"))
    assert _notes(guide) == {"species.trout_note": TROUT_NOTE, "gotchas.trout_includes_char.note": TROUT_NOTE}
    guide["species"]["trout_note"] = "Trout: rainbow, steelhead, cutthroat, brown."
    assert _notes(guide)["species.trout_note"] != TROUT_NOTE
