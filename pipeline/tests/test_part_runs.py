"""Where each part of a water runs (`waters[].parts[].runs`), the cuts by name (`splits`), and the
classified period every designation says outright (`licensing[*].period`).

THE RUNS. The pipeline cut every river at the points its regulations name; the bundle writes each
section's place along its water and its two ends (`section_span`, from the atlas graph's own section
bounds) and the export composes a part's sections into runs between those ends. The synthetic tests
check the composition and the end vocabulary on hand-made input; the corpus tests read the side
bundle `UI_EXPORT_BUNDLE` (default the shipped one) and recompute from its rows.

THE PERIOD. 35 of the 74 designations print no dates ("Class II water when open", "Class I water
all year"); each must say so in `period`, read off its own verbatim, and a designation whose period
cannot be read is refused.
"""
from __future__ import annotations

import copy
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from pipeline.deliver.bundle import spans as SP
from pipeline.deliver.bundle.build import SCHEMA
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


# ---------------------------------------------------------------------------------------------
# The end vocabulary (spans.end_token)
# ---------------------------------------------------------------------------------------------
def _b(bid, kind="split", m=0.0, aliases=()):
    return NS(boundary_id=bid, kind=kind, route_measure=m, label="", aliases=tuple(aliases))


LAKES = {"329083341": ("wbk:329083341", "lake")}.get      # wbk -> (the water, its kind)


@pytest.mark.parametrize("bound,side,want", [
    (None, "up", "source"),
    (None, "down", "mouth"),
    (_b("lake:329083341", "lake"), "up", "lake_outlet:wbk:329083341"),
    (_b("lake:329083341", "lake"), "down", "lake_inlet:wbk:329083341"),
    (_b("lake:1", "lake"), "down", "lake_inlet"),                       # not a named water
    (_b("split:border:356566436:1", "border"), "down", "bc_border"),
    (_b("split:area:6", "area"), "up", "region_line:6"),
    (_b("split:area:7B", "area"), "up", "region_line:7B"),
    (_b("split:area:GLADYS LAKE ECOLOGICAL RESERVE", "area"), "up",
     "area:GLADYS LAKE ECOLOGICAL RESERVE"),
    (_b("split:thompson_river__cnr_bridge"), "up", "thompson_river__cnr_bridge"),
    # an auto cut gives way to the authored split standing at the same place
    (_b("split:gauge__08MH016", aliases=("split:chilliwack_river__x", "split:gauge__1")), "up",
     "chilliwack_river__x"),
    (_b("split:gauge__08MH016"), "up", "gauge__08MH016"),
])
def test_each_bound_has_one_end_token(bound, side, want):
    assert SP.end_token(bound, side, lake_item=LAKES) == want


def test_a_length_cut_at_a_tributary_names_the_tributary_and_otherwise_keeps_its_id():
    cut = _b("split:length:356364690:23466", "confluence", 23466.0)
    assert SP.end_token(cut, "up", lake_item=LAKES, tributary=lambda b: "gnis:1") == \
        "confluence:gnis:1"
    assert SP.end_token(cut, "up", lake_item=LAKES, tributary=lambda b: None) == \
        "length:356364690:23466"


def test_the_main_stem_is_the_longest_blue_line_and_ties_are_stable():
    assert SP.main_stem([("2", 10.0), ("1", 4.0), ("1", 4.0), ("3", 1.0)]) == "2"
    assert SP.main_stem([("9", 5.0), ("4", 5.0)]) == "4"
    assert SP.main_stem([]) is None


# ---------------------------------------------------------------------------------------------
# Composing runs (spans.compose_runs)
# ---------------------------------------------------------------------------------------------
def _span(lo_m, hi_m, lo="x", hi="y", off=0):
    return (lo_m, hi_m, lo, hi, off)


def _strip(runs):
    return [{k: v for k, v in r.items() if k != "sids"} for r in runs]


def test_pieces_joined_end_to_end_are_one_run_from_the_top_end_to_the_bottom_end():
    span = {1: _span(0, 1000, "mouth", "s:a"), 2: _span(1000, 2500, "s:a", "s:b")}
    assert _strip(SP.compose_runs(span, {}, [1, 2])) == [
        {"from": "s:b", "to": "mouth", "km_from": 2.5, "km_to": 0.0}]


def test_a_gap_makes_two_runs_ordered_upstream_to_downstream():
    # 1 and 3 are this part; the piece between them (2, another part) or a lake is not
    span = {1: _span(0, 1000, "mouth", "a"), 3: _span(3000, 4000, "lake_outlet:wbk:1", "source")}
    runs = SP.compose_runs(span, {}, [1, 3])
    assert _strip(runs) == [
        {"from": "source", "to": "lake_outlet:wbk:1", "km_from": 4.0, "km_to": 3.0},
        {"from": "a", "to": "mouth", "km_from": 1.0, "km_to": 0.0}]
    assert [r["sids"] for r in runs] == [[3], [1]]


def test_a_side_channel_joins_the_run_it_borders():
    span = {1: _span(0, 1000), 2: _span(1000, 2000), 9: _span(500, 500, "mouth", "source", 1)}
    runs = SP.compose_runs(span, {9: {1}, 1: {9}}, [1, 2, 9])
    assert len(runs) == 1 and runs[0]["sids"] == [1, 2, 9]
    assert "branch" not in runs[0]


def test_a_side_channel_bordering_no_stem_run_is_a_branch_run_at_its_rejoin_point():
    span = {1: _span(0, 1000), 9: _span(5000, 5000, "mouth", "source", 1),
            8: _span(None, None, "mouth", "source", 1)}
    runs = SP.compose_runs(span, {}, [1, 8, 9])
    assert _strip(runs) == [
        {"from": "source", "to": "mouth", "km_from": 5.0, "km_to": 5.0, "branch": True},
        {"from": "y", "to": "x", "km_from": 1.0, "km_to": 0.0},
        {"from": "source", "to": "mouth", "km_from": None, "km_to": None, "branch": True}]


def test_mutation_the_runs_come_from_the_measures_not_the_order_given():
    """Mutation pin: the same sections, given in reverse, compose the same runs; and moving one
    piece's measure so it no longer meets its neighbour splits the run."""
    span = {1: _span(0, 1000, "mouth", "a"), 2: _span(1000, 2000, "a", "source")}
    assert SP.compose_runs(span, {}, [2, 1]) == SP.compose_runs(span, {}, [1, 2])
    moved = {**span, 2: _span(1100, 2000, "a", "source")}
    assert len(SP.compose_runs(span, {}, [1, 2])) == 1
    assert len(SP.compose_runs(moved, {}, [1, 2])) == 2


# ---------------------------------------------------------------------------------------------
# The export, from a hand-built bundle
# ---------------------------------------------------------------------------------------------
def _bundle(tmp: Path, *, spans: bool = True) -> Path:
    """One river `gnis:1` in three pieces (sets 7 | 8 | 7) and one lake `wbk:5`."""
    path = tmp / "bundle.sqlite"
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA.read_text())
    db.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)",
                   [(1, "gnis:1", "One River", "stream"), (2, "wbk:5", "Five Lake", "lake")])
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)",
                   [(1, 1), (1, 2), (1, 3), (2, 4)])
    db.executemany("INSERT INTO section_ruleset (sid, set_id) VALUES (?,?)",
                   [(1, 7), (2, 8), (3, 7), (4, 9)])
    db.executemany("INSERT INTO section_touch (a, b) VALUES (?,?)", [(1, 2), (2, 3)])
    if spans:
        db.executemany("INSERT INTO span_end (eid, token) VALUES (?,?)",
                       [(1, "mouth"), (2, "one_river__bridge"), (3, "one_river__falls"),
                        (4, "lake_inlet:wbk:5")])
        db.executemany("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi, off_stem) "
                       "VALUES (?,?,?,?,?,?)",
                       [(1, 0, 1200, 1, 2, 0), (2, 1200, 3400, 2, 3, 0), (3, 3400, 5000, 3, 4, 0)])
        db.executemany("INSERT INTO split (split_id, name, kind, at) VALUES (?,?,?,?)",
                       [("one_river__bridge", "bridge", "point", '[["gnis:1", 1.2]]'),
                        ("one_river__falls", "falls", "point", '[["gnis:1", 3.4]]')])
    db.commit()
    db.close()
    return path


def test_the_export_ships_runs_and_never_a_section(tmp_path):
    d = X.read(_bundle(tmp_path))
    parts = {p["ruleset"]: p["runs"] for p in d["waters"]["gnis:1"]["parts"]}
    assert parts == {
        "7": [{"from": "lake_inlet:wbk:5", "to": "one_river__falls", "km_from": 5.0, "km_to": 3.4},
              {"from": "one_river__bridge", "to": "mouth", "km_from": 1.2, "km_to": 0.0}],
        "8": [{"from": "one_river__falls", "to": "one_river__bridge", "km_from": 3.4, "km_to": 1.2}],
    }
    assert d["waters"]["wbk:5"]["parts"][0]["runs"] == [
        {"from": None, "to": None, "km_from": None, "km_to": None, "polygon": "whole"}]
    assert d["splits"]["one_river__bridge"] == {"name": "bridge", "kind": "point",
                                                "water_id": "gnis:1", "km": 1.2}
    assert d["splits"]["region_line:3"]["name"] == "Region 3 boundary"


def test_a_stream_section_with_no_span_is_refused(tmp_path):
    with pytest.raises(SystemExit, match="section_span"):
        X.read(_bundle(tmp_path, spans=False))


def test_a_bundle_without_the_span_table_is_refused(tmp_path):
    path = _bundle(tmp_path)
    db = sqlite3.connect(path)
    db.execute("DROP TABLE section_span")
    db.commit()
    db.close()
    with pytest.raises(SystemExit, match="section_span"):
        X.read(path)


# ---------------------------------------------------------------------------------------------
# The corpus
# ---------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def db():
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


@pytest.fixture(scope="module")
def doc():
    return X.build(BUNDLE)


def _runs(doc, item):
    return [(p["ruleset"], p["runs"]) for p in doc["waters"][item]["parts"]]


def test_every_stream_section_has_a_span_and_nothing_else_does(db):
    n = db.execute("SELECT COUNT(*) FROM item_section s JOIN item i ON i.ord = s.ord "
                   "WHERE i.kind = 'stream'").fetchone()[0]
    assert n > 50_000
    assert db.execute("SELECT COUNT(*) FROM section_span").fetchone()[0] == n
    assert db.execute(
        "SELECT COUNT(*) FROM section_span p WHERE NOT EXISTS (SELECT 1 FROM item_section s "
        "JOIN item i ON i.ord = s.ord WHERE s.sid = p.sid AND i.kind = 'stream')").fetchone()[0] == 0


def test_runs_cover_exactly_the_part_s_sections(doc, db):
    """Recomputed from the bundle rows: each part's sections (the export's own grouping key, in
    SQL) are partitioned by its runs — every section in exactly one run — and the shipped runs are
    those runs without their sections."""
    span = {s: (a, b, lo, hi, off, shape) for s, a, b, lo, hi, off, shape in db.execute(
        "SELECT s.sid, s.lo_m, s.hi_m, a.token, b.token, s.off_stem, s.shape FROM section_span s "
        "JOIN span_end a ON a.eid = s.lo JOIN span_end b ON b.eid = s.hi")}
    touch = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        touch[a].add(b)
        touch[b].add(a)
    groups = defaultdict(list)
    for item, kind, rs, ls, pe, sw, st, s in db.execute(
            "SELECT i.item_id, i.kind, r.set_id, l.set_id, (SELECT group_concat(k, ',') FROM "
            "(SELECT p.area_kind AS k FROM province_except p WHERE p.sid = s.sid ORDER BY "
            "p.area_kind)), EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = s.sid), "
            "CASE WHEN EXISTS (SELECT 1 FROM steelhead_known k WHERE k.sid = s.sid) THEN 'known' "
            "ELSE (SELECT CASE h.code WHEN 1 THEN 'known' WHEN 2 THEN 'possible' END "
            "FROM steelhead_set h WHERE h.set_id = r.set_id) END, s.sid "
            "FROM item i JOIN item_section s ON s.ord = i.ord "
            "LEFT JOIN section_ruleset r ON r.sid = s.sid "
            "LEFT JOIN section_licensing l ON l.sid = s.sid"):
        if kind == "stream":
            groups[(item, None if rs is None else str(rs), None if ls is None else str(ls),
                    pe, bool(sw), st)].append(s)
    checked = 0
    for (item, rs, ls, pe, sw, st), sids in groups.items():
        part = [p for p in doc["waters"][item]["parts"]
                if (p["ruleset"], p["licensing_set"], ",".join(p.get("province_except") or [])
                    or None, bool(p.get("anadromous_rainbow")), p.get("steelhead"))
                == (rs, ls, pe, sw, st)]
        assert len(part) == 1, item
        assert part[0]["sections"] == len(sids)
        if X.is_polygon_part(doc["waters"][item], sorted(sids), span):
            # a stream's polygons its stem does not pass through: one polygon run
            assert part[0]["runs"] == [{"from": None, "to": None, "km_from": None,
                                        "km_to": None, "polygon": "whole"}], item
            checked += 1
            continue
        runs = SP.compose_runs(span, touch, sorted(sids))
        covered = [s for r in runs for s in r["sids"]]
        assert sorted(covered) == sorted(sids), item          # every section, none twice
        assert [{k: v for k, v in r.items() if k != "sids"} for r in runs] == part[0]["runs"]
        checked += 1
    assert checked > 10_000


def test_every_end_is_a_cut_or_an_end_the_file_names(doc):
    # runs follow the SHAPE of the sections: a stream's stretches have ends, its polygon-only
    # parts (a slough drawn only as polygons) are one polygon run with none
    ends = [r[k] for w in doc["waters"].values() if w["kind"] == "stream"
            for p in w["parts"] for r in p["runs"] if "polygon" not in r for k in ("from", "to")]
    assert ends and all(X.valid_end(t, doc) for t in ends), \
        sorted({t for t in ends if not X.valid_end(t, doc)})[:10]
    named = [t for t in ends if t in doc["splits"]]
    assert len(named) > 1_000


def test_runs_go_upstream_to_downstream_and_km_never_climbs(doc):
    assert X.run_problems(doc) == []
    for w in doc["waters"].values():
        for p in w["parts"]:
            km = [r["km_from"] for r in p["runs"] if r["km_from"] is not None]
            assert km == sorted(km, reverse=True)
            for r in p["runs"]:
                if r["km_from"] is not None:
                    assert r["km_from"] >= r["km_to"]


def test_a_part_of_several_stretches_has_several_runs(doc):
    several = [p for w in doc["waters"].values() for p in w["parts"] if len(p["runs"]) > 1]
    assert len(several) > 1_000
    # Bull River: the rest of the river is three stretches, the release reaches two
    bull = dict(_runs(doc, "gnis:10583"))
    assert sorted(len(r) for r in bull.values()) == [2, 3]


def test_a_lake_part_is_its_polygon(doc):
    lakes = [(w, p) for w in doc["waters"].values() if w["kind"] != "stream" for p in w["parts"]]
    assert lakes
    for w, p in lakes:
        assert p["runs"] == [{"from": None, "to": None, "km_from": None, "km_to": None,
                              "polygon": w["name"] if w.get("part_of") else "whole"}]


def test_no_run_carries_a_section(doc):
    keys = {k for w in doc["waters"].values() for p in w["parts"] for r in p["runs"] for k in r}
    assert keys <= {"from", "to", "km_from", "km_to", "branch", "polygon"}


def test_the_splits_table_names_every_cut_once(doc):
    S = doc["splits"]
    assert len(S) > 3_000
    for sid, x in S.items():
        assert x["name"] and x["kind"], sid
        assert (x["water_id"] is None) == (x["km"] is None), sid
        if x["water_id"]:
            assert x["water_id"] in doc["waters"], sid
    assert S["thompson_river__boundary_signs"]["name"] == "boundary signs 1 km downstream of Martel"   # §6 relabel (RULES round)
    assert S["thompson_river__thompson_river_into_fraser_river"]["name"] == \
        "Thompson River confluence"


# ---- the four waters the UI builder named ----------------------------------------------------
def test_chilliwack_closed_above_the_slesse_signs(doc):
    slesse = "chilliwack_vedder_rivers__boundary_signs_below_slesse_creek"
    assert doc["splits"][slesse]["water_id"] == "gnis:8634"
    above = [(rs, runs) for rs, runs in _runs(doc, "gnis:8634") if any(
        r["to"] == slesse for r in runs)]
    assert len(above) == 1
    rs, runs = above[0]
    # FIX round (2026-10-06, C10): the Chilliwack River Ecological Reserve sits on the upper river
    # (blk 380887781: enters 679 m above Chilliwack Lake at 58,646.6 m, inside to the border), so its
    # clean cut divides the stretch above the signs in two — this part starts at the reserve edge,
    # the reserve part above it carries the same closure AND the reserve's.
    reserve = "area:CHILLIWACK RIVER ECOLOGICAL RESERVE"
    assert runs[-1]["from"].startswith("lake_outlet:") and runs[0]["from"] == reserve
    assert runs[-1]["km_to"] == doc["splits"][slesse]["km"]

    def closed(rs_, words):
        return [i for v, ids in doc["rulesets"][rs_].items() if v != "sections" for i in ids
                if words in doc["rules"][i]["label"]]
    signs = "upstream of fishing boundary signs"
    assert closed(rs, signs), "the part above the Slesse signs carries the closure"
    inside = [r for r, rr in _runs(doc, "gnis:8634") if any(x["to"] == reserve for x in rr)]
    assert len(inside) == 1 and closed(inside[0], signs), "so does the reserve part"
    assert closed(inside[0], "ecological reserve"), "with the reserve's own closure"
    below = [runs for _, runs in _runs(doc, "gnis:8634") if any(r["from"] == slesse
                                                                 for r in runs)]
    assert below and below[0][0]["to"] == "chilliwack_vedder_rivers__tamihi_rapids_bridge"


def test_bull_river_rest_is_the_river_outside_its_release_reaches(doc):
    parts = dict(_runs(doc, "gnis:10583"))
    ends = {rs: [(r["from"], r["to"]) for r in runs] for rs, runs in parts.items()}
    release = [e for e in ends.values() if ("bull_river__galbraith_creek_confl",
                                            "bull_river__van_creek_confl") in e]
    assert release == [[("bull_river__galbraith_creek_confl", "bull_river__van_creek_confl"),
                        ("bull_river__aberfeldie_dam", "bull_river__tie_mill_dam")]]
    rest = [e for e in ends.values() if e is not release[0]]
    assert rest == [[("source", "bull_river__galbraith_creek_confl"),
                     ("bull_river__van_creek_confl", "lake_inlet"),
                     ("bull_river__tie_mill_dam", "mouth")]]


def test_thompson_between_the_cnr_bridges(doc):
    a, b = "thompson_river__cnr_bridge", "thompson_river__cnr_bridge_2"
    between = [runs for _, runs in _runs(doc, "gnis:39492")
               if [(r["from"], r["to"]) for r in runs] == [(a, b)]]
    assert len(between) == 1
    assert between[0][0]["km_from"] == doc["splits"][a]["km"] > doc["splits"][b]["km"] == \
        between[0][0]["km_to"]
    around = [runs for _, runs in _runs(doc, "gnis:39492") if any(r["to"] == a for r in runs)]
    assert [(r["from"], r["to"]) for r in around[0]] == [
        ("lake_outlet:wbk:329563838", a), (b, "thompson_river__boundary_signs")]


def test_elk_release_reaches_alternate_down_the_river(doc):
    parts = dict(_runs(doc, "gnis:16880"))
    many = sorted((runs for runs in parts.values() if len(runs) > 1), key=len)
    assert [len(r) for r in many] == [3, 4]
    km = sorted(((r["km_from"], r["km_to"]) for runs in many for r in runs), reverse=True)
    # the two parts tile 219.72..22.95 end to end, each run starting where the last ended
    assert all(km[i][1] == km[i + 1][0] for i in range(len(km) - 1))
    assert km[0][0] > 200 and km[-1][1] == doc["splits"]["elk_river__elko_dam"]["km"]


# ---- mutation: the run checks go red --------------------------------------------------------
@pytest.mark.parametrize("name,mutate,expect", [
    ("invented end", lambda p: p["runs"][0].update({"from": "nowhere__at_all"}), "no cut or end"),
    ("lake that is no water", lambda p: p["runs"][0].update({"to": "lake_inlet:wbk:0"}),
     "no cut or end"),
    ("uphill", lambda p: p["runs"][0].update({"km_from": -1.0}), "uphill"),
    ("order", lambda p: p["runs"].reverse(), "not ordered"),
    ("no runs", lambda p: p.update({"runs": []}), "no runs"),
])
def test_the_run_checks_catch_a_broken_run(doc, name, mutate, expect):
    item = "gnis:10583"
    part = next(i for i, p in enumerate(doc["waters"][item]["parts"]) if len(p["runs"]) == 3)
    bad = dict(doc, waters={item: copy.deepcopy(doc["waters"][item])})
    mutate(bad["waters"][item]["parts"][part])
    assert any(expect in p for p in X.run_problems(bad)), name
    assert X.run_problems(dict(doc, waters={item: doc["waters"][item]})) == []


# ---------------------------------------------------------------------------------------------
# The classified period
# ---------------------------------------------------------------------------------------------
WHEN_OPEN = sorted([
    "r4:abruzzi_creek@4-23#abruzzi_creek",
    "r4:alexander_creek_downstream_of_the_easternmost_hwy_3_bridge@4-23#michel_creek",
    "r4:alexander_creek_upstream_of_the_easternmost_hwy_3_bridge@4-23#michel_creek",
    "r4:bull_river@4-22#bull_river",
    "r4:cadorna_creek@4-23#elk_river",
    "r4:elk_river_downstream_of_elko_dam@4-2#elk_river",
    "r4:elk_river_s_tributaries_see_exceptions@4-2+4-23#elk_river",
    "r4:elk_river_upstream_of_elko_dam@4-2+4-23#elk_river",
    "r4:fording_river_downstream_of_josephine_falls@4-23#elk_river",
    "r4:forsyth_creek@4-23#forsyth_creek",
    "r4:hellroaring_creek@4-20#st_mary_river",
    "r4:kootenay_river_upstream_of_koocanusa_reservoir@4-2+4-21+4-22+4-24+4-25+4-35#kootenay_river",
    "r4:lodgepole_creek_downstream_of_falls_near_the_km_26_post_on_l@4-2#wigwam_river",
    "r4:lodgepole_creek_upstream_of_falls@4-2#wigwam_river",
    "r4:michel_creek_downstream_of_the_easternmost_hwy_3_bridge@4-23#michel_creek",
    "r4:michel_creek_upstream_of_the_easternmost_hwy_3_bridge@4-23#michel_creek",
    "r4:morrissey_creek@4-2#elk_river",
    "r4:north_fork_white_river@4-24#north_white_river",
    "r4:perry_creek@4-20#st_mary_river",
    "r4:quinn_creek@4-22#bull_river",
    "r4:skookumchuck_creek@4-20#skookumchuck_creek",
    "r4:st_mary_river@4-20#st_mary_river",
    "r4:white_river_see_also_east_white_north_white_rivers@4-24#white_river",
    "r4:wigwam_river_downstream_of_the_access_road_adjacent_to_km_42@4-2#wigwam_river",
    "r4:wigwam_river_upstream_of_the_forest_service_recreation_site@4-2#wigwam_river",
    "r6:stellako_river@6-4+7-12#stellako_river",
    "r7:stellako_river@7-12#stellako_river",
])
ALL_YEAR = {
    "r6:ecstall_river@6-11#ecstall_river": "II",
    "r6:gitnadoix_river@6-10#gitnadoix_river": "I",
    "r6:kitseguecla_river@6-9#kitseguecla_river": "II",
    "r6:kitsumkalum_kalum_river@6-15#kitsumkalum_river": "II",
    "r6:kitwanga_river@6-30#kitwanga_river": "II",
    "r6:kluatantan_river@6-18#kluatantan_river": "II",
    "r6:lakelse_river@6-10#lakelse_river": "I",
    "r6:suskwa_bear_river@6-8#suskwa_river": "I",
}


def _designations(doc):
    return {i: x for i, x in doc["licensing"].items() if x["kind"] == "designation"}


def test_every_designation_says_its_period(doc):
    D = _designations(doc)
    assert len(D) == 74
    assert X.period_problems(doc) == []
    kinds = defaultdict(list)
    for i, x in D.items():
        kinds[x["period"]["kind"]].append(i)
    assert sorted(kinds["when_open"]) == WHEN_OPEN
    assert sorted(kinds["all_year"]) == sorted(ALL_YEAR)
    assert len(kinds["dates"]) == 74 - len(WHEN_OPEN) - len(ALL_YEAR)


@pytest.mark.parametrize("rid", WHEN_OPEN)
def test_when_open_reads_whenever_the_water_is_open(doc, rid):
    x = doc["licensing"][rid]
    assert "when" not in x["fields"]
    assert x["period"] == {"kind": "when_open",
                           "says": "Classified (Class II) whenever this water is open"}


@pytest.mark.parametrize("rid", sorted(ALL_YEAR))
def test_all_year_reads_all_year(doc, rid):
    x = doc["licensing"][rid]
    assert "when" not in x["fields"]
    assert x["period"] == {"kind": "all_year", "says": f"Class {ALL_YEAR[rid]} all year"}


def test_a_dated_period_is_its_own_when(doc):
    for i, x in _designations(doc).items():
        if x["period"]["kind"] == "dates":
            assert x["period"]["dates"] == x["fields"]["when"]["dates"], i
            assert x["period"]["says"] == f"Classified (Class {x['fields']['classified']}) " \
                                          f"{x['parts']['when']}", i
    assert doc["licensing"]["r1:copper_creek@6-12#copper_creek"]["period"]["says"] == \
        "Classified (Class II) Sep 1-Apr 30"


# ---- mutation: a period that cannot be read is refused --------------------------------------
DATED = {"classified": "II", "when": {"dates": [{"from_month": 9, "from_day": 1, "to_month": 4,
                                                  "to_day": 30}]}}


@pytest.mark.parametrize("fields,verbatim,match", [
    ({"classified": "II"}, "Class II water", "neither"),                       # says nothing
    ({"classified": "II"}, "Class II water Sept 1-Apr 30", "prints dates"),    # lost its `when`
    ({"classified": "II"}, "Class II water when open, all year", "both"),
    (DATED, "Class II water when open", "says when open"),
    (DATED, "Class II water all year", "says all year"),
    (DATED, "Class II water", "prints none"),
    ({"classified": "II", "when": {"weekdays": ["sat"]}}, "Class II water Sept 1", "plain dates"),
])
def test_a_period_that_cannot_be_read_is_refused(fields, verbatim, match):
    with pytest.raises(SystemExit, match=match):
        X.designation_period("r9:x#x", fields, verbatim, {"when": "Sep 1-Apr 30"})


def test_the_period_check_catches_a_dropped_or_flipped_period(doc):
    rid = "r4:bull_river@4-22#bull_river"
    for mutate in (lambda x: x.pop("period"),
                   lambda x: x.update(period={"kind": "dates", "dates": [], "says": "x"}),
                   lambda x: x.update(period={"kind": "sometimes", "says": "x"})):
        bad = copy.deepcopy(doc["licensing"][rid])
        mutate(bad)
        assert X.period_problems({"licensing": {rid: bad}}), mutate
    dated = "r1:copper_creek@6-12#copper_creek"
    bad = copy.deepcopy(doc["licensing"][dated])
    bad["period"] = {"kind": "when_open", "says": "x"}
    assert X.period_problems({"licensing": {dated: bad}})
    assert X.period_problems({"licensing": {rid: doc["licensing"][rid],
                                            dated: doc["licensing"][dated]}}) == []
