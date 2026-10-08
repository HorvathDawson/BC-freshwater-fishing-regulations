"""ONE OBLIGATION, PRINTED TWICE, IS ONE KEY (RU-12, 2026-10-04), and A DATED WATER RELEASE NEVER
SPEAKS UNDER A BLANKET CLOSURE (the export's `release_under_closure` check).

Against the shipped bundle (`UI_EXPORT_BUNDLE`, else the live one): the reader rulings are
reader-side, so any bundle carrying the Dean's and Shuswap's records answers them."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)
DEAN_I = "r5:dean_river@5-9"
SHUSWAP = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"


@pytest.fixture(scope="module")
def db():
    if not BUNDLE.exists():
        pytest.skip("no bundle")
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sid(db, eid, rid=None):
    q = ("select min(ds.sid) from designation_section ds where ds.entry_id = ?"
         if rid is None else
         "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
         "where r.entry_id = ? and r.rule_id = ?")
    sid = db.execute(q, (eid,) if rid is None else (eid, rid)).fetchone()[0]
    assert sid is not None, (eid, rid)
    return sid


def test_the_dean_holds_one_classified_waters_licence_key_and_one_stamp_key(db):
    """Three records said "Classified Waters Licence" on the Dean (the province's, Region 4's
    wording, the Dean row's restatement) and two said "classified steelhead stamp" (the
    province's, Region 3's). One key each: the province's, with the Dean's restatement under
    `also_printed`; Region 3's and Region 4's wording hold on their own regions only."""
    sids = [s for (s,) in db.execute(
        "select distinct sid from designation_section where entry_id = ? order by sid", (DEAN_I,))]
    assert sids
    stamped = folded = 0
    for sid in sids:
        got = R.requirements_in_force(db, sid, (7, 15))
        cwl = [k for k in got["holds"] if k.endswith("#classified_waters_licence")]
        stamp = [k for k in got["holds"] if k.endswith("#classified_steelhead_stamp")]
        assert cwl == ["zp:classified_waters_licence#classified_waters_licence"]
        assert stamp in ([], ["zp:classified_waters_licence#classified_steelhead_stamp"])
        stamped += bool(stamp)
        also = got["also_printed"].get("zp:classified_waters_licence#classified_waters_licence", [])
        assert also in ([], ["z5:dean_river_classified#classified_waters_licence"])
        folded += bool(also)
        assert "z5:dean_river_classified#classified_waters_licence" not in got["holds"]
        assert not any(k.startswith(("z3:", "z4:")) for k in got["holds"])
    assert stamped, "the Class I reach carries the stamp Jun 1-Sep 30"
    assert folded, "the Dean row's restatement is folded where it holds"


def test_a_rows_stamp_restating_the_provinces_is_folded_into_it(db):
    sid = _sid(db, SHUSWAP, "shuswap_lake.r1") if db.execute(
        "select 1 from rule where entry_id = ? and rule_id = 'shuswap_lake.r1'",
        (SHUSWAP,)).fetchone() else db.execute(
        "select min(sr.sid) from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
        "where r.entry_id = ?", (SHUSWAP,)).fetchone()[0]
    got = R.requirements_in_force(db, sid, (7, 1))
    assert "zp:shuswap_char_stamp#shuswap_char_stamp" in got["holds"]
    assert f"{SHUSWAP}#shuswap_char_stamp" not in got["holds"]
    assert got["also_printed"]["zp:shuswap_char_stamp#shuswap_char_stamp"] == [
        f"{SHUSWAP}#shuswap_char_stamp"]


def test_a_zone_tables_restatement_holds_on_its_own_region_only(db):
    """Region 4's "Classified Waters Licence … East Kootenay rivers" holds on a Region 4
    classified water, folded into the province's as `also_printed`; never on the Dean."""
    sid = db.execute("select min(sid) from designation_section where entry_id like 'r4:%'"
                     ).fetchone()[0]
    assert sid is not None
    got = R.requirements_in_force(db, sid, (7, 15))
    assert "z4:classified_waters_licence#classified_waters_licence" in \
        got["also_printed"].get("zp:classified_waters_licence#classified_waters_licence", [])


def test_every_dated_water_release_under_a_blanket_closure_is_listed_as_known(db):
    """The export refuses one that is not (`RELEASE_UNDER_CLOSURE_KNOWN`); on a bundle from before
    the Nicola lift (RU-2) the Nicola is the one unlisted finding."""
    got = X.release_under_closure(BUNDLE)
    unlisted = [x["rule"] for x in got if not x.get("known")]
    nicola = db.execute("select 1 from rule where entry_id = 'r3:nicola_river@3-13' and "
                        "rule_id = 'nicola_river.r3x'").fetchone()
    assert unlisted == ([] if nicola else ["r3:nicola_river@3-13::nicola_river.r3"])
    assert [x["rule"] for x in got if x.get("known")] == [
        "r4:duck_lake_permit_required_see_note_on_page_34@4-6::duck_lake.r3"]
    assert X.release_under_closure_problems({"about": {"release_under_closure": got}}) == [
        f"a dated water release speaks under a blanket closure and is not listed as known: {r} "
        for r in []] or len(X.release_under_closure_problems(
            {"about": {"release_under_closure": got}})) == len(unlisted)


def test_every_water_quota_silenced_under_a_blanket_closure_is_listed_as_known(db, monkeypatch):
    """Review F5: since RU-7 a full closure silences a water row's group-named quota on its days,
    so a missed exemption shaped like a quota would vanish from the page. The export lists every
    one (`quota_under_closure`) and refuses any not in `QUOTA_UNDER_CLOSURE_KNOWN`. Every finding
    is known AND every known entry is found (a stale entry is a list that stopped meaning
    anything). MUTATION: drop one known entry and the export refuses it."""
    got = X.quota_under_closure(BUNDLE)
    assert len(got) >= 14, got                  # not vacuous: the review's 14 at least
    assert [x["rule"] for x in got if not x.get("known")] == []
    assert {x["rule"] for x in got} == {f"{e}::{r}" for e, r in X.QUOTA_UNDER_CLOSURE_KNOWN}
    assert X.release_under_closure_problems({"about": {"quota_under_closure": got}}) == []
    k = ("r3:nahatlatch_river@3-15", "nahatlatch_river.r3")
    monkeypatch.delitem(X.QUOTA_UNDER_CLOSURE_KNOWN, k)
    probs = X.release_under_closure_problems(
        {"about": {"quota_under_closure": X.quota_under_closure(BUNDLE)}})
    assert len(probs) == 1 and "nahatlatch_river.r3" in probs[0], probs


def _tiny(tmp_path, rules: list) -> str:
    """One section, one rule set of these rules, and its stored verdicts (the export's scans look
    the reader's answers up there — DATAFLOW P5)."""
    from pipeline.deliver.bundle.derived import rule_ids_digest
    from pipeline.deliver.bundle.rules import closure_grade
    from pipeline.deliver.verdicts.build import build as build_verdicts
    path = str(tmp_path / "tiny.sqlite")
    con = sqlite3.connect(path)
    ids = sorted(f"{x['entry']}::{x['rule']}" for x in rules)
    con.executescript(
        "create table meta (k text primary key, v text);"
        "create table rule (entry_id text, rule_id text, closure_grade text);"
        "create table rule_ix (ix integer primary key, entry_id text, rule_id text);"
        "create table rule_key (key_ix integer primary key, set_id integer, steelhead_water integer,"
        " steelhead_rules integer, kind text, sections integer, rep_sid integer);"
        "create table section_ruleset (sid integer, set_id integer, key_ix integer);"
        "create table ruleset (set_id integer, entry_id text, rule_id text, via text);"
        "insert into section_ruleset values (1, 0, 0);"
        "insert into rule_key values (0, 0, 0, 1, NULL, 1, 1);")
    con.executemany("insert into meta values (?, ?)", [("section_handles", "0" * 16),
                    ("reach_digest", "1" * 16), ("rule_ids_sha256", rule_ids_digest(ids))])
    con.executemany("insert into ruleset values (0, ?, ?, 'reach')",
                    [(x["entry"], x["rule"]) for x in rules])
    con.executemany("insert into rule values (?, ?, ?)",
                    [(x["entry"], x["rule"], closure_grade({"type": "retention_limit", **x}))
                     for x in rules])
    con.executemany("insert into rule_ix values (?, ?, ?)",
                    [(i, *k.split("::", 1)) for i, k in enumerate(ids)])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention"}, **x)
        for x in rules}
    build_verdicts(path, tmp_path / "verdicts.sqlite", workers=1, log=lambda *_: None)
    return path


def _dates(fm, fd, tm, td) -> dict:
    return {"dates": [{"from_month": fm, "from_day": fd, "to_month": tm, "to_day": td}]}


def test_the_check_reads_every_start_day_and_every_fish(tmp_path):
    """Review F6: the check sampled ONE day (the release's first) and ONE fish (the first it
    names). A release of cutthroat AND rainbow, Jan 1-Feb 28, under a zone closure of RAINBOW
    from Feb 1: on Jan 1 nothing is closed and a cutthroat is never closed — the old sample saw
    nothing. Read on the closure's first day, for the rainbow, it is found."""
    path = _tiny(tmp_path, [
        {"entry": "z3:c", "rule": "c.r1", "species": ["RB"], "take": 0, "may_target": 0,
         "when": _dates(2, 1, 6, 30), "_rank": 3},
        {"entry": "r3:w", "rule": "w.r1", "species": ["CT", "RB"], "take": 0, "may_target": 1,
         "when": _dates(1, 1, 2, 28), "_rank": 0}])
    got = X.release_under_closure(Path(path))
    assert [(x["rule"], x["date"], x["fish"]) for x in got] == [("r3:w::w.r1", [2, 1], "RB")]
    assert X.release_under_closure_problems({"about": {"release_under_closure": got}})


def test_a_zone_7a_or_7b_table_is_region_7_to_the_rows():
    """Review F7: rows print Region 7 as `r7:`; its zone tables are `z7a:`/`z7b:`. A 7A/7B
    `on_designation` restatement must compare as Region 7, or it never holds."""
    assert R._book_region(R.base_region("z7a:classified")) == R._entry_region("r7:x@7-1") == "7"
    assert R._book_region(R.base_region("z7b:classified")) == "7"
    assert R._book_region(R.base_region("z4:classified")) == "4"
