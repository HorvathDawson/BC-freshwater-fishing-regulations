"""T1053 (PROPOSAL; owner 2026-11-24: "keep earlier rule of closure always winning unless it is exempt) this does not
include no fishiong all year rulings tho those will yield in that case. only the dated closures."): an INHERITED
all-year "No Fishing" yields to the stream's own row, or to a nearer ✱ row (`ladder.ALL_YEAR_INHERITED_CLOSURE_YIELDS`);
an inherited DATED closure still closes (T4/T14), and a stream with no row keeps the all-year closure.

Shapes of the real cases: Frosst Creek (own "No Fishing Oct 15-Apr 15") under Sweltzer River's all-year ✱ "No Fishing"
climbed through Cultus Lake; the Granby (own row) under Kettle River's tributaries' "No Fishing Jul 25-Sept 15"; Dick
Creek (no row) under Eagle Creek's all-year ✱ "No Fishing"."""
from __future__ import annotations

import sqlite3

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.deliver.bundle.read import ladder

ALL = ["ALL_GAME_FISH"]


def _tiny(tmp_path, rules, via, nearer=()):
    path = str(tmp_path / "tiny.sqlite")
    con = sqlite3.connect(path)
    con.execute("create table section_ruleset (sid integer, set_id integer)")
    con.execute("create table ruleset (set_id integer, entry_id text, rule_id text, via text)")
    con.execute("insert into section_ruleset values (1, 0)")
    con.executemany("insert into ruleset values (0, ?, ?, ?)",
                    [(x["entry"], x["rule"], via.get(x["rule"], "reach")) for x in rules])
    con.commit()
    con.close()
    R._RULES_BY_PATH[path] = {(x["entry"], x["rule"]): dict(
        {"type": "retention_limit", "dimension": "daily", "family": "retention", "_rank": 0}, **x) for x in rules}
    ladder._NEARER_BY_PATH[path] = frozenset(nearer)
    return path


def _speaks(path, day, fish="RB"):
    return {x["rule"] for x in R.effective_rules(1, day, fish, path) if x["state"] == "speaks"}


SWELTZER = {"entry": "r2:sweltzer", "rule": "sweltzer.r1", "species": ALL, "take": 0, "may_target": 0}
FROSST = {"entry": "r2:frosst", "rule": "frosst.r1", "species": ALL, "take": 0, "may_target": 0,
          "when": {"dates": [{"from_month": 10, "from_day": 15, "to_month": 4, "to_day": 15}]}}
ZONE = {"entry": "z2:q", "rule": "q.r1", "species": ["TROUT_CHAR"], "take": 2, "_rank": 3}


def test_frosst_creek_answers_by_its_own_row_not_the_inherited_all_year_closure(tmp_path):
    path = _tiny(tmp_path, [SWELTZER, FROSST, ZONE], via={"sweltzer.r1": "trib"})
    assert _speaks(path, (7, 1)) == {"q.r1"}             # open: its own closure is out of force, the zone's 2
    assert "frosst.r1" in _speaks(path, (1, 10))         # closed by its OWN dated closure


def test_a_dated_inherited_closure_still_closes_a_stream_with_its_own_row(tmp_path):
    kettle = {"entry": "r8:kettle_tribs", "rule": "kettle.r1", "species": ALL, "take": 0, "may_target": 0,
              "when": {"dates": [{"from_month": 7, "from_day": 25, "to_month": 9, "to_day": 15}]}}
    granby = {"entry": "r8:granby", "rule": "granby.r3", "species": ["TROUT_CHAR"], "take": 1}
    path = _tiny(tmp_path, [kettle, granby], via={"kettle.r1": "trib"})
    assert "kettle.r1" in _speaks(path, (8, 1))


def test_a_stream_with_no_row_keeps_the_all_year_closure(tmp_path):
    """Dick Creek: only Eagle Creek's ✱ "No Fishing" and the zone reach it."""
    eagle = {**SWELTZER, "entry": "r2:eagle", "rule": "eagle.r1"}
    path = _tiny(tmp_path, [eagle, ZONE], via={"eagle.r1": "trib"})
    assert "eagle.r1" in _speaks(path, (7, 1))


def test_a_nearer_star_row_also_takes_the_all_year_closure_out(tmp_path):
    """"upstream includes tributaries still overrides lower includes tributaries": a nearer ✱ row's quota."""
    near = {"entry": "r2:near_tribs", "rule": "near.r1", "species": ["TROUT_CHAR"], "take": 2}
    path = _tiny(tmp_path, [SWELTZER, near], via={"sweltzer.r1": "trib", "near.r1": "trib"},
                 nearer={("r2:near_tribs", "r2:sweltzer")})
    assert _speaks(path, (7, 1)) == {"near.r1"}


def test_retention_reading_a_bait_ban_row_does_not_reopen(tmp_path, monkeypatch):
    """`retention` (the built default): the own row must say something about keeping or closing; a row that is only a
    bait ban leaves the all-year closure closing. `any` would reopen it."""
    bait = {"entry": "r2:frosst", "rule": "frosst.r9", "type": "gear", "dimension": "bait", "family": "gear",
            "species": ALL}
    path = _tiny(tmp_path, [SWELTZER, bait], via={"sweltzer.r1": "trib"})
    assert "sweltzer.r1" in _speaks(path, (7, 1))
    monkeypatch.setattr(ladder, "ALL_YEAR_INHERITED_CLOSURE_YIELDS", "any")
    assert "sweltzer.r1" not in _speaks(path, (7, 1))


@pytest.mark.parametrize("mode", [None])
def test_mutation_switch_off_restores_t4(tmp_path, monkeypatch, mode):
    monkeypatch.setattr(ladder, "ALL_YEAR_INHERITED_CLOSURE_YIELDS", mode)
    path = _tiny(tmp_path, [SWELTZER, FROSST, ZONE], via={"sweltzer.r1": "trib"})
    assert "sweltzer.r1" in _speaks(path, (7, 1))
