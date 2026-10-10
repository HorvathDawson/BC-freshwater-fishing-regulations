"""THE 2026-10-03 RULINGS ON THE BUILT BUNDLE (Phase 3): one Classified Waters unit per section
(RU-13), a rowed tributary printing no designation is not classified (LI-2), a joining water the
signs pull into a cut is bounded at its first lake (RU-14), Kennedy Lake is outside Pacific Rim
(curated `areas.json` `outside_items`). Reader-side rulings are in test_competition.py.

Against `UI_EXPORT_BUNDLE` (else the live bundle); skips on a bundle from before the Phase 3
corpus (the Stein lift)."""
from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, predates

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)


@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if not con.execute("select count(*) from rule where entry_id = 'r3:nicola_river@3-13' and "
                       "rule_id = 'nicola_river.r3x'").fetchone()[0]:
        predates(f"{BUNDLE} predates the Phase 3 corpus — point UI_EXPORT_BUNDLE at a side build")
    yield con
    con.close()


def _sids_of(db, name: str) -> list[int]:
    return [s for (s,) in db.execute(
        "select s.sid from item i join item_section s on s.ord = i.ord where i.name = ? "
        "order by s.sid", (name,))]


def _bound_by(db, sid: int, entry_id: str, rule_id: str | None = None) -> bool:
    q = ("select 1 from ruleset r join section_ruleset sr on sr.set_id = r.set_id "
         "where sr.sid = ? and r.entry_id = ?" + (" and r.rule_id = ?" if rule_id else ""))
    return db.execute(q, (sid, entry_id) + ((rule_id,) if rule_id else ())).fetchone() is not None


def _designations_of(db, sid: int) -> set[tuple[str, str]]:
    return {(e, d) for e, d in db.execute(
        "select entry_id, designation_id from designation_section where sid = ?", (sid,))}


@pytest.fixture(scope="module")
def units(db) -> dict[tuple[str, str], str]:
    """`(entry_id, designation id) -> unit`, from the bundle under test (its `designation`
    table), so the corpus and the binding are the same build's."""
    out = {(e, d): u for e, d, u in db.execute(
        "select entry_id, designation_id, unit from designation")}
    assert len(out) > 50, len(out)             # not vacuous: the corpus has its designations
    return out


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_no_section_carries_two_classified_waters_units(db, units):
    """RU-13: 10,227 sections carried two units (Bulkley+Morice 2,817, Bulkley+Suskwa 1,704 …);
    after the pass, 115 still did — Limonite Creek and the tributaries joining the Zymoetz at
    its A/B cut, walked by BOTH of one entry's designations (F1). Now none, and the Zymoetz
    walk still binds its tributaries (the positive control)."""
    per: dict[int, set[str]] = defaultdict(set)
    for e, d, sid in db.execute("select entry_id, designation_id, sid from designation_section"):
        per[sid].add(units[(e, d)])
    assert len(per) > 50_000, len(per)         # not vacuous: the bundle binds designations
    two = {s: u for s, u in per.items() if len(u) > 1}
    assert not two, (len(two), sorted(two.items())[:5])
    lim = _sids_of(db, "Limonite Creek")
    assert lim and all(len(per[s]) == 1 for s in lim), {s: per.get(s) for s in lim}
    assert {u for s in lim for u in per[s]} <= {units[("r6:zymoetz_copper_river@6-9", d)]
                                                for d in ("zymoetz_river_a", "zymoetz_river_b")}


@pytest.mark.needs_bundle
def test_gosnell_creek_and_the_endako_are_not_classified(db):
    """LI-2: a tributary with its own row and no designation — Gosnell Creek (under the Morice
    and the Bulkley), the Endako (under the Stellako's [Includes Tributaries]) — carries none."""
    for name in ("Gosnell Creek", "Endako River"):
        sids = _sids_of(db, name)
        assert sids, name
        assert not any(_designations_of(db, s) for s in sids), name


@pytest.mark.needs_bundle
def test_the_nanika_is_the_morices_not_the_bulkleys(db):
    """RU-13: the Nanika, above Morice Lake, flows into the Morice first — the Morice's unit,
    handed on from the Bulkley's walk (the Morice's own walk stops at its lake)."""
    sids = _sids_of(db, "Nanika River")
    assert sids
    got = {d for s in sids for d in _designations_of(db, s)}
    assert got == {("r6:morice_river@6-9", "morice_river")}


@pytest.mark.needs_bundle
def test_the_nass_closure_takes_the_meziadin_to_its_lake_and_no_further(db):
    """RU-14: `nass_river.r2` binds the Meziadin River below Meziadin Lake and none of the
    creeks feeding the lake (Hanna, Tintina, Strohn — 1,178 sections before)."""
    nass = "r6:nass_river@6-30"
    mez = _sids_of(db, "Meziadin River")
    assert mez and all(_bound_by(db, s, nass, "nass_river.r2") for s in mez)
    for name in ("Hanna Creek", "Tintina Creek", "Strohn Creek"):
        sids = [s for s in _sids_of(db, name)
                if db.execute("select 1 from item i join item_section s on s.ord = i.ord "
                              "where s.sid = ? and i.name = ?", (s, name)).fetchone()]
        assert sids, name
        assert not any(_bound_by(db, s, nass, "nass_river.r2") for s in sids), name
    n = db.execute("select count(*) from ruleset r join section_ruleset sr on sr.set_id = "
                   "r.set_id where r.entry_id = ? and r.rule_id = 'nass_river.r2'",
                   (nass,)).fetchone()[0]
    assert n < 100, n


@pytest.mark.needs_bundle
def test_kennedy_lake_is_outside_pacific_rim(db):
    """User ruling 2026-10-03: Kennedy Lake (wbk:329083511, 65 km²) is outside the park. It is
    not closed by the park's rule, not a `province_except` section (the provincial licence is
    valid there), and Region 1's rules answer on it."""
    (sid,) = [s for (s,) in db.execute(
        "select s.sid from item i join item_section s on s.ord = i.ord "
        "where i.item_id = 'wbk:329083511'")]
    assert not _bound_by(db, sid, "zp:national_park_reserves")
    assert not _bound_by(db, sid, "zp:superior_closures")
    assert not db.execute("select 1 from province_except where sid = ?", (sid,)).fetchone()
    got = R.requirements_in_force(db, sid, (7, 1))
    assert "zp:basic_licence#basic_licence" in got["holds"]
    assert any(x["entry"].startswith("z1:") for x in R.effective_rules(sid, (7, 1), "RB", str(BUNDLE)))
    # the park's other lakes are still the park's
    hob = _sids_of(db, "Hobiton Lake")
    assert hob and _bound_by(db, hob[0], "zp:national_park_reserves")
