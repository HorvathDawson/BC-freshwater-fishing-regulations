"""THE OUTRIGHT STEELHEAD STAMP WAIVER (user ruling 2026-10-02).

"Class II water upstream of Brittany Creek [Includes Tributaries], June 11-Oct 31 (Steelhead Stamp
not required)" — the Chilko's row — means NO steelhead stamp at all on that designation's reach
during its dates: neither the classified-water stamp (the designation has no stamp period) nor the
provincial "Conservation Surcharge Stamp to fish for steelhead" (`zp:steelhead`
`steelhead_targeting` and its twin `steelhead_targeting_known`, which consent through
`waived_where: steelhead_stamp_waived`). The Classified Waters Licence is still required (it is
Class II water), and the steelhead rules still apply (release wild, the annual 10, the record duty).
Outside the dates, or below Brittany Creek, the provincial stamp is required as written.

`read.requirements_in_force` is the reference reader. Licensing never votes on open/closed: the
Chilko is closed Nov 1-June 10, and the Nov 15 answer is what holds were it open.

Reads a bundle (`UI_EXPORT_BUNDLE`, else the live one).
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)

CHILKO_ROW = "r5:chilko_river@5-5"
CHILKO = f"{CHILKO_ROW}#chilko_river"
CHILKO_ITEM = "gnis:13741"
STAMP = "zp:steelhead#steelhead_targeting"
STAMP_TWIN = "zp:steelhead#steelhead_targeting_known"
CWL = "zp:classified_waters_licence#classified_waters_licence"
WILD_RELEASE = "zp:steelhead::steelhead.r2"
JUL_1, NOV_15 = (7, 1), (11, 15)


@pytest.fixture(scope="module")
def db():
    if not BUNDLE.exists():
        pytest.skip(f"no bundle at {BUNDLE}")
    c = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield c
    c.close()


def _upstream(db) -> list[int]:
    """The Chilko designation's sections: the river upstream of Brittany Creek, with its
    tributaries."""
    return sorted(s for (s,) in db.execute(
        "SELECT sid FROM designation_section WHERE entry_id = ? AND designation_id = ?",
        tuple(CHILKO.split("#"))))


def _river(db) -> list[int]:
    return sorted(s for (s,) in db.execute(
        "SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord WHERE i.item_id = ?",
        (CHILKO_ITEM,)))


def _stamps(got: dict) -> set[str]:
    return {k for k in got["holds"] if k in (STAMP, STAMP_TWIN)}


def test_the_chilko_row_waives_every_stamp(db):
    rec = json.loads(db.execute("SELECT record FROM designation WHERE entry_id = ? AND "
                                "designation_id = ?", tuple(CHILKO.split("#"))).fetchone()[0])
    assert rec["steelhead_stamp_waived"] == {"verbatim": "Steelhead Stamp not required"}
    assert "steelhead_stamp_during" not in rec
    for k in (STAMP, STAMP_TWIN):
        e, r = k.split("#")
        got = json.loads(db.execute("SELECT record FROM requirement WHERE entry_id = ? AND "
                                    "req_id = ?", (e, r)).fetchone()[0])
        assert got["waived_where"] == "steelhead_stamp_waived", k


def test_upstream_of_brittany_creek_on_jul_1_no_stamp_but_the_licence_and_the_rules(db):
    """Jul 1, upstream of Brittany Creek: no steelhead stamp is required (the provincial stamp is
    placed there and LIFTED by the waiver); the Classified Waters Licence IS required; and wild
    steelhead must still be released."""
    up = _upstream(db)
    river = set(_river(db))
    assert up and set(up) & river and set(up) - river, "the reach and its tributaries"
    for sid in up:
        assert R.stamp_waived_here(db, sid, JUL_1) == [CHILKO], sid
        got = R.requirements_in_force(db, sid, JUL_1)
        assert not _stamps(got), (sid, got)
        assert STAMP in got["waived"] and got["waived"][STAMP] == [CHILKO], (sid, got)
        assert CWL in got["holds"], (sid, got)                      # still Class II water
        assert not any(k.endswith("#classified_steelhead_stamp") for k in got["holds"]), got
    for sid in set(up) & river:
        rules = {f"{e}::{r}" for e, r in db.execute(
            "SELECT r.entry_id, r.rule_id FROM section_ruleset s JOIN ruleset r "
            "ON r.set_id = s.set_id WHERE s.sid = ?", (sid,))}
        assert WILD_RELEASE in rules, sid
        st = [x for x in R.effective_rules(sid, JUL_1, "ST", str(BUNDLE))
              if x["state"] == "speaks"]
        assert any(x.get("take") == 0 for x in st), (sid, [x["rule"] for x in st])


def test_upstream_of_brittany_creek_on_nov_15_the_provincial_stamp_is_required(db):
    """Outside the waiver's dates (June 11-Oct 31) the designation is not in force, so nothing
    lifts the provincial stamp — and the Classified Waters Licence is not required either."""
    for sid in _upstream(db):
        assert R.stamp_waived_here(db, sid, NOV_15) == []
        got = R.requirements_in_force(db, sid, NOV_15)
        assert _stamps(got) and not got["waived"], (sid, got)
        assert CWL not in got["holds"]
    # the dates are the waiver's own, to the day
    sid = _upstream(db)[0]
    assert _stamps(R.requirements_in_force(db, sid, (6, 10)))
    assert not _stamps(R.requirements_in_force(db, sid, (6, 11)))
    assert not _stamps(R.requirements_in_force(db, sid, (10, 31)))
    assert _stamps(R.requirements_in_force(db, sid, (11, 1)))


def test_below_brittany_creek_is_unaffected(db):
    """The Chilko below Brittany Creek carries no designation: the provincial stamp holds on Jul 1
    as everywhere on a steelhead-region stream, and no Classified Waters Licence is required."""
    below = sorted(set(_river(db)) - set(_upstream(db)))
    assert below
    for sid in below:
        assert R.stamp_waived_here(db, sid, JUL_1) == []
        got = R.requirements_in_force(db, sid, JUL_1)
        assert STAMP in got["holds"] and not got["waived"], (sid, got)
        assert CWL not in got["holds"]


def test_a_conditional_waiver_does_not_lift_the_provincial_stamp(db):
    """Skeena River 2: "Steelhead Stamp not mandatory … unless fishing for steelhead" lifts only
    the classified-water stamp; whoever fishes for steelhead needs the provincial stamp."""
    secs = [s for (s,) in db.execute(
        "SELECT sid FROM designation_section WHERE designation_id = 'skeena_river_2'")]
    assert secs
    for sid in secs[:20]:
        got = R.requirements_in_force(db, sid, (8, 1))
        assert STAMP in got["holds"] and not got["waived"], (sid, got)


@pytest.fixture()
def copy_db(db):
    """An in-memory copy of the bundle to mutate."""
    c = sqlite3.connect(":memory:")
    db.backup(c)
    yield c
    c.close()


def test_the_lift_needs_both_the_consent_and_an_outright_waiver(db, copy_db):
    """MUTATION: drop the requirement's `waived_where`, or make the Chilko's waiver conditional
    ("unless fishing for steelhead"), and the provincial stamp holds upstream on Jul 1 again — the
    reader lifts nothing on its own."""
    sid = _upstream(db)[0]
    assert not _stamps(R.requirements_in_force(copy_db, sid, JUL_1))
    e, r = STAMP.split("#")
    rec = json.loads(copy_db.execute("SELECT record FROM requirement WHERE entry_id = ? AND "
                                     "req_id = ?", (e, r)).fetchone()[0])
    rec.pop("waived_where")
    copy_db.execute("UPDATE requirement SET record = ? WHERE entry_id = ? AND req_id = ?",
                    (json.dumps(rec), e, r))
    assert STAMP in R.requirements_in_force(copy_db, sid, JUL_1)["holds"]

    c2 = sqlite3.connect(":memory:")
    db.backup(c2)
    e, d = CHILKO.split("#")
    rec = json.loads(c2.execute("SELECT record FROM designation WHERE entry_id = ? AND "
                                "designation_id = ?", (e, d)).fetchone()[0])
    rec["steelhead_stamp_waived"]["verbatim"] = ("Steelhead Stamp not required unless fishing "
                                                 "for steelhead")
    c2.execute("UPDATE designation SET record = ? WHERE entry_id = ? AND designation_id = ?",
               (json.dumps(rec), e, d))
    got = R.requirements_in_force(c2, sid, JUL_1)
    assert STAMP in got["holds"] and not got["waived"]
    assert R.stamp_waived_here(c2, sid, JUL_1) == []
    c2.close()


def test_the_export_says_where_and_when_the_stamp_is_not_required():
    """The export's designation carries `stamp_waiver`: outright, the requirements it lifts, and
    the sentence naming the reach and the dates per the row; the guide explains it."""
    out = Path(os.environ.get("UI_EXPORT_JSON") or "")
    if not out.is_file():
        pytest.skip("UI_EXPORT_JSON not set")
    # the shipped pair (data + `ui-rules-guide.json` beside it), decoded (`export_codec`)
    doc = X.load(out)
    w = doc["licensing"][CHILKO]["stamp_waiver"]
    assert w["outright"] is True and w["lifts"] == [STAMP, STAMP_TWIN]
    assert "upstream of Brittany Creek" in w["says"] and "Jun 11-Oct 31" in w["says"]
    assert "Classified Waters Licence" in w["says"]
    skeena = next(x for k, x in doc["licensing"].items() if k.endswith("#skeena_river_2"))
    assert skeena["stamp_waiver"]["outright"] is False and skeena["stamp_waiver"]["lifts"] == []
    g = doc["guide"]["licensing"]
    assert any("THE STAMP WAIVER" in x for x in g["rules_of_reading"])
    assert {d["id"] for d in g["stamp_waiver"]["designations"] if d["outright"]} >= {CHILKO}
