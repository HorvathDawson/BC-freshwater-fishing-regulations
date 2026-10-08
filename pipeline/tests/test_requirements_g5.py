"""GAP G5, ADOPTED INTO THE READER (2026-10-06): `read.requirements_in_force` drops a requirement
for the other kind of water (`wrong_water`) and lets a superior authority's requirement displace the
provincial ones (`displaced`); `read.designations_in_force` is public. The licence answer
(`pipeline/deliver/answers/licence.py`) reads these and no longer applies them itself.

Each ruling is pinned on the bundle and by MUTATION on an in-memory copy: change the one field the
ruling reads and the answer moves; change it back and it returns.

Reads a bundle (`UI_EXPORT_BUNDLE`, else the live one).
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.bundle import read as R

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)
JUL_1 = (7, 1)

KHARTOUM_LAKE = "wbk:329197063"               # a book-known steelhead lake by its own row (AGENTS 54)
SHUSWAP_LAKE = "wbk:329518145"
NITINAT_RIVER = "gnis:23318"                  # in Pacific Rim National Park Reserve
STAMP_KNOWN = "zp:steelhead#steelhead_targeting_known"
CHAR_STAMP = "zp:shuswap_char_stamp#shuswap_char_stamp"
CHAR_STAMP_ROW = ("r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"
                  "#shuswap_char_stamp")
PARK_PERMIT = "zp:superior_closures#national_park_permit"
GUIDE = "zp:conduct#angling_guide_licence"


@pytest.fixture(scope="module")
def db():
    if not BUNDLE.exists():
        pytest.skip(f"no bundle at {BUNDLE}")
    c = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield c
    c.close()


@pytest.fixture()
def copy_db(db):
    c = sqlite3.connect(":memory:")
    db.backup(c)
    yield c
    c.close()


def _sid(db, item_id: str) -> int:
    got = db.execute("SELECT MIN(s.sid) FROM item i JOIN item_section s ON s.ord = i.ord "
                     "WHERE i.item_id = ?", (item_id,)).fetchone()[0]
    if got is None:
        pytest.skip(f"{item_id} is not in this bundle")
    return got


def _edit(c, key: str, **fields) -> None:
    """Set (or, with None, remove) fields of a requirement record in the copy."""
    e, r = key.split("#")
    rec = json.loads(c.execute("SELECT record FROM requirement WHERE entry_id = ? AND req_id = ?",
                               (e, r)).fetchone()[0])
    for k, v in fields.items():
        if v is None:
            rec.pop(k, None)
        else:
            rec[k] = v
    c.execute("UPDATE requirement SET record = ? WHERE entry_id = ? AND req_id = ?",
              (json.dumps(rec), e, r))


def test_the_answer_has_every_key(db):
    got = R.requirements_in_force(db, _sid(db, KHARTOUM_LAKE), JUL_1)
    assert set(got) == {"holds", "waived", "not_yet_mapped", "also_printed", "wrong_water",
                        "displaced", "considered"}


def test_designations_in_force_is_public_and_the_waiver_reads_it(db):
    assert not hasattr(R, "_designations_in_force")
    sids = [s for (s,) in db.execute(
        "SELECT sid FROM designation_section WHERE entry_id = 'r5:chilko_river@5-5' "
        "AND designation_id = 'chilko_river' ORDER BY sid LIMIT 1")]
    if not sids:
        pytest.skip("no Chilko designation in this bundle")
    got = R.designations_in_force(db, sids[0], JUL_1)
    assert [f"{d['entry_id']}#{d['id']}" for d in got] == ["r5:chilko_river@5-5#chilko_river"]
    assert R.stamp_waived_here(db, sids[0], JUL_1) == ["r5:chilko_river@5-5#chilko_river"]


# --------------------------------------------------------------------------------------------
# The other kind of water
# --------------------------------------------------------------------------------------------

def test_a_stream_requirement_does_not_hold_on_a_lake(db, copy_db):
    """MUTATION: Khartoum Lake carries the provincial steelhead stamp (`steelhead_targeting_known`,
    no water kind). Print it for streams and it no longer holds on the lake — it is listed under
    `wrong_water`; print it for lakes and it holds again."""
    sid = _sid(db, KHARTOUM_LAKE)
    assert R.section_kind(db, sid) == "lake"
    got = R.requirements_in_force(db, sid, JUL_1)
    assert STAMP_KNOWN in got["holds"] and got["wrong_water"] == {}
    _edit(copy_db, STAMP_KNOWN, water="stream")
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert STAMP_KNOWN not in got["holds"] and got["wrong_water"] == {STAMP_KNOWN: "stream"}
    _edit(copy_db, STAMP_KNOWN, water="lake")
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert STAMP_KNOWN in got["holds"] and got["wrong_water"] == {}


def test_a_section_of_no_named_water_keeps_a_kind_scoped_requirement(db):
    """A walked tributary is in no named water: its kind is unknown, and the stream-only
    steelhead stamp placed there holds (the walk walks streams only)."""
    sid = db.execute("SELECT r.sid FROM requirement_section r WHERE r.entry_id = 'zp:steelhead' "
                     "AND r.req_id = 'steelhead_targeting' AND NOT EXISTS (SELECT 1 FROM "
                     "item_section x WHERE x.sid = r.sid) ORDER BY r.sid LIMIT 1").fetchone()
    if sid is None:
        pytest.skip("no unnamed section carries the stream stamp")
    assert R.section_kind(db, sid[0]) is None
    got = R.requirements_in_force(db, sid[0], (11, 1))
    assert "zp:steelhead#steelhead_targeting" in got["holds"] and got["wrong_water"] == {}


def test_a_restatement_binds_as_the_record_it_restates(db, copy_db):
    """MUTATION, through `restates`: on Shuswap Lake the row's char stamp restates the province's
    (one obligation, folded into it). Unplace the province's record and the row's holds alone,
    binding as the province's (`as_bound`); print the province's for streams and the row's goes
    with it to `wrong_water`."""
    sid = _sid(db, SHUSWAP_LAKE)
    got = R.requirements_in_force(db, sid, JUL_1)
    assert got["also_printed"].get(CHAR_STAMP) == [CHAR_STAMP_ROW]
    copy_db.execute("UPDATE requirement SET placement = 'unresolved' WHERE entry_id = ? AND "
                    "req_id = ?", tuple(CHAR_STAMP.split("#")))
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert CHAR_STAMP_ROW in got["holds"] and CHAR_STAMP not in got["holds"]
    _edit(copy_db, CHAR_STAMP, water="stream")
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert CHAR_STAMP_ROW not in got["holds"]
    assert got["wrong_water"] == {CHAR_STAMP_ROW: "stream"}
    reqs = {f"{e}#{r}": json.loads(x) for e, r, x in copy_db.execute(
        "SELECT entry_id, req_id, record FROM requirement")}
    bound = R.as_bound(reqs[CHAR_STAMP_ROW], reqs)
    assert bound["water"] == "stream"
    assert bound["satisfied_by"] == reqs[CHAR_STAMP]["satisfied_by"]


# --------------------------------------------------------------------------------------------
# A superior authority
# --------------------------------------------------------------------------------------------

def test_a_national_park_permit_displaces_the_provincial_requirements(db, copy_db):
    """On the Nitinat River (Pacific Rim) the park's permit holds and the angling guide licence,
    which would otherwise hold, is displaced by it. MUTATION: take away the permit's
    `authority: superior` and the guide licence holds beside it again; take away the guide
    licence's `satisfied_by` and there is nothing to displace."""
    sid = _sid(db, NITINAT_RIVER)
    got = R.requirements_in_force(db, sid, JUL_1)
    if PARK_PERMIT not in got["holds"]:
        pytest.skip("the Nitinat is not in a national park in this bundle")
    assert got["displaced"] == {GUIDE: [PARK_PERMIT]} and GUIDE not in got["holds"]
    _edit(copy_db, PARK_PERMIT, authority=None)
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert got["displaced"] == {} and {GUIDE, PARK_PERMIT} <= set(got["holds"])
    _edit(copy_db, PARK_PERMIT, authority="superior")
    _edit(copy_db, GUIDE, satisfied_by=None)
    got = R.requirements_in_force(copy_db, sid, JUL_1)
    assert got["displaced"] == {} and GUIDE in got["holds"]


def test_the_licence_answer_reads_the_readers_g5(db):
    """licence.py holds no G5 logic of its own: its `holds` is the reader's, in order, and the
    displaced requirement is shown (a row) but sells nothing."""
    from pipeline.deliver.answers import licence as L
    C = L.corpus(db)
    sid = _sid(db, NITINAT_RIVER)
    h = L.holds(db, C, sid, JUL_1)
    got = R.requirements_in_force(db, sid, JUL_1)
    if not got["displaced"]:
        pytest.skip("nothing displaced on the Nitinat in this bundle")
    assert set(h["holds"]) == set(got["holds"]) and h["displaced"] == got["displaced"]
    assert set(h["rows"]) == set(got["holds"]) | set(got["displaced"])
    for name in ("PROPOSALS_FOR_READER",):
        assert not hasattr(L, name)
    p = {d: v[0] for d, v in L.PROFILE_DIMS}
    p["guidance"] = "guided"
    ans = L.documents(C, h, [], p, C.index.__getitem__)
    assert "angling_guide_licence" not in {d["doc"] for d in ans["documents"]}
