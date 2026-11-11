"""T166 acceptance (owner 2026-10-23/28, RULINGS K7 / C-2): a sample of crossings pinned to ONE tidal cut each, on the
built bundle — the Fraser at Mission, the Squamish, the Skeena — and the Cowichan, whose boundary is its mouth (DFO's
default: no cut, nothing tidal). Every tidal section of each river is owned by its one split, the river keeps water
above it, and nothing tidal sits upstream of a freshwater piece of the same river.
What each catches: a second owner, or the whole river gone tidal ("one cut"); a cut off the river's line or tidal water
above freshwater (the order); the mouth default made a cut (the Cowichan)."""
from __future__ import annotations

import sqlite3

import pytest

from pipeline.tests import under_test
from pipeline.tests.conftest import BUNDLE_HINT, need

pytestmark = pytest.mark.needs_bundle

BUNDLE = str(under_test.path("bundle"))

CUTS = {"gnis:39325": "split:fraser_river__the_cpr_bridge_at_mission",   # Fraser, the CPR bridge at Mission
        "gnis:25671": "split:squamish_river__tidal_boundary",
        "gnis:2936": "split:skeena_river__tidal_boundary"}
COWICHAN = "gnis:12227"


@pytest.fixture(scope="module")
def db(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    con = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    yield con
    con.close()


def _sections(db, item_id: str) -> list[int]:
    return [s for (s,) in db.execute("SELECT s.sid FROM item i JOIN item_section s ON s.ord = i.ord "
                                     "WHERE i.item_id = ?", (item_id,))]


@pytest.mark.parametrize("item_id", sorted(CUTS))
def test_one_tidal_cut_owns_every_tidal_section_of_the_river(db, item_id):
    secs = _sections(db, item_id)
    owners = dict(db.execute(f"SELECT sid, entry_id FROM tidal WHERE sid IN ({','.join('?' * len(secs))})", secs))
    assert set(owners.values()) == {CUTS[item_id]}, f"{item_id}: tidal owners {sorted(set(owners.values()))}"
    assert len(owners) < len(secs), f"{item_id}: the whole river went tidal"


@pytest.mark.parametrize("item_id", sorted(CUTS))
def test_nothing_tidal_lies_above_a_freshwater_piece_of_the_same_river(db, item_id):
    secs = set(_sections(db, item_id))
    tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}
    down = dict(db.execute("SELECT sid, down_sid FROM section_down"))
    for s in secs & tidal:
        at, seen = down.get(s), set()
        while at is not None and at not in seen:
            seen.add(at)
            assert not (at in secs and at not in tidal), f"{item_id}: tidal {s} lies above freshwater {at}"
            at = down.get(at)
    # and the river flows down INTO its tidal piece: the cut is at the bottom, not somewhere off the line
    def reaches(s):
        at, seen = down.get(s), set()
        while at is not None and at not in seen:
            if at in secs and at in tidal:
                return True
            seen.add(at)
            at = down.get(at)
        return False
    assert any(reaches(s) for s in secs - tidal), f"{item_id}: no freshwater piece flows into the tidal one"


def test_the_cowichans_boundary_is_its_mouth_nothing_tidal(db):
    secs = _sections(db, COWICHAN)
    assert secs
    assert not db.execute(f"SELECT 1 FROM tidal WHERE sid IN ({','.join('?' * len(secs))})", secs).fetchone()
