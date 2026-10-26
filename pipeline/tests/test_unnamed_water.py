"""EVERY SECTION HAS AN ANSWER (S1, answers 2.4; user ruling 2026-10-10: "unnamed or named but
unregulated waters should always show base regs/zone and any area they are in, tiered properly").

No data: the unnamed parts a tiny bundle derives (`derived.write` with places), what it refuses, and
the names (`display.place_label`, whole strings — AGENTS 33). S1b (user 2026-10-10: "for labels …
mus just stored in a separate spot, just show regions the section is in"): a name is the water kind
and the regions only; an unnamed part is an answers key's CONTENT (it takes a named part's key where
the content is the same), and where a section lies (`section_ruleset.place` / `.mus`) is apart.

The data tests read a built bundle and its answers (`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point
them at a side build); a bundle from before S1 (no `water_place`) FAILS through `predates`. Three
REAL unnamed sections, chosen by what their rule set holds (never by handle — handles move with the
atlas), each answered as the reader decides and tiered with its scope:

  * an unnamed stream inside Bowron Lake Park (an AREA row, Region 5): the park's "1 trout/char a day
    (30–50 cm)" governs, the zone's quota is replaced by it, the zone's daily total still counts it
    ("also"), and the card's row says `scope.of: area` with the park's entry;
  * a plain Region 3 stream (zone and province rules only): closed by the spring stream closure on
    Jun 1, the zone's 4 a day (stream share) on Jul 1, `scope.of: region`;
  * an unnamed Region 5 wetland (zone only): the zone's 5 a day, its kind `wetland`, no rule for
    streams bound to it.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.display import PLACE_TEMPLATE, place_label, places_table
from pipeline.deliver.bundle import derived
from pipeline.deliver.bundle.derived import DerivedError
from pipeline.tests.bundle_fixture import _Cov, section_sets
from pipeline.tests.conftest import EXPORT_HINT, need, predates

SCHEMA = Path(derived.__file__).with_name("schema.sql")


# --------------------------------------------------------------------------------------------
# no data: the names
# --------------------------------------------------------------------------------------------

def test_the_place_names_are_whole_sentences():
    assert PLACE_TEMPLATE == "Unnamed {kind} · {regions}"
    assert place_label("stream", ("5",)) == "Unnamed stream · Region 5"
    assert place_label("lake", ("3", "8")) == "Unnamed lake · Regions 3 and 8"
    assert place_label("wetland", ("7a",)) == "Unnamed wetland · Region 7A"
    assert place_label("stream", ("1", "2", "3")) == "Unnamed stream · Regions 1, 2 and 3"
    from pipeline.deliver.types import RegionPlace
    assert places_table([RegionPlace(place=0, regions=("3", "8"), sections=4)]) == [
        {"regions": ["3", "8"], "label": "Regions 3 and 8",
         "names": {"lake": "Unnamed lake · Regions 3 and 8",
                   "stream": "Unnamed stream · Regions 3 and 8",
                   "wetland": "Unnamed wetland · Regions 3 and 8"}}]


# --------------------------------------------------------------------------------------------
# no data: a tiny bundle's unnamed parts
# --------------------------------------------------------------------------------------------

def _db(lake_set: int = 9):
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA.read_text())
    db.executemany("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)",
                   [(1, "gnis:1", "One River", "stream")])
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 1), (1, 2)])
    # 1, 2 named; 3, 4 unnamed with set 7; 5 an unnamed lake with set 9; 6 unnamed with set 8;
    # 9 outside B.C. (no set)
    section_sets(db, {1: 7, 2: 7, 3: 7, 4: 7, 5: lake_set, 6: 8})
    db.execute("INSERT INTO outside_bc (sid) VALUES (9)")
    return db


PLACES = {1: ("stream", ("2",), ("2-8",)), 2: ("stream", ("2", "3"), ("2-8", "3-1")),
          3: ("stream", ("2",), ("2-8",)), 4: ("stream", ("2",), ("2-9",)),
          5: ("lake", ("2",), ("2-8",)), 6: ("stream", ("2",), ("2-9",)),
          9: ("stream", (), ())}                      # outside: handed over, never placed


def test_a_tiny_bundle_derives_its_unnamed_parts():
    db = _db()
    derived.write(db, _Cov(), places=PLACES)
    # WHERE every section in B.C. is (named ones too), apart from the parts (S1b)
    assert db.execute("SELECT place, regions, sections FROM region_place ORDER BY place"
                      ).fetchall() == [(0, "2", 5), (1, "2,3", 1)]
    assert db.execute("SELECT mus, codes, sections FROM mu_set ORDER BY mus").fetchall() == \
        [(0, "2-8", 3), (1, "2-8,3-1", 1), (2, "2-9", 2)]
    assert db.execute("SELECT sid, place, mus FROM section_ruleset ORDER BY sid").fetchall() == \
        [(1, 0, 0), (2, 1, 1), (3, 0, 0), (4, 0, 2), (5, 0, 0), (6, 0, 2)]
    # THE PARTS are contents: 3 and 4 share one though their MUs differ; 5 is a lake
    assert db.execute("SELECT up, set_id, kind, sections, rep_sid FROM unnamed_part "
                      "ORDER BY up").fetchall() == [(0, 7, "stream", 2, 3), (1, 8, "stream", 1, 6),
                                                    (2, 9, "lake", 1, 5)]
    assert dict(db.execute("SELECT sid, up FROM section_ruleset")) == \
        {1: None, 2: None, 3: 0, 4: 0, 5: 2, 6: 1}
    U = derived.unnamed_parts(db)
    assert [(u.up, u.kind) for u in U] == [(0, "stream"), (1, "stream"), (2, "lake")]
    assert [(w.place, w.regions) for w in derived.region_places(db)] == [(0, ("2",)), (1, ("2", "3"))]
    assert [m.codes for m in derived.mu_sets(db)] == [("2-8",), ("2-8", "3-1"), ("2-9",)]
    # every rule key has a kind: the named water's, else its unnamed sections' (S1)
    assert db.execute("SELECT set_id, kind FROM rule_key ORDER BY set_id").fetchall() == \
        [(7, "stream"), (8, "stream"), (9, "lake")]
    # the reader's kind of a section: the named water's, else its unnamed part's
    from pipeline.deliver.bundle.read import section_kind
    assert [section_kind(db, s) for s in (1, 3, 5, 9)] == ["stream", "stream", "lake", None]


def test_a_rule_key_on_two_kinds_of_water_is_refused():
    db = _db(lake_set=7)                    # the unnamed lake carries the named stream's key
    with pytest.raises(DerivedError, match="two water kinds"):
        derived.write(db, _Cov(), places=PLACES)


def test_an_unnamed_section_in_bc_with_no_place_is_refused():
    db = _db()
    with pytest.raises(DerivedError, match="have no place"):
        derived.write(db, _Cov(), places={k: v for k, v in PLACES.items() if k != 5})


def test_a_named_section_in_bc_with_no_place_is_refused():
    """S1b: every section in B.C. has a place, named or not."""
    db = _db()
    with pytest.raises(DerivedError, match="have no place"):
        derived.write(db, _Cov(), places={k: v for k, v in PLACES.items() if k != 1})


def test_a_place_in_no_region_is_refused():
    db = _db()
    with pytest.raises(DerivedError, match="no region"):
        derived.write(db, _Cov(), places={**PLACES, 6: ("stream", (), ("2-9",))})


def test_a_fixture_without_places_writes_no_unnamed_part():
    db = _db()
    derived.write(db, _Cov())
    assert db.execute("SELECT COUNT(*) FROM unnamed_part").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM section_ruleset WHERE up IS NOT NULL OR place IS NOT "
                      "NULL OR mus IS NOT NULL").fetchone()[0] == 0


# --------------------------------------------------------------------------------------------
# the built set: three real unnamed sections
# --------------------------------------------------------------------------------------------

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")


def _export_dir() -> Path:
    from pipeline.deliver.export.contract import OUT
    return Path(os.environ.get("ANSWERS_EXPORT_DIR") or OUT.parent)


@pytest.fixture(scope="module")
def built(request):
    exp = need(request, "bundle", _export_dir() / "ui-rules-answers.json", EXPORT_HINT).parent
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    has = db.execute("SELECT 1 FROM sqlite_master WHERE name = 'region_place'").fetchone()
    if not has or not db.execute("SELECT COUNT(*) FROM region_place").fetchone()[0]:
        db.close()
        predates(f"{BUNDLE} predates S1b (no region_place): rebuild the bundle")
    A = json.loads((exp / "ui-rules-answers.json").read_text())
    X = json.loads((exp / "ui-rules-export.json").read_text())
    if "parts" not in A.get("unnamed", {}):
        db.close()
        predates(f"{exp}/ui-rules-answers.json predates S1b (no `unnamed.parts`)")
    yield db, A, X
    db.close()


def _part(db, kind: str, region: str, *, holding: str = None, zone_only: bool = False) -> int:
    """The unnamed part (`up`) with the most sections of this kind in this one region whose rule
    set holds rule `holding` / holds only zone and province rules."""
    best = None
    for up, set_id, n in db.execute(
            "SELECT u.up, u.set_id, COUNT(*) FROM unnamed_part u JOIN section_ruleset r "
            "ON r.up = u.up JOIN region_place w ON w.place = r.place WHERE u.kind = ? AND "
            "w.regions = ? GROUP BY u.up ORDER BY COUNT(*) DESC, u.up", (kind, region)):
        rules = {f"{e}::{r}" for e, r in db.execute(
            "SELECT entry_id, rule_id FROM ruleset WHERE set_id = ?", (set_id,))}
        if holding is not None and holding not in rules:
            continue
        if zone_only and not all(x.startswith(("z", "zp:")) for x in rules):
            continue
        best = up
        break
    assert best is not None, f"no unnamed {kind} part in region {region} as asked"
    return best


def _tap(A, X, up: int, md):
    k = A["unnamed"]["parts"][up]
    return k, E.tap_key(A, X, k, *md)


def _name(db, A, up: int, region: str) -> str:
    """The name an unnamed section of part `up` lying in `region` alone shows."""
    pl = next(i for i, p in enumerate(A["places"]) if p["regions"] == [region])
    assert db.execute("SELECT COUNT(*) FROM section_ruleset WHERE up = ? AND place = ?",
                      (up, pl)).fetchone()[0]
    return A["places"][pl]["names"][A["keys"][A["unnamed"]["parts"][up]][7]]


def _rb(t, X):
    """The rainbow's decided answer (hatchery) with rule ids, and the card row holding it."""
    ids = X["rule_ids"]
    h = t["rows"]["fish"]["RB"]["hatchery"]
    roles = {(ids[r], role, None if b is None else ids[b]) for r, role, b in h["roles"]}
    row = next(r for r in t["rows"]["rows"] if "RB" in r["members"])
    return h, ids[h["win"]], roles, row


BOWRON = "r5:bowron_lake_park_waters_other_than_bowron_lake@5-16"


@pytest.mark.needs_bundle
def test_an_unnamed_stream_in_a_park_takes_the_park_row_over_the_zone(built):
    db, A, X = built
    up = _part(db, "stream", "5", holding=f"{BOWRON}::bowron_lake_park_waters.r1")
    k, t = _tap(A, X, up, (7, 1))
    assert _name(db, A, up, "5") == "Unnamed stream · Region 5"
    assert A["keys"][k][7] == "stream"
    h, win, roles, row = _rb(t, X)
    assert (h["status"], h["daily"], win) == ("keep", 1, f"{BOWRON}::bowron_lake_park_waters.r1")
    # tiered: the park's row governs and REPLACES the zone's size line; the zone's total still holds
    assert ("z5:daily_quotas::trout_char_quota.r2", "replaced",
            f"{BOWRON}::bowron_lake_park_waters.r1") in roles
    assert ("z5:daily_quotas::trout_char_quota.r1", "also", None) in roles
    assert row["scope"] == {"of": "area", "entry": BOWRON, "share": False, "apart": False, "narrow_all": False}
    assert t["display"]["status"] == "own"                 # an area row beyond the zone base


@pytest.mark.needs_bundle
def test_a_plain_unnamed_stream_reads_the_zone_base(built):
    db, A, X = built
    up = _part(db, "stream", "3", zone_only=True)
    assert _name(db, A, up, "3") == "Unnamed stream · Region 3"
    # Jun 1: Region 3's spring stream closure
    _, t = _tap(A, X, up, (6, 1))
    h, win, _, row = _rb(t, X)
    assert (h["status"], win) == ("closed", "z3:spring_stream_closure::spring_stream_closure.r1")
    assert t["display"]["status"] == "closed" and row["kind"] == "closed"
    # Jul 1: the zone's 4 a day, the region's stream share
    _, t = _tap(A, X, up, (7, 1))
    h, win, _, row = _rb(t, X)
    assert (h["status"], h["daily"], win) == ("keep", 4, "z3:daily_quotas::trout_char_quota.r1")
    assert row["scope"] == {"of": "region", "entry": "z3:daily_quotas", "share": True,
                            "apart": False, "narrow_all": True}
    assert t["display"]["status"] == "base"


@pytest.mark.needs_bundle
def test_an_unnamed_wetland_is_a_wetland_and_reads_the_zone_base(built):
    db, A, X = built
    up = _part(db, "wetland", "5", zone_only=True)
    k, t = _tap(A, X, up, (7, 1))
    assert _name(db, A, up, "5") == "Unnamed wetland · Region 5"
    assert A["keys"][k][7] == "wetland"
    h, win, _, row = _rb(t, X)
    assert (h["status"], h["daily"], win) == ("keep", 5, "z5:daily_quotas::trout_char_quota.r1")
    assert row["scope"]["of"] == "region" and row["scope"]["entry"] == "z5:daily_quotas"
    # no rule for streams is bound to it (the kind the rules were placed by is the answer's), and
    # the reader's kind of its sections is the place's
    from pipeline.deliver.bundle import read
    rules = read._rules_of(str(BUNDLE))
    bound = [rules[(e, r)] for e, r in db.execute(
        "SELECT entry_id, rule_id FROM ruleset WHERE set_id = ?", (A["keys"][k][0],))]
    assert bound and not [x for x in bound if x.get("water") == "stream"]
    rep = db.execute("SELECT rep_sid FROM unnamed_part WHERE up = ?", (up,)).fetchone()[0]
    assert read.section_kind(db, rep) == "wetland"


@pytest.mark.needs_bundle
def test_every_unnamed_part_takes_the_key_of_its_content(built):
    """S1b: an unnamed part's key IS its content — a named part's key where one holds the same
    content, else one of the keys from `first` on, each taken by some part; the places are the
    bundle's region sets, named whole."""
    db, A, X = built
    U = A["unnamed"]
    P = derived.unnamed_parts(db)
    assert len(P) == len(U["parts"])
    assert A["places"] == places_table(derived.region_places(db))
    named = {tuple(json.dumps(x) for x in k[:9]): i for i, k in enumerate(A["keys"][:U["first"]])}
    shared = 0
    for p in P:
        k = U["parts"][p.up]
        key = A["keys"][k]
        assert key[7] == p.kind and (key[0], key[1], key[8]) == (p.set_id, p.licensing_set, p.tidal)
        mine = tuple(json.dumps(x) for x in key[:9])
        if k < U["first"]:
            shared += 1
        else:
            assert mine not in named           # a content a named part has takes ITS key
    assert set(U["parts"]) >= set(range(U["first"], len(A["keys"])))
    assert len({json.dumps(A["keys"][k][:9]) for k in range(U["first"], len(A["keys"]))}) == \
        len(A["keys"]) - U["first"]
    assert shared and len(A["keys"]) - U["first"] < len(P)


@pytest.mark.needs_bundle
def test_every_section_in_bc_has_a_water_kind(built):
    """Named: its water's `item.kind`; unnamed: its part's (`water_kind.kind_of`). None without."""
    db, _A, _X = built
    n_bc = db.execute("SELECT COUNT(*) FROM section_ruleset").fetchone()[0]
    named = db.execute("SELECT COUNT(*) FROM section_ruleset r JOIN item_section s ON s.sid = r.sid "
                       "JOIN item i ON i.ord = s.ord WHERE i.kind IN ('stream', 'lake', 'wetland')"
                       ).fetchone()[0]
    unnamed = db.execute("SELECT COUNT(*) FROM section_ruleset r JOIN unnamed_part u ON u.up = r.up"
                         ).fetchone()[0]
    assert named + unnamed == n_bc
    # and a place: every section in B.C., named or not (S1b)
    assert db.execute("SELECT COUNT(*) FROM section_ruleset WHERE place IS NULL OR mus IS NULL"
                      ).fetchone()[0] == 0
