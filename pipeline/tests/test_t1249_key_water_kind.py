"""T1249: THE GEAR READS ITS KEY'S WATER KIND, THE BUNDLE'S (AGENTS 55: decided once, read everywhere).

Until T1249 the answers took a rule SET's kind from its NAMED items only (`Bundle.set_kind`), so a key no named water
carries had no kind: on main's bundle of 2026-11-27 (r-7d7848399e53) 133 keys — 92 unnamed-stream (6,584 sections,
7,020 km), 31 unnamed-wetland (84,162 sections), 10 unnamed-lake (32 sections); the 0.9-era live bundle had 132. The
province's "2 lines while alone in a boat on a lake" was kept as a circumstance — the unnamed streams read
"1 line (rod); 2 while alone in a boat on a lake" — and ice fishing was listed among a stream's ways to fish.
The kind is the bundle's `rule_key.kind` (`common.water_kind`): the named water's `item.kind`, else its unnamed
sections' (`unnamed_part.kind`), one per key or the bundle build stops (`derived.write`).

No data: a tiny bundle derived with places (as `test_unnamed_water.py`), and the province's line rule resolved on each
kind. The data test reads the bundle under test (`UI_EXPORT_BUNDLE`). Each test names the mutation that turns it red.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver.answers import common
from pipeline.deliver.answers import gear as G
from pipeline.deliver.answers.common import AnswersError, RuleKey
from pipeline.deliver.bundle import derived
from pipeline.tests.bundle_fixture import _Cov, section_sets
from pipeline.tests.conftest import BUNDLE_HINT, need

SCHEMA = Path(derived.__file__).with_name("schema.sql")

#: the sections: 1, 2 of a named river (set 7); 3 unnamed with set 7; 4 an unnamed stream (set 8, no named water);
#: 5 an unnamed lake (set 9); 6 an unnamed wetland (set 10)
PLACES = {1: ("stream", ("4",), ("4-1",)), 2: ("stream", ("4",), ("4-1",)), 3: ("stream", ("4",), ("4-1",)),
          4: ("stream", ("4",), ("4-2",)), 5: ("lake", ("4",), ("4-2",)), 6: ("wetland", ("4",), ("4-2",))}


def _bundle(tmp_path, places=PLACES) -> str:
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA.read_text())
    db.execute("INSERT INTO item (ord, item_id, name, kind) VALUES (1, 'gnis:1', 'One River', 'stream')")
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 1), (1, 2)])
    section_sets(db, {1: 7, 2: 7, 3: 7, 4: 8, 5: 9, 6: 10})
    derived.write(db, _Cov(), places=places)
    db.commit()
    path = tmp_path / "bundle.sqlite"
    out = sqlite3.connect(path)
    db.backup(out)
    out.close()
    db.close()
    common._LOADED.pop(str(path), None)
    return str(path)


def test_every_key_reads_the_bundles_kind_named_or_not(tmp_path):
    """A key no named water carries has its unnamed sections' kind; a named one its water's. MUTATION: the pre-T1249
    loader (a set's kind from `item.kind` of its named sections only) leaves sets 8, 9 and 10 kindless: red."""
    B = common.load(_bundle(tmp_path))
    got = {k.set_id: common.water_kind(B, k) for k in B.key_ix}
    assert got == {7: "stream", 8: "stream", 9: "lake", 10: "wetland"}


def test_a_key_with_no_kind_is_refused_by_name(tmp_path):
    """A bundle built without places (a hand-built fixture) has no kind for a key no named water carries: the gear
    is refused, naming the key, never read with None. MUTATION: `water_kind` returning `B.key_kind.get(key)`: red."""
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA.read_text())
    db.execute("INSERT INTO item (ord, item_id, name, kind) VALUES (1, 'gnis:1', 'One River', 'stream')")
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", [(1, 1)])
    section_sets(db, {1: 7, 4: 8})
    derived.write(db, _Cov())                                   # no places: no unnamed part, key 8 kindless
    db.commit()
    path = tmp_path / "noplaces.sqlite"
    out = sqlite3.connect(path)
    db.backup(out)
    out.close()
    B = common.load(str(path))
    assert common.water_kind(B, RuleKey(7, False, False)) == "stream"
    with pytest.raises(AnswersError, match=r"rule key .*set_id=8.* has no water kind"):
        common.water_kind(B, RuleKey(8, False, False))


#: the province's line rule as the bundle holds it (`zp:terminal_tackle::terminal_tackle.r1`): 1 line; 2 while alone
#: in a boat on a lake
_LINES = G.Rule("zp:lines", 4, {"family": "gear_and_method", "type": "tackle_restriction", "gear": [
    {"max": 2.0, "slot": "lines_per_angler", "when": {"angler": "alone_in_boat", "water": "lake"}},
    {"max": 1.0, "slot": "lines_per_angler"}]})


@pytest.mark.parametrize("kind, says", [("stream", "1 line (rod)"), ("wetland", "1 line (rod)"),
                                        ("lake", "1 line (rod); 2 while alone in a boat")])
def test_the_lake_clause_holds_on_a_lake_only(kind, says):
    """The whole Lines row and the sentence, per kind (AGENTS 33: the whole string). MUTATION: restoring G3's old
    branch (a clause's `water` kept as a circumstance `water=lake`) on any kind: red (the unnamed stream read
    "1 line (rod); 2 while alone in a boat on a lake")."""
    a = G.resolve([_LINES], kind, ["angling", "ice_fishing"])
    assert a["counts"]["lines_per_angler"]["says"] == says
    assert all(not t.startswith("water=") for c in a["circumstantial"] for t in c.get("while", []))
    assert a["summary"]["says"] == ("1 line (2 while alone in a boat)." if kind == "lake" else "1 line.")
    assert ("ice_fishing" in {w["method"] for w in a["ways"]}) is (kind != "stream")


def test_no_kind_is_no_answer():
    """MUTATION: `_entries` reading a None kind as "keep every clause": red."""
    with pytest.raises(AnswersError, match="no water kind"):
        G.resolve([_LINES], None, ["angling"])


@pytest.fixture(scope="module")
def bundle(request):
    p = common.bundle_path()
    need(request, "bundle", p, BUNDLE_HINT)
    return common.load(p)


@pytest.mark.needs_bundle
def test_the_keys_no_named_water_carries_read_their_kind(bundle):
    """On the bundle under test: every key no named water carries (133 on main's r-7d7848399e53) has a kind, and its gear
    answer at every reading says no lake clause on a stream or wetland, and lists no ice fishing on a stream. MUTATION:
    reverting `gear_answer` to the set's named kind: every such key is red."""
    from pipeline.deliver.verdicts.store import VerdictStore
    B = bundle
    db = common.connect(B.path)
    try:
        named = {k for (k,) in db.execute(
            "SELECT DISTINCT r.key_ix FROM section_ruleset r JOIN item_section i ON i.sid = r.sid")}
    finally:
        db.close()
    store = VerdictStore.open(Path(B.path).with_name("verdicts.sqlite"), B.path)
    lawful = G.province_methods(B.rules.values())
    keys = [k for k, ix in sorted(B.key_ix.items(), key=lambda t: t[1]) if ix not in named]
    assert len(keys) >= 100, len(keys)
    seen = {}
    for k in keys:
        kind = common.water_kind(B, k)
        for a in G.gear_readings(B, k, lawful, store=store):           # every reading, at its moment
            _no_lake_clause_off_a_lake(k, kind, a)
        seen[kind] = seen.get(kind, 0) + 1
    assert set(seen) >= {"stream", "wetland"}, seen


def _no_lake_clause_off_a_lake(k, kind, a):
    says = (a["counts"].get("lines_per_angler") or {}).get("says") or ""
    assert "on a lake" not in says, (k, kind, says)
    assert all(not t.startswith("water=") for c in a["circumstantial"] for t in c.get("while", [])), k
    if kind == "stream":
        assert "ice_fishing" not in {w["method"] for w in a["ways"]}, k
