"""THE STATUS INDEX AGREES WITH THE REFERENCE READER — `pipeline.deliver.status_index`.

The index answers "closed / own / base" for a section on a day from a precomputed table; the
reference is `read.effective_rules`, asked once per game fish on that one section and day. The
file is a PROJECTION of the stored verdicts (`verdicts.sqlite`: the reader run once per rule key
and reading — DATAFLOW P4); these tests ask the reader DIRECTLY, section by section, with no
shortcut, and hold the file to it.

Fast tests (no bundle needed): the format round-trips, the calendar (Feb 29 is stored as day 60
every year and READ as Feb 28, day 59, P06; a season over New Year), the rollup, and the decoder
refusing a damaged file.

Slow tests (`-m slow`, ~3 min; they build the index from the shipped bundle, or read the file at
`STATUS_INDEX`):
  * PARITY — 6,000+ (section, date) pairs: listed sections on random days, absent sections on
    random days, every profile's boundary days, Dec 31 / Jan 1 (year wrap), Feb 29 (2024 and
    2028), the steelhead sections, the tidal one and outside-B.C. ones.
  * ABSENT IS BASE — every ruleset an unlisted section carries binds no water-table row, and one
    section of each such ruleset reads base on 26 days across the year.
  * VINTAGE — the file's digest is the bundle's and the tiles' sidecar's.
  * MUTATION — the parity check fails on a file with one section's profile swapped, so it is
    not a check that cannot fail (`checks-seeded-from-input-launder`).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import random
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver import status_index as SI
from pipeline.tests import under_test
from pipeline.tests.conftest import need, BUNDLE_HINT

BUNDLE = str(under_test.path("bundle"))


# --------------------------------------------------------------------------- fast: the format
def _tiny() -> dict:
    wrap = [SI.CLOSED] * 90 + [SI.OWN] * (366 - 90 - 31) + [SI.CLOSED] * 31   # Dec 1 - Mar 30
    leap = [SI.BASE] * 366
    leap[58] = SI.CLOSED            # Feb 28 only: Feb 29 must read it (P06), where day 60 would read base
    profiles = sorted({tuple(wrap), tuple(leap), (SI.OWN,) * 366, (SI.OUTSIDE,) * 366},
                      key=lambda p: (SI.runs_of(p), p))
    ix = {p: i for i, p in enumerate(profiles)}
    return {
        "handles": "0123456789abcdef",          # invented, as counting runs (T667: no build's digest in a test)
        "reach_digest": "fedcba9876543210",
        "profiles": profiles,
        "sections": {3: ix[tuple(wrap)], 4: ix[tuple(wrap)], 5: ix[(SI.OWN,) * 366],
                     900: ix[tuple(leap)], 1_958_036: ix[(SI.OUTSIDE,) * 366]},
        "items": {"gnis:1": ix[tuple(wrap)], "gnis:10": ix[(SI.OWN,) * 366],
                  "fwa:é": ix[tuple(leap)]},
    }


def test_the_format_round_trips():
    idx = _tiny()
    data = SI.encode(idx)
    back = SI.Index(data)
    assert back.handles == idx["handles"]
    assert back.reach_digest == idx["reach_digest"]
    assert data[4] == 2 and len(data) > 21                    # version 2: both digests up front
    assert back.profiles == idx["profiles"]
    assert back.sections == idx["sections"]
    assert back.items == idx["items"]
    assert SI.encode(idx) == data                       # deterministic


def test_feb_29_is_day_60_in_every_year_and_march_1_is_61():
    assert CAL.day_of(dt.date(2024, 2, 29)) == 60
    # ...and is READ as Feb 28 (P06: the year keeps its 365 reading days; slot 60 is never read)
    assert CAL.reading_day_of(dt.date(2024, 2, 29)) == 59 == CAL.reading_day_of((2, 28))
    assert all(CAL.reading_day_of(CAL.month_day(d)) == d for d in range(1, 367) if d != 60)
    assert CAL.day_of(dt.date(2025, 3, 1)) == 61 == CAL.day_of(dt.date(2024, 3, 1))
    assert CAL.day_of(dt.date(2025, 12, 31)) == 366
    assert all(CAL.day_of(CAL.month_day(d)) == d for d in range(1, 367))


def test_a_season_over_new_year_and_a_leap_day_read_back():
    back = SI.Index(SI.encode(_tiny()))
    assert back.code(3, dt.date(2025, 12, 31)) == SI.CLOSED
    assert back.code(3, dt.date(2026, 1, 1)) == SI.CLOSED
    assert back.code(3, dt.date(2026, 3, 30)) == SI.CLOSED       # day 90
    assert back.code(3, dt.date(2026, 3, 31)) == SI.OWN          # day 91
    assert back.code(3, dt.date(2026, 12, 1)) == SI.CLOSED       # day 336
    assert back.code(3, dt.date(2026, 11, 30)) == SI.OWN
    assert back.code(900, dt.date(2024, 2, 29)) == SI.CLOSED       # reads Feb 28's day (P06)
    assert back.code(900, dt.date(2024, 2, 28)) == SI.CLOSED
    assert back.code(900, dt.date(2024, 2, 27)) == SI.BASE
    assert back.code(900, dt.date(2024, 3, 1)) == SI.BASE
    assert back.code(6, dt.date(2026, 1, 1)) == SI.BASE           # absent = base
    assert back.item_code("fwa:é", (2, 29)) == SI.CLOSED
    assert back.item_code("gnis:2", (2, 29)) == SI.BASE


def test_a_damaged_file_is_refused():
    data = bytearray(SI.encode(_tiny()))
    with pytest.raises(ValueError):
        SI.Index(b"XXXX" + bytes(data[4:]))
    data[4] = 9
    with pytest.raises(ValueError):
        SI.Index(bytes(data))
    with pytest.raises((ValueError, IndexError)):
        SI.Index(SI.encode(_tiny())[:-3])


def test_the_rollup_reads_the_floor_not_the_days_code():
    closed, own, base = (SI.CLOSED,) * 366, (SI.OWN,) * 366, (SI.BASE,) * 366
    # a part its own row closes, beside a base part: the water still has its own regulations
    assert set(SI.rollup([(closed, SI.OWN), (base, SI.BASE)])) == {SI.OWN}
    # a part a ZONE closure closes, beside a base part: base regulations only
    assert set(SI.rollup([(closed, SI.BASE), (base, SI.BASE)])) == {SI.BASE}
    assert set(SI.rollup([(closed, SI.BASE), (closed, SI.OWN)])) == {SI.CLOSED}
    assert set(SI.rollup([(own, SI.OWN), ((SI.OUTSIDE,) * 366, SI.BASE)])) == {SI.OWN}
    assert set(SI.rollup([((SI.OUTSIDE,) * 366, SI.BASE)])) == {SI.OUTSIDE}


def test_a_tidal_part_does_not_speak_for_a_water_that_has_fresh_water():
    """T672 (T462 audit W2): the tidal piece of a river is not on the freshwater map, so the water's roll-up is its
    freshwater parts' — closed when every one of them is. A water that is tidal throughout is still tidal."""
    closed, own, base = (SI.CLOSED,) * 366, (SI.OWN,) * 366, (SI.BASE,) * 366
    tidal, outside = (SI.TIDAL,) * 366, (SI.OUTSIDE,) * 366
    assert set(SI.rollup([(closed, SI.OWN), (tidal, SI.BASE)])) == {SI.CLOSED}
    assert set(SI.rollup([(closed, SI.BASE), (closed, SI.OWN), (tidal, SI.BASE), (outside, SI.BASE)])) == {SI.CLOSED}
    assert set(SI.rollup([(own, SI.OWN), (tidal, SI.BASE)])) == {SI.OWN}
    assert set(SI.rollup([(base, SI.BASE), (tidal, SI.BASE)])) == {SI.BASE}
    # one freshwater part open: not closed, whatever the tidal piece
    assert set(SI.rollup([(closed, SI.OWN), (base, SI.BASE), (tidal, SI.BASE)])) == {SI.OWN}
    assert set(SI.rollup([(tidal, SI.BASE)])) == {SI.TIDAL}
    assert set(SI.rollup([(tidal, SI.BASE), (outside, SI.BASE)])) == {SI.TIDAL}
    # day by day: closed only on the days the fresh water is
    half = (SI.CLOSED,) * 100 + (SI.OWN,) * 266
    assert SI.rollup([(half, SI.OWN), (tidal, SI.BASE)]) == half


def test_a_closure_must_be_unconditional_and_speak():
    shut = {"take": 0, "may_target": 0, "state": "speaks"}
    assert SI.closes([shut])
    assert not SI.closes([dict(shut, state="beside")])
    assert not SI.closes([dict(shut, state="not_yet_mapped")])
    assert not SI.closes([dict(shut, partly_lifted=True)])
    assert not SI.closes([dict(shut, side="west")])
    assert not SI.closes([dict(shut, lengths=[{"min_cm": 50}])])
    assert not SI.closes([dict(shut, undrawn_part="200 m of the bridge")])
    assert not SI.closes([{"take": 0, "may_target": 1, "state": "speaks"}])     # a release
    assert "CRA" not in SI.GAME_FISH and "RB" in SI.GAME_FISH and len(SI.GAME_FISH) == 21


# --------------------------------------------------------------------------- slow: the shipped data
@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    return SI.Index(SI.encode(SI.compute(BUNDLE, str(under_test.path("verdicts")), log=lambda *_: None)))


def _pairs(ix: SI.Index, n: int = 6000, seed: int = 20261001) -> list:
    rnd = random.Random(seed)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        top = db.execute("SELECT max(sid) FROM section_ruleset").fetchone()[0]
        steel = [s for (s,) in db.execute("SELECT DISTINCT sid FROM steelhead_water")]
        tidal = [s for (s,) in db.execute("SELECT sid FROM tidal")]
        outside = [s for (s,) in db.execute("SELECT sid FROM outside_bc ORDER BY sid")]
    finally:
        db.close()

    def day() -> dt.date:
        return dt.date(2025, 1, 1) + dt.timedelta(days=rnd.randrange(365))

    listed = sorted(ix.sections)
    out = []
    special = [dt.date(2025, 12, 31), dt.date(2026, 1, 1), dt.date(2024, 2, 29),
               dt.date(2028, 2, 29), dt.date(2025, 2, 28), dt.date(2025, 3, 1)]
    # every profile: one section carrying it, on each day its status changes (and the day before)
    by_profile: dict = {}
    for sid in listed:
        by_profile.setdefault(ix.sections[sid], sid)
    for pi, sid in sorted(by_profile.items()):
        d = 1
        for length, _ in SI.runs_of(ix.profiles[pi]):
            for x in (d, d + length - 1):
                m, dd = CAL.month_day(x)
                out.append((sid, dt.date(2024, m, dd)))
            d += length
    # steelhead water is ~149,000 sections since its tributaries joined it (user ruling
    # 2026-10-01): a sample, as for the sections outside B.C. — every one would be 300,000 reads
    for sid in (rnd.sample(steel, min(500, len(steel))) + tidal
                + rnd.sample(outside, min(20, len(outside)))):
        out += [(sid, day()), (sid, rnd.choice(special))]
    while len(out) < n * 0.6:
        out.append((rnd.choice(listed), day() if rnd.random() < 0.8 else rnd.choice(special)))
    while len(out) < n:
        sid = rnd.randrange(1, top + 1)
        if sid not in ix.sections:
            out.append((sid, day() if rnd.random() < 0.8 else rnd.choice(special)))
    return out


def _mismatches(ix: SI.Index, pairs) -> list:
    return [(sid, on, ix.code(sid, on), want) for sid, on in pairs
            if ix.code(sid, on) != (want := SI.status_by_reader(sid, on, BUNDLE))]


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_index_agrees_with_effective_rules_on_6000_section_days(built):
    pairs = _pairs(built)
    assert len(pairs) >= 6000
    assert any(on.month == 2 and on.day == 29 for _, on in pairs)
    assert any(on.month == 12 and on.day == 31 for _, on in pairs)
    bad = _mismatches(built, pairs)
    assert not bad, f"{len(bad)} of {len(pairs)} disagree, e.g. {bad[:5]}"
    seen = {built.code(s, on) for s, on in pairs}
    assert {SI.BASE, SI.OWN, SI.CLOSED, SI.TIDAL, SI.OUTSIDE} <= seen


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_every_rule_key_tidal_and_outside_section_agrees_with_effective_rules(built):
    """T306 (owner: the colouring and the full rule calculation are ONE logic): not a sample of
    sections — EVERY rule key the bundle binds (one section carrying it, on each day its code
    changes, the day before, and mid-run), EVERY tidal section and EVERY outside-B.C. section
    (with or without a rule set: 99 of 100 tidal sections carry none, and the old parallel copy in
    `compute` read them base). 0 disagreements with `status_by_reader`."""
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        one_per_key = dict(db.execute("SELECT key_ix, min(sid) FROM section_ruleset GROUP BY key_ix"))
        tidal, outside = SI.special_sections(db)
    finally:
        db.close()
    pairs = []
    for sid in sorted(one_per_key.values()):
        pi = built.sections.get(sid)
        runs = SI.runs_of(built.profiles[pi]) if pi is not None else [(366, SI.BASE)]
        d = 1
        for length, _ in runs:
            for x in {d, d + length // 2, d + length - 1}:
                m, dd = CAL.month_day(x)
                pairs.append((sid, dt.date(2024, m, dd)))
            d += length
    for sid in sorted(tidal | outside):
        pairs += [(sid, dt.date(2025, 7, 14)), (sid, dt.date(2025, 12, 31)), (sid, dt.date(2024, 2, 29))]
    bad = _mismatches(built, pairs)
    assert not bad, f"{len(bad)} of {len(pairs)} disagree, e.g. {bad[:5]}"
    assert all(built.code(s, dt.date(2025, 7, 14)) == SI.TIDAL for s in tidal)


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_a_water_is_closed_exactly_when_all_its_fresh_water_is(built):
    """T672 (T462 audit W2), over every water of the bundle under test and every day: the water's roll-up (the search
    row's dot) is closed exactly when every section of it the map draws as fresh water (not tidal, not outside B.C.) is
    closed. The sections are the index's own answers, held to the reader by the parity tests above."""
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        secs: dict = {}
        for item, sid in db.execute("SELECT i.item_id, s.sid FROM item i JOIN item_section s ON s.ord = i.ord"):
            secs.setdefault(item, []).append(sid)
    finally:
        db.close()
    days = [dt.date(2024, 1, 1) + dt.timedelta(days=d) for d in range(366)]
    bad, with_tidal = [], 0
    for item, sids in sorted(secs.items()):
        codes = [[built.code(s, d) for d in days] for s in sids]
        if not any(SI.TIDAL in c for c in codes):
            continue                      # no tidal piece: the roll-up the tests above already pin
        with_tidal += 1
        for i, d in enumerate(days):
            fresh = [c[i] for c in codes if c[i] not in (SI.TIDAL, SI.OUTSIDE)]
            if fresh and (built.item_code(item, d) == SI.CLOSED) != all(c == SI.CLOSED for c in fresh):
                bad.append((item, d.isoformat()))
    assert with_tidal, "the bundle has waters with a tidal piece"
    assert not bad, f"{len({i for i, _ in bad})} water(s), {len(bad)} water-days, e.g. {bad[:5]}"


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_shipped_index_is_the_one_the_parity_tests_check(built):
    """The file on disk beside the bundle (the release's `paint.status` is its bytes,
    test_release) IS a fresh projection — so the parity above holds for what ships.

    With `STATUS_INDEX` set, the parity tests checked THAT file, so it is the one the shipped bytes
    must equal. A missing shipped index fails naming the make-command (no silent skip)."""
    shipped = under_test.status_index()
    checked = SI.encode(SI.compute(BUNDLE, str(under_test.path("verdicts")), log=lambda *_: None))
    assert shipped.read_bytes() == checked, "status_index.bin is stale: rebuild deliver"


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_an_absent_section_is_truly_base(built):
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        rows = db.execute("SELECT sid, set_id FROM section_ruleset ORDER BY sid").fetchall()
        rows_of = {}
        for set_id, e in db.execute("SELECT set_id, entry_id FROM ruleset"):
            rows_of.setdefault(set_id, set()).add(e)
    finally:
        db.close()
    absent_sets: dict = {}
    for sid, set_id in rows:
        if sid not in built.sections:
            absent_sets.setdefault(set_id, sid)
    assert absent_sets
    for set_id, sid in absent_sets.items():
        # no water-table row is bound (SQL alone, not the builder's predicate)
        assert all(e.startswith("z") for e in rows_of.get(set_id, ())), (set_id, sid)
        for m in range(1, 13):
            for d in (1, 15):
                assert SI.status_by_reader(sid, (m, d), BUNDLE) == SI.BASE, (sid, m, d)
        assert SI.status_by_reader(sid, (2, 29), BUNDLE) == SI.BASE
        assert SI.status_by_reader(sid, (12, 31), BUNDLE) == SI.BASE


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_vintage_is_the_bundles_and_the_tiles(built):
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    try:
        want = db.execute("SELECT v FROM meta WHERE k = 'section_handles'").fetchone()[0]
    finally:
        db.close()
    assert built.handles == want
    sidecar = under_test.path("tiles") / "atlas.meta.json"
    if sidecar.exists():
        assert json.loads(sidecar.read_text())["section_handles"] == built.handles


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_parity_check_catches_a_swapped_profile(built):
    """Mutation: the check above is not one that cannot fail."""
    pairs = _pairs(built, n=400, seed=7)
    sid, on = next((s, o) for s, o in pairs if built.code(s, o) == SI.CLOSED)
    pi = built.sections[sid]
    other = next(i for i, p in enumerate(built.profiles)
                 if p[CAL.day_of(on) - 1] != SI.CLOSED)
    built.sections[sid] = other
    try:
        assert _mismatches(built, [(sid, on)])
    finally:
        built.sections[sid] = pi


@pytest.mark.needs_bundle
@pytest.mark.slow
def test_the_parity_check_catches_one_flipped_stored_verdict(built, tmp_path):
    """Mutation (DATAFLOW G1): the index is a projection of the verdicts, so a stored verdict
    flipped — one closed reading read as open — must turn the reader parity red."""
    import shutil
    src = under_test.path("verdicts")
    pairs = _pairs(built, n=400, seed=7)
    sid, on = next((s, o) for s, o in pairs if built.code(s, o) == SI.CLOSED)
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    key = db.execute("SELECT key_ix FROM section_ruleset WHERE sid = ?", (sid,)).fetchone()[0]
    db.close()
    from pipeline.deliver.verdicts.store import VerdictStore
    st = VerdictStore.open(src, BUNDLE)
    # a day is closed when it is closed at every moment of its key (answers 2.1): flip them all
    readings = {st.reading_of(key, CAL.day_of(on), m) for m in range(len(st.moments(key)))}
    dst = tmp_path / "verdicts.sqlite"
    shutil.copy(src, dst)
    v = sqlite3.connect(dst)
    v.executemany("UPDATE reading SET closed = 0 WHERE key_ix = ? AND reading = ?",
                  [(key, r) for r in readings])
    v.commit()
    v.close()
    flipped = SI.Index(SI.encode(SI.compute(BUNDLE, str(dst), log=lambda *_: None)))
    assert flipped.code(sid, on) != SI.CLOSED and _mismatches(flipped, [(sid, on)])


# --------------------------------------------------------------------------- the app's fixture
APP_FIXTURE = Path(__file__).resolve().parents[2] / "app/packages/core/src/statusIndex.fixture.ts"
_HEAD = "// GENERATED by pipeline/tests/test_status_index.py — do not edit.\nexport const FIXTURE = "


def app_fixture() -> dict:
    """The tiny file and what the PYTHON reader says it means, for the app's decoder test
    (`statusIndex.test.ts`): one encoding, read by both languages, must give one answer."""
    import base64
    data = SI.encode(_tiny())
    back = SI.Index(data)
    days = [dt.date(2025, 1, 1), dt.date(2025, 3, 30), dt.date(2025, 3, 31),
            dt.date(2025, 11, 30), dt.date(2025, 12, 1), dt.date(2025, 12, 31),
            dt.date(2024, 2, 28), dt.date(2024, 2, 29), dt.date(2024, 3, 1),
            dt.date(2025, 3, 1), dt.date(2025, 7, 15)]
    return {
        "$comment": "GENERATED by pipeline/tests/test_status_index.py (UPDATE_APP_FIXTURE=1). "
                    "The bytes are pipeline.deliver.status_index.encode of the test's tiny index; "
                    "the answers are the Python reader's.",
        "bytes": base64.b64encode(data).decode(),
        "handles": back.handles,
        "reach_digest": back.reach_digest,
        "sections": [[s, d.isoformat(), SI.CODE_NAMES[back.code(s, d)]]
                     for s in (0, 2, 3, 4, 5, 6, 899, 900, 901, 1_958_036, 1_958_037)
                     for d in days],
        "waters": [[i, d.isoformat(), SI.CODE_NAMES[back.item_code(i, d)]]
                   for i in ("gnis:1", "gnis:10", "gnis:2", "fwa:é") for d in days],
    }


def test_the_app_fixture_is_current():
    want = app_fixture()
    if os.environ.get("UPDATE_APP_FIXTURE"):
        APP_FIXTURE.write_text(_HEAD + json.dumps(want, ensure_ascii=False) + ";\n")
    text = APP_FIXTURE.read_text()
    assert text.startswith(_HEAD) and json.loads(text[len(_HEAD):].rstrip().rstrip(";")) == want, \
        "run UPDATE_APP_FIXTURE=1 pytest pipeline/tests/test_status_index.py"
