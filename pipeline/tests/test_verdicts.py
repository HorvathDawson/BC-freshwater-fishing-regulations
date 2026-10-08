"""THE VERDICTS STAGE (DATAFLOW P3, `pipeline/deliver/verdicts/`): the reader run once, stored as
returned. Fast: the typed row conversion refuses a malformed answer, and `check` goes red on a
dropped frame, an orphaned `by` and a flipped `closed` (mutation-pinned on a small real build).
Slow: every stored frame equals a fresh reader call (every key, sampled readings), and the origin
shortcut equals full asks on every key.

The bundle is `UI_EXPORT_BUNDLE` (a bundle with the P2 tables); the full verdicts file is
`UI_EXPORT_VERDICTS` (default: beside the bundle)."""
from __future__ import annotations

import os
import random
import shutil
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.deliver import types as T
from pipeline.deliver.bundle import read
from pipeline.deliver.calendar import month_day
from pipeline.deliver.verdicts import build as VB
from pipeline.deliver.verdicts.check import check
from pipeline.deliver.verdicts.store import VerdictStore, VerdictsError

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")
VERDICTS = Path(os.environ.get("UI_EXPORT_VERDICTS") or BUNDLE.with_name("verdicts.sqlite"))


def _bundle():
    if not BUNDLE.exists():
        pytest.skip(f"no bundle at {BUNDLE}")
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'rule_key'").fetchone():
        pytest.skip(f"{BUNDLE} predates the rule_key table (DATAFLOW P2)")
    return db


def test_speaker_codes_are_the_first_four():
    assert T.SPEAKER_CODES == {0, 1, 2, 3}


def test_a_traced_answer_converts_exactly_and_refuses_a_malformed_one():
    ix = {"z1:a::a.r1": 0, "r1:b::b.r1": 1}
    ans = [{"entry": "z1:a", "rule": "a.r1", "state": "displaced", "reason": "ladder",
            "by": "r1:b::b.r1"},
           {"entry": "r1:b", "rule": "b.r1", "state": "speaks", "partly_lifted": True,
            "lifted_in_part_by": ["z1:a::a.r1"]}]
    assert VB.verdict_of(ans, ix) == ((0, 5, 1, 1, ()), (1, 0, None, None, (0,)))
    with pytest.raises(VerdictsError):
        VB.verdict_of([{**ans[1], "reason": "ladder"}], ix)
    with pytest.raises(ValueError):                      # a reason giving another state
        VB.verdict_of([{**ans[0], "state": "moot"}], ix)
    with pytest.raises(VerdictsError):
        VB.verdict_of([{**ans[1], "lifted_in_part_by": []}], ix)


def test_asked_fish_are_every_game_fish_plus_the_named_extras():
    got = VB.asked_fish([{"species": ["CRA"]}, {"species": ["SALMON"]},
                         {"when_targeting": ["NOOKSACK_DACE"]}])
    assert set(T.GAME_FISH) <= set(got)
    assert {"CRA", "CH", "NOOKSACK_DACE"} <= set(got) and "GREEN_STURGEON" not in got
    assert VB.asked_fish([]) == T.GAME_FISH


@pytest.fixture(scope="module")
def small(tmp_path_factory):
    """A real build of 12 keys (spread over the bundle), one worker."""
    db = _bundle()
    keys = [k for (k,) in db.execute("SELECT key_ix FROM rule_key ORDER BY key_ix")]
    pick = keys[:: max(1, len(keys) // 12)][:12]
    out = tmp_path_factory.mktemp("v") / "verdicts.sqlite"
    VB.build(str(BUNDLE), out, workers=1, keys=pick, log=lambda *_: None)
    return out, pick


def _mutated(src: Path, tmp: Path, sql: str) -> sqlite3.Connection:
    dst = tmp / "m.sqlite"
    shutil.copy(src, dst)
    db = sqlite3.connect(dst)
    db.execute(sql)
    db.commit()
    return db


def test_a_small_build_checks_and_opens(small):
    out, pick = small
    db = sqlite3.connect(out)
    assert check(db, str(BUNDLE), all_keys=False) == []
    S = VerdictStore.open(out, BUNDLE)
    k = pick[0]
    assert S.runs(k)[0][0] == 1 and set(S.fish(k)) >= set(T.GAME_FISH)
    v = S.on_day(k, 200, "RB")
    assert all(isinstance(r, T.VerdictRow) for r in v)


def test_check_goes_red_on_a_dropped_frame_an_orphan_by_and_a_flipped_closed(small, tmp_path):
    out, _ = small
    db = _mutated(out, tmp_path, "DELETE FROM frame WHERE (key_ix, reading, fish, origin) IN "
                  "(SELECT key_ix, reading, fish, origin FROM frame LIMIT 1)")
    assert any("frames" in p for p in check(db, str(BUNDLE), all_keys=False))
    db = _mutated(out, tmp_path, "UPDATE verdict_rule SET by_rule = 999999 WHERE (verdict, rule) IN "
                  "(SELECT verdict, rule FROM verdict_rule WHERE by_rule IS NOT NULL LIMIT 1)")
    assert any("outside its set" in p for p in check(db, str(BUNDLE), all_keys=False))
    db = _mutated(out, tmp_path, "UPDATE reading SET closed = 1 - closed WHERE (key_ix, reading) IN "
                  "(SELECT key_ix, reading FROM reading LIMIT 1)")
    assert any("closed" in p for p in check(db, str(BUNDLE), all_keys=False))


def test_open_refuses_another_bundle_and_a_moved_enum(small, tmp_path):
    out, _ = small
    db = _mutated(out, tmp_path, "UPDATE meta SET v = 'ffffffffffffffff' WHERE k = 'reach_digest'")
    db.close()
    with pytest.raises(VerdictsError, match="reach_digest"):
        VerdictStore.open(tmp_path / "m.sqlite", BUNDLE)
    db = _mutated(out, tmp_path, "UPDATE enum_reason SET name = 'won' WHERE code = 0")
    db.close()
    with pytest.raises(VerdictsError, match="enum_reason"):
        VerdictStore.open(tmp_path / "m.sqlite", BUNDLE)


def test_two_builds_give_the_same_verdicts(small, tmp_path):
    out, pick = small
    again = tmp_path / "again.sqlite"
    VB.build(str(BUNDLE), again, workers=2, keys=pick, log=lambda *_: None)
    assert _dump(out) == _dump(again)


def _dump(path: Path) -> list:
    db = sqlite3.connect(path)
    out = []
    for (t,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"):
        out.append((t, sorted(db.execute(f"SELECT * FROM {t}").fetchall())))
    return out


# --------------------------------------------------------------------------------------------
# Slow, on the full file: the stored answer IS the reader's
# --------------------------------------------------------------------------------------------

def _full():
    _bundle()
    if not VERDICTS.exists():
        pytest.skip(f"no verdicts at {VERDICTS}")
    return VerdictStore.open(VERDICTS, BUNDLE)


@pytest.mark.slow
def test_every_stored_frame_is_a_fresh_reader_call():
    """Every key, one sampled reading and three sampled fish each, all three origins: the stored
    verdict equals the traced reader asked afresh on the reading's first day, at its moment."""
    S = _full()
    db = _bundle()
    bound = {}
    for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset "
                                   "ORDER BY set_id, entry_id, rule_id"):
        bound.setdefault(s, []).append((e, r, via))
    ix = {k: i for i, k in enumerate(S.rule_ids)}
    rng = random.Random(7)
    n = 0
    for k, s, sw, sr in db.execute("SELECT key_ix, set_id, steelhead_water, steelhead_rules "
                                   "FROM rule_key"):
        rd = rng.choice(S.readings(k))
        for f in rng.sample(S.fish(k), 3):
            for o in ("none", "hatchery", "wild"):
                want = VB.verdict_of(read.effective_rules_bound(
                    bound[s], bool(sw), month_day(rd.first_day), f, str(BUNDLE),
                    steelhead_rules_here=bool(sr), origin=None if o == "none" else o, trace=True,
                    at=S.moments(k)[rd.moment]), ix)
                assert S.rows(S.verdict_id(k, rd.ix, f, o)) == want, (k, rd, f, o)
                n += 1
    assert n > 10_000


@pytest.mark.slow
def test_the_origin_shortcut_equals_full_asks_on_every_key():
    """Where `read.origin_matters` is false, hatchery and wild were not asked: asking them on every
    such key (every reading's first day, every game fish) gives the none verdict."""
    S = _full()
    db = _bundle()
    bound = {}
    for s, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset"):
        bound.setdefault(s, []).append((e, r, via))
    ix = {k: i for i, k in enumerate(S.rule_ids)}
    checked = 0
    for k, s, sw, sr in db.execute("SELECT key_ix, set_id, steelhead_water, steelhead_rules "
                                   "FROM rule_key"):
        if read.origin_matters(bound[s], str(BUNDLE)):
            continue
        for rd in S.readings(k):
            for f in T.GAME_FISH:
                for o in read.ASKABLE_ORIGINS:
                    got = VB.verdict_of(read.effective_rules_bound(
                        bound[s], bool(sw), month_day(rd.first_day), f, str(BUNDLE),
                        steelhead_rules_here=bool(sr), origin=o, trace=True,
                        at=S.moments(k)[rd.moment]), ix)
                    assert got == S.rows(S.verdict_id(k, rd.ix, f, "none")), (k, rd.ix, f, o)
                    checked += 1
    assert checked > 10_000
