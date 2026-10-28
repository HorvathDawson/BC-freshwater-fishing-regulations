"""T213: THE METHOD FILE (`pipeline/deliver/method.py`) — bait ban / fly fishing only per section and day.

  * a day's class from its segments' open-days strips: a day closed at every moment is none (closed is the status
    index's); else the gear held at EVERY open moment; fly (fly fishing only, artificial flies only) over bait ban;
  * the strip bits are the answers' own (`answers.display.strip.STRIP`);
  * method/1 round-trips;
  * LIVE: no section is bait ban or fly only on a day the status index closes it, and every day the status index leaves
    open with a gear class is one the answers' strips name (two outputs, two paths: the verdicts' `reading.closed` vs
    the answers' display strips).
"""
from __future__ import annotations

import os
import random
from pathlib import Path

import pytest

from pipeline.deliver import method as M


def test_the_strip_bits_are_the_answers_own():
    from pipeline.deliver.answers.display.strip import STRIP_BIT
    assert (M.STRIP_CLOSED, M.STRIP_FLY, M.STRIP_ARTIFICIAL_FLY, M.STRIP_BAIT_BAN) == \
        (STRIP_BIT["closed"], STRIP_BIT["fly_only"], STRIP_BIT["artificial_fly_only"], STRIP_BIT["bait_ban"])


@pytest.mark.parametrize("strips, code", [
    ([0], M.NONE), ([None], M.NONE),            # open; a tidal frame has no strip
    ([2], M.NONE),                              # release only is not a gear class
    ([16], M.BAIT), ([18], M.BAIT),
    ([20], M.FLY), ([4], M.FLY), ([24], M.FLY), ([8], M.FLY),
    ([1], M.NONE), ([1, 1], M.NONE),            # closed: the status index says it
    ([1, 16], M.BAIT),                          # a night closure never closes the day; the open moment's gear holds
    ([16, 0], M.NONE),                          # a bait ban of some weekdays only is not painted
    ([20, 16], M.BAIT),                         # fly only some moments, bait ban all day
])
def test_a_days_class(strips, code):
    assert M.day_code(strips) == code


def test_a_keys_year_follows_its_segments_and_moments():
    # Cowichan-like: fly only Jan 1 → Jul 3 and from Sep 1, closed between; a moment pair on day 300
    p = M.key_profile([1, 186, 245, 300, 300], [20, 1, 20, 1, 16])
    assert len(p) == 366
    assert (p[0], p[184], p[185], p[243], p[244], p[298], p[299], p[365]) == \
        (M.FLY, M.FLY, M.NONE, M.NONE, M.FLY, M.FLY, M.BAIT, M.BAIT)
    with pytest.raises(SystemExit):
        M.key_profile([2], [0])


def test_method_1_round_trips():
    fly = tuple([M.FLY] * 100 + [M.NONE] * 66 + [M.BAIT] * 200)
    bait = (M.BAIT,) * 366
    m = {"handles": "b6b5000e171023c9", "profiles": [bait, fly],
         "sections": {3: 0, 4: 0, 5: 1, 900_000: 0, 900_001: 1}}
    ix = M.Index(M.encode(m))
    assert ix.handles == m["handles"] and ix.sections == m["sections"] and ix.profiles == m["profiles"]
    assert (ix.code(5, 1), ix.code(5, 101), ix.code(5, 366), ix.code(6, 1)) == (M.FLY, M.NONE, M.BAIT, M.NONE)
    with pytest.raises(ValueError):
        M.Index(M.encode(m) + b"\0")


@pytest.mark.needs_bundle
def test_live_no_gear_class_on_a_closed_day(request, live_store_of):
    from pipeline.common.curated import GENERATED
    from pipeline.deliver import status_index as SI
    from pipeline.deliver.bundle import read as R
    from pipeline.deliver.export.contract import OUT as EXPORT
    from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
    bundle = need(request, "bundle", Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE), BUNDLE_HINT)
    si = SI.Index(need(request, "bundle", GENERATED.bundle / "status_index.bin", BUNDLE_HINT).read_bytes())
    exp = need(request, "bundle", EXPORT.parent / "ui-rules-answers.json", EXPORT_HINT).parent
    m = M.compute(live_store_of(exp, bundle))
    assert m["handles"] == si.handles
    ix = M.Index(M.encode(m))
    sids = sorted(set(ix.sections) | set(si.sections))
    rng = random.Random(213)
    sample = rng.sample(sids, min(20_000, len(sids)))
    bad = []
    for sid in sample:
        sp = si.profiles[si.sections[sid]] if sid in si.sections else (SI.BASE,) * 366
        mp = ix.profiles[ix.sections[sid]] if sid in ix.sections else (M.NONE,) * 366
        for d in range(366):
            if sp[d] == SI.CLOSED and mp[d] != M.NONE:
                bad.append((sid, d + 1, SI.CODE_NAMES[sp[d]], M.CODE_NAMES[mp[d]]))
                break
    assert not bad, f"{len(bad)} sections with a gear class on a closed day, e.g. {bad[:5]}"
    ever = {c for p in ix.profiles for c in p}
    assert {M.BAIT, M.FLY} <= ever
