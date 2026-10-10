"""THE ANSWERS FILE v0 (`pipeline/deliver/answers/`): its shape, its pairing, and that every tap reads
exactly what the reference reader says.

  * the spec covers every section, and a section without a codec or spec text is refused;
  * encode -> decode is the identity, every integer ref resolves;
  * the file carries the export pair's digests and refuses another pair;
  * a tap (part -> key -> segment by date -> fish -> origin) equals `read.effective_rules` on a REAL
    section of that part, on real waters (Chilliwack, Denetiah, Kitimat, Ahbau, Thompson);
  * every day of a segment answers alike (the segmentation changes no answer);
  * the decided answer is `rows`' alone (answers/2: the v0 `answer` section and `decide` are gone).

`UI_EXPORT_BUNDLE` and `ANSWERS_EXPORT_DIR` (the export pair cut from that bundle) point the suite at
a side bundle; the bundle tests skip without them.
"""
from __future__ import annotations

import copy
import json
import os
import random
import sqlite3
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")
SPEC_MD = Path(A.__file__).with_name("ANSWERS-SPEC.md")

WATERS = ("gnis:8634",        # Chilliwack River: steelhead water, hatchery/wild, a closure stretch
          "gnis:39298",       # Denetiah Creek: the water's own closure (DENETIAH)
          "gnis:3225",        # Kitimat River: origin-qualified lifts (per-origin answers)
          "wbk:329097613",    # Ahbau Lake: two regions' tables (RU-8)
          "gnis:39492")       # Thompson River: a row's dated release over its quota (RU-3)


# --------------------------------------------------------------------------------------------
# The spec: every section has a codec, spec text, and a place in ANSWERS-SPEC.md
# --------------------------------------------------------------------------------------------

def test_every_section_has_a_codec_and_spec_text():
    names = {s.name for s in A.SECTIONS}
    assert names == set(E.CODECS)
    assert names <= set(E.SPEC["sections"])
    assert set(A.RESERVED) == set(E.SPEC["reserved"])
    assert not names & set(A.RESERVED)
    md = SPEC_MD.read_text(encoding="utf-8")
    for n in names | set(A.RESERVED):
        assert f"`{n}`" in md, f"ANSWERS-SPEC.md says nothing of section {n}"


def test_spec_gaps_refuse_an_undescribed_section():
    wire = {k: None for k in E.SPEC["top level"]}
    wire["sections"] = {"ladder": {k: None for k in E.SPEC["sections"]["ladder"]}}
    assert E.spec_gaps(wire) == []
    bad = copy.deepcopy(wire)
    bad["sections"]["mystery"] = {"version": 0}
    bad["sections"]["ladder"]["extra"] = []
    bad["surprise"] = 1
    gaps = " | ".join(E.spec_gaps(bad))
    for want in ("'mystery' has no codec", "'mystery' has no spec text",
                 "ladder.extra", "'surprise'"):
        assert want in gaps


# --------------------------------------------------------------------------------------------
# The calendar and the segments
# --------------------------------------------------------------------------------------------

def test_segments_start_on_day_one_and_wrap_as_two():
    sig = ["winter"] * 59 + ["feb29"] + ["spring"] * 200 + ["winter"] * 106
    assert A.segments_of(sig) == [1, 60, 61, 261]
    assert E.segment_index([1, 60, 61, 261], 366) == 3
    assert E.segment_index([1, 60, 61, 261], CAL.day_of((2, 29))) == 1
    assert CAL.day_of((3, 1)) == 61 and CAL.month_day(60) == (2, 29)
    with pytest.raises(CAL.CalendarError):
        A.segments_of(sig[:-1])
    with pytest.raises(A.AnswersError):
        E.segment_index([1], 367)


# --------------------------------------------------------------------------------------------
# On the bundle: a small file for five real waters
# --------------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = A.load_export(EXPORT_DIR)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["ladder", "rows"],
                    log=lambda *_: None)
    wire = E.encode(model, data)
    return data, guide, model, json.loads(json.dumps(wire))


@pytest.mark.needs_bundle
def test_round_trip(built):
    data, _, model, wire = built
    assert E.decode(wire, data) == model
    assert set(wire["sections"]) == {"ladder", "rows"}
    assert not set(wire["sections"]) & set(A.RESERVED)             # reserved: absent, not empty
    assert E.spec_gaps(wire) == []


@pytest.mark.needs_bundle
def test_integer_refs_resolve(built):
    data, _, _, wire = built
    nR, nF = len(data["rule_ids"]), len(wire["fish"])
    rule = lambda i: isinstance(i, int) and 0 <= i < nR             # noqa: E731
    for k in wire["keys"]:
        assert 0 <= k[E.SEG_SLOT] < len(wire["segments"])
    for item, ks in wire["parts"].items():
        assert len(ks) == len(data["waters"][item]["parts"])
        assert all(k is None or 0 <= k < len(wire["keys"]) for k in ks)
    for name, sec in wire["sections"].items():
        assert len(sec["at"]) == len(wire["keys"])
        for k, at in enumerate(sec["at"]):
            assert len(at) == len(wire["segments"][wire["keys"][k][E.SEG_SLOT]])
            assert all(0 <= f < len(sec["frames"]) for f in at)
    lad = wire["sections"]["ladder"]
    for common, rows in lad["frames"]:
        assert 0 <= common < len(lad["verdicts"])
        for row in rows:
            assert 0 <= row[0] < nF and len(row) in (2, 4)
            assert all(0 <= v < len(lad["verdicts"]) for v in row[1:])
    for v in lad["verdicts"]:
        assert all(rule(i) for lst in v[:4] for i in lst)
        assert all(rule(i) and all(rule(b) for b in by) for i, by in v[4])
        assert all(rule(i) and 0 <= r < len(lad["reasons"]) and rule(by) for i, r, by in v[5])
    rows = wire["sections"]["rows"]
    for d in rows["decided"]:
        assert rule(d["win"]) and (d["narrow"] is None or rule(d["narrow"]))


@pytest.mark.needs_bundle
def test_the_digest_stamp(built):
    data, guide, _, wire = built
    assert wire["about"]["bundle"] == data["about"]["bundle"] == guide["about"]["bundle"]
    assert wire["about"]["export"]["rule_ids_sha256"] == E.rule_ids_digest(data["rule_ids"])
    E.check_pair(wire, data, guide)
    other = copy.deepcopy(wire)
    other["about"]["bundle"]["reach_digest"] = "0" * 16
    with pytest.raises(A.AnswersError):
        E.check_pair(other, data, guide)
    shifted = dict(data, rule_ids=data["rule_ids"][1:] + data["rule_ids"][:1])
    with pytest.raises(A.AnswersError):
        E.check_pair(wire, shifted, guide)
    with pytest.raises(A.AnswersError):
        E.decode(wire, shifted)


@pytest.mark.needs_bundle
def test_a_pair_from_another_bundle_is_refused(built):
    data, guide, _, _ = built
    bad = copy.deepcopy(data)
    bad["about"]["bundle"]["section_handles"] = "f" * 16
    B = C.load(BUNDLE)
    with pytest.raises(A.AnswersError):
        C.check_export(B, bad, guide)
    with pytest.raises(A.AnswersError):
        C.check_export(B, bad, copy.deepcopy(bad) | {"about": bad["about"]})


def _section_of(db, key: tuple) -> int:
    """A real section carrying this part key's rule set and steelhead flags."""
    k = A.key_dict(key)
    sid = db.execute(
        "SELECT MIN(r.sid) FROM section_ruleset r WHERE r.set_id = ? "
        "AND EXISTS (SELECT 1 FROM steelhead_water w WHERE w.sid = r.sid) = ? "
        "AND EXISTS (SELECT 1 FROM section_steelhead_rules x WHERE x.sid = r.sid) = ?",
        (k["ruleset"], int(k["steelhead_water"]), int(k["steelhead_rules"]))).fetchone()[0]
    assert sid is not None, key
    return sid


@pytest.mark.needs_bundle
def test_taps_equal_the_reader_on_real_sections(built):
    data, _, model, wire = built
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    rng = random.Random(6)
    checked = 0
    try:
        for item in WATERS:
            for pi, k in enumerate(wire["parts"][item]):
                if k is None:
                    continue
                sid = _section_of(db, model.keys[k])
                for m, d in [(1, 1), (2, 29), (5, 15), (7, 10), (9, 23), (12, 31)] + \
                        [CAL.month_day(rng.randint(1, 366)) for _ in range(3)]:
                    fishes = list(model.sections["ladder"][k][0])
                    for fish in rng.sample(fishes, min(4, len(fishes))):
                        for origin in A.ORIGINS:
                            got = E.tap(wire, data, item, pi, m, d, fish, origin)
                            want = A.ladder_verdict(R.effective_rules(
                                sid, (m, d), fish, BUNDLE, trace=True,
                                origin=None if origin == "none" else origin))
                            assert got["ladder"] == want, (item, pi, m, d, fish, origin)
                            checked += 1
    finally:
        db.close()
    assert checked > 300


@pytest.mark.needs_bundle
def test_denetiah_closure_is_the_answer(built):
    data, _, _, wire = built
    got = E.tap(wire, data, "gnis:39298", 0, 7, 10, "DV", "wild")
    dv = got["rows"]["fish"]["DV"]["wild"]                     # the decided answer: rows' alone
    assert dv["status"] == "closed"
    assert data["rule_ids"][dv["win"]] == "r7:denetiah_creek@7-52::denetiah_creek.r1"
    liard = [k for k, v in got["ladder"].items() if k.startswith("r7:liard_river_watershed")
             and v[0] == "displaced"]
    # the row's daily 1 loses by the ladder (its own key); the possession 1 by DENETIAH
    assert {got["ladder"][k][2] for k in liard} == {"ladder", "water_closure"}
    assert all(got["ladder"][k][3] == "r7:denetiah_creek@7-52::denetiah_creek.r1" for k in liard)


@pytest.mark.needs_bundle
def test_every_day_of_a_segment_answers_alike(built):
    """The segmentation (member and lift `when` readings) changes no answer: every day of every
    segment of the Chilliwack's keys, for two fish, reads what the segment's value says."""
    data, _, model, wire = built
    ctx = A.Context(BUNDLE, C.check_export(C.load(BUNDLE), data, A.load_export(EXPORT_DIR)[1]))
    for k in {k for k in wire["parts"]["gnis:8634"] if k is not None}:
        rk = A.eval_key(model.keys[k])
        set_id, sw, sr = rk.set_id, rk.steelhead_water, rk.steelhead_rules
        starts = model.segments[k] + [A.DAYS + 1]
        for s in range(len(starts) - 1):
            for fish in ("RB", "CT"):
                want = model.sections["ladder"][k][s][fish]["wild"]
                for day in range(starts[s], starts[s + 1]):
                    got = A.ladder_verdict(R.effective_rules_bound(
                        ctx.sets[set_id], sw, CAL.month_day(day), fish, BUNDLE,
                        steelhead_rules_here=sr, origin="wild", trace=True))
                    assert got == want, (k, day, fish)
