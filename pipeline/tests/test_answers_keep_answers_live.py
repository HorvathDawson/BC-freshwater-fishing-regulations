"""THE KEEP TAB'S LAST CHOICES (rows 31, display 18) on the REAL waters: the answers build (this code, the bundle on
this machine) for the waters the tapped states name, read at each state's own key and segment (taps.json). The checks
that compared against the prototype's golden.json went with it (T439, T450). The no-data twin is
`test_answers_keep_answers.py`; the app's parity (banner, group head, tree, group lines) proves the rest end to end on
a release built from this code.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers.display import closers_of
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "fixtures" / "golden"
TAPS = json.loads((GOLDEN / "taps.json").read_text())["states"]
IDS = sorted(TAPS)


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS[i]["item"] for i in IDS})
    m = A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)
    rid = {ix: k for k, ix in C.check_export(C.load(BUNDLE), data, guide).items()}
    return m, rid


def _at(built, sid, section):
    m, _ = built
    tap = TAPS[sid]
    k = m.parts[tap["item"]][tap["partIx"]]
    assert m.segments[k] == tap["starts"]                       # the oracle's own key year
    return m.sections[section][k][tap["s"]]


@pytest.mark.needs_bundle
def test_1_cow0_the_white_sturgeon_closure_closes_nothing_more(built):
    _, rid = built
    f = _at(built, "cow0_1010", "display")
    assert f["status"] == "closed"
    shut = {rid[r]: fs for r, fs in f["closing"]}
    assert [rid[r] for r in f["closers"]] == [k for k, fs in shut.items() if len(fs) > 1]
    assert any(fs == ["WSG"] for fs in shut.values())           # the Province's white sturgeon closure is closing
    assert all(shut[rid[r]] != ["WSG"] for r in f["closers"])   # … and is not a closer


@pytest.mark.needs_bundle
@pytest.mark.parametrize("sid", IDS)
def test_1_closers_are_set_exactly_on_a_closed_frame(built, sid):
    f = _at(built, sid, "display")
    if f["status"] == "tidal":
        return
    assert ("closers" in f) == (f["status"] == "closed")
    if f["status"] == "closed":
        assert f["closers"] == closers_of(f["closing"])


@pytest.mark.needs_bundle
def test_2_vedder_all_region_2_streams(built):
    # vedder_1010: the badge reads "All Region 2 streams" (page v36 nar0: a stream share binding every fish)
    rows = _at(built, "vedder_1010", "rows")["rows"]
    keep = [r for r in rows if r["kind"] == "keep" and r["scope"]["share"]]
    assert keep and all(r["scope"]["narrow_all"] for r in keep)


@pytest.mark.needs_bundle
def test_4_klmb_the_kootenay_record_rule_is_a_duty(built):
    # klmb_1010: "Write each rainbow trout over 50 cm (20 in) on your licence right away." is a duty (page v36
    # ladderHtml moved the deciding record rule to `duty`)
    _, rid = built
    rb = _at(built, "klmb_1010", "rows")["fish"]["RB"]
    roles = {rid[r]: role for o in ("hatchery", "wild") if rb[o] for r, role, _by in rb[o]["roles"]}
    assert roles["zp:kootenay_rainbow_stamp::kootenay_rainbow_stamp.r1"] == "duty"
