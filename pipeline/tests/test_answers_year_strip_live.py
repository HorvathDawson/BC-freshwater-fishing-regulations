"""THE YEAR STRIP (rows 30) on the REAL waters: the answers build (this code, the bundle on this machine) for the waters
the golden states name, and every drawn row's stored `year` equals page v36's own dayStatus runs for that state, as page
v36's CODE reads the release under test (app/fixtures/golden/page-oracle.json, `app/tools/regs-recut/page-oracle.mjs` —
the page's strip over the release's rows frames, never this code's). The no-data twin is `test_answers_year_strip.py`.

(T667 slice 5b) Until 2026-11-13 the oracle was page v36's FROZEN output (hydrate.json), amended per approved ruling
(v36-rulings.json: the Kitimat's Nov 1-June 30 release, Lois Lake's folded row, the Peace kokanee). The release under
test carries every ruling, so nothing is amended: this test now asks whether this code rebuilds the strips the release
ships, by the page's rule.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "fixtures" / "golden"
ORACLE = json.loads((GOLDEN / "page-oracle.json").read_text())
TAPS = json.loads((GOLDEN / "taps.json").read_text())
IDS = sorted(ORACLE["states"])


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS["states"][i]["item"] for i in IDS})
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)


def test_the_oracle_is_page_v38_over_every_tapped_state():
    assert (ORACLE["meta"]["page"], ORACLE["meta"]["version"], ORACLE["meta"]["year"]) == ("page_v36.js", "v38", 2026)
    assert ORACLE["meta"]["golden"] == TAPS["meta"]["golden"], "page-oracle.json and taps.json are from one sync"
    assert IDS == sorted(TAPS["states"]) and IDS


@pytest.mark.needs_bundle
@pytest.mark.parametrize("sid", IDS)
def test_every_rows_stored_year_is_page_v36s_day_status(built, sid):
    tap, oracle = TAPS["states"][sid], ORACLE["states"][sid]
    k = built.parts[tap["item"]][tap["partIx"]]
    assert built.segments[k] == tap["starts"]                   # the same key year as the oracle's tap
    frame = built.sections["rows"][k][oracle["s"]]
    assert [r["year"] for r in frame["rows"]] == oracle["dayStatus"]


#: (T667 5b) KEPT ONLY because pipeline/tests/smoke.txt (another lane's file) names this node id; delete the helper and
#: this test with that line when the smoke set is derived (T667 D6). Nothing reads `_nov_jun_released` any more.
NOV1, JUN30 = 305, 181          # book day-of-year (365-day year; Feb 29 is not a book day)


def _nov_jun_released(runs):
    days = [st for st, n in runs for _ in range(n)]
    days = ["release" if st == "keep" and (d >= NOV1 or d <= JUN30) else st for d, st in enumerate(days, 1)]
    out = []
    for st in days:
        if out and out[-1][0] == st: out[-1][1] += 1
        else: out.append([st, 1])
    return out


def test_the_ruling_moves_only_the_nov_jun_keep_days():
    assert _nov_jun_released([["keep", 74], ["closed", 92], ["keep", 199]]) == \
        [["release", 74], ["closed", 92], ["release", 15], ["keep", 123], ["release", 61]]
