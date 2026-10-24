"""THE YEAR STRIP (rows 30) on the REAL waters, against the GOLDEN ORACLE: the answers build (this code, the bundle on
this machine) for the 42 waters the 134 golden states name, and every drawn row's stored `year` equals page v36's own
dayStatus runs for that state (app/fixtures/golden/hydrate.json, the prototype's output — never this code's).
The no-data twin is `test_answers_year_strip.py`.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "fixtures" / "golden"
HYDRATE = json.loads((GOLDEN / "hydrate.json").read_text())
TAPS = json.loads((GOLDEN / "taps.json").read_text())
#: (T115) the hydrated states the store taps: not the 4 whose part the release merged away (taps.json `gone`)
IDS = sorted(i for i in HYDRATE["states"] if i not in TAPS["gone"])


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS["states"][i]["item"] for i in IDS})
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)


@pytest.mark.needs_bundle
def test_the_oracle_is_page_v38_over_134_states():
    assert (HYDRATE["meta"]["version"], HYDRATE["meta"]["year"], len(HYDRATE["states"]), len(IDS)) == ("v38", 2026, 134, 128)
    assert sorted(set(HYDRATE["states"]) - set(IDS)) == sorted(TAPS["gone"])


@pytest.mark.needs_bundle
@pytest.mark.parametrize("sid", IDS)
def test_every_rows_stored_year_is_page_v36s_day_status(built, sid):
    tap, oracle = TAPS["states"][sid], HYDRATE["states"][sid]
    k = built.parts[tap["item"]][tap["partIx"]]
    assert built.segments[k] == tap["starts"]                   # the same key year as the oracle's tap
    frame = built.sections["rows"][k][oracle["s"]]
    want = oracle["dayStatus"]
    if sid in KITIMAT_RULED:
        want = [_nov_jun_released(runs) if i == KITIMAT_RULED[sid] else runs for i, runs in enumerate(want)]
    assert [r["year"] for r in frame["rows"]] == want


#: OWNER RULING 2026-10-21 (a) — pipeline/docs/GOTCHAS.md: on the Kitimat, r5's hatchery-steelhead quota does NOT lift
#: Region 6's Nov 1-June 30 stream release, where page v36 (the oracle) lifted it. The ONE row that differs per state
#: (index) is the row the ruling changes; every other row of these states, and every other state, must equal the page.
KITIMAT_RULED = {"x14p0_1010": 2, "x14p1_1010": 2, "x14p2_1010": 2}
NOV1, JUN30 = 305, 181          # book day-of-year (365-day year; Feb 29 is not a book day)


def _nov_jun_released(runs):
    """The page's strip with the ruling applied: a 'keep' day on Nov 1-June 30 becomes 'release'; nothing else moves."""
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
