"""THE YEAR STRIP (rows 30) on the REAL waters, against the GOLDEN ORACLE: the answers build (this code, the bundle on
this machine) for the 42 waters the 134 golden states name, and every drawn row's stored `year` equals page v36's own
dayStatus runs for that state (app/design/regs-v11/port/golden/hydrate.json, the prototype's output — never this code's).
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
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "design" / "regs-v11" / "port" / "golden"
HYDRATE = json.loads((GOLDEN / "hydrate.json").read_text())
TAPS = json.loads((GOLDEN / "taps.json").read_text())
IDS = sorted(HYDRATE["states"])


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS["states"][i]["item"] for i in IDS})
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)


@pytest.mark.needs_bundle
def test_the_oracle_is_page_v38_over_134_states():
    assert (HYDRATE["meta"]["version"], HYDRATE["meta"]["year"], len(IDS)) == ("v38", 2026, 134)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("sid", IDS)
def test_every_rows_stored_year_is_page_v36s_day_status(built, sid):
    tap, oracle = TAPS["states"][sid], HYDRATE["states"][sid]
    k = built.parts[tap["item"]][tap["partIx"]]
    assert built.segments[k] == tap["starts"]                   # the same key year as the oracle's tap
    frame = built.sections["rows"][k][oracle["s"]]
    assert [r["year"] for r in frame["rows"]] == oracle["dayStatus"]
