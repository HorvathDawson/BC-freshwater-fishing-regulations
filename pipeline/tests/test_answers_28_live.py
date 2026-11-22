"""ANSWERS 2.8 on the REAL waters: the card gate (`card_check`) on the pinned waters and, slow, on every
part; what is not a sentence on the Vedder, Kootenay Lake and a plain Region 2 stream. The card's words on
those waters, a Region 4 stream with its own lift (the Duncan), one inheriting it (Lardeau Creek) and a plain
Region 2 lake are `test_answers_card_words_live.py` (T866). The no-data twins of these tests (CI's no-data tier)
are `test_answers_28_card.py` and `test_answers_card_words.py`.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

VEDDER, DUNCAN, LARDEAU_CREEK = "gnis:3062", "gnis:22255", "gnis:16357"
KOOTENAY_MAIN = "wbk:-20"
R2_LAKE, R2_STREAM = "wbk:328961745", "gnis:10014"          # Judson Lake, Anderson Creek: zone rules only
WATERS = (VEDDER, DUNCAN, LARDEAU_CREEK, KOOTENAY_MAIN, R2_LAKE, R2_STREAM)


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.fail(f"pinned waters missing from this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["rows"],
                    export=(data, guide), log=lambda *_: None)
    return data, model


def _trout(built, item, part=0, md=(10, 10)):
    data, model = built
    if isinstance(part, tuple):
        part = part_at(data, item, *part)
    k = model.parts[item][part]
    v = model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]
    return next(r for r in v["rows"] if r.get("group") and {"CT", "DV"} & set(r["group"]["kinds"]))


def _text(row):
    g = row["group"]
    out = [g["headline"]["says"], g["origin"], *[L["says"] for L in g["lifts"]], g["total_line"]]
    for b in g["buckets"]:
        out.append(b["title"])
        out += [n["says"] for n in b.get("notes") or []] + [n["says"] for n in b.get("sizes") or []]
        out += [f"{'/'.join(k['species'])} {k['n']}: " + " · ".join(k["notes"] + k["duties"])
                for k in b["kinds"]]
    return [t for t in out if t]


@pytest.mark.needs_bundle
def test_the_card_gate_proves_the_pinned_waters():
    got = CC.check_built(BUNDLE, EXPORT_DIR, items=list(WATERS), log=lambda *_: None)
    assert got["groups"] > 20 and got["problems"] == [], got["problems"][:10]


@pytest.mark.needs_bundle
def test_vedder_mouth_oct_10_card(built):
    # (T866) the card's words (every line, the share's origin, possession, the rainbow's size, the worked list):
    # test_answers_card_words_live.py LIVE
    row = _trout(built, VEDDER, ("mouth",))
    # ROUND20h 3: possession on the group's TOTAL (p.21 "Possession quotas = 2 daily quotas"), not the stream share
    ps = row["group"]["possession"]
    assert (ps["per_daily"], ps["of"], ps["n"]) == (2, 4, 8)
    assert row["elsewhere"]["ask"] == []


@pytest.mark.needs_bundle
def test_kootenay_lake_plain_lake_and_plain_stream_cards(built):
    # (T866) the Duncan's, Lardeau Creek's, Kootenay Lake's, the plain lake's and the plain stream's words:
    # test_answers_card_words_live.py LIVE
    k = _text(_trout(built, KOOTENAY_MAIN))
    assert not any("rule replaces" in t for t in k)
    stream = _trout(built, R2_STREAM)
    assert stream["group"]["lifts"] == [] and not [w for w in stream["group"]["worked"] if w["at"] == "here"]
    # ROUND12 4: the examples are the card's one list
    assert stream["elsewhere"]["ask"] == []


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_the_card_gate_proves_every_part():
    got = CC.check_built(BUNDLE, EXPORT_DIR, log=lambda *_: None)
    assert got["groups"] > 1000 and got["problems"] == [], got["problems"][:20]
