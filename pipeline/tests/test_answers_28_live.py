"""ANSWERS 2.8 on the REAL waters: the card gate (`card_check`) on the pinned waters and, slow, on every
part; the card's words pinned on the Vedder, a Region 4 stream with its own lift (the Duncan) and one
inheriting it (Lardeau Creek), Kootenay Lake, a plain Region 2 lake and a plain Region 2 stream.
The no-data twins of these tests (CI's no-data tier) are `test_answers_28_card.py`.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")

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
    row = _trout(built, VEDDER, ("mouth",))
    assert _text(row) == [
        "2 a day from this stream",
        "Keep hatchery fish only — wild trout and char go back.",
        "On this river, hatchery rainbow trout 50 cm (20 in) or less: up to 4 a day — this river’s rule "
        "replaces Region 2’s 2-from-streams limit for them.",
        "No more than 4 trout and char a day across Region 2, lakes included.",
        # ROUND12 2: the river's own bucket first
        "This river’s rule",
        "RB 4: counts toward the 4 a day",
        "Up to 2 a day between them — all Region 2 streams combined",
        "Cutthroat trout, brown trout, Dolly Varden/bull trout, lake trout or brook trout: only 1 over 50 cm "
        "between them",
        "EB/GB/CT 2: ",
        "DV/LT 1: ",
        "ST 2: 10 a year · stop fishing for the day after 2 · record each one on your licence"]
    # ROUND12 1: the lift's size, said as the book says it (p.22 "of any length 50 cm or less")
    assert next(k for b in row["group"]["buckets"] for k in b["kinds"] if k["species"] == ["RB"])["detail"] \
        == "hatchery, any length, 50 cm or less"
    # ROUND12 4: ONE list — the keep-N count, then the examples it does not repeat
    assert [w["q"] + " → " + w["a"] for w in row["group"]["worked"]] == [
        "Keep 4 rainbows here → that’s your trout and char for the day, everywhere.",
        "Keep 3 rainbows here → 1 more: any kind from here, or one from a lake.",
        "Keep 2 rainbows here → no more trout and char from any other stream today; up to 2 more from here or from "
        "lakes.",
        "Kept 1 at a lake? → Keep 3 more here. Fish from lakes count toward the 4 a day, not toward the 2 from streams.",
        "Kept trout or char over 50 cm (20 in) elsewhere today? → It counts here too: at most 2 over 50 cm (20 in) a day in all — "
        "only 1 of them may be anything other than a hatchery steelhead."]
    assert row["elsewhere"]["ask"] == []


@pytest.mark.needs_bundle
def test_duncan_and_lardeau_cards(built):
    d = _text(_trout(built, DUNCAN, ("mouth", "duncan_river__lardeau_river_into_duncan_river")))
    # ROUND10 W1: the row's two lifts are ONE line, each kind with its number, highest first
    assert d[:2] == ["2 a day from this stream",
                     "On this river, its own rule replaces Region 4’s 2-from-streams limit for rainbow "
                     "trout (up to 5 a day) and Dolly Varden/bull trout (up to 2 a day)."]
    lc = _text(_trout(built, LARDEAU_CREEK))
    assert lc[1] == ("On the Duncan River and its tributaries, rainbow trout: up to 5 a day — the "
                     "Duncan River’s rule replaces Region 4’s 2-from-streams limit for them.")


@pytest.mark.needs_bundle
def test_kootenay_lake_plain_lake_and_plain_stream_cards(built):
    k = _text(_trout(built, KOOTENAY_MAIN))
    assert k[0] == "5 a day" and not any("rule replaces" in t for t in k)
    lake = _text(_trout(built, R2_LAKE))
    assert lake[0] == "4 a day" and ("Rainbow trout, cutthroat trout, brown trout, Dolly Varden/bull trout, lake "
                                     "trout or brook trout: only 1 over 50 cm between them") in lake
    stream = _trout(built, R2_STREAM)
    assert stream["group"]["headline"]["says"] == "2 a day from this stream"
    assert stream["group"]["lifts"] == [] and not [w for w in stream["group"]["worked"] if w["at"] == "here"]
    # ROUND12 4: the examples are the card's one list
    assert stream["elsewhere"]["ask"] == []
    assert [stream["group"]["worked"][0][k] for k in ("q", "a", "here")] == ["Kept 2 on another stream?",
                                                                            "None more here", 0]


@pytest.mark.slow
@pytest.mark.needs_bundle
def test_the_card_gate_proves_every_part():
    got = CC.check_built(BUNDLE, EXPORT_DIR, log=lambda *_: None)
    assert got["groups"] > 1000 and got["problems"] == [], got["problems"][:20]
