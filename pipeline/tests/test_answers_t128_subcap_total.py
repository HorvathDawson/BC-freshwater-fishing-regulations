"""T128 (owner 2026-10-22): a sub-cap line names the total it is part of.

Owner: "instead of something like 'Up to 2 may be hatchery brook, brown or cutthroat trout' it needs to add 'of your total X
daily quota in a stream for trout and char' in there to make it more obvious to an untrained user." A kind line of a card's
tree now reads "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout": the
number, the period and the group's name of the nearest total, stream share or water's own rule above it, and its kind of
water only where that total is limited to one (`tree.total_named` decides which tier, `tree.total_words` its words — the
one place). A kind that is the only kind of that tier at its number names nothing (the tier's line says the whole of it:
the header, or the tier it is folded into). The card gate proves the named number like every other.

The no-data tier runs over `answers28_fixture` (Region 2's real table through the real reader and rows); the data tier
(`needs_bundle`) over real cards of the live bundle: a Region 2 and a Region 1 stream with the hatchery-only sub-cap (no
Region 4 card carries a hatchery sub-cap: Region 4's trout/char table has none), a Region 6 lake, Kootenay Lake — Main
Body (its trout/char total and its one-kind rainbow group, T80). No live group's total is limited to a kind of water
(z8 trout_char_quota.r5 is a one-kind row with no group, T80's open shape), so that branch is pinned on the fixture.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import card_check as CC
from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.tree import walk
from pipeline.tests import answers28_fixture as F

OCT10 = (10, 10)
S2 = "Of your 2 trout and char a day in streams, "


def _group(water, md=OCT10, P=None):
    """The card's trout and char group (whichever pool holds it: Region 2's total or a water's own)."""
    from pipeline.deliver.answers import rows as RW
    if P is None:
        out, P = F.card(water, md)
    else:
        out = RW.produce(P)
    (g,) = [r["group"] for r in out["rows"] if r.get("group") and set(r["group"]["kinds"]) & {"RB", "EB", "GB"}]
    return g, P


def _kinds(g):
    return [(x["says"], x["sentence"]) for x, _p in walk(g["tree"]) if x["node"] == "kind"]


# -- the no-data tier --------------------------------------------------------------------------------------------------

def test_the_vedder_kinds_name_the_stream_share_and_the_folded_one_names_nothing():
    g, P = _group(F.VEDDER)
    assert _kinds(g) == [
        (S2 + "up to 2 may be hatchery brook, brown or cutthroat trout",
         S2 + "up to 2 may be hatchery brook, brown or cutthroat trout, 30 cm (12 in) or longer"),
        (S2 + "only 1 may be hatchery Dolly Varden/bull trout or lake trout",
         S2 + "only 1 may be hatchery Dolly Varden/bull trout or lake trout, 60 cm (24 in) or longer"),
        (S2 + "up to 2 may be hatchery steelhead only — wild steelhead go back",
         S2 + "up to 2 may be hatchery steelhead, rainbow trout over 50 cm (20 in)"),
        # folded into "This river’s rule: up to 4 hatchery rainbow trout a day": that line says it (not drawn)
        ("Up to 4 may be hatchery rainbow trout",
         "Of the 4 a day: up to 4 may be hatchery rainbow trout, any length, 50 cm (20 in) or less")]
    assert CC.check_card(P, F.card(F.VEDDER, OCT10)[0]) == []


def test_a_lake_total_names_the_total_with_no_water():
    g, _P = _group(F.LAKE)
    assert g["header"]["says"] == "4 a day per angler across Region 2"
    assert _kinds(g) == [
        ("Of your 4 trout and char a day, up to 4 may be brook, brown, cutthroat or rainbow trout",
         "Of your 4 trout and char a day, up to 4 may be brook, brown, cutthroat or rainbow trout, any size"),
        ("Of your 4 trout and char a day, only 1 may be Dolly Varden/bull trout or lake trout",
         "Of your 4 trout and char a day, only 1 may be Dolly Varden/bull trout or lake trout, 60 cm (24 in) or longer")]


def test_a_water_own_total_and_a_count_tier_name_the_total_around_them():
    g, P = _group(F.COUNT)
    assert [s for s, _ in _kinds(g)] == [
        "Of your 2 trout and char a day, up to 2 may be hatchery brook or brown trout",
        "Of your 2 trout and char a day, only 1 may be hatchery Dolly Varden/bull trout or lake trout",
        "Of your 2 trout and char a day, up to 2 may be hatchery steelhead only — wild steelhead go back",
        "Of your 2 trout and char a day, only 1 may be hatchery cutthroat trout",       # under "1 cutthroat or rainbow
        "Of your 2 trout and char a day, only 1 may be hatchery rainbow trout"]         # trout in all"
    g, P = _group(F.TWO)
    own = next(t for t, _p in walk(g["tree"]) if t["node"] == "tier" and t["tier"] == "own")
    assert [k["says"] for k in own["children"]] == [
        "Of your 4 trout and char a day under this river’s rule, only 3 may be hatchery cutthroat trout",
        "Of your 4 trout and char a day under this river’s rule, up to 4 may be hatchery rainbow trout"]


def test_a_zone_total_limited_to_one_kind_of_water_says_it():
    """The z8 trout_char_quota.r5 shape ("20 brook trout from streams"): a zone total printed for one kind of water says
    it. No live group carries such a total, so the fixture lake's Region 2 total is made one printed for lakes."""
    P = F.part(F.LAKE, OCT10)
    T = P.by_key[F.TOTAL]
    T.f = {**T.f, "water": "lake"}
    g, _P = _group(F.LAKE, P=P)
    assert [s for s, _ in _kinds(g)] == [
        "Of your 4 trout and char a day in lakes, up to 4 may be brook, brown, cutthroat or rainbow trout",
        "Of your 4 trout and char a day in lakes, only 1 may be Dolly Varden/bull trout or lake trout"]


def test_mutation_without_the_named_total_the_old_words_come_back(monkeypatch):
    """MUTATION: `total_named` naming nothing gives back the pre-T128 lines — the test above bites on the decision."""
    monkeypatch.setattr(TR, "total_named", lambda chain, kind: None)
    g, _P = _group(F.LAKE)
    assert [s for s, _ in _kinds(g)] == ["Up to 4 may be brook, brown, cutthroat or rainbow trout",
                                         "Only 1 of the 4 may be Dolly Varden/bull trout or lake trout"]


def test_mutation_the_card_gate_refuses_a_named_total_it_cannot_prove(monkeypatch):
    """MUTATION: a lead naming a number no tier around the kind holds ("Of your 9 …") is refused by the card gate — the
    gate proves the named number, it does not trust the words."""
    real = TR.total_words
    monkeypatch.setattr(TR, "total_words", lambda t, what, where: real({**t, "n": 9}, what, where))
    out, P = F.card(F.VEDDER, OCT10)
    probs = CC.check_card(P, out)
    assert any("kind" in p and "9" in p for p in probs), probs


def test_one_place_words_the_named_total():
    """No second copy: "Of your " is written once in the answers package, in `tree.total_words`."""
    pkg = Path(TR.__file__).parent
    hits = [(p.name, i) for p in sorted(pkg.glob("*.py")) for i, line in enumerate(p.read_text().splitlines(), 1)
            if re.search(r"f[\"']Of your ", line)]
    assert len(hits) == 1 and hits[0][0] == "tree.py", hits


# -- the data tier: real cards ----------------------------------------------------------------------------------------

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE")
EXPORT_DIR = os.environ.get("ANSWERS_EXPORT_DIR")
R2_STREAM, R1_STREAM, R6_LAKE, KLMB = "blk:355991777", "gnis:10015", "gnis:10031", "wbk:-20"


@pytest.fixture(scope="module")
def live(request):
    from pipeline.deliver.answers import answers as A
    from pipeline.deliver.answers import common as C
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
    bundle = BUNDLE or R.BUNDLE
    export = Path(EXPORT_DIR or Path(R.BUNDLE).parent.parent / "regs")
    need(request, "bundle", bundle, BUNDLE_HINT)
    need(request, "bundle", export / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(export)
    model = A.build(bundle, export, workers=1, items=[R2_STREAM, R1_STREAM, R6_LAKE, KLMB], sections=["rows", "ladder"],
                    log=lambda *_: None)
    return data, model, bundle


def _live_group(live, item, md, pool):
    from pipeline.deliver.answers import encode as E
    data, model, _b = live
    k = model.parts[item][0]
    segs = model.segments[k]
    start = segs[E.segment_index(segs, CAL.day_of(md))]
    card = model.sections["rows"][k][segs.index(start)]          # the day's first moment (outside any hours window)
    (g,) = [r["group"] for r in card["rows"] if r.get("group") and data["rule_ids"][r["group"]["pool"]] == pool]
    return g


@pytest.mark.needs_bundle
def test_a_region_2_stream_names_its_2_from_streams(live):
    g = _live_group(live, R2_STREAM, (1, 15), "z2:trout_char_quota::trout_char_quota.r1")
    assert g["header"]["says"] == "4 a day per angler across Region 2, lakes included"
    assert [s for s, _ in _kinds(g)] == [
        S2 + "up to 2 may be hatchery brook, brown or cutthroat trout",
        S2 + "only 1 may be hatchery Dolly Varden/bull trout or lake trout",
        S2 + "up to 2 may be hatchery steelhead only — wild steelhead go back",
        S2 + "up to 2 may be hatchery rainbow trout"]


@pytest.mark.needs_bundle
def test_a_region_1_stream_names_its_2_trout_from_streams(live):
    g = _live_group(live, R1_STREAM, (6, 15), "z1:trout_quota::trout_quota.r1")
    assert [s for s, _ in _kinds(g)] == [
        "Of your 2 trout a day in streams, up to 2 may be hatchery brown, cutthroat or rainbow trout",
        "Of your 2 trout a day in streams, up to 2 may be hatchery steelhead only — wild steelhead go back"]


@pytest.mark.needs_bundle
def test_a_region_6_lake_names_its_5(live):
    g = _live_group(live, R6_LAKE, (7, 10), "z6:trout_char_quota::trout_char_quota.r1")
    assert [s for s, _ in _kinds(g)] == [
        "Of your 5 trout and char a day, up to 5 may be brook trout",
        "Of your 5 trout and char a day, only 3 may be lake trout",
        "Of your 5 trout and char a day, only 1 may be brown, cutthroat or rainbow trout",
        "Of your 5 trout and char a day, only 1 may be hatchery steelhead — wild steelhead go back"]


@pytest.mark.needs_bundle
def test_kootenay_main_body_the_trout_total_names_its_5_the_one_kind_rainbow_group_nothing(live):
    g = _live_group(live, KLMB, OCT10, "z4:trout_char_quota::trout_char_quota.r1")
    assert _kinds(g) == [
        ("Up to 1 may be Dolly Varden/bull trout", "Of the 1 a day: up to 1 may be Dolly Varden/bull trout, any size"),
        ("Of your 5 trout and char a day, up to 5 may be brook, brown, cutthroat or lake trout",
         "Of your 5 trout and char a day, up to 5 may be brook, brown, cutthroat or lake trout, any size — only 1 cutthroat "
         "trout over 50 cm (20 in)")]
    rb = _live_group(live, KLMB, OCT10,
                     "r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19::kootenay_lake_main_body.r4")
    assert (rb["header"]["says"], _kinds(rb)) == ("This lake’s rule: up to 10 a day per angler", [
        ("Up to 10 may be rainbow trout", "Of the 10 a day: up to 10 may be rainbow trout, any size")])


@pytest.mark.needs_bundle
def test_the_card_gate_proves_the_real_cards(live):
    data, model, bundle = live
    assert CC.check_model(model, bundle, data, log=lambda *_: None)["problems"] == []
