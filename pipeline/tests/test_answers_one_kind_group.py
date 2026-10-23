"""D19 (a), T80 3: a trout/char total of ONE kind with no sub-cap is a group on its own row.

`rows.group_of` gave no group to a total of one kind with no sub-cap, so its row shipped pooled and headline-less — and
the app's Keep tab (core `keep.ts`) draws a row only as a group (stored headline) or a plain row (no pool). Such a row
was drawn nowhere: Kootenay Lake — Main Body's "Rainbow trout, separate from your 5 trout and char, 10 a day"
(klmb_1010, decisions D19), and on the 2026-10-22 release 1,022 waters in all — Region 8's brook trout quota
(z8 trout_char_quota.r5) on most of its streams, Duncan River's Dolly Varden, Tranquille Lake's rainbow, the Region 7
brook trout lakes, and more (T80 3). Every such row was trout or char: the Other fish list draws the rest by its own
compact path, so the ruling is limited to TROUT_CHAR and Other fish rows are untouched. It covers a total that is the
row's OWN pool on every kind of water. Two shapes stay open (pinned whole below, T80 follow-up): a row whose total is
another rule's (Duncan River's Dolly Varden inside Region 4's 5), and a total limited to a kind of water (Region 8's
"20 brook trout from streams"), where the card gate's zone-total proof counts lake fish and refuses the group.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")

KLMB, ANGELO, TRANQUILLE, DUNCAN_TRIB = "wbk:-20", "gnis:10053", "wbk:329563842", "gnis:12861"
BOW_LAKE = "wbk:329343476"                     # Region 7: its own brook trout quota
WATERS = (KLMB, ANGELO, TRANQUILLE, DUNCAN_TRIB, BOW_LAKE)
Z8_EB = "z8:trout_char_quota::trout_char_quota.r5"             # "20 brook trout from streams" (Angelo Creek)
KLMB_RB = "r4:kootenay_lake_main_body_for_location_see_map_on_page_34@4-19::kootenay_lake_main_body.r4"
TROUT_CHAR = dict(C._closed_groups())["TROUT_CHAR"]


def _build(items):
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=list(items), sections=["rows"], log=lambda *_: None)


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    return data, _build(WATERS)


def _card(model, item, md, part=0):
    k = model.parts[item][part]
    return model.sections["rows"][k][E.segment_index(model.segments[k], CAL.day_of(md))]


def _row(data, card, pool):
    return next(r for r in card["rows"] if r["pool"] is not None and data["rule_ids"][r["pool"]] == pool)


def _headless(data, model):
    """Every (item, segment, pool) where a trout/char pooled row with no parent ships without a group headline —
    the rows the app's Keep tab cannot draw."""
    out = []
    for item in WATERS:
        for k in model.parts[item]:
            if k is None:
                continue
            for s, card in enumerate(model.sections["rows"][k]):
                for r in card["rows"]:
                    g = r.get("group")
                    if r["pool"] is not None and r.get("parent") is None and set(r["members"]) <= TROUT_CHAR \
                            and not (g and g.get("headline")):
                        out.append((item, s, data["rule_ids"][r["pool"]]))
    return out


@pytest.mark.needs_bundle
def test_kootenay_main_body_rainbow_is_a_group_of_its_own(built):
    data, model = built
    row = _row(data, _card(model, KLMB, (10, 10)), KLMB_RB)
    g = row["group"]
    assert (row["members"], row.get("parent"), data["rule_ids"][g["pool"]], g["total"], g["kinds"], g["subcaps"]) \
        == (["RB"], None, KLMB_RB, 10, ["RB"], [])
    assert g["headline"] == {"n": 10, "unit": "a day per angler", "says": "10 a day per angler"}


DUNCAN_DV = "r4:duncan_river@4-19::duncan_river.r5"


@pytest.mark.needs_bundle
def test_no_trout_or_char_row_ships_headless_but_the_two_known_shapes(built):
    """Every headless trout/char row is gone but two shapes, listed whole: Duncan River's Dolly Varden (a one-kind row
    inside Region 4's 5, on Crazy Creek, its tributary) and Region 8's stream brook trout (Angelo Creek). When either
    is fixed this list shrinks and must be edited."""
    data, model = built
    got = {(i, p) for i, _s, p in _headless(data, model)}
    assert got == {(DUNCAN_TRIB, DUNCAN_DV), (ANGELO, Z8_EB)}


@pytest.mark.needs_bundle
def test_tranquille_and_a_region_7_brook_trout_lake_are_groups(built):
    data, model = built
    rb = _row(data, _card(model, TRANQUILLE, (6, 15)), "r3:tranquille_lake@3-29::tranquille_lake.r1")
    eb = _row(data, _card(model, BOW_LAKE, (6, 15)), "r7:bow_lake@7-15::bow_lake.r1")
    assert [(r["members"], r["group"]["kinds"], data["rule_ids"][r["group"]["pool"]]) for r in (rb, eb)] == [
        (["RB"], ["RB"], "r3:tranquille_lake@3-29::tranquille_lake.r1"),
        (["EB"], ["EB"], "r7:bow_lake@7-15::bow_lake.r1")]


@pytest.mark.needs_bundle
def test_mutation_the_old_one_kind_rule_leaves_the_rows_headless(built, monkeypatch):
    """MUTATION: without the ruling the rows are back to no group — klmb's, Tranquille's and Bow Lake's."""
    data, _ = built
    monkeypatch.setattr(RW, "A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP", False)
    pools = {p for _i, _s, p in _headless(data, _build(WATERS))}
    assert {KLMB_RB, "r3:tranquille_lake@3-29::tranquille_lake.r1", "r7:bow_lake@7-15::bow_lake.r1"} <= pools


@pytest.mark.needs_bundle
def test_other_fish_totals_of_one_kind_stay_without_a_group(built):
    """The ruling is trout/char only: Kootenay's crayfish 25 a day (an Other fish row) keeps no group."""
    data, model = built
    row = _row(data, _card(model, KLMB, (10, 10)), "z4:species_quotas::species_quotas.r3")
    assert (row["members"], row.get("group")) == (["CRA"], None)


# --------------------------------------------------------------------------------------------
# T80 review: the claim over EVERY shipped water, not the five above
# --------------------------------------------------------------------------------------------

def _shipped_headless(data):
    """`{item: {pool}}` — every water the SHIPPED answers file gives a headless trout/char pooled row (1,022 on the
    2026-10-22 release; after the deliver that ships this change, only the two open shapes' waters)."""
    import json
    model = E.decode(json.loads((EXPORT_DIR / "ui-rules-answers.json").read_bytes()), data)
    out: dict = {}
    for item, ks in model.parts.items():
        for k in ks:
            if k is None:
                continue
            for card in model.sections["rows"][k]:
                for r in card["rows"]:
                    g = r.get("group")
                    if r["pool"] is not None and r.get("parent") is None and set(r["members"]) <= TROUT_CHAR \
                            and not (g and g.get("headline")):
                        out.setdefault(item, set()).add(data["rule_ids"][r["pool"]])
    return out


def _rebuilt_headless(data, items):
    """The same scan over `items` rebuilt by the code under test, and the card gate over what it built."""
    from pipeline.deliver.answers import card_check as CC
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=sorted(items), sections=["rows", "ladder"],
                    log=lambda *_: None)
    out: dict = {}
    for item in items:
        for k in model.parts[item]:
            if k is None:
                continue
            for card in model.sections["rows"][k]:
                for r in card["rows"]:
                    g = r.get("group")
                    if r["pool"] is not None and r.get("parent") is None and set(r["members"]) <= TROUT_CHAR \
                            and not (g and g.get("headline")):
                        out.setdefault(item, set()).add(data["rule_ids"][r["pool"]])
    return out, CC.check_model(model, BUNDLE, data, log=lambda *_: None)["problems"]


@pytest.fixture(scope="module")
def shipped(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-answers.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    return data, _shipped_headless(data)


@pytest.mark.needs_bundle
def test_every_shipped_headless_water_is_drawn_but_the_two_open_shapes(shipped):
    """Every water the shipped file leaves with a headless trout/char row, rebuilt: what stays headless is ONLY the two
    open shapes' pools (z8 r5 on Region 8's streams, Duncan River's DV on its tributaries), and the card gate proves
    every group the rebuild drew (0 problems). On the 2026-10-22 release: 1,022 waters in, 996 left (982 z8 r5, 14
    Duncan), 19 own pools turned into groups, 638 groups on 247 distinct cards proved."""
    data, before = shipped
    assert before, "the shipped file has no headless trout/char row: nothing left to prove here"
    after, problems = _rebuilt_headless(data, before)
    assert ({p for ps in after.values() for p in ps}, problems) == ({Z8_EB, DUNCAN_DV}, [])


@pytest.mark.needs_bundle
def test_mutation_without_the_ruling_shipped_waters_keep_other_headless_pools(shipped, monkeypatch):
    """MUTATION of the whole-set test: without the ruling, pools beyond the two open shapes stay headless. It bites only
    on a file shipped WITHOUT the ruling (the 2026-10-22 release); once the deliver ships the groups, the shipped scan
    holds only the open shapes' waters and this skips (the five-water mutation above still bites)."""
    data, before = shipped
    monkeypatch.setattr(RW, "A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP", False)
    after, _ = _rebuilt_headless(data, before)
    if not ({p for ps in after.values() for p in ps} - {Z8_EB, DUNCAN_DV}):
        pytest.skip("the shipped file already carries the ruling's groups: no own-pool row is headless in it")
    assert {p for ps in after.values() for p in ps} - {Z8_EB, DUNCAN_DV}


# --------------------------------------------------------------------------------------------
# The answers MODEL (model.py) knows the shape: the real build validates every rows frame
# (`encode.encode` -> `model.validate_section`), which the tests above never ran — they passed while
# `pipeline.deliver all` stopped on "a group is a total several kinds share, or one with a sub-cap".
# --------------------------------------------------------------------------------------------

def _validate_rows(model):
    """The build's own check, as `encode.encode` calls it: every distinct rows frame against its model."""
    from pipeline.deliver.answers.model import validate_section
    return validate_section("rows", (v for vals in model.sections["rows"] for v in vals),
                            model.statics.get("rows"))


@pytest.mark.needs_bundle
def test_the_rows_frames_of_the_t80_waters_pass_the_model(built):
    """klmb's rainbow, Tranquille's rainbow, Bow Lake's brook trout (and the rest of WATERS): what `rows.produce` makes
    is what `model.RowsFrame` accepts — a divergence between rows.py and model.py fails here, not in the deliver."""
    _, model = built
    assert _validate_rows(model) > 0


def _one_kind_frame(data, model):
    """klmb Oct 10's frame (a copy) and the index of its rainbow row, whose group is the one-kind trout/char shape."""
    import copy
    card = copy.deepcopy(_card(model, KLMB, (10, 10)))
    i = next(i for i, r in enumerate(card["rows"])
             if r["pool"] is not None and data["rule_ids"][r["pool"]] == KLMB_RB)
    assert (card["rows"][i]["group"]["kinds"], card["rows"][i]["group"]["subcaps"]) == (["RB"], [])
    return card, i


def _check(frame):
    from pipeline.deliver.answers.model import validate_section
    return validate_section("rows", [frame])


@pytest.mark.needs_bundle
def test_the_one_kind_trout_group_validates(built):
    data, model = built
    card, _ = _one_kind_frame(data, model)
    assert _check(card) == 1


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_group_of_a_fish_not_trout_or_char_is_refused(built):
    """MUTATION: the same frame with its rainbow turned into crayfish everywhere its row names it (the row and its group)
    — every other check of the group and the frame still holds, so only the one-kind rule can refuse it."""
    import json
    from pipeline.deliver.answers.model import ShapeError
    data, model = built
    card, i = _one_kind_frame(data, model)
    assert '"CRA"' not in json.dumps(card["rows"][i])
    card["rows"][i] = json.loads(json.dumps(card["rows"][i]).replace('"RB"', '"CRA"'))
    with pytest.raises(ShapeError, match="several kinds share"):
        _check(card)


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_trout_group_on_another_rules_pool_is_refused(built):
    """MUTATION: the group's pool moved to a rule no row of the frame pools rainbow under (the crayfish row's pool) — a
    one-kind group is valid only as its own row's total (not Duncan River's DV inside Region 4's 5), so it is refused."""
    from pipeline.deliver.answers.model import ShapeError
    data, model = built
    card, i = _one_kind_frame(data, model)
    other = next(r["pool"] for r in card["rows"] if r["pool"] is not None and "RB" not in r["members"])
    card["rows"][i]["group"]["pool"] = other
    with pytest.raises(ShapeError, match="its own row's pool"):
        _check(card)


# --------------------------------------------------------------------------------------------
# T80 follow-up (batch-3 deliver, 2026-10-22): a one-kind row HELD BY ANOTHER POOL is no group of its own.
# Perry Creek (Region 4, a St. Mary River tributary): "brook trout daily quota = 20" (perry_creek.r3) is its row's own
# pool, but the row is held by the St. Mary's "trout/char 1 on all tributaries" row (its `parent`: that row
# cross-references the brook trout). The app draws no article for a held row (page v36 parentRow, core `drawnRows`),
# so a group there is never drawn — and the card worded it through the parent's cross-reference ("1", against the
# St. Mary's 1, which the reader DISPLACED for brook trout: 20 may be kept), so the card gate failed 30 times on the
# deliver. The one-kind group fires only on a row no other pool holds (`rows.parent_of`, the one definition).
# --------------------------------------------------------------------------------------------

PERRY = "gnis:14741"
PERRY_EB = "r4:perry_creek@4-20::perry_creek.r3"
ST_MARY_TRIBS = "r4:st_mary_river@4-20::st_mary_river.r2"


def _perry(data, model, md=(6, 15)):
    card = _card(model, PERRY, md)
    row = _row(data, card, PERRY_EB)
    parent = card["rows"][row["parent"]] if row.get("parent") is not None else None
    return row, parent


def _gate(model, items):
    from pipeline.deliver.answers import card_check as CC
    data, _ = C.load_export(EXPORT_DIR)
    return CC.check_model(model, BUNDLE, data, log=lambda *_: None)["problems"]


@pytest.fixture(scope="module")
def perry(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[PERRY], sections=["rows", "ladder"], log=lambda *_: None)
    return data, model


@pytest.mark.needs_bundle
def test_perry_creek_brook_trout_held_by_the_st_mary_row_is_no_group(perry):
    data, model = perry
    for md in ((6, 15), (7, 15), (10, 10)):
        row, parent = _perry(data, model, md)
        assert (row["members"], parent is not None and data["rule_ids"][parent["pool"]], row.get("group")) \
            == (["EB"], ST_MARY_TRIBS, None)
    assert _gate(model, [PERRY]) == []


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_group_on_a_held_row_fails_the_card_gate(perry, monkeypatch):
    """MUTATION: without the drawn-row condition Perry Creek's brook trout row gets a group again, worded "1" through
    the St. Mary's cross-reference, and the card gate refuses it (the batch-3 deliver's 30 problems, one key here)."""
    data, _ = perry
    monkeypatch.setattr(RW, "A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW", False)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[PERRY], sections=["rows", "ladder"], log=lambda *_: None)
    row, _parent = _perry(data, model)
    assert row["group"]["kinds"] == ["EB"]
    probs = _gate(model, [PERRY])
    assert any(f"total {PERRY_EB}: headline says 1, with no lift 2 may be kept here" in p for p in probs), probs


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_trout_group_on_a_held_row_is_refused_by_the_model(built):
    """MUTATION: klmb's one-kind rainbow group with its row made held by another pooled row of the frame — the model
    (`RowsFrame._one_kind_groups`) refuses it, as the build's own validation would."""
    from pipeline.deliver.answers.model import ShapeError
    data, model = built
    card, i = _one_kind_frame(data, model)
    card["rows"][i]["parent"] = next(j for j, r in enumerate(card["rows"]) if j != i and r["pool"] is not None)
    with pytest.raises(ShapeError, match="held by another pool"):
        _check(card)
