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

from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import rows as RW
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = under_test.path("bundle")
EXPORT_DIR = under_test.path("export_dir")

KLMB, ANGELO, TRANQUILLE, DUNCAN_TRIB = "wbk:-20", "gnis:10053", "wbk:329563842", "gnis:12861"
BOW_LAKE = "wbk:329343476"                     # Region 7: its own brook trout quota
WATERS = (KLMB, ANGELO, TRANQUILLE, DUNCAN_TRIB, BOW_LAKE)
Z8_EB = "z8:daily_quotas::trout_char_quota.r5"             # "20 brook trout from streams" (Angelo Creek)
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


#: T834 (RULINGS T17, owner 2026-11-17 "Strict everywhere"; card 13:15 "Lead with 5"): a water's own daily quota above
#: its region's cap raises that cap for its fish, and the card says ONE box led by the region's number
#: (`rows.groups.A_RAISED_TOTAL_IS_ONE_BOX`). Kootenay Lake — Main Body's rainbow 10, Tranquille's rainbow 8 and Bow
#: Lake's brook trout 10 are each such a quota, so since T834 their rows are HELD by the region's raised box and D19 (a)
#: makes no group on them (T931 A, re-pinned 2026-11-20). The D19 (a) shape is still built wherever a one-kind own pool
#: is not raised into a box; with T834's box off (`pre_t834`) these five waters build it again, which is how the
#: D19 (a) rule, its mutation and the model's one-kind checks stay pinned on frames the builder really makes.
Z4_TOTAL, Z3_TOTAL, Z7A_TOTAL = ("z4:daily_quotas::trout_char_quota.r1", "z3:daily_quotas::trout_char_quota.r1",
                                 "z7a:daily_quotas::trout_char_quota.r1")
ALL_TROUT = ["EB", "GB", "CT", "DV", "LT", "RB"]


def _build_pre_t834(items, mp):
    mp.setattr(RW.groups, "A_RAISED_TOTAL_IS_ONE_BOX", False)
    return _build(items)


@pytest.fixture(scope="module")
def pre_t834(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    mp = pytest.MonkeyPatch()
    try:
        return data, _build_pre_t834(WATERS, mp)
    finally:
        mp.undo()


def _raised_box(data, card, pool):
    """The row `pool` and the raised box holding it: (row members, row group, holder pool, box pool, box total, box
    kinds, head, its "also" lines, the box's first sub-cap as (rule, take, kinds, [(lifted fish, by)]))."""
    row = _row(data, card, pool)
    holder = card["rows"][row["parent"]]
    g = holder["group"]
    s0 = g["subcaps"][0]
    return (row["members"], row.get("group"), data["rule_ids"][holder["pool"]], data["rule_ids"][g["pool"]], g["total"],
            g["kinds"], g["headline"]["says"], g["headline"].get("also"),
            (data["rule_ids"][s0["r"]], s0["take"], s0["of"],
             [(L["members"], data["rule_ids"][L["by"]]) for L in s0["lifted"]]))


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
def test_kootenay_main_body_rainbow_raises_the_regions_box(built):
    """T834 (RULINGS T17; owner 2026-11-17 13:15, Kootenay "Yes, they read right"): "Keep 5, rainbow trout up to 10" —
    the rainbow row is held by Region 4's total, raised to 10 for rainbow only (T931 A re-pin; was D19 (a)'s own group)."""
    data, model = built
    assert _raised_box(data, _card(model, KLMB, (10, 10)), KLMB_RB) == (
        ["RB"], None, Z4_TOTAL, Z4_TOTAL, 10, ALL_TROUT, "5 trout/char a day",
        [{"n": 10, "says": "10 if they’re rainbow trout"}],
        (Z4_TOTAL, 5, ["EB", "GB", "CT", "DV", "LT"], [(["RB"], KLMB_RB)]))


@pytest.mark.needs_bundle
def test_kootenay_main_body_rainbow_is_a_group_of_its_own_without_t834(pre_t834):
    """D19 (a), as built before T834 (its one box off): the rainbow's own 10 is a group of its own row."""
    data, model = pre_t834
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
def test_tranquille_and_a_region_7_brook_trout_lake_raise_their_regions_box(built):
    """T834 (RULINGS T17), as Kootenay: Tranquille's rainbow 8 over Region 3's 5, Bow Lake's brook trout 10 over Zone A's
    5 — each row held by its region's total raised for that fish only (T931 A re-pin)."""
    data, model = built
    tq, bow = "r3:tranquille_lake@3-29::tranquille_lake.r1", "r7:bow_lake@7-15::bow_lake.r1"
    assert _raised_box(data, _card(model, TRANQUILLE, (6, 15)), tq) == (
        ["RB"], None, Z3_TOTAL, Z3_TOTAL, 8, ALL_TROUT, "5 trout/char a day",
        [{"n": 8, "says": "8 if they’re rainbow trout"}], (Z3_TOTAL, 5, ["EB", "GB", "CT", "DV", "LT"], [(["RB"], tq)]))
    got = _raised_box(data, _card(model, BOW_LAKE, (6, 15)), bow)
    assert got[:8] == (["EB"], None, Z7A_TOTAL, Z7A_TOTAL, 10, ALL_TROUT, "5 trout/char a day",
                       [{"n": 10, "says": "10 if they’re brook trout"}])
    assert got[8][1:] == (5, ["GB", "CT", "DV", "LT", "RB"], [(["EB"], bow)])


@pytest.mark.needs_bundle
def test_tranquille_and_a_region_7_brook_trout_lake_are_groups_without_t834(pre_t834):
    """D19 (a), as built before T834 (its one box off)."""
    data, model = pre_t834
    rb = _row(data, _card(model, TRANQUILLE, (6, 15)), "r3:tranquille_lake@3-29::tranquille_lake.r1")
    eb = _row(data, _card(model, BOW_LAKE, (6, 15)), "r7:bow_lake@7-15::bow_lake.r1")
    assert [(r["members"], r["group"]["kinds"], data["rule_ids"][r["group"]["pool"]]) for r in (rb, eb)] == [
        (["RB"], ["RB"], "r3:tranquille_lake@3-29::tranquille_lake.r1"),
        (["EB"], ["EB"], "r7:bow_lake@7-15::bow_lake.r1")]


@pytest.mark.needs_bundle
def test_mutation_the_old_one_kind_rule_leaves_the_rows_headless(built, monkeypatch):
    """MUTATION: without the ruling the rows are back to no group — klmb's, Tranquille's and Bow Lake's. On the D19 (a)
    build (T834's box off, `pre_t834`): with the box on they are held by their region's raised box whatever D19 says."""
    data, _ = built
    monkeypatch.setattr(RW.groups, "A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP", False)
    pools = {p for _i, _s, p in _headless(data, _build_pre_t834(WATERS, monkeypatch))}
    assert {KLMB_RB, "r3:tranquille_lake@3-29::tranquille_lake.r1", "r7:bow_lake@7-15::bow_lake.r1"} <= pools


@pytest.mark.needs_bundle
def test_other_fish_totals_of_one_kind_stay_without_a_group(built):
    """The ruling is trout/char only: Kootenay's crayfish 25 a day (an Other fish row) keeps no group."""
    data, model = built
    row = _row(data, _card(model, KLMB, (10, 10)), "z4:daily_quotas::species_quotas.r3")
    assert (row["members"], row.get("group")) == (["CRA"], None)


# --------------------------------------------------------------------------------------------
# T80 review: the claim over EVERY shipped water, not the five above
# --------------------------------------------------------------------------------------------

#: T834 + T854 (RULINGS C2 refined 2026-11-18): on the Kitimat's tributaries (Humphrys Creek) Nov 1-Jun 30 every other
#: trout goes back, so the river's own "Hatchery rainbow trout … daily quota = 5" is the only trout pool and the zone's
#: total keeps no kind to hold it: a third one-kind row the rows card leaves headless (T931 A, 2026-11-20). Since the T810
#: cutover the trout TABLE draws every trout/char row (`Keep 5 in all` there), so it is drawn; the card gate proves it.
KITIMAT_HATCHERY_RB = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3::kitimat_river.r4"


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
    """Every water the shipped file leaves with a headless trout/char row, rebuilt: what stays headless is ONLY the open
    shapes' pools (z8 r5 on Region 8's streams, Duncan River's DV on its tributaries, and since T854 the Kitimat's
    hatchery rainbow on its tributaries), and the card gate proves every group the rebuild drew (0 problems). On the
    2026-10-22 release: 1,022 waters in, 996 left (982 z8 r5, 14 Duncan), 19 own pools turned into groups, 638 groups on
    247 distinct cards proved.
    T810 CUTOVER (ticket L, contract Q-D9; T931 A re-pin 2026-11-20): the shipped rows the trout table covers carry no
    `group` (the table says them), so every shipped headless trout/char row must sit on a card WITH a trout table — that
    is what draws it now; the rebuild (rows only: no table, no cut) still holds the builder's groups to the open shapes."""
    data, before = shipped
    assert before, "the shipped file has no headless trout/char row: nothing left to prove here"
    import json
    model = E.decode(json.loads((EXPORT_DIR / "ui-rules-answers.json").read_bytes()), data)
    untabled = sorted({item for item in before for k in model.parts[item] if k is not None
                       for card, tbl in zip(model.sections["rows"][k], model.sections["trout"][k])
                       if tbl is None and any(r["pool"] is not None and r.get("parent") is None
                                              and set(r["members"]) <= TROUT_CHAR
                                              and not ((r.get("group") or {}).get("headline")) for r in card["rows"])})
    assert untabled == [], untabled[:10]
    after, problems = _rebuilt_headless(data, before)
    assert ({p for ps in after.values() for p in ps}, problems) == ({Z8_EB, DUNCAN_DV, KITIMAT_HATCHERY_RB}, [])


@pytest.mark.needs_bundle
def test_mutation_without_the_ruling_shipped_waters_keep_other_headless_pools(shipped, monkeypatch):
    """MUTATION of the whole-set test: without the ruling, pools beyond the two open shapes stay headless. The rebuild
    runs over the shipped scan PLUS the five named waters: a file shipped WITHOUT the ruling (the 2026-10-22 release)
    puts its 1,022 waters in the scan, and once the deliver ships the groups the scan holds only the open shapes'
    waters — so the named own pools (klmb's, Tranquille's, Bow Lake's) are what keep this biting either way, never a
    skip."""
    data, before = shipped
    monkeypatch.setattr(RW.groups, "A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP", False)
    monkeypatch.setattr(RW.groups, "A_RAISED_TOTAL_IS_ONE_BOX", False)    # T834's box holds them otherwise (above)
    after, _ = _rebuilt_headless(data, set(before) | set(WATERS))
    left = {p for ps in after.values() for p in ps} - {Z8_EB, DUNCAN_DV, KITIMAT_HATCHERY_RB}
    assert {KLMB_RB, "r3:tranquille_lake@3-29::tranquille_lake.r1", "r7:bow_lake@7-15::bow_lake.r1"} <= left


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
    """klmb Oct 10's frame (a copy) and the index of its rainbow row, whose group is the one-kind trout/char shape (on the
    D19 (a) build, `pre_t834`: with T834's box on the row is held by Region 4's raised total instead)."""
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
def test_the_one_kind_trout_group_validates(pre_t834):
    data, model = pre_t834
    card, _ = _one_kind_frame(data, model)
    assert _check(card) == 1


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_group_of_a_fish_not_trout_or_char_is_refused(pre_t834):
    """MUTATION: the same frame with its rainbow turned into crayfish everywhere its row names it (the row and its group)
    — every other check of the group and the frame still holds, so only the one-kind rule can refuse it."""
    import json
    from pipeline.deliver.answers.model import ShapeError
    data, model = pre_t834
    card, i = _one_kind_frame(data, model)
    assert '"CRA"' not in json.dumps(card["rows"][i])
    card["rows"][i] = json.loads(json.dumps(card["rows"][i]).replace('"RB"', '"CRA"'))
    with pytest.raises(ShapeError, match="several kinds share"):
        _check(card)


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_trout_group_on_another_rules_pool_is_refused(pre_t834):
    """MUTATION: the group's pool moved to a rule no row of the frame pools rainbow under (the crayfish row's pool) — a
    one-kind group is valid only as its own row's total (not Duncan River's DV inside Region 4's 5), so it is refused."""
    from pipeline.deliver.answers.model import ShapeError
    data, model = pre_t834
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


def _xref_eb(card) -> bool:
    """Does any row of the card cross-reference brook trout (an `xref` item naming EB — the displaced St. Mary's 1)?"""
    return any(it.get("xref") and "EB" in it["members"] for r in card["rows"] for it in r.get("items") or ())


@pytest.mark.needs_bundle
def test_perry_creek_brook_trout_is_no_longer_held_by_the_st_mary_row(perry):
    """T127: the reader displaced the St. Mary's 1 for brook trout, so that row no longer cross-references them.
    RE-PINNED (T931 A, 2026-11-20) to T834 (RULINGS T17): Perry Creek's own "brook trout 20" is above the caps that hold
    brook trout there, so it raises them for brook trout ("… 20 if they’re brook trout") and its row is HELD by that one
    box (the St. Mary's tributaries row), no group of its own — but through the raise, never the displaced
    cross-reference (no EB `xref` on the card; the mutation below brings it back)."""
    data, model = perry
    for md in ((6, 15), (7, 15), (10, 10)):
        card = _card(model, PERRY, md)
        row, parent = _perry(data, model, md)
        g = parent["group"]
        assert (row["members"], data["rule_ids"][parent["pool"]], row.get("group"), g["kinds"], g["total"],
                g["headline"].get("also")) \
            == (["EB"], ST_MARY_TRIBS, None, ["EB", "GB", "LT", "RB"], 20, [{"n": 20, "says": "20 if they’re brook trout"}])
        assert not _xref_eb(card), md
    assert _gate(model, [PERRY]) == []


@pytest.mark.needs_bundle
def test_mutation_with_the_displaced_cross_reference_perry_creek_is_held_and_no_group(perry, monkeypatch):
    """MUTATION (T127 off): the St. Mary's row cross-references the brook trout again, so Perry Creek's row is held
    (`parent`) and, by the drawn-row condition, no group — the shape the batch-3 deliver shipped."""
    data, _ = perry
    monkeypatch.setattr(RW.card_model, "A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE", False)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[PERRY], sections=["rows", "ladder"], log=lambda *_: None)
    row, parent = _perry(data, model)
    assert (row["members"], parent is not None and data["rule_ids"][parent["pool"]], row.get("group")) \
        == (["EB"], ST_MARY_TRIBS, None)
    assert _xref_eb(_card(model, PERRY, (6, 15)))          # (T931 A) the displaced 1's cross-reference is back


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_group_on_a_held_row_fails_the_card_gate(perry, monkeypatch):
    """MUTATION: with the St. Mary's displaced cross-reference back (T127 off) and without the drawn-row condition Perry
    Creek's brook trout row gets a group again, worded "1" through that cross-reference, and the card gate refuses it
    (the batch-3 deliver's 30 problems, one key here). (T931 A) On the D19 (a) build, T834's box off: with it on, the
    row is held by the raised box and no group forms either way. The gate's words are its current ones: the kind says
    1 where brook trout alone keep 2."""
    data, _ = perry
    monkeypatch.setattr(RW.card_model, "A_DISPLACED_POOL_IS_NO_CROSS_REFERENCE", False)
    monkeypatch.setattr(RW.assemble, "A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW", False)
    monkeypatch.setattr(RW.groups, "A_RAISED_TOTAL_IS_ONE_BOX", False)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[PERRY], sections=["rows", "ladder"], log=lambda *_: None)
    row, _parent = _perry(data, model)
    assert row["group"]["kinds"] == ["EB"]
    probs = _gate(model, [PERRY])
    assert any(f"total {PERRY_EB}: kind ['EB'] says 1, EB alone keeps 2" in p for p in probs), probs


@pytest.mark.needs_bundle
def test_mutation_a_one_kind_trout_group_on_a_held_row_is_refused_by_the_model(pre_t834):
    """MUTATION: klmb's one-kind rainbow group with its row made held by another pooled row of the frame — the model
    (`RowsFrame._one_kind_groups`) refuses it, as the build's own validation would."""
    from pipeline.deliver.answers.model import ShapeError
    data, model = pre_t834
    card, i = _one_kind_frame(data, model)
    card["rows"][i]["parent"] = next(j for j, r in enumerate(card["rows"]) if j != i and r["pool"] is not None)
    with pytest.raises(ShapeError, match="held by another pool"):
        _check(card)


# --------------------------------------------------------------------------------------------
# T139 (2026-10-23): a third headless pool on the shipped file — Beattie Lake (r7 7-57, in the Liard River watershed).
# The reader lets the Liard's "bull trout 1 (none under 30 cm or over 50 cm)" (liard_river_watershed.r2) displace the
# lake's "trout 1 (none over 50 cm)" for bull trout; both count toward Zone B's 5 (z7b trout_char_quota.r1, `contains`),
# whose group sits on the lake's trout row and already says the bull trout in a bucket of the Liard's rule. Until T127
# the bull trout row was held through the lake row's cross-reference of the DISPLACED lake pool; T127 removed that
# cross-reference (rightly) and the row was left pooled, groupless and unheld — drawn nowhere. Not a third open shape:
# the Duncan shape is a row whose total has NO other row to sit on; here it has one. Fixed at the cause:
# `rows.parent_of`'s second hold (`A_ROW_HELD_BY_ITS_TOTALS_GROUP`).
# --------------------------------------------------------------------------------------------

BEATTIE = "wbk:329665706"
BEATTIE_TROUT = "r7:beattie_lake@7-57::beattie_lake.r2"
LIARD_DV = "r7:liard_river_watershed_see_map_on_page_63@7-53::liard_river_watershed.r2"
Z7B_TOTAL = "z7b:daily_quotas::trout_char_quota.r1"
LIARD_BUCKET = "LIARD RIVER WATERSHED (see map on page 63)’s rule"


@pytest.fixture(scope="module")
def beattie(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, _ = C.load_export(EXPORT_DIR)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[BEATTIE], sections=["rows", "ladder"], log=lambda *_: None)
    return data, model


def _beattie(data, card):
    """(the bull trout row: members, its holder's pool, its group), (the holder's group: pool, total, kinds,
    headline), the holder group's Liard bucket."""
    dv = _row(data, card, LIARD_DV)
    holder = card["rows"][dv["parent"]] if dv.get("parent") is not None else None
    g = (holder or {}).get("group") or {}
    bucket = next(({"title": b["title"], "cap": b["cap"], "by": data["rule_ids"][b["by"]],
                    "kinds": [(k["species"], k["detail"], k["n"], k["notes"]) for k in b["kinds"]]}
                   for b in g.get("buckets") or () if b["title"] == LIARD_BUCKET), None)
    return ((dv["members"], holder and data["rule_ids"][holder["pool"]], dv.get("group")),
            (g.get("pool") is not None and data["rule_ids"][g["pool"]], g.get("total"), g.get("kinds"),
             (g.get("headline") or {}).get("says")),
            bucket)


def _beattie_trout(data, card):
    """The lake's trout row: (its pool, its parent, the group's total pool, kinds, headline), and its tree's kinds as
    (species, n, size_rule)."""
    from pipeline.deliver.answers.tree import walk
    (row,) = [r for r in card["rows"] if r["pool"] is not None and data["rule_ids"][r["pool"]] == BEATTIE_TROUT]
    g = row["group"]
    return ((row.get("parent"), data["rule_ids"][g["pool"]], g["total"], g["kinds"], g["headline"]["says"]),
            [(n["species"], n["n"], n.get("size_rule")) for n, _p in walk(g["tree"]) if n["node"] == "kind"])


@pytest.mark.needs_bundle
def test_beattie_lake_bull_trout_sits_inside_the_lakes_trout_quota(beattie):
    """OWNER 2026-10-28 (Q87, reverses T139's built default): the Liard River watershed's "bull trout 1 (30–50 cm)" sits
    INSIDE Beattie Lake's "Trout daily quota = 1" — 1 trout or char a day in all (was "2 a day": 1 trout + 1 bull
    trout). The reader lets both speak (`named_inside_group`); the rows read the Liard's 1 as inside the lake's 1
    (`evaluate.A_NAMED_POOL_IS_INSIDE`): no bull trout row of its own, the bull trout a kind of the lake's row, kept
    only at 30–50 cm. Sep 1 (bull trout released): no bull trout kind. The card gate: 0 problems."""
    data, model = beattie
    keep = "must be 30–50 cm (12–20 in); release any other size"
    small = "must be 50 cm (20 in) or less; release any longer"
    # (T931 A) bull trout and lake trout are now a kind node each (the same words, the same 1)
    for md, kinds, tree in (
            ((6, 15), ["EB", "GB", "CT", "DV", "LT", "RB"], [(["EB", "GB", "CT", "RB"], 1, small),
                                                              (["DV"], 1, keep), (["LT"], 1, keep)]),
            ((10, 25), ["EB", "GB", "CT", "DV", "RB"], [(["EB", "GB", "CT", "RB"], 1, small), (["DV"], 1, keep)])):
        card = _card(model, BEATTIE, md)
        assert not any(r["pool"] is not None and data["rule_ids"][r["pool"]] == LIARD_DV for r in card["rows"]), md
        assert _beattie_trout(data, card) == ((None, Z7B_TOTAL, 5, kinds, "1 a day per angler"), tree), md
    sep = _card(model, BEATTIE, (9, 1))
    assert "DV" not in _beattie_trout(data, sep)[0][3]
    assert _gate(model, [BEATTIE]) == []


@pytest.mark.needs_bundle
def test_mutation_without_the_named_pool_inside_beattie_bull_trout_loses_the_liards_floor(beattie, monkeypatch):
    """MUTATION (`evaluate.A_NAMED_POOL_IS_INSIDE = False`): the Liard's 1 is read as the lake's OUTER total again and
    its sizes are dropped — the bull trout is said "50 cm or less", without the Liard's 30 cm floor."""
    data, _ = beattie
    monkeypatch.setattr(RW.evaluate, "A_NAMED_POOL_IS_INSIDE", False)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[BEATTIE], sections=["rows"], log=lambda *_: None)
    _head, tree = _beattie_trout(data, _card(model, BEATTIE, (10, 25)))
    small = "must be 50 cm (20 in) or less; release any longer"
    assert tree == [(["EB", "GB", "CT", "RB"], 1, small), (["DV"], 1, small)]     # (T931 A) bull trout its own node
