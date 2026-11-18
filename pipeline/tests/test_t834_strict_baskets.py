"""T834 ON THE BUNDLE UNDER TEST (owner 2026-11-17 11:06 "Strict everywhere", RULINGS T17; GOTCHAS "Questions the owner
will put to BC Fish and Wildlife"): the card gate's engine (`card_check.Engine`: the rules and the reader's verdicts)
keeps exactly the owner's baskets, and the Kaslo card holds its bull trout inside the "2 from streams".

For each region cap C and a water's own daily quota N: N <= C, the fish sits inside C; N > C, C is raised to N over
every fish it holds and the other kinds stay at or under C (`bundle.rules.lifts._cap_lifts`, `answers.rows.conds.pools`).

  Kaslo River, Keen Creek (bull trout 2 under "2 from streams"): 2 in all, both may be bull trout.
  Lardeau River, Duncan River (rainbow 5 over the 2 from streams; bull trout 2): 5 in all; bull trout and the other
      trout 2 at most.
  Kootenay Lake main body (rainbow 10 over "Trout/char: 5"; bull trout 1): 10 in all; the other kinds 5; bull trout 1.
  Adams River (rainbow 2 under "4 from streams"): 4 in all, at most 2 rainbow.
  Buckley Creek (rainbow 1 under "1 trout from streams", Jul 1-Oct 31): 1 trout in all.
  Peace River, Hwy 29 to Site C (lake trout 3 over Zone 7's "2 from streams"): 3 in all, the other trout 2 at most.

MUTATION: with `OWN_QUOTA_ABOVE_A_CAP_RAISES_IT` off the raise is gone (Kootenay keeps 10 rainbow AND a cutthroat; the
Peace 3 lake trout and a cutthroat); with `tree.AN_EQUAL_OWN_TIER_SITS_IN_THE_SHARE` off the Kaslo card says "Not part
of the 2 from streams" again. A bundle built before T834 (the Lardeau's bull trout still lifted out of the share)
fails here (`predates`)."""
from __future__ import annotations

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.tests.conftest import need, predates, BUNDLE_HINT, EXPORT_HINT
from pipeline.tests import under_test

BUNDLE = str(under_test.path("bundle"))
EXPORT_DIR = under_test.path("export_dir")
JUL15 = (7, 15)
Z3, Z4, Z6 = ("z3:daily_quotas::trout_char_quota.r1", "z4:daily_quotas::trout_char_quota.r1",
              "z6:daily_quotas::trout_char_quota.r1")
Z7A, Z7B = "z7a:daily_quotas::trout_char_quota.r1", "z7b:daily_quotas::trout_char_quota.r1"
KASLO, KEEN, LARDEAU, DUNCAN = "gnis:11345", "gnis:11410", "gnis:16359", "gnis:22255"
KOOTENAY, ADAMS, BUCKLEY, PEACE = "wbk:-20", "gnis:39257", "gnis:10123", "gnis:14619"


def f(S: str, n: int = 1, cm: float = 35.0) -> list:
    return [(S, "wild", cm, "here")] * n


#: (water, its region total, the fish its group must hold, {basket name: (basket, legal)})
CASES = [
    (KASLO, (Z4,), "DV", {"2 bull trout": (f("DV", 2), True), "2 bull trout + a cutthroat": (f("DV", 2) + f("CT"), False),
                          "bull trout + cutthroat": (f("DV") + f("CT"), True), "3 cutthroat": (f("CT", 3), False)}),
    (KEEN, (Z4,), "DV", {"2 bull trout": (f("DV", 2), True), "2 bull trout + a cutthroat": (f("DV", 2) + f("CT"), False)}),
    (LARDEAU, (Z4,), "RB", {"5 rainbow": (f("RB", 5), True), "5 rainbow + a cutthroat": (f("RB", 5) + f("CT"), False),
                            "3 rainbow + 2 bull trout": (f("RB", 3) + f("DV", 2), True),
                            "3 rainbow + bull trout + cutthroat": (f("RB", 3) + f("DV") + f("CT"), True),
                            "2 bull trout + a cutthroat": (f("DV", 2) + f("CT"), False)}),
    (DUNCAN, (Z4,), "RB", {"5 rainbow": (f("RB", 5), True), "3 rainbow + 2 bull trout": (f("RB", 3) + f("DV", 2), True),
                           "2 bull trout + a cutthroat": (f("DV", 2) + f("CT"), False)}),
    (KOOTENAY, (Z4,), "RB", {"10 rainbow": (f("RB", 10), True), "10 rainbow + a cutthroat": (f("RB", 10) + f("CT"), False),
                             "9 rainbow + a bull trout": (f("RB", 9) + f("DV"), True),
                             "5 rainbow + 5 cutthroat": (f("RB", 5) + f("CT", 5), True),
                             "6 cutthroat": (f("CT", 6), False), "2 bull trout": (f("DV", 2), False)}),
    (ADAMS, (Z3,), "RB", {"2 rainbow + 2 cutthroat": (f("RB", 2) + f("CT", 2), True), "3 rainbow": (f("RB", 3), False),
                          "2 rainbow + 3 cutthroat": (f("RB", 2) + f("CT", 3), False)}),
    (BUCKLEY, (Z6,), "RB", {"a rainbow": (f("RB"), True), "a rainbow + a cutthroat": (f("RB") + f("CT"), False)}),
    (PEACE, (Z7A, Z7B), "LT", {"3 lake trout": (f("LT", 3), True), "3 lake trout + a cutthroat": (f("LT", 3) + f("CT"), False),
                               "lake trout + 2 cutthroat": (f("LT") + f("CT", 2), True), "3 cutthroat": (f("CT", 3), False)}),
]
#: the baskets the raise decides: legal with `OWN_QUOTA_ABOVE_A_CAP_RAISES_IT` off
RAISED = {(KOOTENAY, "10 rainbow + a cutthroat"), (PEACE, "3 lake trout + a cutthroat")}


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    import sqlite3
    with sqlite3.connect(BUNDLE) as db:
        got = db.execute("select exempts from rule where rule_id = 'lardeau_river.r5'").fetchone()
    if got and got[0] and "trout_char_quota.r3" in got[0]:
        predates("this bundle predates T834: the Lardeau's bull trout 2 still lifts Region 4's 2 from streams")
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({w for w, *_ in CASES})
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, sections=["rows", "ladder"], export=(data, guide),
                    log=lambda *_: None)
    return data, model


#: the water's own row: a part counts only where one of its rules speaks for the fish (Duncan's quotas bind below
#: Duncan Lake; the Peace River row is Hwy 29 to Site C)
OWN = {KASLO: "r4:kaslo_river", KEEN: "r4:keen_creek", LARDEAU: "r4:lardeau_river", DUNCAN: "r4:duncan_river",
       KOOTENAY: "r4:kootenay_lake_main_body", ADAMS: "r3:adams_river_upstream", BUCKLEY: "r6:buckley_creek",
       PEACE: "r7:peace_river_from_hwy_29"}


def _group_parts(data, model, item, totals, fish):
    """(answer key, segment, the group, its total) of every part of `item` whose card on JUL15 has a group of one of
    `totals` holding `fish`, where the water's own row speaks for that fish."""
    out = []
    for k in sorted({k for k in model.parts[item] if k is not None}):
        i = E.segment_index(model.segments[k], CAL.day_of(JUL15))
        lad = model.sections["ladder"][k][i]
        if not any(key.startswith(OWN[item]) and (v or [None])[0] == "speaks"
                   for o in (lad.get(fish) or {}).values() for key, v in (o or {}).items()):
            continue
        for r in model.sections["rows"][k][i]["rows"]:
            g = r.get("group")
            pool = data["rule_ids"][g["pool"]] if g else None
            # the region's total, or (Kootenay Lake's rainbow, drawn as its own group until the one-box change) the
            # water's own pool: the engine reads every rule of the part whichever group it is built for
            if g and (pool in totals or pool.startswith(OWN[item])) and fish in g["kinds"]:
                out.append((k, i, g, pool))
    return out


def _engine(model, k, i, total):
    from pipeline.deliver.answers import card_check as CC
    from pipeline.deliver.answers import rows as RW
    B = C.load(BUNDLE)
    rk, kd = C.rule_key(model.keys[k]), C.key_dict(model.keys[k])
    P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                model.sections["ladder"][k][i], JUL15, {})
    return CC.Engine(P, total)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item,totals,fish,baskets", CASES, ids=[c[0] for c in CASES])
def test_the_engine_keeps_the_owners_baskets(built, item, totals, fish, baskets):
    data, model = built
    parts = _group_parts(data, model, item, totals, fish)
    assert parts, (item, "no card group holds", fish)
    for k, i, _g, total in parts:
        eng = _engine(model, k, i, total)
        got = {name: eng.legal(b) for name, (b, _ok) in baskets.items()}
        assert got == {name: ok for name, (_b, ok) in baskets.items()}, (item, k)


@pytest.mark.needs_bundle
def test_mutation_without_the_raise_the_lifted_fish_is_counted_apart(built, monkeypatch):
    from pipeline.deliver.bundle.rules import lifts as L
    data, model = built
    monkeypatch.setattr(L, "OWN_QUOTA_ABOVE_A_CAP_RAISES_IT", False)
    for item, totals, fish, baskets in CASES:
        for name, (b, ok) in baskets.items():
            if (item, name) not in RAISED:
                continue
            k, i, _g, total = _group_parts(data, model, item, totals, fish)[0]
            assert ok is False and _engine(model, k, i, total).legal(b), (item, name)


def _kaslo_tree(model, data, item):
    (k, i, g, _t), *_ = _group_parts(data, model, item, (Z4,), "DV")
    return g["tree"][0]


def _walk(t, up=None):
    yield t, up
    for x in t.get("children") or []:
        if "tier" in x:
            yield from _walk(x, t)


@pytest.mark.needs_bundle
@pytest.mark.parametrize("item", [KASLO, KEEN])
def test_kaslo_card_holds_its_bull_trout_inside_the_2_from_streams(built, item):
    data, model = built
    tiers = list(_walk(_kaslo_tree(model, data, item)))
    own = [(t, up) for t, up in tiers if t["tier"] == "own"]
    assert own and all(up is not None and up["tier"] == "share" for _t, up in own), item
    assert not any("Not part of" in (t.get("relation") or "") for t, _up in tiers), item


@pytest.mark.needs_bundle
def test_mutation_the_old_nesting_says_not_part_of_the_2(request, monkeypatch):
    from pipeline.deliver.answers import tree as TR
    monkeypatch.setattr(TR, "AN_EQUAL_OWN_TIER_SITS_IN_THE_SHARE", False)
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[KASLO], sections=["rows", "ladder"], export=(data, guide),
                    log=lambda *_: None)
    tiers = list(_walk(_kaslo_tree(model, data, KASLO)))
    assert any("Not part of the 2 from streams" in (t.get("relation") or "") for t, _up in tiers)


def _duncan_dv_groups(model, data):
    """The Duncan's Jan 15 card groups of Region 4's total holding bull trout alone, where its own rule speaks."""
    out = []
    for k in sorted({k for k in model.parts[DUNCAN] if k is not None}):
        i = E.segment_index(model.segments[k], CAL.day_of((1, 15)))
        lad = model.sections["ladder"][k][i]
        if not any(key.startswith(OWN[DUNCAN]) and (v or [None])[0] == "speaks"
                   for o in (lad.get("DV") or {}).values() for key, v in (o or {}).items()):
            continue
        out += [r["group"] for r in model.sections["rows"][k][i]["rows"]
                if r.get("group") and data["rule_ids"][r["group"]["pool"]] == Z4 and r["group"]["kinds"] == ["DV"]]
    return out


@pytest.mark.needs_bundle
def test_duncan_jan_15_its_bull_trout_alone_inside_the_share_is_its_row_not_a_group_of_5(built, request, monkeypatch):
    """Jan 15: the winter release holds every trout but bull trout (the Duncan's exemption is bull trout's); its 2 sits
    inside the 2 from streams — the row says it, never "5 a day across Region 4" over one kind that can never reach 5."""
    from pipeline.deliver.answers.rows import groups as G
    data, model = built
    assert _duncan_dv_groups(model, data) == []
    monkeypatch.setattr(G, "A_ONE_KIND_INSIDE_ITS_SHARE_IS_ITS_ROW", False)
    data2, guide = C.load_export(EXPORT_DIR)
    m2 = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[DUNCAN], sections=["rows", "ladder"], export=(data2, guide),
                 log=lambda *_: None)
    assert _duncan_dv_groups(m2, data2)


# ---- the one box (owner 2026-11-17 12:29: "the total is 5 not 20 or 25 … 5 with exception to 20 brook trout which also
# uses up your 5 quota") -------------------------------------------------------------------------------------------------
JEWEL, PERRY, LOIS = "wbk:329216614", "gnis:14741", "wbk:329197058"


def _tables(item, md, monkeypatch=None, **flags):
    """The trout tables of `item` on `md` (every part whose table keeps something), with module flags set."""
    from pipeline.deliver.answers.trout import table as T
    from pipeline.deliver.answers.rows import groups as G
    from pipeline.deliver.export.codec import expand
    for name, v in flags.items():
        monkeypatch.setattr(T if hasattr(T, name) else G, name, v)
    data, guide = C.load_export(EXPORT_DIR)
    rules = T.Rules.from_export(data, expand(data, guide))
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=[item], sections=["rows", "display", "licence"],
                    export=(data, guide), log=lambda *_: None)
    out = []
    for k in sorted({k for k in model.parts[item] if k is not None}):
        got = T.compose_key(model.sections["rows"][k], model.sections["display"][k], model.segments[k], model.moments[k],
                            rules, licences=model.sections["licence"][k])
        t = got[E.segment_index(model.segments[k], CAL.day_of(md))]
        if t is not None and any(b["kind"] == "keep" for b in t["table"]["boxes"]):
            out.append(t["table"])
    return out


@pytest.mark.needs_bundle
def test_jewel_lake_is_one_box_5_or_20_if_brook_trout(built, monkeypatch):
    (tb,) = _tables(JEWEL, JUL15)
    assert _full(tb) == "Keep 5 trout and char {day}, or 20 if they’re brook trout"     # (T810 L: lead + its subline)
    assert [b["head"] for b in tb["boxes"] if b["kind"] == "keep"] == ["Keep 5 in all, or 20 if they’re brook trout"]
    # MUTATION: two boxes that read as 25
    (tb,) = _tables(JEWEL, JUL15, monkeypatch, A_RAISED_TOTAL_IS_ONE_BOX=False)
    assert [b["kind"] for b in tb["boxes"]].count("keep") == 2


@pytest.mark.needs_bundle
def test_peace_river_says_its_3_in_all_from_the_river(built, monkeypatch):
    tbs = [t for t in _tables(PEACE, JUL15) if "lake trout" in _full(t)]
    assert tbs and all(_full(t) == "Keep 2 trout and char {day}, or 3 if they’re lake trout" for t in tbs)
    assert all(b["head"] == "Keep 2 in all, or 3 if they’re lake trout" for t in tbs for b in t["boxes"]
               if b["kind"] == "keep")
    # MUTATION: without the raised share the box says the zone's 5 as the day's most
    tbs = [t for t in _tables(PEACE, JUL15, monkeypatch, A_RAISED_SHARE_IS_A_BAND=False) if "lake trout" in _full(t)]
    assert any(b["head"] == "Keep 2 in all, or 5 if they’re lake trout" for t in tbs for b in t["boxes"]
               if b["kind"] == "keep")


@pytest.mark.needs_bundle
def test_the_box_head_leads_with_the_regions_number(built, monkeypatch):
    """Owner 13:15 ("lead with 5"): "Keep 5 in all, or 10 if they’re rainbow trout" on Kootenay Lake's main body (the
    top line's form since 2026-11-18); never "Keep 10 in all". MUTATION: the flag off says "Keep 10 in all"."""
    heads = [b["head"] for t in _tables(KOOTENAY, JUL15) for b in t["boxes"] if b["kind"] == "keep"]
    assert heads == ["Keep 5 in all, or 10 if they’re rainbow trout"], heads
    heads = [b["head"] for t in _tables(KOOTENAY, JUL15, monkeypatch, A_HEAD_LEADS_WITH_THE_REGION=False)
             for b in t["boxes"] if b["kind"] == "keep"]
    assert heads == ["Keep 10 in all"], heads


def _full(t) -> str:
    """The top line whole, its subline after it (`words.join_top`)."""
    from pipeline.deliver.answers.trout import words as W
    return W.join_top(t["top"]["lead"], t["top"].get("sub"))


def test_the_excepted_head_reads_back():
    from pipeline.deliver.answers.trout import words as W
    h = W.head_keep_except(5, W.fish_words(["EB"]), 20)
    assert h == "Keep 5 in all, or 20 if they’re brook trout"
    assert W.parse_head(h) == ("keep", 20) and W.parse_head_except(h) == (5, ("EB",), 20)
    assert W.parse_head_except("Keep 5 in all") is None


@pytest.mark.needs_bundle
def test_perry_creek_june_16_the_other_trout_come_to_1(built, monkeypatch):
    tbs = _tables(PERRY, (6, 16))
    assert any(_full(t).startswith("Keep 1 trout and char {day}, or 20 if they’re brook trout") for t in tbs), \
        [_full(t) for t in tbs]
    tbs = _tables(PERRY, (6, 16), monkeypatch, A_TOP_SAYS_WHAT_THE_OTHER_FISH_COME_TO=False,
                  A_TOP_NEVER_PAST_THE_BOX=False)
    assert any(_full(t).startswith("Keep 2 trout and char {day}, or 20 if they’re brook trout") for t in tbs)


@pytest.mark.needs_bundle
def test_lois_lake_its_wild_2_holds_the_rainbow_the_raised_total_brought_in(built, monkeypatch):
    (tb,) = _tables(LOIS, JUL15)
    keep = [b for b in tb["boxes"] if b["kind"] == "keep"]
    assert len(keep) == 1 and "wild" in (keep[0]["strip"] or ""), keep[0]["strip"]
    (tb,) = _tables(LOIS, JUL15, monkeypatch, A_WILD_QUOTA_HOLDS_EVERY_FISH_OF_ITS_RULE=False)
    assert "wild" not in ([b for b in tb["boxes"] if b["kind"] == "keep"][0]["strip"] or "")
