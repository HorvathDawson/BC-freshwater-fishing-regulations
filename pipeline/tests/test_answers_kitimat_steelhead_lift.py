"""KITIMAT RIVER (gnis:3225, Region 6): the river's hatchery steelhead quota lifts Region 6's
"1 over 50 cm" for hatchery steelhead only (ticket "add test", owner 2026-10-20; F9 found the lift
in handoff/PLAN/app-data/F9-72-rows.md and nothing tested it).

THE TEXT (synopsis 2025-2027; PRINTED pages, the repo PDF is not byte-identical: printed = PDF-6
after p.40).

  Region 6 Daily Quotas, printed p.49 (PDF p.55), "(See tables for exceptions)":
    "Trout/char: 5, but not more than
       • 1 over 50 cm (quota includes hatchery steelhead)          <- z6 trout_char_quota.r2
       • 3 Dolly Varden/bull trout and/or lake trout combined
       • 1 trout from streams July 1-Oct 31                        <- z6 trout_char_quota.r4
     And you must release: ...
       • Trout under 30 cm from any stream                         <- z6 trout_char_quota.r6
       • Trout of any size from streams, Nov 1-June 30             <- z6 trout_char_quota.r7
       • all wild steelhead"                                       <- z6 trout_char_quota.r9

  KITIMAT RIVER (6-3) table row, printed p.51 (PDF p.57):
    "Hatchery rainbow trout (adipose clipped, <50 cm) daily quota = 5, no minimum size, all year"
                                                                   <- r6 kitimat_river.r4
    "Hatchery steelhead (adipose clipped, >50 cm) daily quota = 2; province-wide hatchery steelhead
     annual quota = 10. When you have caught and retained your daily steelhead quota from the
     Kitimat, you must stop fishing the Kitimat for the remainder of that day"
                                                                   <- r6 kitimat_river.r5 (+ .r7)

THE READING. r2 is one clause: at most 1 trout/char over 50 cm a day, and it says in so many
words that hatchery steelhead count toward it. Every hatchery steelhead the Kitimat row lets you
keep is over 50 cm ("adipose clipped, >50 cm"), and the row lets you keep 2. Both cannot hold, and
the box sends you to the table for exceptions — so on the Kitimat r2 is LIFTED for hatchery
steelhead (replaced by the row's 2), and only for them:
  * wild steelhead: the row says nothing about them; "all wild steelhead" (r9) still releases them,
    and r2 is NOT lifted for them (it simply falls with the pool they are released from);
  * other trout (cutthroat): r2 still speaks — 1 over 50 cm;
  * r1, the 5 trout/char a day, is NOT lifted: the row raises the steelhead count, it never takes
    hatchery steelhead out of the 5 ("quota includes hatchery steelhead");
  * r4 ("1 trout from streams July 1-Oct 31") cannot hold beside "2 hatchery steelhead" either:
    lifted for hatchery steelhead in its window.
The row prints NO dates on the steelhead quota, so r5 has no window of its own; the two dates
below are in and out of the zone windows it meets: Aug 15 (r4's Jul 1-Oct 31) and Dec 1 (r7's
Nov 1-Jun 30). On both, r2 is lifted for hatchery steelhead and for nothing else.

WHERE THE TEXT WAS AMBIGUOUS — both now ruled by the owner (2026-10-21):
  (a) Dec 1, hatchery steelhead under r7 "Trout of any size from streams, Nov 1-June 30": the
      Kitimat's RAINBOW line says "all year" and its STEELHEAD line does not. OWNER RULING: r7 is
      NOT lifted — hatchery steelhead are catch-and-release Nov 1-June 30, keep 2 only outside it.
      The curated `exempts` of kitimat_river.r5 no longer name r7 (cc775df3; GOTCHAS.md).
      Pinned in `test_kitimat_hatchery_steelhead_are_released_nov_1_to_june_30_owner_ruling`.
  (b) whether the 2 hatchery steelhead still count toward r2's 1-over-50 for the OTHER trout
      (keep 2 hatchery steelhead, then may you keep 1 cutthroat over 50?). OWNER RULING
      2026-10-21: "no you can't. look at the prototype vedder it is similar. it is more a +1 for
      steelhead." THE VEDDER (Region 2, printed p.21): "1 over 50 cm (2 hatchery steelhead over 50
      cm allowed)" — in the data z2 trout_char_quota.r2 "1 over 50 cm" for trout/char OTHER than
      steelhead and .r2b "no more than 2 over 50 cm" for every trout/char, steelhead included; the
      prototype's card (regs-v11 `tierWords`, the size tier's `extra`) says it "Of these, only 1 may
      be over 50 cm (20 in) — 2 if one is a hatchery steelhead", and its kept-elsewhere line "at most
      2 over 50 cm (20 in) a day in all — only 1 of them may be anything other than a hatchery
      steelhead". So "+1 for steelhead" is, in data terms: the other trout/char stay at r2's 1 over
      50 cm (r2 keeps `except: ["ST"]`), and EVERY fish over 50 cm, the hatchery steelhead included,
      counts toward one tally of r5's 2 (`rows.raises`: r5's lift of r2 RAISES it for the steelhead,
      it does not take them out of it). Keep 2 hatchery steelhead and no other trout over 50 cm;
      keep 1 and 1 other may be over 50 cm. The Vedder is the parity witness (`test_..._plus_one_..`):
      the same baskets, the same card words, from its own data.

MUTATION PINS: the text-decided check FAILS if r5 no longer lifts r2, and if it lifts r2 for wild
steelhead too (the reader's cached r5 `exempts` mutated, the Kitimat's verdicts rebuilt into
tmp_path, the answers rebuilt from them). An unmutated rebuild passes it (control). The ruling's
check FAILS on the Kitimat — never on the Vedder — when the raise is gone (the steelhead taken out
of the tally, as before the ruling) and when it lets one more past (r5's 2 + 1).

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path

import pytest

from pipeline.deliver import calendar as CAL
from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.bundle import read as R
from pipeline.deliver.verdicts import build as VB
from pipeline.tests.conftest import need, part_at, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")

KITIMAT = "gnis:3225"
ROW = "r6:kitimat_river_angling_regulations_for_the_kitimat_river_are@6-3"
R5 = (ROW, "kitimat_river.r5")
AUG15, DEC1 = (8, 15), (12, 1)

# short names: "<entry head> <rule>"
K4, K5, K7 = "r6 kitimat_river.r4", "r6 kitimat_river.r5", "r6 kitimat_river.r7"
Z1, Z2, Z4, Z6, Z7, Z9 = (f"z6 trout_char_quota.r{n}" for n in (1, 2, 4, 6, 7, 9))
STOP, POSS = "z6 hatchery_steelhead_stop.r1", "z6 possession_quota.r1"
DEFAULT_POSS = "zp quota_defaults.r1"
ANNUAL, WILD_ST, RECORD = "zp steelhead.r1", "zp steelhead.r2", "zp steelhead.r4"


# ---- building -------------------------------------------------------------------------------

def _export(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    if KITIMAT not in data["waters"]:
        pytest.fail(f"{KITIMAT} (Kitimat River) is missing from this export")
    return data, guide


def _build(data, guide, verdicts=None):
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=[KITIMAT], sections=["rows", "ladder"],
                   export=(data, guide), verdicts=verdicts, log=lambda *_: None)


@pytest.fixture(scope="module")
def shipped(request):
    """The Kitimat's answers from the SHIPPED verdicts (the bundle's sibling verdicts.sqlite)."""
    data, guide = _export(request)
    return data, _build(data, guide)


def _rebuilt(data, guide, tmp_path, monkeypatch, mutate=None):
    """The Kitimat's answers from verdicts REBUILT for its rule keys, with the reader's cached
    kitimat_river.r5 `exempts` passed through `mutate` first (restored by monkeypatch)."""
    B = C.load(BUNDLE)
    keys, parts = C.part_keys(B, data)
    kix = sorted({B.key_ix[C.rule_key(keys[k])] for k in parts[KITIMAT] if k is not None})
    r5 = R._rules_of(BUNDLE)[R5]
    if mutate is not None:
        monkeypatch.setitem(r5, "exempts", mutate(copy.deepcopy(r5["exempts"])))
    out = tmp_path / "verdicts.sqlite"
    VB.build(BUNDLE, out, workers=1, keys=kix, log=lambda *_: None)
    return _build(data, guide, verdicts=str(out))


# ---- reading --------------------------------------------------------------------------------

def _name(ids, i):
    if i is None:
        return None
    e, r = ids[i].split("::")
    return f"{e.split(':')[0]} {r}"


def _named(ids, a):
    """A decided answer with its rule indices as short names."""
    return {**a, "win": _name(ids, a["win"]), "narrow": _name(ids, a["narrow"]),
            "lines": [{k: (_name(ids, v) if k in ("r", "outer") else v) for k, v in L.items()}
                      for L in a["lines"]],
            "roles": [[_name(ids, x), role, _name(ids, y)] for x, role, y in a["roles"]]}


def _at(data, model, md):
    """(rows, ladder) on the mouth part (Hwy 37 bridge to the mouth) on the day."""
    k = model.parts[KITIMAT][part_at(data, KITIMAT, "mouth")]
    i = E.segment_index(model.segments[k], CAL.day_of(md))
    return model.sections["rows"][k][i], model.sections["ladder"][k][i]


def _answer(data, model, md, fish, origin):
    rows, _ = _at(data, model, md)
    return _named(data["rule_ids"], rows["fish"][fish][origin])


def _rung(data, model, md, fish, origin, rule):
    """The reader's verdict on one zone rule: [state, partly lifted by, reason, by]."""
    _, ladder = _at(data, model, md)
    return ladder[fish][origin][f"z6:daily_quotas::trout_char_quota.{rule}"]


# ---- the expected answers, from the text (module docstring) ---------------------------------

HATCHERY_ST_LINES = [{"t": "outer", "r": Z1}, {"t": "annual", "r": ANNUAL}, {"t": "duty", "r": K7},
                     {"t": "duty", "r": RECORD}, {"t": "record", "r": RECORD}]
HATCHERY_ST_TAIL = [[DEFAULT_POSS, "replaced", POSS], [K5, "governs", None], [Z1, "contains", None],
                    [Z6, "floor", None], [ANNUAL, "season", None], [K7, "duty", None],
                    [RECORD, "duty", None], [POSS, "possession", None]]

EXPECTED = {
    AUG15: {
        # keep 2 (over 50 cm); r2 and r4 lifted by r5; still inside the 5 trout/char (r1)
        ("ST", "hatchery"): {
            "status": "keep", "win": K5, "daily": 2, "narrow": None, "lines": HATCHERY_ST_LINES,
            "roles": [[STOP, "replaced", K7], [Z2, "lifted", K5], [Z4, "lifted", K5]] + HATCHERY_ST_TAIL,
            "lift_notes": []},
        # released ("all wild steelhead"); r2 NOT lifted — it falls with the pool
        ("ST", "wild"): {
            "status": "release", "win": Z9, "daily": None, "narrow": None, "lines": [],
            "roles": [[DEFAULT_POSS, "replaced", POSS], [WILD_ST, "replaced", Z9], [Z9, "governs", None],
                      [Z1, "moot", Z9], [POSS, "possession", None], [Z2, "falls", Z1],
                      [Z4, "falls", Z1], [Z6, "moot", Z9]],
            "lift_notes": []},
        # the row's own 5, "no minimum size, all year": lifts r4 and r6, never r2
        ("RB", "hatchery"): {
            "status": "keep", "win": K4, "daily": 5, "narrow": None,
            "lines": [{"t": "steel", "r": K4}, {"t": "outer", "r": Z1}],
            "roles": [[Z4, "lifted", K4], [Z6, "lifted", K4], [DEFAULT_POSS, "replaced", POSS],
                      [K4, "governs", None], [Z1, "contains", None], [POSS, "possession", None]],
            "lift_notes": []},
        ("RB", "wild"): {
            "status": "keep", "win": Z1, "daily": 1, "narrow": Z4,
            "lines": [{"t": "rel", "r": Z6, "a": 0, "b": 30}, {"t": "steel", "r": Z1},
                      {"t": "tnote", "r": Z1, "only": False}],
            "roles": [[DEFAULT_POSS, "replaced", POSS], [Z1, "governs", None], [Z4, "narrows", None],
                      [Z6, "floor", None], [POSS, "possession", None]],
            "lift_notes": []},
        # the positive control: for another trout r2 still speaks — 1 over 50 cm
        ("CT", "wild"): {
            "status": "keep", "win": Z1, "daily": 1, "narrow": Z4,
            "lines": [{"t": "cap", "r": Z2, "a": 50, "b": None, "take": 1},
                      {"t": "rel", "r": Z6, "a": 0, "b": 30}, {"t": "tnote", "r": Z1, "only": False}],
            "roles": [[DEFAULT_POSS, "replaced", POSS], [Z1, "governs", None], [Z4, "narrows", None],
                      [Z2, "limit", None], [Z6, "floor", None], [POSS, "possession", None]],
            "lift_notes": []},
    },
    DEC1: {
        # Nov 1-Jun 30: every trout released from streams (r7), except what the row keeps
        ("ST", "wild"): {
            "status": "release", "win": Z7, "daily": None, "narrow": None, "lines": [],
            "roles": [[Z1, "replaced", Z7], [Z2, "replaced", Z7], [Z6, "moot", Z7],
                      [DEFAULT_POSS, "replaced", POSS], [WILD_ST, "replaced", Z9], [Z7, "governs", None],
                      [Z9, "agrees", Z7], [POSS, "possession", None]],
            "lift_notes": []},
        ("RB", "hatchery"): {
            "status": "keep", "win": K4, "daily": 5, "narrow": None,
            "lines": [{"t": "steel", "r": K4}, {"t": "outer", "r": Z1}],
            "roles": [[Z6, "lifted", K4], [Z7, "lifted", K4], [DEFAULT_POSS, "replaced", POSS],
                      [K4, "governs", None], [Z1, "contains", None], [POSS, "possession", None]],
            "lift_notes": []},
        ("RB", "wild"): {
            "status": "release", "win": Z7, "daily": None, "narrow": None, "lines": [],
            "roles": [[Z1, "replaced", Z7], [Z6, "moot", Z7], [DEFAULT_POSS, "replaced", POSS],
                      [Z7, "governs", None], [POSS, "possession", None]],
            "lift_notes": []},
        ("CT", "wild"): {
            "status": "release", "win": Z7, "daily": None, "narrow": None, "lines": [],
            "roles": [[Z1, "replaced", Z7], [Z2, "replaced", Z7], [Z6, "moot", Z7],
                      [DEFAULT_POSS, "replaced", POSS], [Z7, "governs", None], [POSS, "possession", None]],
            "lift_notes": []},
    },
}

#: ambiguity (b), OWNER RULING 2026-10-21 ("+1 for steelhead"): the pool's general cap (r2) still holds the OTHER
#: trout/char to 1 over 50 cm (steelhead excepted, as the Vedder's), and r5 raises it to one tally of 2 over 50 cm that
#: counts every fish, the hatchery steelhead included (the Vedder's "2 over 50 cm" r2b, here r5's own number)
CAP_CONDS_RULING = [{"c": "cap", "r": Z2, "sub": 1, "a": 50, "b": None, "take": 1, "of": None,
                     "general": True, "except": ["ST"]},
                    {"c": "cap", "r": K5, "sub": 2, "a": 50, "b": None, "take": 2, "of": None,
                     "general": True}]

LIFTED_BY_R5 = ["lifted", None, "lifted", f"{ROW}::kitimat_river.r5"]


def _check_text_decided(data, model):
    """Everything the text decides (module docstring), on Aug 15 and Dec 1."""
    for md, want in EXPECTED.items():
        for (fish, origin), answer in want.items():
            assert _answer(data, model, md, fish, origin) == answer, (md, fish, origin)
    # r2 for hatchery steelhead is lifted by r5 on BOTH dates — the reader's own verdict
    for md in (AUG15, DEC1):
        assert _rung(data, model, md, "ST", "hatchery", "r2") == LIFTED_BY_R5, md
        assert [r for x, r, by in _answer(data, model, md, "ST", "hatchery")["roles"]
                if x == Z2] == ["lifted"], md
    # ... and never for wild steelhead: it speaks in Aug (then falls with the released pool) and
    # is displaced by the Nov-Jun release in Dec — never lifted
    assert _rung(data, model, AUG15, "ST", "wild", "r2") == ["speaks", None, None, None]
    assert _rung(data, model, DEC1, "ST", "wild", "r2") == \
        ["displaced", None, "zone_release", "z6:daily_quotas::trout_char_quota.r7"]


# ---- the tests ------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_kitimat_hatchery_steelhead_quota_lifts_region_6s_one_over_50_for_hatchery_steelhead_only(shipped):
    data, model = shipped
    _check_text_decided(data, model)


@pytest.mark.needs_bundle
def test_kitimat_hatchery_steelhead_are_released_nov_1_to_june_30_owner_ruling(shipped):
    """OWNER RULING 2026-10-21 on (a) (pipeline/docs/GOTCHAS.md): r5's undated hatchery-steelhead quota does NOT lift
    Region 6's "Trout of any size from streams, Nov 1-June 30" release (r7). Hatchery steelhead are catch-and-release
    Nov 1-June 30 and keep 2 only outside it — the window's both edges pinned."""
    data, model = shipped
    head = lambda md: {k: _answer(data, model, md, "ST", "hatchery")[k] for k in ("status", "win", "daily")}
    released = {"status": "release", "win": "z6 trout_char_quota.r7", "daily": None}
    kept = {"status": "keep", "win": "r6 kitimat_river.r5", "daily": 2}
    assert [head(md) for md in (DEC1, (6, 20), (7, 2), (10, 31), (11, 1))] == [released, released, kept, kept, released]
    # r7 SPEAKS for hatchery steelhead in December — never lifted by r5 again
    assert _rung(data, model, DEC1, "ST", "hatchery", "r7") == ["speaks", None, None, None]


@pytest.mark.needs_bundle
def test_kitimat_pool_says_the_hatchery_steelhead_count_toward_a_2_over_50_tally(shipped):
    """OWNER RULING 2026-10-21 on (b) (module docstring): the pool's conditions keep r2's 1 over
    50 cm for the other trout/char (steelhead excepted) and add r5's one tally of 2 over 50 cm,
    every fish counted — while r5 applies (Jul 1-Oct 31, ruling (a)); in December there is no steelhead tally."""
    data, model = shipped

    def caps(md):
        rows, _ = _at(data, model, md)
        (pool,) = [r for r in rows["rows"] if r.get("pool") is not None and
                   data["rule_ids"][r["pool"]] == "z6:daily_quotas::trout_char_quota.r1"]
        return [{**c, "r": _name(data["rule_ids"], c["r"])} for c in pool["conds"] if c["c"] == "cap"]
    assert caps(AUG15) == CAP_CONDS_RULING
    # December (ruling (a)): the steelhead are released, so there is no steelhead tally — r2's plain 1 over 50 cm
    assert caps(DEC1) == [{"c": "cap", "r": "z6 trout_char_quota.r2", "sub": 1, "a": 50, "b": None, "take": 1,
                           "of": None, "general": True}]


# ---- the ruling, with the Vedder as its parity witness ---------------------------------------

VEDDER = "gnis:3062"
OCT10 = (10, 10)
#: (water, the day, the trout/char total, the origin of the cutthroat it keeps): the Vedder on the
#: prototype's own day (regs-v11 `vedder_1010`), where Region 2 keeps hatchery trout only from
#: streams; the Kitimat in August, where Region 6 keeps either origin
PARITY = ((VEDDER, OCT10, "z2:daily_quotas::trout_char_quota.r1", "hatchery"),
          (KITIMAT, AUG15, "z6:daily_quotas::trout_char_quota.r1", "wild"))


def _baskets(origin):
    """What may be kept over 50 cm on one day: hatchery steelhead (all over 50 cm) and a cutthroat
    over 50 cm, here and — the kept-elsewhere line — on a lake of the region first."""
    st, ct = ("ST", "hatchery", 60.0, "here"), ("CT", origin, 55.0, "here")
    lake = ("CT", origin, 55.0, "lake")
    return {"2 hatchery steelhead": [st, st],
            "1 hatchery steelhead, then 1 cutthroat over 50 cm": [st, ct],
            "1 cutthroat over 50 cm, then 1 hatchery steelhead": [ct, st],
            "2 hatchery steelhead, then 1 cutthroat over 50 cm": [st, st, ct],
            "1 cutthroat over 50 cm between 2 hatchery steelhead": [st, ct, st],
            "2 cutthroat over 50 cm": [ct, ct],
            "a cutthroat over 50 cm kept at a lake, then 1 hatchery steelhead": [lake, st],
            "a cutthroat over 50 cm kept at a lake, then 2 hatchery steelhead": [lake, st, st]}


#: "+1 for steelhead": 1 over 50 cm, 2 if one is a hatchery steelhead — never 2 steelhead AND another
PLUS_ONE = {"2 hatchery steelhead": True,
            "1 hatchery steelhead, then 1 cutthroat over 50 cm": True,
            "1 cutthroat over 50 cm, then 1 hatchery steelhead": True,
            "2 hatchery steelhead, then 1 cutthroat over 50 cm": False,
            "1 cutthroat over 50 cm between 2 hatchery steelhead": False,
            "2 cutthroat over 50 cm": False,
            "a cutthroat over 50 cm kept at a lake, then 1 hatchery steelhead": True,
            "a cutthroat over 50 cm kept at a lake, then 2 hatchery steelhead": False}

SIZE_TIER = {"tier": "size", "n": 1, "over_cm": 50,
             "says": "Of these, only 1 may be over 50 cm (20 in) — 2 if one is a hatchery steelhead",
             "extra": {"n": 1, "species": ["ST"]}}
OVER_50_ELSEWHERE = {
    "q": "Kept trout or char over 50 cm (20 in) elsewhere today?",
    "a": "It counts here too: at most 2 over 50 cm (20 in) a day in all — only 1 of them may be anything "
         "other than a hatchery steelhead."}


def _walk(nodes):
    for n in nodes or []:
        yield n
        yield from _walk(n.get("children"))


def _check_plus_one(data, model):
    """The ruling on each water of PARITY: the card gate's engine (`card_check.Engine`, the rules and
    the reader's verdicts) keeps exactly the PLUS_ONE baskets, and the card says it in the Vedder's
    words — its size tier and its kept-elsewhere line."""
    from pipeline.deliver.answers import card_check as CC
    from pipeline.deliver.answers import rows as RW
    B = C.load(BUNDLE)
    for item, md, total, origin in PARITY:
        if item == KITIMAT:
            k = model.parts[KITIMAT][part_at(data, KITIMAT, "mouth")]
        else:
            (k,) = [k for k in model.parts[item] if k is not None]
        i = E.segment_index(model.segments[k], CAL.day_of(md))
        rk, kd = C.rule_key(model.keys[k]), C.key_dict(model.keys[k])
        P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                    model.sections["ladder"][k][i], md, {})
        eng = CC.Engine(P, total)
        got = {name: eng.legal(b) for name, b in _baskets(origin).items()}
        assert got == PLUS_ONE, item
        (g,) = [r["group"] for r in model.sections["rows"][k][i]["rows"]
                if r.get("group") and data["rule_ids"][r["group"]["pool"]] == total]
        sizes = [{**{x: t[x] for x in ("tier", "n", "over_cm", "says")},
                  "extra": {x: t["extra"][x] for x in ("n", "species")} if t.get("extra") else None}
                 for t in _walk(g["tree"]) if t.get("tier") == "size"]
        assert sizes == [SIZE_TIER], item
        over = [{"q": w["q"], "a": w["a"]} for w in g["worked"] if w.get("over_cm") == 50]
        assert over == [OVER_50_ELSEWHERE], item


def _build_pair(data, guide):
    return A.build(BUNDLE, EXPORT_DIR, workers=1, items=[KITIMAT, VEDDER], sections=["rows", "ladder"],
                   export=(data, guide), log=lambda *_: None)


@pytest.fixture(scope="module")
def pair(request):
    """The Kitimat's and the Vedder's answers, built by the code from the SHIPPED verdicts."""
    data, guide = _export(request)
    return data, _build_pair(data, guide)


@pytest.mark.needs_bundle
def test_kitimat_hatchery_steelhead_are_plus_one_over_50_like_the_vedders(pair):
    """OWNER RULING 2026-10-21 on (b): on the Kitimat, as on the Vedder, the hatchery steelhead are
    "+1" over 50 cm: keep 2 and no other trout over 50 cm; keep 1 and 1 other may be."""
    _check_plus_one(*pair)


def _no_raise(orig):
    return lambda P: []


def _raise_by_one_more(orig):
    from pipeline.deliver.answers import rows as RW
    return lambda P: [RW.raised(x.clause, x.lifter, x.lifted, x.f["take"] + 1) for x in orig(P)]


@pytest.mark.needs_bundle
@pytest.mark.parametrize("mutate", [_no_raise, _raise_by_one_more], ids=["no-raise", "raise-by-one-more"])
def test_the_ruling_check_fails_on_the_kitimat_only_when_the_raise_is_mutated(request, monkeypatch, mutate):
    """MUTATION PINS: the steelhead taken out of the tally (the behaviour before the ruling) and a
    tally one larger than r5's 2 each FAIL the ruling's check — on the Kitimat's baskets, never on
    the Vedder, whose "+1" is its own data (r2b)."""
    from pipeline.deliver.answers import rows as RW
    data, guide = _export(request)
    monkeypatch.setattr(RW.conds, "raises", mutate(RW.raises))
    with pytest.raises(AssertionError) as got:
        _check_plus_one(data, _build_pair(data, guide))
    assert str(got.value).splitlines()[0] == KITIMAT, str(got.value)[:400]


@pytest.mark.needs_bundle
def test_an_unmutated_rebuild_of_the_kitimats_verdicts_passes_the_check(request, tmp_path, monkeypatch):
    """CONTROL for the mutation pins below: rebuilding the Kitimat's verdicts is not, by itself,
    what makes them fail."""
    data, guide = _export(request)
    _check_text_decided(data, _rebuilt(data, guide, tmp_path, monkeypatch))


def _no_r2_lift(ex):
    return [e for e in ex if e["rule_id"] != "trout_char_quota.r2"]


def _r2_lift_for_every_origin(ex):
    for e in ex:
        if e["rule_id"] == "trout_char_quota.r2":
            assert e.pop("origin") == "hatchery"
    return ex


@pytest.mark.needs_bundle
@pytest.mark.parametrize("mutate, first_wrong", [
    (_no_r2_lift, "((8, 15), 'ST', 'hatchery')"),
    (_r2_lift_for_every_origin, "((8, 15), 'ST', 'wild')"),
], ids=["the-lift-removed", "the-lift-also-for-wild-steelhead"])
def test_the_check_fails_when_the_lift_is_mutated(request, tmp_path, monkeypatch, mutate, first_wrong):
    """MUTATION PINS: r5 no longer lifting r2 (hatchery steelhead get r2's 1-over-50 back), and
    r5 lifting r2 for wild steelhead too, each FAIL the text-decided check — at the answer the
    mutation is about, not somewhere else."""
    data, guide = _export(request)
    mutated = _rebuilt(data, guide, tmp_path, monkeypatch, mutate)
    with pytest.raises(AssertionError) as got:
        _check_text_decided(data, mutated)
    assert str(got.value).splitlines()[0] == first_wrong, str(got.value)[:400]



#: Region 6 "No fishing: in all rivers and streams for steelhead, May 15-June 15" (printed p.49) is a ST_CLOSURE_NAME,
#: and r5's lift of the quota cap must never reopen it: on May 20 both origins are closed by it, and the row's
#: own quota (r5) is moot under it; on June 20 the Nov-Jun release still holds, and from July 2 r5's keep 2 applies.
MAY20, JUN20 = (5, 20), (6, 20)
ST_CLOSURE_NAME = "z6 steelhead_stream_closure.r1"
R5_NAME = "r6 kitimat_river.r5"


def _head(a):
    return {k: a[k] for k in ("status", "win", "daily")}


@pytest.mark.needs_bundle
def test_r5s_lift_never_reopens_region_6s_may_15_june_15_steelhead_closure(shipped):
    data, model = shipped
    hatch, wild = (_answer(data, model, MAY20, "ST", o) for o in ("hatchery", "wild"))
    assert _head(hatch) == {"status": "closed", "win": ST_CLOSURE_NAME, "daily": None}
    assert _head(wild) == {"status": "closed", "win": ST_CLOSURE_NAME, "daily": None}
    assert [[x, r, by] for x, r, by in hatch["roles"] if x == R5_NAME] == [[R5_NAME, "moot", ST_CLOSURE_NAME]]
    assert [[x, r, by] for x, r, by in hatch["roles"] if x == ST_CLOSURE_NAME] == [[ST_CLOSURE_NAME, "governs", None]]
    # June 20: the closure is over but the Nov 1-June 30 release still holds (ruling (a)); the quota returns July 2
    assert _head(_answer(data, model, JUN20, "ST", "hatchery")) == \
        {"status": "release", "win": "z6 trout_char_quota.r7", "daily": None}
    assert _head(_answer(data, model, (7, 2), "ST", "hatchery")) == {"status": "keep", "win": R5_NAME, "daily": 2}
    assert _head(_answer(data, model, JUN20, "ST", "wild"))["status"] == "release"


