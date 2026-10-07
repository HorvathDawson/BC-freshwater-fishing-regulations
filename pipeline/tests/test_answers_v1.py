"""THE ANSWERS FILE v1: every section (ladder, answer, rows, gear, licence, display) keyed alike,
round-tripped, its refs in range, every tap equal to its producer, and the card's rows pinned on real
waters.

  * encode -> decode is the identity for every section and static table; the spec covers every key;
  * every rule ref indexes the export's `rules`, every licensing ref its `licensing`;
  * a tap's gear, licence, display and rows equal what the producer gives for that key and day,
    asked afresh (gear.gear_answer, licence.key_year, status_index.set_profile, rows.produce on the
    reader's own ladder);
  * the card (consumer 5.2-5.8) on real waters: the Peace River's "Really 2 a day", the Chilliwack's
    Region 2 stream share and its hatchery-only origin line, the Kitimat's wild-fish origin2 line;
  * mutation: drop a clause from the ladder and the narrowed number moves.

`UI_EXPORT_BUNDLE` and `ANSWERS_EXPORT_DIR` (the export pair cut from that bundle) point the suite at
a side bundle; the bundle tests skip without them.
"""
from __future__ import annotations

import copy
import json
import os
import random
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import gear as G
from pipeline.deliver.answers import licence as L
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")

CHILLIWACK, KITIMAT, PEACE = "gnis:8634", "gnis:3225", "gnis:14619"
WATERS = (CHILLIWACK, KITIMAT, PEACE, "gnis:39298", "wbk:329518145")    # + Denetiah, Shuswap Lake


@pytest.fixture(scope="module")
def built():
    if not Path(BUNDLE).is_file() or not (EXPORT_DIR / "ui-rules-export.json").is_file():
        pytest.skip("no bundle / export pair (UI_EXPORT_BUNDLE, ANSWERS_EXPORT_DIR)")
    data, guide = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.skip(f"not in this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, log=lambda *_: None)
    wire = json.loads(json.dumps(E.encode(model, data)))
    return data, guide, model, wire


def test_every_section_round_trips_and_is_described(built):
    data, guide, model, wire = built
    assert set(wire["sections"]) == {s.name for s in A.SECTIONS}
    back = E.decode(wire, data)
    assert back == model
    assert E.spec_gaps(wire) == []
    E.check_pair(wire, data, guide)
    for name, sec in wire["sections"].items():
        assert len(sec["at"]) == len(wire["keys"])
        for k, at in enumerate(sec["at"]):
            assert len(at) == len(wire["segments"][wire["keys"][k][E.SEG_SLOT]])
            assert all(0 <= f < len(sec["frames"]) for f in at)


def test_a_section_without_spec_or_codec_is_refused(built):
    _, _, _, wire = built
    bad = copy.deepcopy(wire)
    bad["sections"]["rows"]["surprise"] = []
    bad["sections"]["mystery"] = {"version": 1}
    gaps = " | ".join(E.spec_gaps(bad))
    assert "rows.surprise" in gaps and "'mystery' has no codec" in gaps


def test_rule_and_licensing_refs_are_in_range(built):
    data, _, _, wire = built
    nR, nL = len(data["rule_ids"]), len(data["licensing_ids"])
    rows = wire["sections"]["rows"]
    for r in rows["rows"]:
        for f in ("pool", "win", "narrow"):
            assert r[f] is None or 0 <= r[f] < nR
        for l in r["everyone"] + [l for g in r["groups"] for l in g["facts"]]:
            assert 0 <= l["r"] < nR and all(0 <= i < nR for i in l["rules"])
    for d in rows["decided"]:
        assert 0 <= d["win"] < nR
        assert all(0 <= l["r"] < nR for l in d["lines"])
        assert all(0 <= k < nR and (by is None or 0 <= by < nR) for k, _, by in d["roles"])
    for g in wire["sections"]["gear"]["frames"]:
        for v in g["elements"].values():
            assert 0 <= v["by"][0] < nR and v["by"][1] >= 0
        for v in g["counts"].values():
            assert 0 <= v["by"][0] < nR
    lic = wire["sections"]["licence"]
    for h in lic["holds"]:
        assert all(0 <= i < nL for i in h["holds"] + h["considered"] + h["designations"])
    for a in lic["answers"]:
        assert all(0 <= r["req"] < nL for r in a["requirements"])
    assert all(len(d) == 60 for d in lic["documents"])
    assert len(wire["sections"]["display"]["rules"]) == nR


def _days(wire, k, rng, n=4):
    starts = wire["segments"][wire["keys"][k][E.SEG_SLOT]]
    return sorted({rng.choice(range(s, (starts[i + 1] if i + 1 < len(starts) else 367)))
                   for i, s in enumerate(starts)} | {rng.randint(1, 366) for _ in range(n)})


def test_taps_equal_the_producers(built):
    """Every section's frame for a (key, day) is what its producer says for that key and day."""
    from pipeline.deliver import status_index as SI
    data, _, model, wire = built
    B = C.load(BUNDLE)
    rix = C.check_export(B, data, C.load_export(EXPORT_DIR)[1])
    lawful = G.province_methods(B.rules.values())
    db = C.connect(B.path)
    Cp = L.corpus(db)
    K = L.keys(db)
    rng = random.Random(7)
    checked = 0
    try:
        for k, key in enumerate(model.keys):
            rk = C.rule_key(key)
            lk = L.section_scope(key, B)
            prof = SI.set_profile(B.sets[rk.set_id], rk.steelhead_water, B.path, rk.steelhead_rules)
            for day in _days(wire, k, rng):
                md = C.month_day(day)
                s = E.segment_index(model.segments[k], day)
                g = G.gear_answer(B, rk, md, lawful, ref=lambda x: rix[C.rule_id(x)])
                assert model.sections["gear"][k][s] == json.loads(json.dumps(g)), (key, day)
                h = L.holds(db, Cp, K[lk][0], md)
                assert [Cp.index[x] for x in h["holds"]] == \
                    model.sections["licence"][k][s]["holds"]["holds"], (key, day)
                assert model.sections["display"][k][s]["status"] == \
                    {SI.BASE: "base", SI.OWN: "own", SI.CLOSED: "closed"}[prof[day - 1]]
                ladder = {f: {o: A.ladder_verdict(R.effective_rules_bound(
                    B.sets[rk.set_id], rk.steelhead_water, md, f, B.path,
                    steelhead_rules_here=rk.steelhead_rules,
                    origin=None if o == "none" else o, trace=True)) for o in A.ORIGINS}
                    for f in A.fish_of(B.sets[rk.set_id], B.rules, rk.steelhead_rules)}
                assert model.sections["ladder"][k][s] == ladder, (key, day)
                kd = C.key_dict(key)

                class Ctx:
                    pass
                ctx = Ctx()
                ctx.B, ctx.sets, ctx.rules, ctx.bundle, ctx.cache = B, B.sets, B.rules, B.path, {}
                P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"],
                            kd["steelhead"], ladder, md, RW.open_states(ctx, rk, md))
                want = RW.to_refs(json.loads(json.dumps(RW.produce(P))), rix.__getitem__)
                assert model.sections["rows"][k][s] == want, (key, day)
                checked += 1
    finally:
        db.close()
    assert checked > 40


# --------------------------------------------------------------------------------------------
# The card on real waters
# --------------------------------------------------------------------------------------------

def _tap(built, item, part, md):
    data, _, _, wire = built
    return E.tap(wire, data, item, part, *md)


def _rid(data, i):
    return data["rule_ids"][i].split("::", 1)[1]


FIXTURE = Path(__file__).with_name("fixtures") / "answers_v1_peace_page.json.gz"


def test_really_2_a_day_on_the_pages_own_ladder():
    """The real daily limit (5.7) on a real water: the Peace River below the Site C dam, Jul 1,
    read with the page v35's OWN ladder (its golden states, fixture): lake trout 3 a day here, but
    only 2 can be kept — "Really 2 a day here". The port reproduces the page's card exactly."""
    import gzip
    if not Path(BUNDLE).is_file():
        pytest.skip("no bundle")
    fx = json.loads(gzip.open(FIXTURE, "rt").read())
    B = C.load(BUNDLE)
    data, guide = C.load_export(EXPORT_DIR)
    keys, parts = C.part_keys(B, data)
    k = keys[parts[fx["water"]][1]]
    rk, kd = C.rule_key(k), C.key_dict(k)
    ladder: dict = {}
    for fo, states in fx["ladder"].items():
        f, o = fo.split("|")
        ladder.setdefault(f, {})[o] = {r: [s[0], ["?"] if s[1] else None, None, s[2]]
                                       for r, s in states.items()}
    for f in fx["spp"]:
        ladder.setdefault(f, {"hatchery": {}, "wild": {}})

    class Ctx:
        pass
    ctx = Ctx()
    ctx.B, ctx.sets, ctx.rules, ctx.bundle, ctx.cache = B, B.sets, B.rules, B.path, {}
    md = tuple(fx["md"])
    P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                ladder, md, RW.open_states(ctx, rk, md))
    out = RW.produce(P)
    assert out["spp"] == fx["spp"]
    got = [{"kind": r["kind"], "pool": r["pool"], "win": r["win"], "members": r["members"],
            "daily": r["daily"]} for r in out["rows"]]
    assert got == [{k: v for k, v in r.items() if k != "real_daily"} for r in fx["rows"]]
    lt = next(r for r in out["rows"] if r["pool"] and r["pool"].endswith("::peace_river.r4"))
    assert lt["daily"] == 3
    assert lt["real_daily"]["all"] and lt["real_daily"]["sum"] == 2     # "Really 2 a day here"
    # mutation: without the page's lake-trout clause in force the cap is gone
    for o in ladder["LT"].values():
        o.pop("z7b:trout_char_quota::trout_char_quota.r4", None)
    P2 = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                 ladder, md, RW.open_states(ctx, rk, md))
    lt2 = next(r for r in RW.produce(P2)["rows"] if r["pool"] and r["pool"].endswith("::peace_river.r4"))
    assert lt2["real_daily"] is None


def test_peace_river_on_our_ladder_keeps_the_lakes_own_3(built):
    """The shipped answer: our reader lets the lake row's "Lake trout 3" replace Region 7B's lake
    trout clause (the same statement, the water's number), so no smaller cap binds and there is no
    "Really 2" — a ladder difference with the page (COMPARISON.md, class b1)."""
    data = built[0]
    t = _tap(built, PEACE, 1, (7, 1))
    row = next(r for r in t["rows"]["rows"] if r["pool"] is not None
               and _rid(data, r["pool"]) == "peace_river.r4")
    assert row["daily"] == 3 and row["real_daily"] is None
    lt = t["ladder"]["LT"]["wild"]["z7b:trout_char_quota::trout_char_quota.r4"]
    assert lt[0] == "displaced" and lt[3].endswith("::peace_river.r4")


def test_chilliwack_stream_share_and_hatchery_only(built):
    """Region 2's trout and char: 4 a day region-wide, 2 of them from streams (the share holds on
    the river), hatchery only (wild ones go back)."""
    data = built[0]
    t = _tap(built, CHILLIWACK, 1, (7, 1))["rows"]
    row = next(r for r in t["rows"] if r["pool"] is not None
               and _rid(data, r["pool"]) == "trout_char_quota.r1")
    assert row["daily"] == 2 and _rid(data, row["narrow"]) == "trout_char_quota.r4"
    assert row["scope"] == {"of": "region", "entry": "z2:trout_char_quota", "share": True}
    origin = [l for l in row["everyone"] if l["t"] == "origin"]
    assert origin and origin[0]["o"] == "wild" and origin[0]["keepO"] == "hatchery"
    rb = t["fish"]["RB"]["hatchery"]
    assert rb["status"] == "keep" and rb["daily"] == 2
    assert t["fish"]["RB"]["wild"]["status"] == "release"
    assert t["steelhead_line"] == "known_with_rules"


def test_kitimat_wild_fish_have_their_own_limit(built):
    data = built[0]
    t = _tap(built, KITIMAT, 0, (7, 1))["rows"]
    lines = [l for r in t["rows"] for g in [{"facts": r["everyone"]}] + r["groups"]
             for l in g["facts"] if l["t"] == "origin2"]
    assert lines and all(l["o"] == "wild" for l in lines)


def test_a_clause_dropped_from_the_ladder_moves_the_number(built):
    """MUTATION: take the stream share out of the Chilliwack's ladder and the number is the
    region's 4 again; the rows read the ladder, never re-decide it."""
    data, _, model, wire = built
    B = C.load(BUNDLE)
    k = wire["parts"][CHILLIWACK][1]
    key = model.keys[k]
    rk, kd = C.rule_key(key), C.key_dict(key)
    s = E.segment_index(model.segments[k], C.day_of(7, 1))
    ladder = copy.deepcopy(model.sections["ladder"][k][s])
    share = next(i for i in data["rule_ids"] if i.endswith("::trout_char_quota.r4")
                 and i.startswith("z2:"))
    for f in ladder.values():
        for o in f.values():
            if share in o:
                o[share] = ["displaced", None, "ladder", data["rule_ids"][0]]

    class Ctx:
        pass
    ctx = Ctx()
    ctx.B, ctx.sets, ctx.rules, ctx.bundle, ctx.cache = B, B.sets, B.rules, B.path, {}
    P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                ladder, (7, 1), RW.open_states(ctx, rk, (7, 1)))
    out = RW.produce(P)
    assert out["fish"]["RB"]["hatchery"]["daily"] == 4
