"""THE CARD'S ROWS, FIXED where the consumer page v35 contradicts the book (handoff ROWS-REVIEW;
rows.DECISIONS F1-F10). Every fix on the real card it was found on, each with a mutation:

  * F1  Anderson R., Jul 1: brook, brown and cutthroat trout are "up to 4, only 1 over 50 cm"
        (not "keep 1, over 60 cm"): one item per kind;
  * F2  Chilliwack Lake, Jul 15: "No wild trout over 50 cm" shows although the wild daily number
        and winner are the hatchery's;
  * F3  Kitimat R., Feb 15: the cutthroat and brown trout release survives (fish SETS compared);
  * F4  Vedder R., Jan 15: hatchery rainbow counts toward Region 2's 4, not the stream share's 2;
        its own row reads 0-50 cm (F5c) and still counts toward the region (F7);
  * F5a Teslin L.: slots are bands of 0; F6 its possession limits are not yearly limits;
  * F5b Thompson R. below Kamloops L., Aug: the zone's "1 over 50 cm" caps the water's own 2;
  * F5d Region 8 stream: every shared cap counts ("only 2 over 30 cm" too);
  * F7/F8 Tranquille's rainbow 8 is counted apart; Haida Gwaii's quota is an area's.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read as R

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR")
                  or Path(R.BUNDLE).parent.parent / "regs")

ANDERSON, CHILLIWACK_L, KITIMAT, VEDDER = "gnis:10030", "wbk:329083341", "gnis:3225", "gnis:3062"
TESLIN, THOMPSON, MAYER, TRANQUILLE = "wbk:328961703", "gnis:39492", "wbk:329163196", "wbk:329563842"
R8_STREAM = "gnis:10098"            # Buchanan Creek (Region 8)
WATERS = (ANDERSON, CHILLIWACK_L, KITIMAT, VEDDER, TESLIN, THOMPSON, MAYER, TRANQUILLE, R8_STREAM)


@pytest.fixture(scope="module")
def built():
    if not Path(BUNDLE).is_file() or not (EXPORT_DIR / "ui-rules-export.json").is_file():
        pytest.skip("no bundle / export pair (UI_EXPORT_BUNDLE, ANSWERS_EXPORT_DIR)")
    data, _ = C.load_export(EXPORT_DIR)
    missing = [w for w in WATERS if w not in data["waters"]]
    if missing:
        pytest.skip(f"not in this export: {missing}")
    model = A.build(BUNDLE, EXPORT_DIR, workers=1, items=WATERS, sections=["rows"],
                    log=lambda *_: None)
    return data, model, C.load(BUNDLE)


def _rid(data, i):
    return data["rule_ids"][i]


def _ks(model, item, part, md):
    k = model.parts[item][part]
    return k, E.segment_index(model.segments[k], C.day_of(*md))


def card(built, item, part, md, mutate=None, patch=None):
    """The card's rows for one part and day: as built, or recomputed from the built ladder after
    `mutate(ladder, rid)` (an input mutation)."""
    data, model, B = built
    k, s = _ks(model, item, part, md)
    if mutate is None:
        return model.sections["rows"][k][s]
    ladder = copy.deepcopy(model.sections["ladder"][k][s])
    mutate(ladder, lambda tail: next(i for i in data["rule_ids"] if i.endswith(tail)))
    key = model.keys[k]
    rk, kd = C.rule_key(key), C.key_dict(key)

    class Ctx:
        pass
    ctx = Ctx()
    ctx.B, ctx.sets, ctx.rules, ctx.bundle, ctx.cache = B, B.sets, B.rules, B.path, {}
    P = RW.Part(B, rk.set_id, rk.steelhead_water, rk.steelhead_rules, kd["kind"], kd["steelhead"],
                ladder, md, RW.open_states(ctx, rk, md))
    rix = {x: i for i, x in enumerate(data["rule_ids"])}
    return RW.to_refs(json.loads(json.dumps(RW.produce(P))), rix.__getitem__)


def row_of(built, t, pool_tail):
    data = built[0]
    return next(r for r in t["rows"] if r["pool"] is not None
                and _rid(data, r["pool"]).endswith(pool_tail))


def item_of(row, S):
    got = [it for it in row["items"] if S in it["members"]]
    assert len(got) == 1, (S, row["items"])          # F1: every kind in exactly one item
    return got[0]


def displace(*tails, fish=None, origins=("none", "hatchery", "wild")):
    """An input mutation: the named rules no longer speak (for `fish`, or every fish)."""
    def m(ladder, rid):
        for t in tails:
            k = rid(t)
            for f, v in ladder.items():
                if fish and f not in fish:
                    continue
                for o in origins:
                    if k in v.get(o, {}):
                        v[o][k] = ["displaced", None, "ladder", k]
    return m


# --------------------------------------------------------------------------------------------
# F1 one item per kind
# --------------------------------------------------------------------------------------------

def test_anderson_brook_brown_cutthroat_up_to_4_only_1_over_50(built):
    data = built[0]
    row = row_of(built, card(built, ANDERSON, 0, (7, 1)), "z3:trout_char_quota::trout_char_quota.r1")
    members = [S for it in row["items"] for S in it["members"]]
    assert len(members) == len(set(members))
    eb = item_of(row, "EB")
    assert sorted(eb["members"]) == ["CT", "EB", "GB"]
    assert eb["bands"] == [[0, 50, 4], [50, None, 1]]
    dv = item_of(row, "DV")
    assert sorted(dv["members"]) == ["DV", "LT"] and dv["bands"] == [[60, None, 1]] and dv["sub"] == 1
    assert item_of(row, "RB")["bands"] == [[0, 50, 4]]                 # F5c
    cap = next(c for c in row["conds"] if c["c"] == "cap")
    assert cap["general"] and cap["of"] is None and cap["except"] == ["RB"]       # F1b
    assert _rid(data, cap["r"]).endswith("trout_char_quota.r3")
    # MUTATION: without the bull/lake trout 60 cm minimum their range moves; brook trout's does not
    m = row_of(built, card(built, ANDERSON, 0, (7, 1), displace("z3:trout_char_quota::trout_char_quota.r4b")),
               "z3:trout_char_quota::trout_char_quota.r1")
    assert item_of(m, "DV")["bands"] != [[60, None, 1]]
    assert item_of(m, "EB")["bands"] == [[0, 50, 4], [50, None, 1]]


# --------------------------------------------------------------------------------------------
# F2 the wild size limit
# --------------------------------------------------------------------------------------------

def test_chilliwack_lake_no_wild_trout_over_50(built):
    data = built[0]
    row = row_of(built, card(built, CHILLIWACK_L, 0, (7, 15)), "z2:trout_char_quota::trout_char_quota.r1")
    o2 = [(i, c) for i, c in enumerate(row["conds"]) if c["c"] == "origin2"]
    assert len(o2) == 1
    i, c = o2[0]
    assert c["o"] == "wild" and c["max"] == 50 and c["daily"] == 4
    assert {"CT", "GB", "RB"} <= set(c["who"])
    assert _rid(data, c["r"]).endswith("chilliwack_lake.r1")
    assert i in item_of(row, "RB")["conds"]
    # MUTATION: without the lake's wild size rule the wild answer is the hatchery's: no line
    m = row_of(built, card(built, CHILLIWACK_L, 0, (7, 15), displace("chilliwack_lake.r1")),
               "z2:trout_char_quota::trout_char_quota.r1")
    assert not any(c["c"] == "origin2" for c in m["conds"])


# --------------------------------------------------------------------------------------------
# F3 fish sets, not counts
# --------------------------------------------------------------------------------------------

def test_kitimat_february_cutthroat_and_brown_go_back(built, monkeypatch):
    data = built[0]
    t = card(built, KITIMAT, 1, (2, 15))
    row = row_of(built, t, "z6:trout_char_quota::trout_char_quota.r1")
    assert sorted(row["members"]) == ["EB", "LT"]
    back = [c for c in row["conds"] if c["c"] == "back" and sorted(c["who"]) == ["CT", "GB"]]
    assert back and _rid(data, back[0]["r"]).endswith("trout_char_quota.r7")
    assert item_of(row, "CT")["back"] and item_of(row, "GB")["back"]
    assert "CT" not in (row["real_daily"] or {}).get("open", [])
    # MUTATION: the page's count comparison drops the release (two fish, like the row's two)
    monkeypatch.setattr(RW, "_who_n", lambda l, r: None if (not l.get("members") or l.get("general")
                        or len(l["members"]) == len(r["members"])) else list(l["members"]))
    m = row_of(built, card(built, KITIMAT, 1, (2, 15), lambda *_: None),
               "z6:trout_char_quota::trout_char_quota.r1")
    assert not any(c["c"] == "back" and sorted(c["who"]) == ["CT", "GB"] for c in m["conds"])


# --------------------------------------------------------------------------------------------
# F4 / F5c / F7 Vedder hatchery rainbow
# --------------------------------------------------------------------------------------------

def test_vedder_hatchery_rainbow_4_of_the_regions_4(built):
    t = card(built, VEDDER, 1, (1, 15))
    row = row_of(built, t, "z2:trout_char_quota::trout_char_quota.r1")
    assert row["daily"] == 2                                   # the stream share
    rb = item_of(row, "RB")
    assert rb["xref"] and rb["against"] == 4
    assert rb["origins"][0] == {"o": "hatchery", "n": 4, "lo": 0, "hi": 50}
    own = row_of(built, t, "chilliwack_vedder_rivers.r9")
    assert own["items"][0]["bands"] == [[0, 50, 4]]           # F5c
    assert own["scope"]["apart"] is False                      # F7: it still counts toward 4
    assert any(l["t"] == "outer" for l in own["everyone"])
    # MUTATION: if the water's rule did not lift the stream share, the 2 would hold for it
    def keep_share(ladder, rid):
        k = rid("z2:trout_char_quota::trout_char_quota.r4")
        ladder["RB"]["hatchery"][k] = ["speaks", None, None, None]
    m = row_of(built, card(built, VEDDER, 1, (1, 15), keep_share), "z2:trout_char_quota::trout_char_quota.r1")
    assert item_of(m, "RB")["against"] == 2


def test_tranquille_rainbow_counted_apart_mayer_lake_an_area(built):
    t = card(built, TRANQUILLE, 0, (6, 15))
    rb = row_of(built, t, "tranquille_lake.r1")
    assert rb["scope"] == {"of": "water", "entry": None, "share": False, "apart": True}
    hg = row_of(built, card(built, MAYER, 0, (6, 15)), "z1:hg_quota::hg_quota.r1")
    assert hg["scope"]["of"] == "area" and hg["scope"]["entry"] == "z1:hg_quota"   # F8


# --------------------------------------------------------------------------------------------
# F5a slots, F6 possession
# --------------------------------------------------------------------------------------------

def test_teslin_slots_and_possession(built):
    data = built[0]
    t = card(built, TESLIN, 0, (6, 15))
    gr = row_of(built, t, "teslin_lake.r4")
    assert gr["items"][0]["bands"] == [[0, 36, 4], [36, 44, 0], [44, None, 1]]
    np_ = row_of(built, t, "teslin_lake.r8")
    assert np_["items"][0]["bands"] == [[0, 70, 4], [70, 100, 0], [100, None, 1]]
    lt = row_of(built, t, "teslin_lake.r1")
    assert lt["items"][0]["bands"] == [[0, 60, 1], [60, 90, 0], [90, None, 1]]
    pc = [l for l in lt["everyone"] if l["t"] == "possession_cap"]
    assert pc and _rid(data, pc[0]["r"]).endswith("teslin_lake.r2")
    assert not any(l["t"] == "annual" for r in t["rows"] for l in r["everyone"])
    roles = {_rid(data, k): role for k, role, _ in t["fish"]["LT"]["wild"]["roles"]}
    assert roles[next(x for x in roles if x.endswith("teslin_lake.r2"))] == "possession_cap"
    # MUTATION: without the grayling slot rule the 36-44 cm band is gone
    m = row_of(built, card(built, TESLIN, 0, (6, 15), displace("teslin_lake.r7")), "teslin_lake.r4")
    assert m["items"][0]["bands"] == [[0, 44, 4], [44, None, 1]]


def test_a_possession_limit_is_not_a_day_or_a_year():
    """F6, the rule's plain sentence (display): Teslin's lake trout 1 in possession."""
    from pipeline.deliver.answers.display import plain
    x = {"type": "retention_limit", "species": ["LT"], "take": 1, "period": "possession"}
    assert plain(x) == "Have no more than 1 lake trout in possession."
    y = dict(x, take=1, lengths=[{"min_cm": 50}])
    assert plain(y) == "Only 1 lake trout over 50 cm in possession."
    # MUTATION: the same rule as a daily limit reads as one
    assert plain(dict(x, period="daily")) == "Keep up to 1 lake trout a day."


# --------------------------------------------------------------------------------------------
# F5b the zone's outer size cap, F5d every shared cap
# --------------------------------------------------------------------------------------------

def test_thompson_below_kamloops_lake_zone_1_over_50(built):
    row = row_of(built, card(built, THOMPSON, 0, (8, 15)), "thompson_river_downstream_of_kamloops_lake.r2")
    assert item_of(row, "CT")["bands"] == [[35, 50, 2], [50, None, 1]]
    # MUTATION: without the zone's '1 over 50 cm' the water's 2 holds to the top
    m = row_of(built, card(built, THOMPSON, 0, (8, 15), displace("z3:trout_char_quota::trout_char_quota.r3")),
               "thompson_river_downstream_of_kamloops_lake.r2")
    assert item_of(m, "CT")["bands"] == [[35, None, 2]]


def test_region8_every_shared_cap_counts(built):
    """Region 8 streams: 4 a day, only 1 over 50 cm AND only 2 over 30 cm — both general caps of
    the real card reach the real daily limit (the page reads only the first). No shipped card has
    two kinds whose own range starts at 30 cm, so the real caps are fed two such kinds."""
    row = row_of(built, card(built, R8_STREAM, 0, (1, 1)), "z8:trout_char_quota::trout_char_quota.r1")
    general = [(c["a"], c["take"]) for c in row["conds"]
               if c["c"] == "cap" and not c.get("who") and c.get("general")]
    assert sorted(general) == [(30, 2), (50, 1)]
    assert item_of(row, "CT")["bands"] == [[0, 30, 4], [30, 50, 2], [50, None, 1]]
    over30 = [{"it": {"members": ["CT"]}, "lo": 30, "c": 2},
              {"it": {"members": ["GB"]}, "lo": 30, "c": 2}]
    assert RW._shared_caps(over30, general, False) == (2, [{"take": 2, "over_cm": 30}])
    nested = over30 + [{"it": {"members": ["LT"]}, "lo": 50, "c": 1},
                       {"it": {"members": ["RB"]}, "lo": 50, "c": 1}]
    assert RW._shared_caps(nested, general, False) == (4, [{"take": 2, "over_cm": 30},
                                                           {"take": 1, "over_cm": 50}])
    # MUTATION: the first cap alone (the page) lets all 4 over 30 cm through
    assert general[0] == (50, 1) and RW._shared_caps(over30, general[:1], False)[0] == 0
