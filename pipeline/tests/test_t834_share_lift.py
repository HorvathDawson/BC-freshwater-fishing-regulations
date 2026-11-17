"""T834 (owner 2026-11-17 11:06 "Strict everywhere", RULINGS T17; GOTCHAS "Questions the owner will put to BC Fish and
Wildlife") — no data.

A WATER'S OWN DAILY QUOTA ABOVE A REGION CAP RAISES THAT CAP. For each region cap C (its trout/char total; on a stream
its "from streams" share) and the water's own daily quota N for fish C counts: N <= C, the fish sits inside C (nothing
lifted); N > C, the bundle lifts the fish out of C (`lifts.row_caps`, `_cap_lifts`) and the answers raise C to N over
every fish it holds (`answers.rows.conds.pools`). Mutation-pinned by `OWN_QUOTA_ABOVE_A_CAP_RAISES_IT`, and the Kaslo
card by `tree.AN_EQUAL_OWN_TIER_SITS_IN_THE_SHARE`."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from pipeline.deliver.bundle import rules as RM
from pipeline.deliver.bundle.rules import lifts as L
from pipeline.regs.parsing import catalogue as C

V = C.CatalogueRule.model_validate
REGION = [{"op": "within", "area_kind": "region"}]
STREAMS = [{"op": "within", "area_kind": "region", "feature_types": ["stream"]}]
Z = "z4:daily_quotas"


def _table(region: str = "4", share: int = 2, total: int = 5, share_except: list[str] | None = None) -> dict:
    """A region's quota table: the total, its stream share, the share's size clause, its bull trout clause."""
    eid = f"z{region}:daily_quotas"
    return {eid: {
        "q.r1": V({"rule_id": "q.r1", "type": "retention_limit", "verbatim": f"Trout/char: {total}",
                   "species": ["TROUT_CHAR"], "take": total, "extents": REGION}),
        "q.r3": V({"rule_id": "q.r3", "type": "retention_limit", "verbatim": f"{share} from streams",
                   "species": ["TROUT_CHAR"], "species_except": share_except or [], "take": share, "within": "q.r1",
                   "water": "stream", "extents": STREAMS}),
        "q.r4": V({"rule_id": "q.r4", "type": "retention_limit", "verbatim": "only 1 over 30 cm",
                   "species": ["TROUT_CHAR"], "take": 1, "within": "q.r3", "water": "stream",
                   "lengths": [{"min_cm": 30}], "extents": STREAMS}),
        "q.r5": V({"rule_id": "q.r5", "type": "retention_limit", "verbatim": "1 bull trout (Dolly Varden)",
                   "species": ["DV"], "take": 1, "within": "q.r1", "extents": REGION})}}


def _q(rid: str, verbatim: str, species: list[str], take: int, **kw):
    return V({"rule_id": rid, "type": "retention_limit", "verbatim": verbatim, "species": species, "take": take,
              "extents": [{"op": "whole"}], **kw})


def _row(entry_id: str, *rules, on_stream: bool = True, table: dict | None = None) -> dict:
    """{rule_id: [(cap entry, cap rule, terms)]} — the pool lifts each of the row's rules derives."""
    table = table or _table()
    caps = L.region_caps([SimpleNamespace(entry_id=e, rules=list(rs.values())) for e, rs in table.items()])
    pooled = L.row_caps(SimpleNamespace(entry_id=entry_id, rules=list(rules)), caps, on_stream)
    out = {}
    for r in rules:
        got = RM._exempts(entry_id, r, {}, table, None, None, None, pooled)
        out[r.rule_id] = sorted((x["entry_id"], x["rule_id"], tuple(x.get("species") or ()))
                                for x in (json.loads(got) if got else []) if x.get("basis") == L.OWN_QUOTA_BASIS)
    return out


LARDEAU = ("r4:lardeau_river@4-29", _q("lardeau_river.r4", "Rainbow trout daily quota = 5 (any size)", ["RB"], 5),
           _q("lardeau_river.r5", "bull trout daily quota = 2 (any size)", ["DV"], 2))


def test_the_caps_are_the_total_and_the_stream_share():
    caps = L.region_caps([SimpleNamespace(entry_id=Z, rules=list(_table()[Z].values()))])
    assert [(r.rule_id, share) for _e, r, share in caps["4"]] == [("q.r1", False), ("q.r3", True)]


def test_lardeau_rainbow_5_is_above_the_share_its_bull_trout_2_is_not():
    """Lardeau / Duncan: the rainbow 5 lifts the 2 from streams (raised to 5); 5 is not above the total's 5; the bull
    trout 2 is not above the share's 2 — 5 in all, bull trout and the other trout 2 at most."""
    eid, *rules = LARDEAU
    assert _row(eid, *rules) == {"lardeau_river.r4": [(Z, "q.r3", ("RB",))], "lardeau_river.r5": []}


def test_kaslo_its_2_is_the_shares_2_nothing_is_lifted():
    """Kaslo River / Keen Creek: 2 fish from the stream in all, both may be bull trout (the owner: "Yes: 2 in all")."""
    assert _row("r4:kaslo_river@4-18", _q("kaslo_river.r1", "Bull trout daily quota = 2 (any size)", ["DV"], 2)) == {
        "kaslo_river.r1": []}


def test_adams_a_smaller_number_stays_inside_the_share():
    """Adams River (rainbow 2 under Region 3's "4 from streams"): 4 in all, at most 2 rainbow."""
    assert _row("r4:adams@4-1", _q("adams.r1", "Rainbow trout daily quota = 2", ["RB"], 2),
                table=_table(share=4)) == {"adams.r1": []}


def test_kootenay_lake_rainbow_10_lifts_the_total_the_bull_trout_1_does_not():
    """Kootenay Lake main body: 10 in all, the other kinds 5 at most, bull trout 1; a lake meets no stream share."""
    got = _row("r4:kootenay_lake@4-19", _q("kl.r3", "Bull trout daily quota = 1", ["DV"], 1),
               _q("kl.r4", "rainbow trout daily quota = 10 (any size)", ["RB"], 10), on_stream=False)
    assert got == {"kl.r3": [], "kl.r4": [(Z, "q.r1", ("RB",))]}


def test_a_brook_trout_20_on_a_stream_lifts_the_share_and_the_total():
    got = _row("r4:lavington_creek@4-26", _q("lc.r1", "Brook trout daily quota = 20", ["EB"], 20))
    assert got == {"lc.r1": [(Z, "q.r1", ("EB",)), (Z, "q.r3", ("EB",))]}


def test_a_quota_over_every_fish_replaces_the_cap_on_its_key_and_lifts_nothing():
    """Region 5's "Trout/char daily quota = 8" lakes: the water's number already replaces the region's on that key."""
    assert _row("r5:bootjack_lake@5-2", _q("b.r1", "Trout/char daily quota = 8", ["TROUT_CHAR"], 8),
                on_stream=False, table=_table("5")) == {"b.r1": []}


def test_a_fish_the_share_does_not_count_lifts_nothing_from_it():
    """Region 6's share counts no char: a bull trout quota above it lifts nothing from it."""
    got = _row("r6:x_creek@6-1", _q("x.r1", "Bull trout daily quota = 3", ["DV"], 3),
               table=_table("6", share=1, share_except=["CHAR"]))
    assert got == {"x.r1": []}


def test_a_hatchery_quota_lifts_for_its_origin_only():
    """Kitimat River: hatchery rainbow 5 and hatchery steelhead 2 above Region 6's "1 trout from streams" (not above
    its 5): each lifts the share, for hatchery fish only."""
    table = _table("6", share=1, share_except=["CHAR"])
    caps = L.region_caps([SimpleNamespace(entry_id=e, rules=list(rs.values())) for e, rs in table.items()])
    k4 = _q("k.r4", "Hatchery rainbow trout daily quota = 5", ["RB"], 5, origin="hatchery")
    k5 = _q("k.r5", "Hatchery steelhead daily quota = 2", ["ST"], 2, origin="hatchery")
    rc = L.row_caps(SimpleNamespace(entry_id="r6:kitimat@6-3", rules=[k4, k5]), caps, True)
    for r in (k4, k5):
        got = json.loads(RM._exempts("r6:kitimat@6-3", r, {}, table, None, None, None, rc))
        assert [(x["rule_id"], x["origin"]) for x in got] == [("q.r3", "hatchery")]


def test_only_a_keeping_daily_quota_counts_toward_the_pool():
    eid, *rules = LARDEAU
    release = _q("x.r9", "Bull trout catch and release", ["DV"], 0, may_target=True)
    possession = _q("x.r8", "Rainbow trout possession quota = 10", ["RB"], 10, period="possession")
    assert _row("r4:x@4-1", release, possession) == {"x.r9": [], "x.r8": []}


def test_mutation_the_flag_off_derives_nothing(monkeypatch):
    monkeypatch.setattr(L, "OWN_QUOTA_ABOVE_A_CAP_RAISES_IT", False)
    eid, *rules = LARDEAU
    assert _row(eid, *rules) == {"lardeau_river.r4": [], "lardeau_river.r5": []}


def test_a_lake_row_is_not_offered_the_stream_share():
    reg = {"wbk:1": SimpleNamespace(kind="lake"), "gnis:2": SimpleNamespace(kind="stream")}
    assert not L.row_on_a_stream(SimpleNamespace(matched=["wbk:1"]), reg)
    assert not L.row_on_a_stream(SimpleNamespace(matched=[]), reg)
    assert L.row_on_a_stream(SimpleNamespace(matched=["wbk:1", "gnis:2"]), reg)


def test_two_alternative_quotas_over_every_fish_are_never_summed():
    """Bella Coola: "trout/char 1" and its mainstem's "trout/char 2, Apr 1-May 31" are alternatives, never 3."""
    got = _row("r5:bella_coola@5-11", _q("bc.r3", "Trout/char daily quota = 1", ["TROUT_CHAR"], 1),
               _q("bc.r5", "trout/char daily quota = 2", ["TROUT_CHAR"], 2,
                  when={"dates": [{"from_month": 4, "from_day": 1, "to_month": 5, "to_day": 31}]}),
               table=_table("5"))
    assert got == {"bc.r3": [], "bc.r5": []}


def test_the_region_7_rows_meet_both_zone_tables():
    table = {**_table("7a"), **_table("7b")}
    got = _row("r7:peace_river@7-31", _q("p.r4", "Lake trout daily quota = 3", ["LT"], 3), table=table)
    assert got["p.r4"] == [("z7a:daily_quotas", "q.r3", ("LT",)), ("z7b:daily_quotas", "q.r3", ("LT",))]
