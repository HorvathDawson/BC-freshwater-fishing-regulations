"""A NO-DATA card (answers 2.8): Region 2's trout/char table and one water's lift, run through the REAL
reader and the REAL rows — no bundle, no verdicts store, no export.

`fixtures/answers28_region2.json` holds 15 rule records cut from the bundle: Region 2's trout and char
table (p.21: 4 a day, 2 from streams, 1 Dolly Varden/lake trout, the Q16 over-50 pair, the 30 cm and
60 cm floors, wild released on streams), the provincial steelhead rules (p.8: 10 a year, record it) and
Region 2's "stop after your hatchery steelhead", and ONE water's lift — the Vedder's r9 (p.22: hatchery
rainbow trout 50 cm or less, 4 a day, Jul 1-Apr 30, lifting the 2-from-streams share and the 30 cm
floor for them). Three waters are bound from it:

  VEDDER  a Region 2 steelhead stream with the lift (every rule)
  STREAM  a plain Region 2 steelhead stream (every rule but the lift)
  LAKE    a plain Region 2 lake (the rules not limited to streams)
  TWO     the Vedder with a SECOND lift from its own row (ROUND10 W1, the Duncan's shape: one row lifting
          two kinds out of the share with two numbers) — a made-up r90, hatchery cutthroat trout 3 a day
          lifting the 2-from-streams share; never bound on the other three
  CLAKE   Chilliwack Lake (ROUND11 A1): a plain Region 2 lake plus its own row's "1 bull trout over 60 cm"
          (`chilliwack_lake.r2`, p.22, copied from the bundle) — the over-50 cap spans two buckets

ROUND12 (several size caps in one group) adds four waters, each a plain Region 2 water plus one row:
  WAHLEACH  Wahleach (Jones) Lake, p.21 — "Trout 4; one over 40 cm" (`wahleach_jones_lake.r1/r2`, copied
            from the bundle): its 1 over 40 cm makes Region 2's over-50 caps say nothing more
  SLOT      a lake with Bennett Lake's shape (p.47: "lake trout 2, only 1 over 90 cm, none between 60 and 90",
            lifting the region's 1 over 50 cm) made up for rainbow trout on a Region 2 lake (never bound
            elsewhere)
  COUNT     a stream with the Bella Coola mainstem's shape (p.42: "trout/char 2, of which only one may be a
            trout (cutthroat or rainbow)") made up on a Region 2 stream
  NESTED    a lake with Region 8's stream shape (p.66: 2 over 30 cm and 1 over 50 cm) made up as a Region 2
            lake's own "trout 4, only 2 over 30 cm" beside Region 2's 1 over 50 cm

ROUND13 (owner review of v14) adds two Region 2 steelhead streams, each a plain one plus a row:
  BELLA     the Bella Coola River's row (p.42, `atnarko_bella_coola_rivers.r3/r4/r13`, copied from the bundle
            onto a Region 2 entry): "Trout/char daily quota = 1 (none under 25 cm and all cutthroat trout
            catch and release)" and "No Fishing for steelhead" — the cutthroat kind keeps none (Z) and the
            steelhead closure is said on the card (S)
  FRASER    the Fraser above Mission's "No Fishing for steelhead Fraser River Mainstem Aug 15-Dec 31"
            (p.23, `fraser_river_upstream_of_mission.r5`, the bundle's record): a DATED species closure

ROUND14 (owner review of v15) adds four waters, each a plain Region 2 water plus real rows (the bundle's
records, copied onto a Region 2 entry):
  BELLA_MAY   the Bella Coola mainstem, Apr 1-May 31 (`atnarko_bella_coola_rivers.r5-r8`, p.42): "trout/char
              daily quota = 2, of which only one may be a trout (cutthroat or rainbow) and none may be under 25
              cm", no cutthroat over 33 cm, no rainbow over 50 cm — the count over two kinds is a nested group
              (item 3), and the 2 is the river's OWN rule though it equals Region 2's 2 from streams
  VEDDER_MAY  the Vedder on May 15 (`chilliwack_vedder_rivers.r6/r7`, p.22): hatchery rainbow trout (50 cm or
              less) and hatchery cutthroat catch and release May 1-31 — one not-kept line (item 6)
  BELLA_TRIB  BELLA plus `atnarko_bella_coola_rivers.r9` "char catch and release, Sept 1-May 31" — a group said
              with its members (item 6)
  LWA         Kootenay Lake's Lower West Arm kokanee (`kootenay_lake_lower_west_arm.r3/r4`, p.37): "5 a day,
              only Saturday and Sunday" / "catch and release Monday through Friday" on a lake — asked at the
              two moments (item 5)

`card(water, md)` asks the reader (`read.effective_rules_bound`, trace, per fish and origin — what the
verdicts stage stores) over the rule set, builds the `rows.Part` and returns `rows.produce` plus the
Part: what the answers build would write for that key and day.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read

FIXTURE = Path(__file__).parent / "fixtures" / "answers28_region2.json"
#: the reader's rule table for the fixture is seeded under this path (never a file)
PATH = "fixture:answers28_region2"
LIFT = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r9"
SHARE = "z2:trout_char_quota::trout_char_quota.r4"
TOTAL = "z2:trout_char_quota::trout_char_quota.r1"
FISH = ("RB", "CT", "EB", "GB", "DV", "LT", "ST")
ORIGINS = ("none", "hatchery", "wild")

LIFT2 = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r90"
CLAKE_DV = "r2:chilliwack_lake@2-4::chilliwack_lake.r2"
VEDDER, STREAM, LAKE, TWO, CLAKE = "vedder", "stream", "lake", "two_lifts", "chilliwack_lake"
WAHLEACH, SLOT, COUNT, NESTED = "wahleach_lake", "slot_lake", "count_stream", "nested_lake"
BELLA, FRASER = "bella_coola", "fraser_mission"
BELLA_MAY, VEDDER_MAY, BELLA_TRIB, LWA = "bella_coola_may", "vedder_may", "bella_coola_trib", "lwa_lake"
BELLA_MAY_ST, SHUT = "bella_coola_may_steelhead_closed", "shut_stream"
BELLA_R5 = "r2:bella_coola_river@2-3::bella_coola_river.r5"
BELLA_R6 = "r2:bella_coola_river@2-3::bella_coola_river.r6"
VEDDER_R6 = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r6"
VEDDER_R7 = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r7"
BELLA_R9 = "r2:bella_coola_river@2-3::bella_coola_river.r9"
BELLA_R2 = "r2:bella_coola_river@2-3::bella_coola_river.r2"
LWA_R3 = "r2:lower_west_arm@2-3::kootenay_lake_lower_west_arm.r3"
LWA_R4 = "r2:lower_west_arm@2-3::kootenay_lake_lower_west_arm.r4"
LWA_R5 = "r2:lower_west_arm@2-3::kootenay_lake_lower_west_arm.r5"
BELLA_ST = "r2:bella_coola_river@2-3::bella_coola_river.r13"
FRASER_ST = ("r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4::"
             "fraser_river_upstream_of_mission.r5")
#: water -> (set id, kind, steelhead water, steelhead rules here, presence)
WATERS = {VEDDER: (1, "stream", True, True, "known"),
          STREAM: (2, "stream", True, True, "known"),
          LAKE: (3, "lake", False, True, None),
          TWO: (4, "stream", True, True, "known"),
          CLAKE: (5, "lake", False, True, None),
          WAHLEACH: (6, "lake", False, True, None),
          SLOT: (7, "lake", False, True, None),
          COUNT: (8, "stream", True, True, "known"),
          NESTED: (9, "lake", False, True, None),
          BELLA: (10, "stream", True, True, "known"),
          FRASER: (11, "stream", True, True, "known"),
          BELLA_MAY: (12, "stream", True, True, "known"),
          VEDDER_MAY: (13, "stream", True, True, "known"),
          BELLA_TRIB: (14, "stream", True, True, "known"),
          LWA: (15, "lake", False, True, None),
          BELLA_MAY_ST: (16, "stream", True, True, "known"),
          SHUT: (17, "stream", True, True, "known")}


#: set id -> the water's (set id, kind, steelhead water, steelhead rules here, presence)
WATERS_BY_SET = {v[0]: v for v in WATERS.values()}


@dataclass
class _B:
    """The two things `rows.Part` reads of a bundle."""
    rules: Dict[Tuple[str, str], dict]
    sets: Dict[int, List[Tuple[str, str, str]]]


def _load() -> _B:
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        got[tuple(LIFT2.split("::"))] = _second_lift(got[tuple(LIFT.split("::"))])
        got[tuple(CLAKE_DV.split("::"))] = dict(_CHILLIWACK_LAKE_DV)
        for x in _r12_rows():
            got[(x["entry"], x["rule"])] = x
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift, lift2, dv = tuple(LIFT.split("::")), tuple(LIFT2.split("::")), tuple(CLAKE_DV.split("::"))
    own = {k: set_id for set_id, ks in _R12_SETS.items() for k in ks}
    every = sorted(k for k in got if k not in (lift2, dv) and k not in own)
    sets = {1: [(e, r, "reach") for e, r in every],
            4: [(e, r, "reach") for e, r in sorted(got) if (e, r) != dv and (e, r) not in own],
            2: [(e, r, "reach") for e, r in every if (e, r) != lift],
            3: [(e, r, "reach") for e, r in every
                if (e, r) != lift and got[(e, r)].get("water") != "stream"]}
    sets[5] = sorted(sets[3] + [(*dv, "reach")])
    for set_id, ks in _R12_SETS.items():
        base = sets[2] if WATERS_BY_SET[set_id][1] == "stream" else sets[3]
        sets[set_id] = sorted(base + [(e, r, _VIA.get(set_id, "reach")) for e, r in ks])
    return _B(rules=got, sets=sets)


# ROUND12 rows (see the module docstring): (entry, entry name, rule, fields)
_R12 = {
    6: ("r2:wahleach_jones_lake@2-3", "Wahleach Lake", [
        ("wahleach_jones_lake.r1", {"label": "Trout and char — 4 per day, all species combined",
                                    "species": ["TROUT_CHAR"], "take": 4, "verbatim": "Trout daily quota = 4"}),
        ("wahleach_jones_lake.r2", {"label": "Trout and char (no more than 1 over 40 cm)", "species": ["TROUT_CHAR"],
                                    "take": 1, "lengths": [{"min_cm": 40}], "within": "wahleach_jones_lake.r1",
                                    "verbatim": "one over 40 cm"})]),
    7: ("r2:slot_lake@2-3", "Slot Lake", [
        ("slot_lake.r1", {"label": "Rainbow trout — 2 per day", "species": ["RB"], "take": 2,
                          "verbatim": "Rainbow trout daily quota = 2",
                          "exempts": [{"entry_id": "z2:trout_char_quota", "rule_id": r, "species": ["RB"],
                                       "note": "the lake's own rainbow sizes replace the over-50 cm caps"}
                                      for r in ("trout_char_quota.r2", "trout_char_quota.r2b")]}),
        ("slot_lake.r2", {"label": "Rainbow trout (no more than 1, none between 60 cm and 90 cm)", "species": ["RB"],
                          "take": 1, "lengths": [{"max_cm": 90, "min_cm": 60, "take": 0}, {"min_cm": 90}],
                          "within": "slot_lake.r1", "verbatim": "only 1 over 90 cm, none between 60 cm and 90 cm"})]),
    8: ("r2:count_creek@2-3", "Count Creek", [
        ("count_creek.r1", {"label": "Trout and char — 2 per day, all species combined", "species": ["TROUT_CHAR"],
                            "take": 2, "verbatim": "Trout/char daily quota = 2"}),
        ("count_creek.r2", {"label": "Cutthroat trout and Rainbow trout — 1 per day, all species combined",
                            "species": ["CT", "RB"], "take": 1, "within": "count_creek.r1",
                            "verbatim": "of which only one may be a trout (cutthroat or rainbow)"})]),
    9: ("r2:nested_lake@2-3", "Nested Lake", [
        ("nested_lake.r1", {"label": "Trout and char — 4 per day, all species combined", "species": ["TROUT_CHAR"],
                            "take": 4, "verbatim": "Trout daily quota = 4"}),
        ("nested_lake.r2", {"label": "Trout and char (no more than 2 over 30 cm)", "species": ["TROUT_CHAR"],
                            "take": 2, "lengths": [{"min_cm": 30}], "within": "nested_lake.r1",
                            "verbatim": "only 2 over 30 cm"})]),
    10: ("r2:bella_coola_river@2-3", "Bella Coola River", [
        ("bella_coola_river.r3", {"label": "Trout and char — 1 per day, all species combined (none under 25 cm)",
                                  "species": ["TROUT_CHAR"], "take": 1,
                                  "lengths": [{"min_cm": 25}, {"max_cm": 25, "take": 0}],
                                  "verbatim": "Trout/char daily quota = 1 (none under 25 cm and all cutthroat "
                                              "trout catch and release)"}),
        ("bella_coola_river.r4", {"label": "Cutthroat trout — release all", "species": ["CT"], "take": 0,
                                  "may_target": 1, "within": "bella_coola_river.r3",
                                  "verbatim": "all cutthroat trout catch and release"}),
        ("bella_coola_river.r13", {"label": "No fishing for steelhead", "species": ["ST"], "take": 0,
                                   "may_target": 0, "verbatim": "No Fishing for steelhead"})]),
    11: ("r2:fraser_river_upstream_of_the_cpr_bridge_at_mission@2-4", "Fraser River", [
        ("fraser_river_upstream_of_mission.r5", {
            "label": "No fishing for steelhead, Aug 15-Dec 31", "species": ["ST"], "take": 0, "may_target": 0,
            "when": {"dates": [{"from_day": 15, "from_month": 8, "to_day": 31, "to_month": 12}]},
            "verbatim": "No Fishing for steelhead Fraser River Mainstem Aug 15-Dec 31"})]),
}
_APR_MAY = {"dates": [{"from_day": 1, "from_month": 4, "to_day": 31, "to_month": 5}]}
_MAY = {"dates": [{"from_day": 1, "from_month": 5, "to_day": 31, "to_month": 5}]}
_VEDDER_E = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4"
_R12.update({
    12: ("r2:bella_coola_river@2-3", "Bella Coola River", [
        ("bella_coola_river.r5", {"label": "Trout and char — 2 per day, all species combined (none under 25 cm), "
                                           "Apr 1-May 31", "species": ["TROUT_CHAR"], "take": 2,
                                  "lengths": [{"min_cm": 25}, {"max_cm": 25, "take": 0}], "when": _APR_MAY,
                                  "verbatim": "on Bella Coola R. MAINSTEM ONLY, trout/char daily quota = 2, of which "
                                              "only one may be a trout (cutthroat or rainbow) and none may be under "
                                              "25 cm"}),
        ("bella_coola_river.r6", {"label": "Cutthroat trout and Rainbow trout — 1 per day, all species combined, "
                                           "Apr 1-May 31", "species": ["CT", "RB"], "take": 1,
                                  "within": "bella_coola_river.r5", "when": _APR_MAY,
                                  "verbatim": "of which only one may be a trout (cutthroat or rainbow) and none may "
                                              "be under 25 cm"}),
        ("bella_coola_river.r7", {"label": "Cutthroat trout (none over 33 cm), Apr 1-May 31", "species": ["CT"],
                                  "take": None, "dimension": "daily/size", "lengths": [{"min_cm": 33, "take": 0}],
                                  "within": "bella_coola_river.r6", "when": _APR_MAY,
                                  "verbatim": "no cutthroat may be over 33 cm"}),
        ("bella_coola_river.r8", {"label": "Rainbow trout (none over 50 cm), Apr 1-May 31", "species": ["RB"],
                                  "take": None, "dimension": "daily/size", "lengths": [{"min_cm": 50, "take": 0}],
                                  "within": "bella_coola_river.r6", "when": _APR_MAY,
                                  "verbatim": "no rainbow may be over 50 cm"})]),
    13: (_VEDDER_E, "Chilliwack River", [
        ("chilliwack_vedder_rivers.r6", {"label": "Rainbow trout (none 50 cm or less), hatchery only, May 1-May 31",
                                         "species": ["RB"], "take": None, "dimension": "daily/size@origin=hatchery",
                                         "lengths": [{"closed": True, "max_cm": 50, "take": 0}], "origin": "hatchery",
                                         "may_target": 1, "when": _MAY,
                                         "verbatim": "hatchery rainbow trout catch and release (50 cm or less)"}),
        ("chilliwack_vedder_rivers.r7", {"label": "Cutthroat trout — release all, hatchery only, May 1-May 31",
                                         "species": ["CT"], "take": 0, "dimension": "daily@origin=hatchery",
                                         "origin": "hatchery", "may_target": 1, "when": _MAY,
                                         "verbatim": "hatchery cutthroat catch and release"})]),
    14: ("r2:bella_coola_river@2-3", "Bella Coola River", [
        *_R12[10][2],
        ("bella_coola_river.r9", {"label": "Char — release all, Sep 1-May 31 — tributaries only",
                                  "species": ["CHAR"], "take": 0, "may_target": 1, "tributaries_only": True,
                                  "when": {"dates": [{"from_day": 1, "from_month": 9, "to_day": 31, "to_month": 5}]},
                                  "verbatim": "char catch and release (on TRIBUTARIES ONLY), Sept 1-May 31"})]),
    15: ("r2:lower_west_arm@2-3", "Kootenay Lake — Lower West Arm", [
        ("kootenay_lake_lower_west_arm.r3", {"label": "Kokanee — 5 per day, on Saturdays and Sundays",
                                             "species": ["KO"], "take": 5,
                                             "when": {"weekdays": ["Saturday", "Sunday"]},
                                             "verbatim": "kokanee daily quota = 5, but only on Saturday and Sunday "
                                                         "of each week."}),
        ("kootenay_lake_lower_west_arm.r4", {"label": "Kokanee — release all, Monday to Friday", "species": ["KO"],
                                             "take": 0, "may_target": 1,
                                             "when": {"weekdays": ["Monday", "Tuesday", "Wednesday", "Thursday",
                                                                   "Friday"]},
                                             "verbatim": "Kokanee catch and release Monday through Friday of each "
                                                         "week."}),
        ("kootenay_lake_lower_west_arm.r5", {"label": "Kokanee — 5 per day, on Saturdays and Sundays",
                                             "species": ["KO"], "take": 5,
                                             "when": {"weekdays": ["Saturday", "Sunday"]},
                                             "verbatim": "NOTE: the combined daily quota for kokanee from the Upper "
                                                         "West Arm (when open to\nkokanee harvest) and the Lower West "
                                                         "Arm (when open to kokanee harvest) cannot exceed 5"})]),
})
_R12.update({
    16: ("r2:bella_coola_river@2-3", "Bella Coola River", [*_R12[12][2], _R12[10][2][2]]),
    17: ("r2:shut_creek@2-3", "Shut Creek", [
        ("shut_creek.r1", {"label": "No fishing, Jun 1-Jul 31", "species": ["ALL_GAME_FISH"], "take": 0,
                           "may_target": 0,
                           "when": {"dates": [{"from_day": 1, "from_month": 6, "to_day": 31, "to_month": 7}]},
                           "verbatim": "No Fishing June 1-July 31"})]),
})
#: set id -> how its own rows reach it (ROUND15 6: Ape Creek takes the Bella Coola's rows by the walk)
_VIA = {14: "trib"}
_R12_SETS = {set_id: [(e, r) for r, _f in rows] for set_id, (e, _n, rows) in _R12.items()}
#: the fish asked on a water beyond `FISH` (the kokanee on the Lower West Arm)
_EXTRA_FISH = {LWA: ("KO",)}


def _r12_rows() -> List[dict]:
    out = []
    for e, name, rows in _R12.values():
        for r, f in rows:
            x = {"entry_id": e, "rule_id": r, "type": "retention_limit", "family": "retention",
                 "dimension": "daily", "parts": json.dumps({"what": f["label"]}), "scope": "section",
                 "species_except": [], "may_target": None, "uncertain": 0, "unresolved": None,
                 "extent_text": None, "undrawn_part": None, "extents": [{"op": "whole"}], "period": "daily",
                 "rule": r, "entry": e, "entry_name": name, **f}
            out.append(x)
    return out


#: Chilliwack Lake's own row, p.22: "1 bull trout over 60 cm" (the bundle's record, verbatim)
_CHILLIWACK_LAKE_DV = {
    "entry_id": "r2:chilliwack_lake@2-4", "rule_id": "chilliwack_lake.r2", "type": "retention_limit",
    "family": "retention", "dimension": "daily",
    "label": "Dolly Varden/bull trout — 1 per day (none under 60 cm)",
    "parts": '{"what":"Dolly Varden/bull trout — 1 per day","size":"none under 60 cm"}',
    "scope": "section", "species": ["DV"], "species_except": [], "take": 1, "may_target": None,
    "uncertain": 0, "unresolved": None, "verbatim": "1 bull trout over 60 cm", "extent_text": None,
    "undrawn_part": None, "extents": [{"op": "whole"}],
    "lengths": [{"min_cm": 60}, {"max_cm": 60, "take": 0}], "period": "daily",
    "rule": "chilliwack_lake.r2", "entry": "r2:chilliwack_lake@2-4", "entry_name": "Chilliwack Lake"}


def _second_lift(r9: dict) -> dict:
    """A made-up second rule of the Vedder's row: hatchery cutthroat trout, 3 a day, all year, lifting
    Region 2's 2-from-streams share for them (only the share — the 30 cm floor stays)."""
    x = json.loads(json.dumps(r9))
    x.update(rule="chilliwack_vedder_rivers.r90", rule_id="chilliwack_vedder_rivers.r90", species=["CT"],
             take=3, lengths=None, when=None, verbatim="hatchery cutthroat trout: daily quota = 3",
             label="Cutthroat trout — 3 per day, hatchery only",
             parts=json.dumps({"what": "Cutthroat trout — 3 per day", "conditions": "hatchery only"}),
             exempts=[{"entry_id": "z2:trout_char_quota", "rule_id": "trout_char_quota.r4",
                       "species": ["CT"], "note": "hatchery cutthroat trout quota = 3 replaces '2 from streams'"}])
    return {k: v for k, v in x.items() if v is not None or k in r9 and r9[k] is None}


def ladder(water: str, md: Tuple[int, int], at=None) -> dict:
    """Every asked fish's traced verdict per origin, as the verdicts stage stores it (at a moment,
    `calendar.Moment`, where the water's rules hold some weekdays)."""
    B = _load()
    set_id, _kind, sw, sr, _pres = WATERS[water]
    bound = B.sets[set_id]
    return {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, sw, md, f, PATH, steelhead_rules_here=sr, origin=None if o == "none" else o,
        trace=True, at=at)) for o in ORIGINS} for f in FISH + _EXTRA_FISH.get(water, ())}


def part(water: str, md: Tuple[int, int], lad: dict | None = None, at=None) -> RW.Part:
    B = _load()
    set_id, kind, sw, sr, pres = WATERS[water]
    return RW.Part(B, set_id, sw, sr, kind, pres, lad if lad is not None else ladder(water, md, at), md, {},
                   at=at)


def card(water: str, md: Tuple[int, int], lad: dict | None = None, at=None):
    """(the rows `produce` writes for the water on the day, its Part)."""
    P = part(water, md, lad, at)
    return RW.produce(P), P


def trout_group(out: dict) -> dict:
    """The trout and char total's group (on one row of the card)."""
    got = [r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL]
    assert len(got) == 1, [r.get("group") for r in out["rows"]]
    return got[0]


def trout_row(out: dict) -> dict:
    return next(r for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL)
