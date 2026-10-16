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
#: water -> (set id, kind, steelhead water, steelhead rules here, presence)
WATERS = {VEDDER: (1, "stream", True, True, "known"),
          STREAM: (2, "stream", True, True, "known"),
          LAKE: (3, "lake", False, True, None),
          TWO: (4, "stream", True, True, "known"),
          CLAKE: (5, "lake", False, True, None),
          WAHLEACH: (6, "lake", False, True, None),
          SLOT: (7, "lake", False, True, None),
          COUNT: (8, "stream", True, True, "known"),
          NESTED: (9, "lake", False, True, None)}


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
        sets[set_id] = sorted(base + [(e, r, "reach") for e, r in ks])
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
}
_R12_SETS = {set_id: [(e, r) for r, _f in rows] for set_id, (e, _n, rows) in _R12.items()}


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


def ladder(water: str, md: Tuple[int, int]) -> dict:
    """Every asked fish's traced verdict per origin, as the verdicts stage stores it."""
    B = _load()
    set_id, _kind, sw, sr, _pres = WATERS[water]
    bound = B.sets[set_id]
    return {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, sw, md, f, PATH, steelhead_rules_here=sr, origin=None if o == "none" else o,
        trace=True)) for o in ORIGINS} for f in FISH}


def part(water: str, md: Tuple[int, int], lad: dict | None = None) -> RW.Part:
    B = _load()
    set_id, kind, sw, sr, pres = WATERS[water]
    return RW.Part(B, set_id, sw, sr, kind, pres, lad if lad is not None else ladder(water, md), md, {})


def card(water: str, md: Tuple[int, int], lad: dict | None = None):
    """(the rows `produce` writes for the water on the day, its Part)."""
    P = part(water, md, lad)
    return RW.produce(P), P


def trout_group(out: dict) -> dict:
    """The trout and char total's group (on one row of the card)."""
    got = [r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL]
    assert len(got) == 1, [r.get("group") for r in out["rows"]]
    return got[0]


def trout_row(out: dict) -> dict:
    return next(r for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL)
